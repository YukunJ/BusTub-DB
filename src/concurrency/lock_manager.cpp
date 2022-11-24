//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::IsLockRequestValid(Transaction *transaction, AbortReason &reason, bool &is_upgrade,
                                     LockMode &prev_mode, std::shared_ptr<LockRequestQueue> &queue, bool on_table,
                                     LockManager::LockMode mode, table_oid_t table_id, RID rid) -> bool {
  /** supported lock mode */
  if (on_table) {
    // Table locking should support all lock modes
  } else {
    // Row locking should not support Intention locks
    if (mode != LockMode::SHARED && mode != LockMode::EXCLUSIVE) {
      reason = AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW;
      return false;
    }
  }

  /** isolation level */
  if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // all locks required, no lock on shrinking stage
    if (transaction->GetState() == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  } else if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    // all locks required, only IS/S on shrinking stage
    if (transaction->GetState() == TransactionState::SHRINKING) {
      if (mode != LockMode::INTENTION_SHARED && mode != LockMode::SHARED) {
        reason = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
    }
  } else if (transaction->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // only required to take IX, X locks
    if (mode != LockMode::INTENTION_EXCLUSIVE && mode != LockMode::EXCLUSIVE) {
      reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      return false;
    }
    if (transaction->GetState() == TransactionState::SHRINKING) {
      reason = AbortReason::LOCK_ON_SHRINKING;
      return false;
    }
  }

  /** multiple-level locking */
  if (!on_table) {
    if (mode == LockMode::EXCLUSIVE) {
      // need X, IX, or SIX lock on table
      if (!transaction->IsTableExclusiveLocked(table_id) && !transaction->IsTableIntentionExclusiveLocked(table_id) &&
          !transaction->IsTableSharedIntentionExclusiveLocked(table_id)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    } else if (mode == LockMode::SHARED) {
      // any lock on table suffices
      if (!transaction->IsTableSharedLocked(table_id) && !transaction->IsTableIntentionSharedLocked(table_id) &&
          !transaction->IsTableExclusiveLocked(table_id) && !transaction->IsTableIntentionExclusiveLocked(table_id) &&
          !transaction->IsTableSharedIntentionExclusiveLocked(table_id)) {
        reason = AbortReason::TABLE_LOCK_NOT_PRESENT;
        return false;
      }
    }
  }

  /** lock upgrade */
  is_upgrade = false;  // reset to default for safety
  if (on_table) {
    // table locking request
    if (transaction->IsTableSharedLocked(table_id)) {
      prev_mode = LockMode::SHARED;
      is_upgrade = true;
    } else if (transaction->IsTableIntentionSharedLocked(table_id)) {
      prev_mode = LockMode::INTENTION_SHARED;
      is_upgrade = true;
    } else if (transaction->IsTableExclusiveLocked(table_id)) {
      prev_mode = LockMode::EXCLUSIVE;
      is_upgrade = true;
    } else if (transaction->IsTableIntentionExclusiveLocked(table_id)) {
      prev_mode = LockMode::INTENTION_EXCLUSIVE;
      is_upgrade = true;
    } else if (transaction->IsTableSharedIntentionExclusiveLocked(table_id)) {
      prev_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
      is_upgrade = true;
    }
  } else {
    // row locking request
    if (transaction->IsRowSharedLocked(table_id, rid)) {
      prev_mode = LockMode::SHARED;
      is_upgrade = true;
    } else if (transaction->IsRowExclusiveLocked(table_id, rid)) {
      prev_mode = LockMode::EXCLUSIVE;
      is_upgrade = true;
    }
  }
  if (is_upgrade) {
    if (upgrade_matrix_[prev_mode].find(mode) == upgrade_matrix_[prev_mode].end() && prev_mode != mode) {
      // incompatible upgrade type
      reason = AbortReason::INCOMPATIBLE_UPGRADE;
      return false;
    }
    if (queue->upgrading_ != INVALID_TXN_ID && prev_mode != mode) {
      // only one transaction should be allowed to upgrade its lock on a given resource
      reason = AbortReason::UPGRADE_CONFLICT;
      return false;
    }
  }

  return true;
}

auto LockManager::IsUnlockRequestValid(Transaction *transaction, AbortReason &reason,
                                       std::shared_ptr<LockRequestQueue> &queue, bool on_table, table_oid_t table_id,
                                       RID rid) -> bool {
  return false;
}

auto LockManager::CouldLockRequestProceed(LockManager::LockRequest *request, Transaction *txn,
                                          const std::shared_ptr<LockRequestQueue> &queue, bool is_upgrade,
                                          bool &already_abort) -> bool {
  txn->LockTxn();
  already_abort = false;  // set by default for safety
  if (txn->GetState() == TransactionState::ABORTED) {
    already_abort = true;
    txn->UnlockTxn();
    return true;
  }
  // Check if this transaction is the first one un-granted
  auto self = std::find(queue->request_queue_.begin(), queue->request_queue_.end(), request);
  auto first_ungranted = std::find_if_not(queue->request_queue_.begin(), queue->request_queue_.end(),
                                          [](LockRequest *request) { return request->granted_; });
  if (self != first_ungranted) {
    // not this request's turn yet
    txn->UnlockTxn();
    return false;
  }

  // Check if current request lock mode is compatible with all previous granted request's lock mode
  auto is_compatible = queue->IsCompatibleUntil(self, compatible_matrix_);
  if (!is_compatible) {
    txn->UnlockTxn();
    return false;
  }

  // This request can proceed, add lock record into txn's set
  if (request->on_table_) {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->insert(request->oid_);
    } else if (request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(request->oid_);
    }
  } else {
    if (request->lock_mode_ == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->operator[](request->oid_).insert(request->rid_);
    } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->operator[](request->oid_).insert(request->rid_);
    }
  }

  // mark as granted
  request->granted_ = true;
  if (is_upgrade) {
    // no more waiting upgrading request now
    queue->upgrading_ = INVALID_TXN_ID;
  }
  txn->UnlockTxn();
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /* lock and fetch queue & transaction */
  auto queue = GetTableQueue(oid);  // in lock mode to read from map
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();
  bool is_upgrade;
  AbortReason reason;
  LockMode prev_mode;
  /* check if this is a validate request */
  auto is_valid_request = IsLockRequestValid(txn, reason, is_upgrade, prev_mode, queue, true, lock_mode, oid, RID());
  if (!is_valid_request) {
    /* not valid, unlock + abort + throw exception */
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  /* if it's upgrade request, special treatment */
  if (is_upgrade) {
    if (prev_mode == lock_mode) {
      txn->UnlockTxn();
      return true;
    }
    /* an upgrade is equivalent to an unlock + a new lock */
    txn->UnlockTxn();
    lock.unlock();
    UnlockTable(txn, oid);
    lock.lock();
    txn->LockTxn();
  }

  /* valid, make a Request and add to queue for this resource at proper position (tail or first un-granted) */
  auto *request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  queue->InsertIntoQueue(request, is_upgrade);

  /* acquire coordination mutex and wait for it's the first un-granted request and could proceed */
  txn->UnlockTxn();
  bool already_abort = false;
  /* proceed, add into this transaction's lock set and notify all in the queue if not aborted */
  queue->cv_.wait(lock,
                  [&]() -> bool { return CouldLockRequestProceed(request, txn, queue, is_upgrade, already_abort); });
  if (already_abort) {
    return false;
  }
  // notify other waiting threads
  lock.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /* lock and fetch queue & transaction */
  auto queue = GetRowQueue(rid);  // in lock mode to read from map
  std::unique_lock<std::mutex> lock(queue->latch_);
  txn->LockTxn();
  bool is_upgrade;
  AbortReason reason;
  LockMode prev_mode;
  /* check if this is a validate request */
  auto is_valid_request = IsLockRequestValid(txn, reason, is_upgrade, prev_mode, queue, false, lock_mode, oid, rid);
  if (!is_valid_request) {
    /* not valid, unlock + abort + throw exception */
    txn->SetState(TransactionState::ABORTED);
    txn->UnlockTxn();
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  /* if it's upgrade request, special treatment */
  if (is_upgrade) {
    if (prev_mode == lock_mode) {
      txn->UnlockTxn();
      return true;
    }
    /* an upgrade is equivalent to an unlock + a new lock */
    txn->UnlockTxn();
    lock.unlock();
    UnlockRow(txn, oid, rid);
    lock.lock();
    txn->LockTxn();
  }

  /* valid, make a Request and add to queue for this resource at proper position (tail or first un-granted) */
  auto *request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  queue->InsertIntoQueue(request, is_upgrade);

  /* acquire coordination mutex and wait for it's the first un-granted request and could proceed */
  txn->UnlockTxn();
  bool already_abort = false;

  /* proceed, add into this transaction's lock set and notify all in the queue if not aborted */
  queue->cv_.wait(lock,
                  [&]() -> bool { return CouldLockRequestProceed(request, txn, queue, is_upgrade, already_abort); });
  if (already_abort) {
    return false;
  }
  // notify other waiting threads
  lock.unlock();
  queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
