//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      end_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)->table_->End()),
      cursor_(end_) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
       txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      !txn->IsTableIntentionSharedLocked(plan_->GetTableOid()) &&
      !txn->IsTableIntentionExclusiveLocked(plan_->GetTableOid())) {
    // grab S lock on table if not locked yet
    auto table_lock_success =
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
    if (!table_lock_success) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::Exception(ExceptionType::EXECUTION, "SeqScan cannot get IS lock on table");
    }
  }
  cursor_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto oid = plan_->GetTableOid();
  if (cursor_ == end_) {
    // reach end of table
    return false;
  }
  *rid = cursor_->GetRid();
  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
       txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      !txn->IsRowSharedLocked(oid, *rid) && !txn->IsRowExclusiveLocked(oid, *rid)) {
    obtain_lock_ = true;
    exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, oid, *rid);
  }
  *tuple = *cursor_++;
  if (obtain_lock_ && (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
    // S lock is released immediately in READ_COMMITTED
    exec_ctx_->GetLockManager()->UnlockRow(txn, oid, *rid);
  }
  obtain_lock_ = false;
  return true;
}

}  // namespace bustub
