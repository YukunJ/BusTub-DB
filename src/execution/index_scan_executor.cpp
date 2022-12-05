//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // Fetch the corresponding B+ Tree Index
  filtered_result_.clear();
  auto index_oid = plan_->GetIndexOid();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(index_oid);
  //  auto oid = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->oid_;
  //  auto txn = exec_ctx_->GetTransaction();
  //  if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
  //       txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
  //      !txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid)) {
  //    // grab S lock on table if not locked yet
  //    auto table_lock_success = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED,
  //    oid); if (!table_lock_success) {
  //      txn->SetState(TransactionState::ABORTED);
  //      throw bustub::Exception(ExceptionType::EXECUTION, "IndexScan cannot get IS lock on table");
  //    }
  //  }
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  // Fetch the table heap pointer
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
  // Locate the index iterator cursor
  cursor_ = tree_->GetBeginIterator();
  end_ = tree_->GetEndIterator();
  if (plan_->filter_predicate_ != nullptr) {
    // with filtering predicate on this Index Scan
    auto values = std::vector<Value>{plan_->filter_value_};
    auto index_schema = index_info->key_schema_;
    Tuple scan_key = Tuple(values, &index_schema);
    tree_->ScanKey(scan_key, &filtered_result_, exec_ctx_->GetTransaction());
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //  auto oid =
  //  exec_ctx_->GetCatalog()->GetTable(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->table_name_)->oid_; auto
  //  txn = exec_ctx_->GetTransaction();
  if (plan_->filter_predicate_ != nullptr) {
    // Index Scan with predicate optimization mode
    if (filtered_result_.empty()) {
      return false;
    }
    *rid = filtered_result_.back();
    //    if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
    //         txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
    //        !txn->IsRowSharedLocked(oid, *rid) && !txn->IsRowExclusiveLocked(oid, *rid)) {
    //      obtain_lock_ = true;
    //      exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, oid, *rid);
    //    }
    filtered_result_.pop_back();
    table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    //    if (obtain_lock_ && (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
    //      // S lock is released immediately in READ_COMMITTED
    //      exec_ctx_->GetLockManager()->UnlockRow(txn, oid, *rid);
    //    }
    //    obtain_lock_ = false;
    return true;
  }

  // normal old P3 style Index Scan
  if (cursor_ == end_) {
    // end of scanning
    return false;
  }
  *rid = (*cursor_).second;
  table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++cursor_;
  return true;
}

}  // namespace bustub
