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
  auto index_oid = plan_->GetIndexOid();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(index_oid);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  // Fetch the table heap pointer
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
  // Locate the index iterator cursor
  cursor_ = tree_->GetBeginIterator();
  end_ = tree_->GetEndIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
