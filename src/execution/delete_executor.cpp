//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  delete_finished_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (delete_finished_) {
    return false;
  }
  /* begin deletion, IX lock on table if not locked yet */
  auto txn = exec_ctx_->GetTransaction();
  if (!txn->IsTableIntentionExclusiveLocked(plan_->TableOid())) {
    auto table_lock_success =
        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!table_lock_success) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::Exception(ExceptionType::EXECUTION, "DeleteExecutor cannot get IX lock on table");
    }
  }
  Tuple child_tuple{};
  RID rid_holder{};
  int64_t count = 0;
  // fetch any available indexes on this table
  auto table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  // table handler
  auto table_ptr = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  // tuple's schema
  auto tuple_schema = child_executor_->GetOutputSchema();
  auto oid = plan_->TableOid();
  auto status = child_executor_->Next(&child_tuple, rid);
  while (status) {
    // actual delete
    auto remove_rid = child_tuple.GetRid();
    if (!txn->IsRowExclusiveLocked(oid, remove_rid)) {
      // no other transaction should see such delete until we commit
      exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, remove_rid);
    }
    count += static_cast<int64_t>(table_ptr->MarkDelete(child_tuple.GetRid(), exec_ctx_->GetTransaction()));
    // update any indexes available
    if (!table_indexes.empty()) {
      std::for_each(table_indexes.begin(), table_indexes.end(), [&](auto lt) {
        auto key_schema = lt->index_->GetKeySchema();
        auto key_attrs = lt->index_->GetKeyAttrs();
        auto key = child_tuple.KeyFromTuple(tuple_schema, *key_schema, key_attrs);
        lt->index_->DeleteEntry(key, rid_holder, exec_ctx_->GetTransaction());
      });
    }
    // increment cursor
    status = child_executor_->Next(&child_tuple, rid);
  }
  auto return_value = std::vector<Value>{{TypeId::BIGINT, count}};
  auto return_schema = Schema(std::vector<Column>{{"success_delete_count", TypeId::BIGINT}});
  *tuple = Tuple(return_value, &return_schema);
  delete_finished_ = true;
  return true;
}

}  // namespace bustub
