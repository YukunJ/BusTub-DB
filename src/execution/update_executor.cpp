//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  update_finished_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (update_finished_) {
    return false;
  }
  /* Begin Update, grab IX lock on the table first */
  auto txn = exec_ctx_->GetTransaction();
  //  if (!txn->IsTableIntentionExclusiveLocked(plan_->TableOid())) {
  //    // grab IX lock on table if not locked yet
  //    auto table_lock_success =
  //        exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
  //    if (!table_lock_success) {
  //      txn->SetState(TransactionState::ABORTED);
  //      throw bustub::Exception(ExceptionType::EXECUTION, "Update cannot get IX lock on table");
  //    }
  //  }
  auto table_ptr = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  //  auto oid = plan_->TableOid();
  Tuple child_tuple{};
  RID rid_holder{};
  int64_t count = 0;
  auto status = child_executor_->Next(&child_tuple, &rid_holder);
  while (status) {
    /* Grab X lock on row first */
    //    auto update_rid = child_tuple.GetRid();
    //    if (!txn->IsRowExclusiveLocked(oid, update_rid)) {
    //      // no other transaction should see such update until we commit
    //      exec_ctx_->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, update_rid);
    //    }
    count += static_cast<int64_t>(table_ptr->UpdateTuple(child_tuple, rid_holder, txn));
    // increment cursor
    status = child_executor_->Next(&child_tuple, &rid_holder);
  }
  auto return_value = std::vector<Value>{{TypeId::BIGINT, count}};
  auto return_schema = Schema(std::vector<Column>{{"success_update_count", TypeId::BIGINT}});
  *tuple = Tuple(return_value, &return_schema);
  update_finished_ = true;
  return true;
}

}  // namespace bustub
