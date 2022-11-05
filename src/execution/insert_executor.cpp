//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  insert_finished_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (insert_finished_) {
    return false;
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

  auto status = child_executor_->Next(&child_tuple, rid);
  while (status) {
    // actual insert
    count += static_cast<int64_t>(table_ptr->InsertTuple(child_tuple, &rid_holder, exec_ctx_->GetTransaction()));
    // update any indexes available
    if (!table_indexes.empty()) {
      std::for_each(table_indexes.begin(), table_indexes.end(), [&](auto lt) {
        auto key_schema = lt->index_->GetKeySchema();
        auto key_attrs = lt->index_->GetKeyAttrs();
        auto key = child_tuple.KeyFromTuple(tuple_schema, *key_schema, key_attrs);
        lt->index_->InsertEntry(key, rid_holder, exec_ctx_->GetTransaction());
      });
    }
    // increment cursor
    status = child_executor_->Next(&child_tuple, rid);
  }
  auto return_value = std::vector<Value>{{TypeId::BIGINT, count}};
  auto return_schema = Schema(std::vector<Column>{{"success_insert_count", TypeId::BIGINT}});
  *tuple = Tuple(return_value, &return_schema);
  insert_finished_ = true;
  return true;
}

}  // namespace bustub
