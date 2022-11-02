//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      inner_schema_(plan_->InnerTableSchema()),
      outer_schema_(plan_->GetChildPlan()->OutputSchema()),
      key_schema_(std::vector<Column>{{"index_key", plan->KeyPredicate()->GetReturnType()}}) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  auto index_oid = plan_->GetIndexOid();
  index_ = exec_ctx_->GetCatalog()->GetIndex(index_oid);
  match_rids_.clear();
  inner_table_ptr_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // Outer table cursor always increment by 1 and try to find a match
    auto status = child_executor_->Next(&outer_tuple_, &outer_rid_);
    if (!status) {
      // join finish
      return false;
    }
    std::vector<Value> key_value{plan_->KeyPredicate()->Evaluate(&outer_tuple_, outer_schema_)};
    auto key_index = Tuple(key_value, &key_schema_);
    index_->index_->ScanKey(key_index, &match_rids_, exec_ctx_->GetTransaction());
    if (!match_rids_.empty()) {
      // by assumption, can have only 1 match in indexed table
      inner_rid_ = match_rids_.back();
      match_rids_.clear();
      inner_table_ptr_->table_->GetTuple(inner_rid_, &inner_tuple_, exec_ctx_->GetTransaction());
      std::vector<Value> value;
      MergeValueFromTuple(value, false);
      *tuple = Tuple(value, &GetOutputSchema());
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> value;
      MergeValueFromTuple(value, true);  // null value placeholder for inner table
      *tuple = Tuple(value, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
