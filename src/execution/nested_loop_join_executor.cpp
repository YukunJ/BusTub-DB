//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  RID rid_holder;
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_ = Tuple{};
  right_tuple_ = Tuple{};
  left_status_ = left_executor_->Next(&left_tuple_, &rid_holder);
  right_status_ = false;
  left_join_found_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // simple nested join
  RID rid_holder;
  while (true) {
    // the Inner table always increment 1 position per loop
    right_status_ = right_executor_->Next(&right_tuple_, &rid_holder);
    if (!right_status_) {
      // reach the end of Inner table, try loop around
      if (plan_->GetJoinType() == JoinType::LEFT && !left_join_found_) {
        // emit the left tuple plus null value on right tuple side if using Left join
        std::vector<Value> value;
        MergeValueFromTuple(value, true);
        *tuple = Tuple(value, &plan_->OutputSchema());
        left_join_found_ = true;
        return true;
      }
      left_status_ = left_executor_->Next(&left_tuple_, &rid_holder);
      left_join_found_ = false;  // refresh
      if (!left_status_) {
        // the end of join operation or Outer table empty
        return false;
      }
      right_executor_->Init();
      right_status_ = right_executor_->Next(&right_tuple_, &rid_holder);
      if (!right_status_) {
        // empty Inner table, still need to walk through Outer table if Left Join
        continue;
      }
    }
    auto pred_value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                      right_executor_->GetOutputSchema());
    if (!pred_value.IsNull() && pred_value.GetAs<bool>()) {
      std::vector<Value> value;
      MergeValueFromTuple(value, false);
      *tuple = Tuple(value, &plan_->OutputSchema());
      left_join_found_ = true;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
