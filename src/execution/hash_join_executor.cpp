//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  joined_.clear();
  auto value_hash = [](const Value &v) -> size_t { return std::hash<std::string>()(v.ToString()); };
  auto value_equal = [](const Value &lhs, const Value &rhs) -> bool {
    return lhs.CompareEquals(rhs) == CmpBool::CmpTrue;
  };
  using JoinHashTable = std::unordered_map<Value, std::vector<Tuple>, decltype(value_hash), decltype(value_equal)>;
  Tuple tuple_holder{};
  RID rid_holder{};
  // fetch each tuple from left and right child
  // build a hashtable of <Value (i.e. key), vector<Tuple> > for each side
  const int magic_prime = 23;
  JoinHashTable left_table(magic_prime, value_hash, value_equal);
  JoinHashTable right_table(magic_prime, value_hash, value_equal);
  auto status = left_executor_->Next(&tuple_holder, &rid_holder);
  while (status) {
    const auto key = plan_->left_key_expression_->Evaluate(&tuple_holder, plan_->GetLeftPlan()->OutputSchema());
    left_table[key].emplace_back(tuple_holder);
    status = left_executor_->Next(&tuple_holder, &rid_holder);
  }
  status = right_executor_->Next(&tuple_holder, &rid_holder);
  while (status) {
    const auto key = plan_->right_key_expression_->Evaluate(&tuple_holder, plan_->GetRightPlan()->OutputSchema());
    right_table[key].emplace_back(tuple_holder);
    status = right_executor_->Next(&tuple_holder, &rid_holder);
  }
  // Build Phase
  for (const auto &[key, tuple_vec] : left_table) {
    auto right_find = right_table.find(key);
    if (right_find != right_table.end()) {
      for (const auto &left_tuple : tuple_vec) {
        for (const auto &right_tuple : right_find->second) {
          std::vector<Value> value;
          MergeValueFromTuple(left_tuple, right_tuple, value, false);
          joined_.emplace_back(value, &plan_->OutputSchema());
        }
      }
    } else {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        Tuple tmp{};
        for (const auto &left_tuple : tuple_vec) {
          std::vector<Value> value;
          MergeValueFromTuple(left_tuple, tmp, value, true);
          joined_.emplace_back(value, &plan_->OutputSchema());
        }
      }
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (joined_.empty()) {
    return false;
  }
  *tuple = joined_.back();
  joined_.pop_back();
  return true;
}

}  // namespace bustub
