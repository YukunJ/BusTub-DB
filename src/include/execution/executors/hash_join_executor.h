//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** Merge the current left_tuple and right_tuple's value into one vector for new tuple creation */
  void MergeValueFromTuple(const Tuple &left_tuple, const Tuple &right_tuple, std::vector<Value> &value,
                           bool right_null) const {
    auto left_column_count = left_executor_->GetOutputSchema().GetColumnCount();
    auto right_column_count = right_executor_->GetOutputSchema().GetColumnCount();
    for (unsigned int i = 0; i < left_column_count; i++) {
      value.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (unsigned int i = 0; i < right_column_count; i++) {
      if (!right_null) {
        value.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
      } else {
        value.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
    }
  }
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The left child executor from which join tuples are obtained */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** The right child executor from which join tuples are obtained */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** The joined result set for Next() to yield */
  std::vector<Tuple> joined_;
};

}  // namespace bustub
