//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      cursor_(aht_.End()),
      end_(aht_.End()) {}

void AggregationExecutor::Init() {
  // build the aggregate values
  aht_.Clear();
  child_->Init();
  allow_empty_output_ = true;
  Tuple child_tuple{};
  RID rid_holder{};
  auto status = child_->Next(&child_tuple, &rid_holder);
  while (status) {
    auto key = MakeAggregateKey(&child_tuple);
    auto agg_val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, agg_val);
    status = child_->Next(&child_tuple, &rid_holder);
  }
  cursor_ = aht_.Begin();
  end_ = aht_.End();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto return_schema = plan_->OutputSchema();
  if (cursor_ == end_) {
    if (cursor_ == aht_.Begin() && allow_empty_output_) {
      // empty table, special treatment
      if (!plan_->GetGroupBys().empty()) {
        // empty table with non-null group by returns nothing
        return false;
      }
      auto value = aht_.GenerateInitialAggregateValue();
      *tuple = Tuple(value.aggregates_, &return_schema);
      allow_empty_output_ = false;
      return true;
    }
    // aggregation finished or no more empty-table output
    return false;
  }
  auto key = cursor_.Key();
  auto value = cursor_.Val();
  // merge group-by column and aggregated column together into a tuple
  std::copy(value.aggregates_.begin(), value.aggregates_.end(), std::back_inserter(key.group_bys_));
  ++cursor_;
  *tuple = Tuple(key.group_bys_, &return_schema);
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
