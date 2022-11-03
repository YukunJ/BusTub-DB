#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple_holder{};
  RID rid_holder{};
  child_executor_->Init();
  sorted_.clear();
  auto status = child_executor_->Next(&tuple_holder, &rid_holder);
  while (status) {
    // fetch all the tuples from child and store
    sorted_.emplace_back(tuple_holder);
    status = child_executor_->Next(&tuple_holder, &rid_holder);
  }
  // sort backward so that the next one is sorted.back() to save shifting left in vector
  auto orderby_keys = plan_->GetOrderBy();
  auto schema = GetOutputSchema();
  auto sorter = [&](Tuple &lhs, Tuple &rhs) -> bool {
    for (const auto &[order_type, expr] : orderby_keys) {
      auto left_value = expr->Evaluate(&lhs, schema);
      auto right_value = expr->Evaluate(&rhs, schema);
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        // equal on this field, go check next one
        continue;
      }
      auto comp = left_value.CompareLessThan(right_value);
      // if should place right value at back of vector
      return (comp == CmpBool::CmpTrue && order_type == OrderByType::DESC) ||
             (comp == CmpBool::CmpFalse && order_type != OrderByType::DESC);
    }
    return false;
  };
  std::sort(sorted_.begin(), sorted_.end(), sorter);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  const auto &tuple_next = sorted_.back();
  *tuple = tuple_next;
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
