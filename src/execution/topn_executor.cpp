#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  sorted_.clear();
  // use a fixed size heap to store the top N elements we want
  size_t n = plan_->GetN();
  auto orderby_keys = plan_->GetOrderBy();
  auto schema = GetOutputSchema();
  auto comp = [&](const Tuple &lhs, const Tuple &rhs) -> bool {
    for (const auto &[order_type, expr] : orderby_keys) {
      auto left_value = expr->Evaluate(&lhs, schema);
      auto right_value = expr->Evaluate(&rhs, schema);
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        // equal on this field, go check next one
        continue;
      }
      auto comp = left_value.CompareLessThan(right_value);
      return (comp == CmpBool::CmpTrue && order_type != OrderByType::DESC) ||
             (comp == CmpBool::CmpFalse && order_type == OrderByType::DESC);
    }
    return false;
  };

  Tuple tuple_holder{};
  RID rid_holder{};
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);
  auto status = child_executor_->Next(&tuple_holder, &rid_holder);
  while (status) {
    if (pq.size() < n) {
      pq.push(tuple_holder);
    } else {
      if (comp(tuple_holder, pq.top())) {
        // new tuple is better than the top
        pq.pop();
        pq.push(tuple_holder);
      }
    }
    status = child_executor_->Next(&tuple_holder, &rid_holder);
  }
  sorted_.reserve(pq.size());
  while (!pq.empty()) {
    sorted_.push_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_.empty()) {
    return false;
  }
  *tuple = sorted_.back();
  sorted_.pop_back();
  return true;
}

}  // namespace bustub
