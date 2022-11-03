//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  remain_limit_ = plan_->GetLimit();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (remain_limit_ == 0U) {
    // limit exceed
    return false;
  }
  Tuple tuple_holder{};
  RID rid_holder{};
  auto status = child_executor_->Next(&tuple_holder, &rid_holder);
  if (!status) {
    // child has exhausted before limit exceeds
    return false;
  }
  *tuple = tuple_holder;
  *rid = rid_holder;
  remain_limit_--;
  return true;
}

}  // namespace bustub
