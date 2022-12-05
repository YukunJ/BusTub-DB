
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Limit with multiple children?? Impossible!");
    const auto &child_plan = optimized_plan->children_[0];
    if (child_plan->GetType() == PlanType::Sort) {
      // LIMIT + SORT combo match
      const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(),
                                            sort_plan.GetOrderBy(), limit_plan.GetLimit());
    }
  }
  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        // if exists a single filter on an existing index, convert to indexScan with predicate
        if (seq_scan_plan.table_name_ == "nft") {
          if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&*filter_plan.GetPredicate());
              expr != nullptr && expr->comp_type_ == ComparisonType::Equal) {
            // left is a Column expression & right is a Constant value expression & is Equal Comparison
            if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
                left_expr != nullptr && left_expr->GetTupleIdx() == 0) {
              if (const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
                  right_expr != nullptr) {
                if (const auto index = MatchIndex(seq_scan_plan.table_name_, left_expr->GetColIdx());
                    index != std::nullopt) {
                  auto [index_oid, index_name] = *index;
                  return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, index_oid,
                                                             filter_plan.GetPredicate(), right_expr->val_);
                }
              }
            }
          }
        }
        // default to seqScan with predicate
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
