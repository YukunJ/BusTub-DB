#include <memory>
#include <vector>
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

#ifdef BUSTUB_OPTIMIZER_HACK_REMOVE_AFTER_2022_FALL

auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        // if a single filter is on an existing index, convert to indexScan with predicate
        //        if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&*filter_plan.GetPredicate());
        //            expr != nullptr) {
        //          // left a Column expression & right is a Constant expression & is Equal Comparison
        //          if (expr->comp_type_ == ComparisonType::Equal) {
        //            if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
        //                left_expr != nullptr && left_expr->GetTupleIdx() == 0) {
        //              if (const auto *right_expr = dynamic_cast<const ConstantValueExpression
        //              *>(expr->children_[1].get());
        //                  right_expr != nullptr) {
        //                if (auto index = MatchIndex(seq_scan_plan.table_name_, left_expr->GetColIdx()); index !=
        //                std::nullopt) {
        //                  auto [index_oid, index_name] = *index;
        //                  return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, index_oid,
        //                                                             filter_plan.GetPredicate());
        //                }
        //              }
        //            }
        //          }
        //        }
        // default to seqScan with predicate
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }
  return optimized_plan;
}

#endif

}  // namespace bustub
