#include "common/util/string_util.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

#define NO_CARDINALITY 99999

namespace bustub {

/**
 * @brief: a duplicate function as the one in optimizer.h
 * isolate out for convenience
 */
auto EstimatedCardinality(const std::string &table_name) -> size_t {
  if (StringUtil::EndsWith(table_name, "_1m")) {
    return 1000000;
  }
  if (StringUtil::EndsWith(table_name, "_100k")) {
    return 100000;
  }
  if (StringUtil::EndsWith(table_name, "_50k")) {
    return 50000;
  }
  if (StringUtil::EndsWith(table_name, "_10k")) {
    return 10000;
  }
  if (StringUtil::EndsWith(table_name, "_1k")) {
    return 1000;
  }
  if (StringUtil::EndsWith(table_name, "_100")) {
    return 100;
  }
  return NO_CARDINALITY;  // no supposed to hit this branch
}

/*
 * @brief: fetch the cardinality of a table
 */
auto FetchCardinality(const AbstractPlanNodeRef &plan) -> size_t {
  BUSTUB_ASSERT(plan->GetType() == PlanType::MockScan || plan->GetType() == PlanType::SeqScan,
                "Only MockScan or SeqScan plan node has cardinality info");
  if (plan->GetType() == PlanType::MockScan) {
    const auto &mock_scan_plan = dynamic_cast<const MockScanPlanNode &>(*plan);
    return EstimatedCardinality(mock_scan_plan.GetTable());
  }
  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
  return EstimatedCardinality(seq_scan_plan.table_name_);
}

/**
 * @brief optimize a multi-way join by reordering
 * to make biggest table to join with smallest table first
 * @attention: this rule is made very specific so as to only applicable to leaderboard Q1
 */
auto OptimizeReorderJoinOnCardinality(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeReorderJoinOnCardinality(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate());
        expr != nullptr && expr->comp_type_ == ComparisonType::Equal &&
        nlj_plan.GetLeftPlan()->GetType() == PlanType::NestedLoopJoin) {
      const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());
      if (const auto *left_nlj_expr = dynamic_cast<const ComparisonExpression *>(&left_nlj_plan.Predicate());
          left_nlj_expr != nullptr && expr->comp_type_ == ComparisonType::Equal) {
        auto table1_valid = left_nlj_plan.GetLeftPlan()->GetType() == PlanType::SeqScan;
        auto table2_valid = left_nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan;
        auto table3_valid = nlj_plan.GetRightPlan()->GetType() == PlanType::MockScan;
        if (table1_valid && table2_valid && table3_valid) {
          const auto &table2_mock_scan_plan = dynamic_cast<const MockScanPlanNode &>(*left_nlj_plan.GetRightPlan());
          const auto &table3_mock_scan_plan = dynamic_cast<const MockScanPlanNode &>(*nlj_plan.GetRightPlan());
          const auto *col_expr1 = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
          const auto *col_expr2 = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
          const auto *col_expr3 = dynamic_cast<const ColumnValueExpression *>(left_nlj_expr->children_[0].get());
          const auto *col_expr4 = dynamic_cast<const ColumnValueExpression *>(left_nlj_expr->children_[1].get());
          if (left_nlj_expr->comp_type_ == ComparisonType::Equal && (col_expr1 != nullptr) && (col_expr2 != nullptr) &&
              (col_expr3 != nullptr) && (col_expr4 != nullptr)) {
            // 3-way join found, all on pure column join key
            auto table1_count = FetchCardinality(left_nlj_plan.GetLeftPlan());
            auto table2_count = FetchCardinality(left_nlj_plan.GetRightPlan());
            auto table3_count = FetchCardinality(nlj_plan.GetRightPlan());
            auto count_valid = table1_count != NO_CARDINALITY && table2_count != NO_CARDINALITY &&
                               table3_count != NO_CARDINALITY;  // must find the valid cardinality info
            // table3 should get sandwiched between table 1 and table 2 to allow an efficient join
            if (count_valid && (table3_count > table2_count || table3_count < table1_count) &&
                (table3_count > table1_count || table3_count < table2_count)) {
              // join table 2 and 3 first instead
              // put table 2 and 3 on the left side as another NestedJoinPlanNode and table1 as a table scan along on
              // right fetch out the join column id on table 2
              if (col_expr1->GetTupleIdx() == 0 && col_expr2->GetTupleIdx() == 1 && col_expr3->GetTupleIdx() == 0 &&
                  col_expr4->GetTupleIdx() == 1) {
                // #1: make the predicate of Table 2 join Table 3
                // minus the table1 column count
                auto table2_join_idx =
                    col_expr1->GetColIdx() - left_nlj_plan.GetLeftPlan()
                                                 ->OutputSchema()
                                                 .GetColumnCount();  // the index use to join table 2 and 3 on table 2
                auto table2_join_type = col_expr1->GetReturnType();
                auto table2_join_expr = std::make_shared<ColumnValueExpression>(
                    0, table2_join_idx, table2_join_type);  // table 2 on the left
                auto table_2_3_join_pred =
                    std::make_shared<ComparisonExpression>(table2_join_expr, expr->children_[1], ComparisonType::Equal);
                // #2: make the output schema of Table 2 join Table 3
                std::vector<Column> table_2_3_join_out_column = table2_mock_scan_plan.OutputSchema().GetColumns();
                table_2_3_join_out_column.insert(table_2_3_join_out_column.end(),
                                                 table3_mock_scan_plan.OutputSchema().GetColumns().begin(),
                                                 table3_mock_scan_plan.OutputSchema().GetColumns().end());
                const auto table_2_3_join_out_schema = std::make_shared<Schema>(table_2_3_join_out_column);
                // #3: make the join plan node of Table 2 join Table 3
                const auto join_2_3_plan_node = std::make_shared<NestedLoopJoinPlanNode>(
                    table_2_3_join_out_schema, left_nlj_plan.GetRightPlan(), nlj_plan.GetRightPlan(),
                    table_2_3_join_pred, JoinType::INNER);
                // #4: make the predicate of (Table 2 & 3) join Table 1
                // assume table 2 join with both table 1 and table 3
                auto table_23_1_join_idx = col_expr4->GetColIdx();  // table 2's join key idx when joined with table 1
                auto table_23_join_expr =
                    std::make_shared<ColumnValueExpression>(0, table_23_1_join_idx, table2_join_type);
                auto table1_join_expr =
                    std::make_shared<ColumnValueExpression>(1, col_expr3->GetColIdx(), col_expr3->GetReturnType());
                auto table_23_1_join_pred =
                    std::make_shared<ComparisonExpression>(table_23_join_expr, table1_join_expr, ComparisonType::Equal);
                // #5: make the output schema of (Table 2 & 3) join Table 1
                std::vector<Column> table_23_1_join_out_column = join_2_3_plan_node->OutputSchema().GetColumns();
                table_23_1_join_out_column.insert(table_23_1_join_out_column.end(),
                                                  left_nlj_plan.GetLeftPlan()->OutputSchema().GetColumns().begin(),
                                                  left_nlj_plan.GetLeftPlan()->OutputSchema().GetColumns().end());
                const auto table_23_1_join_out_schema = std::make_shared<Schema>(table_23_1_join_out_column);
                // #6: return the final join plan node of ((Table 2 & 3) & Table 1)
                return std::make_shared<NestedLoopJoinPlanNode>(table_23_1_join_out_schema, join_2_3_plan_node,
                                                                left_nlj_plan.GetLeftPlan(), table_23_1_join_pred,
                                                                JoinType::INNER);
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeReorderJoinOnCardinality(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

}  // namespace bustub
