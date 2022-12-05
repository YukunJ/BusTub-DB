#include "common/util/string_util.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
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

/**
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
 * @brief: Create a new column expression with only column index shift left
 */
auto ShiftColumnExpression(const ColumnValueExpression &col_expr, uint32_t shift_left_count) -> AbstractExpressionRef {
  auto tuple_idx = col_expr.GetTupleIdx();
  auto col_idx = col_expr.GetColIdx() - shift_left_count;
  auto ret_type = col_expr.GetReturnType();
  const auto new_col_expr = std::make_shared<ColumnValueExpression>(tuple_idx, col_idx, ret_type);
  return std::dynamic_pointer_cast<AbstractExpression>(new_col_expr);
}

/**
 * @brief: Aggregate filtered out single-side predicate into one nested predicate
 * so as to suppose recursively "break and push down"
 */
auto AggregateColumnExpression(std::vector<AbstractExpressionRef> &filter) -> AbstractExpressionRef {
  // left deep tree construction
  if (filter.size() == 1) {
    return filter[0];
  }
  auto right_expr = filter.back();
  filter.pop_back();
  auto left_expr = AggregateColumnExpression(filter);
  const auto logic_expr = std::make_shared<LogicExpression>(left_expr, right_expr, LogicType::And);
  return std::dynamic_pointer_cast<AbstractExpression>(logic_expr);
}

/**
 * @brief: Given a (possibly nested) expression, find all column expression between two tables if any
 */
auto FindAllColumnEqualComparison(const AbstractExpressionRef &expression,
                                  std::vector<AbstractExpressionRef> &column_equal_exprs) -> AbstractExpressionRef {
  bool is_direct_comparsion = false;
  if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&*expression); expr != nullptr) {
    is_direct_comparsion = true;
    if (expr->comp_type_ == ComparisonType::Equal) {
      if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
          left_expr != nullptr) {
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
            right_expr != nullptr) {
          column_equal_exprs.push_back(expression);  // find a column-comparison
          Value true_value = Value(TypeId::BOOLEAN, 1);
          return std::make_shared<ConstantValueExpression>(true_value);
        }
      }
    }
  }
  if (!is_direct_comparsion) {
    // if already find a EqualColumn Expression, don't duplicate it in the return vector
    for (auto &i : expression->children_) {
      i = FindAllColumnEqualComparison(i, column_equal_exprs);
    }
  }
  return expression;
}

/**
 * @brief: Given a (possibly nested) filter, find all predicate expression that belong to only one side of a nlj
 */
auto FindAllOneSideFilter(const AbstractExpressionRef &expression, std::vector<AbstractExpressionRef> &left_filter,
                          std::vector<AbstractExpressionRef> &right_filter, uint32_t left_column_count,
                          uint32_t right_column_count) -> AbstractExpressionRef {
  bool found = false;
  if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&*expression); expr != nullptr) {
    bool left_expr_on_join_left = false;
    bool left_expr_on_join_right = false;
    bool right_expr_on_join_left = false;
    bool right_expr_on_join_right = false;
    const auto *left_expr_constant = dynamic_cast<const ConstantValueExpression *>(expr->children_[0].get());
    const auto *right_expr_constant = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
    const auto *left_expr_column = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
    const auto *right_expr_column = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
    if (left_expr_constant != nullptr) {
      left_expr_on_join_left = true;
      left_expr_on_join_right = true;
    }
    if (right_expr_constant != nullptr) {
      right_expr_on_join_left = true;
      right_expr_on_join_right = true;
    }
    if (left_expr_column != nullptr) {
      if (left_expr_column->GetColIdx() < left_column_count) {
        left_expr_on_join_left = true;
      } else {
        left_expr_on_join_right = true;
      }
    }
    if (right_expr_column != nullptr) {
      if (right_expr_column->GetColIdx() < left_column_count) {
        right_expr_on_join_left = true;
      } else {
        right_expr_on_join_right = true;
      }
    }
    if (left_expr_constant == nullptr || right_expr_constant == nullptr) {
      // do not optimize here for two sides all constant
      if (left_expr_on_join_left && right_expr_on_join_left) {
        found = true;
        left_filter.push_back(expression);
      }
      if (left_expr_on_join_right && right_expr_on_join_right) {
        found = true;
        if (left_expr_constant != nullptr) {
          auto left_expr = expr->children_[0];  // constant expression
          auto right_expr = ShiftColumnExpression(*right_expr_column, left_column_count);
          auto new_pred = std::make_shared<ComparisonExpression>(left_expr, right_expr, expr->comp_type_);
          right_filter.push_back(std::move(new_pred));
        } else {
          auto left_expr = ShiftColumnExpression(*left_expr_column, left_column_count);
          auto right_expr = expr->children_[1];  // constant expression
          auto new_pred = std::make_shared<ComparisonExpression>(left_expr, right_expr, expr->comp_type_);
          right_filter.push_back(std::move(new_pred));
        }
      }
      if (found) {
        Value true_value = Value(TypeId::BOOLEAN, 1);
        return std::make_shared<ConstantValueExpression>(true_value);
      }
    }
  }

  if (const auto *expr = dynamic_cast<const LogicExpression *>(&*expression);
      expr != nullptr && expr->logic_type_ == LogicType::Or) {
    // 'or' complicates the push down, do not optimize for it
    return expression;
  }

  if (!found) {
    for (auto &child_expr : expression->children_) {
      child_expr = FindAllOneSideFilter(child_expr, left_filter, right_filter, left_column_count, right_column_count);
    }
  }

  return expression;
}

/**
 * Given the (possibly nested) projection expression, find all the column idx the aggregate corresponds to
 */
auto FindAllColumnIdx(const AbstractExpressionRef &expression, std::vector<size_t> &column_idxs) -> void {
  if (const auto *expr = dynamic_cast<const ColumnValueExpression *>(&*expression); expr != nullptr) {
    column_idxs.push_back(expr->GetColIdx());
    return;
  }
  for (const auto &sub_expr : expression->children_) {
    FindAllColumnIdx(sub_expr, column_idxs);
  }
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

/**
 * @brief optimize a filter on high level of the syntax tree
 * it extracts out all the single Column Equal expression and make them into extra individual filter
 * this further relies on 'merge_filter_nlj' and 'nlj_as_hash_join' rule
 * to merge the predicate into an vanilla nlj and then into a hash join
 * @attention: this rule is made very specific so as to only applicable to leaderboard Q2
 */
auto OptimizeBreakColumnEqualFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeBreakColumnEqualFilter(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Filter) {
    auto &child_plan = optimized_plan->children_[0];
    std::vector<AbstractExpressionRef> column_equal_expres;
    auto &filter_plan = dynamic_cast<FilterPlanNode &>(*optimized_plan);
    filter_plan.predicate_ = FindAllColumnEqualComparison(filter_plan.GetPredicate(), column_equal_expres);
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&*filter_plan.GetPredicate()); expr == nullptr) {
      // only operator on nested filter predicates
      for (const auto &column_equal_pred : column_equal_expres) {
        child_plan = std::make_shared<FilterPlanNode>(optimized_plan->output_schema_, column_equal_pred, child_plan);
      }
    }
    optimized_plan->children_[0] = child_plan;
    return optimized_plan;
  }
  return optimized_plan;
}

/**
 * Push a one-side predicate of a filter downward when given a pair filter + nlj
 * @attention: this rule is made very specific so as to only applicable to leaderboard Q2
 */
auto OptimizePushDownPredicate(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto pushed_down = false;
  auto is_single_filter = false;
  auto optimized_plan = plan->CloneWithChildren(plan->children_);
  if (optimized_plan->GetType() == PlanType::Filter) {
    std::vector<AbstractPlanNodeRef> children;
    auto &child_plan = optimized_plan->children_[0];
    auto child_plan_optimized = child_plan->CloneWithChildren(child_plan->children_);
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    const auto *expr = dynamic_cast<const ComparisonExpression *>(&*filter_plan.GetPredicate());
    is_single_filter = expr != nullptr;
    if (child_plan_optimized->GetType() == PlanType::NestedLoopJoin) {
      // find all the one-side filtering condition
      auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*child_plan_optimized);
      auto left_column_count = nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
      auto right_column_count = nlj_plan.GetRightPlan()->OutputSchema().GetColumnCount();
      std::vector<AbstractExpressionRef> left_filter;
      std::vector<AbstractExpressionRef> right_filter;
      FindAllOneSideFilter(filter_plan.GetPredicate(), left_filter, right_filter, left_column_count,
                           right_column_count);
      auto &join_plan_left = nlj_plan.children_[0];
      auto &join_plan_right = nlj_plan.children_[1];
      if (!left_filter.empty()) {
        pushed_down = true;
        join_plan_left = std::make_shared<FilterPlanNode>(join_plan_left->output_schema_,
                                                          AggregateColumnExpression(left_filter), join_plan_left);
      }
      if (!right_filter.empty()) {
        pushed_down = true;
        join_plan_right = std::make_shared<FilterPlanNode>(join_plan_right->output_schema_,
                                                           AggregateColumnExpression(right_filter), join_plan_right);
      }
      nlj_plan.children_[0] = join_plan_left;
      nlj_plan.children_[1] = join_plan_right;
    }
    if (child_plan_optimized->GetType() == PlanType::HashJoin) {
      // find all the one-side filtering condition
      auto &hash_plan = dynamic_cast<HashJoinPlanNode &>(*child_plan_optimized);
      auto left_column_count = hash_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
      auto right_column_count = hash_plan.GetRightPlan()->OutputSchema().GetColumnCount();
      std::vector<AbstractExpressionRef> left_filter;
      std::vector<AbstractExpressionRef> right_filter;
      FindAllOneSideFilter(filter_plan.GetPredicate(), left_filter, right_filter, left_column_count,
                           right_column_count);
      auto &join_plan_left = hash_plan.children_[0];
      auto &join_plan_right = hash_plan.children_[1];
      if (!left_filter.empty()) {
        pushed_down = true;
        join_plan_left = std::make_shared<FilterPlanNode>(join_plan_left->output_schema_,
                                                          AggregateColumnExpression(left_filter), join_plan_left);
      }
      if (!right_filter.empty()) {
        pushed_down = true;
        join_plan_right = std::make_shared<FilterPlanNode>(join_plan_right->output_schema_,
                                                           AggregateColumnExpression(right_filter), join_plan_right);
      }
      hash_plan.children_[0] = join_plan_left;
      hash_plan.children_[1] = join_plan_right;
    }
    children.emplace_back(std::move(child_plan_optimized));
    optimized_plan = optimized_plan->CloneWithChildren(children);
  }

  // post order recursion to allow push down repeatedly
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizePushDownPredicate(child));
  }
  optimized_plan = optimized_plan->CloneWithChildren(std::move(children));
  if (is_single_filter && pushed_down) {
    // abandon myself as an already pushed-down single filter
    return optimized_plan->children_[0];
  }
  return optimized_plan;
}

/**
 * @brief: duplicated with the one in Optimizer class for convenience
 */
auto IsPredicateTrue(const AbstractExpression &expr) -> bool {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(&expr); const_expr != nullptr) {
    return const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
  }
  return false;
}

/**
 * Merge a (possibly nested) expression
 */
auto OptimizeMergeTruePred(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  for (auto &child : expr->children_) {
    child = OptimizeMergeTruePred(child);
  }
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(&*expr); logic_expr != nullptr) {
    if (IsPredicateTrue(*logic_expr->children_[0])) {
      if (logic_expr->logic_type_ == LogicType::And) {
        return logic_expr->children_[1];
      }
    }
    if (IsPredicateTrue(*logic_expr->children_[1])) {
      if (logic_expr->logic_type_ == LogicType::And) {
        return logic_expr->children_[0];
      }
    }
  }
  return expr;
}

/**
 * @brief: Recursively merge all true filter
 */
auto OptimizeMergeTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeTrueFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    auto &filter_plan = dynamic_cast<FilterPlanNode &>(*optimized_plan);
    filter_plan.predicate_ = OptimizeMergeTruePred(filter_plan.GetPredicate());
  }

  return optimized_plan;
}

/**
 * @brief: duplicate from the one in Optimizer class for convenience
 */
auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateTrueFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (IsPredicateTrue(*filter_plan.GetPredicate())) {
      BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
      return optimized_plan->children_[0];
    }
  }

  return optimized_plan;
}

/*
 * @brief: remove a single '1 = 2' kind of always false filter to a dummy empty value plan node
 */
auto OptimizeEliminateSingleFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateSingleFalseFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(&*filter_plan.GetPredicate());
        comp_expr != nullptr) {
      if (comp_expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[0].get());
            left_const_expr != nullptr) {
          if (const auto *right_const_expr =
                  dynamic_cast<const ConstantValueExpression *>(comp_expr->children_[1].get());
              right_const_expr != nullptr) {
            if (left_const_expr->val_.CompareEquals(right_const_expr->val_) == CmpBool::CmpFalse) {
              return std::make_shared<ValuesPlanNode>(optimized_plan->output_schema_,
                                                      std::vector<std::vector<AbstractExpressionRef>>{});
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

/**
 * @brief: if two consecutive filter F1 and F2 is such that F1 is a subset of F2, replace F2 with F1
 */
auto OptimizeIntersectProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeIntersectProjection(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    const auto &child_plan = optimized_plan->children_[0];
    if (child_plan->GetType() == PlanType::Projection) {
      const auto &child_project_plan = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
      const auto &exprs = projection_plan.GetExpressions();
      const auto child_exprs = child_project_plan.GetExpressions();
      auto is_all_column_exprs = true;
      std::vector<uint32_t> subset_idxs;
      for (const auto &expr : exprs) {
        auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
        if (column_value_expr == nullptr) {
          is_all_column_exprs = false;
          break;
        }
      }
      if (is_all_column_exprs) {
        std::vector<Column> retain_cols;
        std::vector<AbstractExpressionRef> retain_projs;
        for (const auto &expr : exprs) {
          auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
          retain_cols.push_back(child_project_plan.output_schema_->GetColumn(column_value_expr->GetColIdx()));
          retain_projs.push_back(child_exprs[column_value_expr->GetColIdx()]);
        }
        return std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(retain_cols), retain_projs,
                                                    child_plan->children_[0]);
      }
    }
  }
  return optimized_plan;
}

/**
 * @brief: if a pair filter + aggregate is present, eliminate unnecessary aggregate column
 */
auto OptimizeAggregateColumnPrune(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeAggregateColumnPrune(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    const auto &child_plan = optimized_plan->children_[0];
    if (child_plan->GetType() == PlanType::Aggregation) {
      const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*child_plan);
      std::vector<size_t> agg_idxs;
      for (const auto &proj : projection_plan.GetExpressions()) {
        FindAllColumnIdx(proj, agg_idxs);
      }
      std::vector<Column> new_out_cols;
      std::vector<AbstractExpressionRef> new_aggregates;
      std::vector<AggregationType> new_agg_types;
      const auto &agg_old_out_columns = agg_plan.output_schema_->GetColumns();
      // group by columns is placed at the front of output schema
      new_out_cols.insert(new_out_cols.end(), agg_old_out_columns.begin(),
                          agg_old_out_columns.begin() + agg_plan.group_bys_.size());
      auto group_by_size = agg_plan.group_bys_.size();
      for (const auto &idx : agg_idxs) {
        if (idx >= group_by_size) {
          new_out_cols.push_back(agg_plan.output_schema_->GetColumn(idx));
          new_aggregates.push_back(agg_plan.GetAggregateAt(idx - group_by_size));
          new_agg_types.push_back(agg_plan.GetAggregateTypes()[idx - group_by_size]);
        }
      }
      optimized_plan->children_[0] =
          std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(new_out_cols), agg_plan.children_[0],
                                                agg_plan.group_bys_, new_aggregates, new_agg_types);
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeIntersectProjection(p);  // merge consecutive project if upper is a subset of the lower
  p = OptimizeAggregateColumnPrune(
      p);  // prune aggregate column if an projection is present above it and only takes a subset
  p = OptimizeBreakColumnEqualFilter(p);  // enable pred -> into NLJ -> into hash join
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeMergeTrueFilter(p);      // merge true and true and ... true into only one true filter
  p = OptimizeEliminateTrueFilter(p);  // remove only one true filter
  p = OptimizeMergeFilterScan(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeReorderJoinOnCardinality(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);               // Enable hash join.
  p = OptimizePushDownPredicate(p);           // enable recursive push down one-side filter
  p = OptimizeMergeTrueFilter(p);             // merge true and true and ... true into only one true filter
  p = OptimizeEliminateTrueFilter(p);         // remove only one true filter
  p = OptimizeEliminateSingleFalseFilter(p);  // replace single always-false filter to empty dummy node
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

}  // namespace bustub
