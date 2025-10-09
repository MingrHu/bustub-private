#include <algorithm>
#include <memory>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
// cur_exp: 当前的表达式节点
// left_keys:
// right_keys:
// 和seqscan转为indexscan类似 需要用dynamic_cast获取子类进行判断
// 通过递归检查比较子类的表达式判断是否可以优化
auto Check(const AbstractExpressionRef &cur_exp, std::vector<AbstractExpressionRef> &left_keys,
           std::vector<AbstractExpressionRef> &right_keys) -> bool {
  auto logic_expr = dynamic_cast<const LogicExpression *>(cur_exp.get());
  // 如果不是逻辑表达式
  // 判断比较是否为 “=” 并且都是为列比较表达式 不能有常量
  if (logic_expr == nullptr) {
    auto comparison_expr = dynamic_cast<const ComparisonExpression *>(cur_exp.get());
    // 当前的比较表达式不合法
    if (comparison_expr == nullptr || comparison_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    // 左右两个子比较表达式
    // 需要保证都是列表达式
    auto left_expr = comparison_expr->GetChildAt(0);
    auto right_expr = comparison_expr->GetChildAt(1);
    if (dynamic_cast<const ColumnValueExpression *>(left_expr.get()) != nullptr &&
        dynamic_cast<const ColumnValueExpression *>(right_expr.get()) != nullptr) {
      // 先获取左边的表达式
      const auto left_val = dynamic_cast<const ColumnValueExpression *>(cur_exp->GetChildAt(0).get());
      const auto right_val = dynamic_cast<const ColumnValueExpression *>(cur_exp->GetChildAt(1).get());
      // 如果对应
      if (left_val->GetTupleIdx() == 0 && right_val->GetTupleIdx() == 1) {
        left_keys.emplace_back(left_expr);
        right_keys.emplace_back(right_expr);
        return true;
      }
      left_keys.emplace_back(right_expr);
      right_keys.emplace_back(left_expr);
      return true;
    }
  }
  if (logic_expr->logic_type_ != LogicType::And) {
    return false;
  }
  return Check(logic_expr->GetChildAt(0), left_keys, right_keys) &&
         Check(logic_expr->GetChildAt(1), left_keys, right_keys);
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  // 拿到子计划节点
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // 压根没谓词
    if (nlj_plan.predicate_ == nullptr) {
      return optimized_plan;
    }
    if (nlj_plan.GetJoinType() == JoinType::INNER || nlj_plan.GetJoinType() == JoinType::LEFT) {
      std::vector<AbstractExpressionRef> left_key_expr;
      std::vector<AbstractExpressionRef> right_key_expr;
      if (Check(nlj_plan.predicate_, left_key_expr, right_key_expr)) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_key_expr),
                                                  std::move(right_key_expr), nlj_plan.join_type_);
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
