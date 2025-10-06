#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {
auto Check(std::vector<AbstractExpressionRef>& pred_keys,std::vector<uint32_t>& filt_idx,
  const AbstractExpressionRef& cur_exp)->bool{
  // 
  const auto logic_expr = dynamic_cast<const LogicExpression*>(cur_exp.get());
  if(logic_expr == nullptr){
    auto comparison_expr = dynamic_cast<const ComparisonExpression*>(cur_exp.get());
    // 当前的比较表达式不合法
    if(comparison_expr == nullptr || comparison_expr->comp_type_ !=ComparisonType::Equal){
      return false;
    }
    // 尝试获取其表达式的常量值
    // 先反着获取其表达式的情况
    auto col_val = dynamic_cast<const ColumnValueExpression*>(cur_exp->GetChildAt(0).get());
    if(col_val == nullptr){
      col_val = dynamic_cast<const ColumnValueExpression*>(cur_exp->GetChildAt(1).get());
      pred_keys.emplace_back(comparison_expr->GetChildAt(0));
    }
    else{
      pred_keys.emplace_back(comparison_expr->GetChildAt(1));
    }
    // 这里检查的原因是索引只有一列 那么必须相同且只有一个
    if(filt_idx.empty()){
      filt_idx.emplace_back(col_val->GetColIdx());
    }
    // 如果任何一个子表达式的列和已有的不同 直接返回false
    else if(filt_idx[0] != col_val->GetColIdx()){
      return false;
    }
    
    return true;
  }

  if(logic_expr->logic_type_ != LogicType::Or){
    return false;
  }
  return Check(pred_keys, filt_idx, logic_expr->GetChildAt(0)) &&
         Check(pred_keys, filt_idx, logic_expr->GetChildAt(1));
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  // 拿到子计划节点
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // 看子计划是否为顺序扫描
  if(optimized_plan->GetType() == PlanType::SeqScan){
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if(seq_plan.filter_predicate_ == nullptr){
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> pred_keys;
    std::vector<uint32_t> filt_idx;
    if(!Check(pred_keys,filt_idx,seq_plan.filter_predicate_)){
      return optimized_plan;
    }
    // 获取对应的索引信息
    auto table_info = catalog_.GetTable(seq_plan.GetTableOid());
    auto indexs_info = catalog_.GetTableIndexes(table_info->name_);
    for(const auto& index_info:indexs_info){
      if(index_info->index_->GetKeyAttrs() == filt_idx){
        return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, seq_plan.GetTableOid(), index_info->index_oid_,
                    seq_plan.filter_predicate_, pred_keys);
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
