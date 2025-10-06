//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),left_executor_(std::move(left_executor)), 
    right_executor_(std::move(right_executor)){}

void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  next_left_tuple_ = true;
  is_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // 本质上是每一个left_tuple就需要遍历整个right_tuples
  while(!next_left_tuple_ || left_executor_->Next(&left_tuple_, &left_rid_)){
    std::vector<Value> values;
    // 满足条件的情况下每次初始化right
    if(next_left_tuple_){
      right_executor_->Init();
    }
    // 如果匹配成功 则移动指向right的位置到下一位 从而使得下一次检查从下一位置开始
    // 如果匹配失败 则按照循环一个个寻找下一个位置
    Tuple right_tuple{};
    RID right_rid{};
    next_left_tuple_ = false;
    while(right_executor_->Next(&right_tuple, &right_rid)){
      // 检查是否匹配
      if(Check(&left_tuple_, &right_tuple)){
        // 获取当前tuple的每一列值存入结果集 先left后right
        for(size_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount();i++){
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for(size_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount();i++){
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        is_matched_ = true;
        *tuple = {values,&plan_->OutputSchema()};
        *rid = {};
        return true;
      }
    }

    // 到达这里说明需要访问下一个left_tuple
    next_left_tuple_ = true;
    // right_idx如果为0 说明当前都没有匹配成功的 则需要看聚合类型是否为LEFT 从而添加NULL值
    if(!is_matched_ && plan_->GetJoinType() == JoinType::LEFT){
      is_matched_ = false;
      for(size_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount();i++){
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for(size_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount();i++){
        // 添加NULL值
        values.emplace_back(ValueFactory::GetNullValueByType(
          right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = {values,&plan_->OutputSchema()};
      *rid = {};
      return true;
    }
    // 重置匹配成功信息
    is_matched_ = false;
  }
  return false; 
}

auto NestedLoopJoinExecutor::Check(Tuple *left_tuple, Tuple *right_tuple)-> bool {
  // 通过plan的接口判断左右元组是否相等
  auto value = plan_->Predicate()->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                               right_executor_->GetOutputSchema());
  // 为NULL值的不等价于是空的
  return !value.IsNull() && value.GetAs<bool>();
}

}  // namespace bustub
