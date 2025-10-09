//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

// 右表构建哈希表 左表探测匹配
void HashJoinExecutor::Init() {
  // 初始化
  left_child_->Init();
  right_child_->Init();
  hash_table_.clear();
  next_left_tuple_ = true;
  inner_indx_ = 0;
  left_key_.keys_.clear();

  Tuple tuple{};
  RID rid{};
  // 右表存储元组
  while (right_child_->Next(&tuple, &rid)) {
    std::vector<Value> org_key;
    // 获取原始元组的键构成列
    for (const auto &exp : plan_->RightJoinKeyExpressions()) {
      org_key.emplace_back(exp->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    // 构造键
    HashJoinKey key{org_key};
    // 不管键是新的还是旧的 将所得到的元组值放入
    hash_table_[key].emplace_back(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!next_left_tuple_ || left_child_->Next(&left_tuple_, &left_rid_)) {
    std::vector<Value> values;
    std::vector<Value> org_key;
    // 如果到下一个左元组 则更新左键
    if (next_left_tuple_) {
      for (const auto &exp : plan_->LeftJoinKeyExpressions()) {
        org_key.emplace_back(exp->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
      }
      left_key_.keys_ = org_key;
    }
    next_left_tuple_ = false;
    // 先看是否有匹配的
    if (hash_table_.count(left_key_) != 0) {
      for (size_t idx = inner_indx_; idx < hash_table_[left_key_].size(); idx++) {
        for (size_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (size_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
          values.emplace_back(hash_table_[left_key_][idx].GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
        }
        *tuple = {values, &plan_->OutputSchema()};
        *rid = {};
        inner_indx_ = idx + 1;
        return true;
      }
    }
    next_left_tuple_ = true;
    // 如果没有任何匹配的且为左连接
    if (inner_indx_ == 0 && plan_->join_type_ == JoinType::LEFT) {
      for (size_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        // 添加NULL值
        values.emplace_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
      }
      *tuple = {values, &plan_->OutputSchema()};
      *rid = {};
      return true;
    }
    // 重置inner_idx
    inner_indx_ = 0;
  }
  return false;
}

}  // namespace bustub
