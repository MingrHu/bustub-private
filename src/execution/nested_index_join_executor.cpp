//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_)),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  next_outer_tuple_ = true;
  inner_idx_ = 0;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!next_outer_tuple_ || child_executor_->Next(&outer_tuple_, &outer_rid_)) {
    std::vector<Value> values;
    // 如果移动到了下一个外表元组
    if (next_outer_tuple_) {
      // 定义构成索引的元组值
      std::vector<Value> val;
      Value key_val = plan_->KeyPredicate()->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());
      val.emplace_back(key_val);
      // 获取索引键
      Tuple index_key{val, &index_info_->key_schema_};
      index_info_->index_->ScanKey(index_key, &inner_rids_, exec_ctx_->GetTransaction());
    }
    // 默认到这里更新为不需要切换下一个元组
    next_outer_tuple_ = false;
    // child_executor默认为外表的执行器
    // plan_的内表模式可访问内表的列
    if (inner_idx_ < inner_rids_.size()) {
      auto [meta, inner_tuple] = table_info_->table_->GetTuple(inner_rids_[inner_idx_++]);
      //
      if (meta.is_deleted_) {
        continue;
      }

      for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        values.emplace_back(inner_tuple.GetValue(&plan_->OutputSchema(), i));
      }
      *tuple = {values, &plan_->OutputSchema()};
      *rid = {};
      return true;
    }
    // 需要切换下一个元组
    next_outer_tuple_ = true;
    inner_rids_.clear();
    // 如果一个都没匹配到且是左连接
    if (inner_idx_ == 0 && plan_->GetJoinType() == JoinType::LEFT) {
      for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(outer_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        auto col = plan_->InnerTableSchema().GetColumn(i);
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = {values, &plan_->OutputSchema()};
      *rid = {};
      return true;
    }
    // 重置inner_idx_
    inner_idx_ = 0;
  }
  return false;
}
}  // namespace bustub
