//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "catalog/catalog.h"
#include "execution/executors/update_executor.h"
#include "storage/index/index.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { 
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  child_executor_->Init();
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  update_count_ = 0;
  is_finished_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 

  if(is_finished_){
    return false;
  }

  Tuple child_tp = {};
  RID child_rd = {};
  while(child_executor_->Next(&child_tp, &child_rd)){
    auto [old_meta,old_tuple] = table_info_->table_->GetTuple(child_rd);
    
    if(old_meta.is_deleted_){
      continue;
    }

    // 先去获得新的元组 并进行新tuple的构造
    std::vector<Value> new_value;
    // target_expressions_ 定义了更新操作后新元组的每一列应该如何计算
    for(const auto& exp:plan_->target_expressions_){
      // 根据旧元组更新新元组
      new_value.emplace_back(exp->Evaluate(&child_tp, child_executor_->GetOutputSchema()));
    }
    // 构造新元组
    Tuple new_tuple = {new_value,&table_info_->schema_};
    TupleMeta new_tpmeta = {0,false};

    auto new_rid = table_info_->table_->InsertTuple(new_tpmeta, new_tuple,
      exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction());
    // 先插入表堆 插入成功后更新索引
    if(new_rid.has_value()){
      update_count_ += 1;
      old_meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(old_meta, child_rd);
      // 索引更新
      for(auto & index_info:indexs_info_){
        auto old_index_key= old_tuple.KeyFromTuple(table_info_->schema_, 
          index_info->key_schema_,index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_index_key, child_rd, exec_ctx_->GetTransaction());

        auto new_index_key = new_tuple.KeyFromTuple(table_info_->schema_, 
          index_info->key_schema_,index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_index_key, new_rid.value(), exec_ctx_->GetTransaction());
      }
    }
 }
  std::vector<Value> res_val;
  res_val.emplace_back(TypeId::INTEGER,update_count_);
  *tuple ={res_val,&plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub
