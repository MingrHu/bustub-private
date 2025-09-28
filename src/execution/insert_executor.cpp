//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)),is_finished_(false){}

void InsertExecutor::Init() {
 child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
// 如果已经执行过获取
  if(is_finished_){
    return false;
  }
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  int32_t insert_count = 0;
  BUSTUB_ENSURE(table_info, "table_metedata is empty!");
  // 直接拿所有的索引信息
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  Tuple tp{};
  RID rd{};

  while(child_executor_->Next(&tp, &rd)){
    TupleMeta tp_meta = {0,false};
    auto insert_rid = table_info->table_->InsertTuple(tp_meta, tp,exec_ctx_->GetLockManager(),
    exec_ctx_->GetTransaction(),table_info->oid_);
    // 插入成功
    if(insert_rid.has_value()){
      insert_count += 1;
      for(auto& index_info:indexs_info){
        // 更新索引先必须获得索引的键 然后是插入的信息和事物上下文
        // 索引的键需要从元组中获取 构造方式一般是元组的几个属性列
        // 因此这里传入的是元组的模式schema 键的模式key_schema 键对应属性列下标位置attrs
        auto index_key = tp.KeyFromTuple(table_info->schema_, index_info->key_schema_,
             index_info->index_->GetKeyAttrs());
        // 更新索引
        index_info->index_->InsertEntry(index_key, insert_rid.value(), exec_ctx_->GetTransaction());
      }
    }
  }
  // 返回插入的行数信息 等待全部插入完成后统计信息
  // 必须是Value类型
  std::vector<Value> val;
  val.emplace_back(TypeId::INTEGER,insert_count);
  *tuple = {val,&plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub
