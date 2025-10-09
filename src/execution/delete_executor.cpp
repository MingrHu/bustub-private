//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  is_finished_ = false;
  delete_count_ = 0;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  child_executor_->Init();
  BUSTUB_ENSURE(table_info_, "table_info is empty!");
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }

  Tuple child_tp = {};
  RID child_rid = {};
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&child_tp, &child_rid)) {
    auto child_meta = table_info_->table_->GetTupleMeta(child_rid);
    if (child_meta.is_deleted_) {
      continue;
    }

    child_meta.is_deleted_ = true;
    delete_count_ += 1;

    table_info_->table_->UpdateTupleMeta(child_meta, child_rid);
    for (auto &index_info : indexs_info) {
      auto old_index_key =
          child_tp.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_index_key, child_rid, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> res_val;
  res_val.emplace_back(TypeId::INTEGER, delete_count_);
  *tuple = {res_val, &plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub
