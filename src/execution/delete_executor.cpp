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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
  RID child_rd = {};
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&child_tp, &child_rd)) {
    auto child_meta = table_info_->table_->GetTupleMeta(child_rd);
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();

    // 1.检查是否有写入冲突
    if(child_meta.ts_ > txn->GetReadTs() 
    && txn->GetTransactionId() != child_meta.ts_){
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in UpdateExecutor.");
    }  

    // TODO1:这里我认为删除了也没必要再删一遍
    if (child_meta.is_deleted_) {
      continue;
    }

    // 2.预定删除信息和版本链
    auto old_undolink_opt = txn_mgr->GetUndoLink(child_rd);
    TupleMeta meta = {txn->GetTransactionTempTs(),true};
    txn->AppendWriteSet(plan_->GetTableOid(), child_rd);

    // 如果是被当前的事务删除
    if(child_meta.ts_ == txn->GetTransactionTempTs()){
      // 如果原本有undolog 那么就更新 由于是当前事务插入删除的 因此没有undolog就无需生成
      if(old_undolink_opt.has_value() && old_undolink_opt->IsValid()){
        auto old_undolog = txn->GetUndoLog(old_undolink_opt->prev_log_idx_);
        auto undolog = GenerateUpdatedUndoLog(&table_info_->schema_,&child_tp,
           nullptr, old_undolog);
        txn->ModifyUndoLog(old_undolink_opt->prev_log_idx_, undolog);
      }
    }
    else{
      auto cur_undolog = GenerateNewUndoLog(&table_info_->schema_, 
        &child_tp, nullptr, child_meta.ts_, UndoLink{});
      if(old_undolink_opt.has_value()){
        cur_undolog.prev_version_ = old_undolink_opt.value();
      }
      // 生成新的undolink
      auto new_undolink = txn->AppendUndoLog(cur_undolog);
      // 更新undolink
      // TODO2:这里的undolink到底怎么存储的？
      txn_mgr->UpdateUndoLink(child_rd, std::make_optional(new_undolink));
    }
    // 更新信息
    table_info_->table_->UpdateTupleMeta(meta,child_rd);
    delete_count_ += 1;
    // 再更新索引
    for (auto &index_info : indexs_info) {
      auto old_index_key =
          child_tp.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_index_key, child_rd, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> res_val;
  res_val.emplace_back(TypeId::INTEGER, delete_count_);
  *tuple = {res_val, &plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub
