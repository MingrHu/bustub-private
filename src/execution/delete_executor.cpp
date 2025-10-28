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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }

  Tuple child_tp = {};
  RID child_rd = {};
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  while (child_executor_->Next(&child_tp, &child_rd)) {
    auto child_meta = table_info_->table_->GetTupleMeta(child_rd);
    auto base_ts = child_meta.ts_;
    // 1.检查是否有写入冲突
    // bool lk_state = txn_mgr->LockTuple(child_rd);
    if (child_meta.ts_ > txn->GetReadTs() && txn->GetTransactionId() != child_meta.ts_) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in DeleteExecutor.");
    }

    // 这里我认为删除了也没必要再删一遍
    if (child_meta.is_deleted_) {
      continue;
    }

    // 2.预定删除信息和版本链
    auto old_undolink_opt = txn_mgr->GetUndoLink(child_rd);
    TupleMeta meta = {txn->GetTransactionTempTs(), true};
    txn->AppendWriteSet(plan_->GetTableOid(), child_rd);
    bool state = false;

    // 如果是被当前的事务删除
    if (base_ts == txn->GetTransactionTempTs()) {
      // 如果原本有undolog 那么就更新 由于是当前事务插入删除的 因此没有undolog就无需生成
      if (old_undolink_opt.has_value() && old_undolink_opt->IsValid()) {
        auto old_undolog = txn->GetUndoLog(old_undolink_opt->prev_log_idx_);
        auto undolog = GenerateUpdatedUndoLog(&table_info_->schema_, &child_tp, nullptr, old_undolog);
        txn->ModifyUndoLog(old_undolink_opt->prev_log_idx_, undolog);
      }
      state = table_info_->table_->UpdateTupleInPlace(
      meta, child_tp, child_rd, [txn,base_ts](const TupleMeta &o_meta, const Tuple &table, RID rid) -> bool {
        if(base_ts != o_meta.ts_){
          return false;
        }
        return o_meta.ts_ <= txn->GetReadTs() || o_meta.ts_ == txn->GetTransactionTempTs();
      });   
   
    } else {
      auto cur_undolog = GenerateNewUndoLog(&table_info_->schema_, &child_tp, nullptr, child_meta.ts_, UndoLink{});
      if (old_undolink_opt.has_value()) {
        cur_undolog.prev_version_ = old_undolink_opt.value();
      }
      // 生成新的undolink
      auto new_undolink = txn->AppendUndoLog(cur_undolog);
      state = UpdateTupleAndUndoLink(txn_mgr, child_rd, new_undolink, table_info_->table_.get(), txn, meta, child_tp,
      [txn,base_ts](const TupleMeta &o_meta, const Tuple &o_tuple, RID rid, std::optional<UndoLink> undolink){
        if(base_ts != o_meta.ts_){
          return false;
        }
        return o_meta.ts_ <= txn->GetReadTs() || o_meta.ts_ == txn->GetTransactionTempTs();
      });
    }
    if(!state){
      txn->SetTainted();
      throw ExecutionException("Primary key unique violation detected in DeleteExecutor!");
    }
    // 更新信息
    table_info_->table_->UpdateTupleMeta(meta, child_rd);
    delete_count_ += 1;
  }
  std::vector<Value> res_val;
  res_val.emplace_back(TypeId::INTEGER, delete_count_);
  *tuple = {res_val, &plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub