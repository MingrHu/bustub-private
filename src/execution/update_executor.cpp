
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
#include <cstdint>
#include <optional>
#include <tuple>
#include <utility>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/update_executor.h"
#include "storage/index/index.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  child_executor_->Init();
  indexs_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  is_finished_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_finished_) {
    return false;
  }
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  bool exist_prim_key = false;
  int32_t update_count = 0;

  Tuple child_tp = {};
  RID child_rd = {};
  std::vector<Tuple> tuples;

  while (child_executor_->Next(&child_tp, &child_rd)) {
    auto [old_meta, old_tuple] = table_info_->table_->GetTuple(child_rd);
    auto base_ts = old_meta.ts_;
    // 1.先检查是否有写写冲突
    // 事务读取时间戳小于元组的记录时间戳 且和事务ID不相等 说明有别的事务占用了元组进行提交或者修改未提交
    // 此时需要抛出异常
    if (base_ts > txn->GetReadTs() && txn->GetTransactionId() != base_ts) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in UpdateExecutor.");
    }
    // 这里我认为是需要跳过已经被删除的元组的 因为无法对一个已删除的元组更新
    if (old_meta.is_deleted_) {
      continue;
    }

    // 2.没问题就先去获得新的元组 并进行新tuple的构造
    std::vector<Value> new_value;
    for (const auto &exp : plan_->target_expressions_) {
      // 根据旧元组更新新元组
      new_value.emplace_back(exp->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple = {new_value, &table_info_->schema_};
    TupleMeta new_tpmeta = {txn->GetTransactionTempTs(), false};

    // 存在主键索引的情况 否则是一般情况或一般索引
    if (!indexs_info_.empty() && indexs_info_[0]->is_primary_key_) {
      exist_prim_key = true;
      PmKeyDeleteTuple(txn, txn_mgr, old_tuple, child_rd, table_info_);
      tuples.emplace_back(new_tuple);
    } else {
      // 原子更新操作 先锁住 看旧数据 再更新
      bool update_state = false;
      auto old_undolink_opt = txn_mgr->GetUndoLink(child_rd);
      // 旧元组是当前事务修改的
      if (base_ts == txn->GetTransactionTempTs()) {
        // 这里和delete类似 如果元组压根没有undolink 那么说明这个元组是当前事务生成的
        // 无需在不存在的undolink上加入undolog undolog本质上是事务与事务之间的日志
        if (old_undolink_opt.has_value() && old_undolink_opt->IsValid()) {
          auto old_undolog = txn->GetUndoLog(old_undolink_opt->prev_log_idx_);
          auto undolog = GenerateUpdatedUndoLog(&table_info_->schema_, &old_tuple, &new_tuple, old_undolog);
          txn->ModifyUndoLog(old_undolink_opt->prev_log_idx_, undolog);
        }
        update_state = table_info_->table_->UpdateTupleInPlace(
            new_tpmeta, new_tuple, child_rd,
            [txn, base_ts](const TupleMeta &o_meta, const Tuple &table, RID rid) -> bool {
              if (base_ts != o_meta.ts_) {
                return false;
              }
              return o_meta.ts_ <= txn->GetReadTs() || o_meta.ts_ == txn->GetTransactionTempTs();
            });
      } else {
        // 否则不是当前事务修改的或生成的
        // 这部分会涉及到更新版本链的问题
        auto cur_undolog = GenerateNewUndoLog(&table_info_->schema_, &old_tuple, &new_tuple, old_meta.ts_, UndoLink{});
        if (old_undolink_opt.has_value()) {
          cur_undolog.prev_version_ = old_undolink_opt.value();
        }
        // 获取当前的undolink
        auto new_undolink = txn->AppendUndoLog(cur_undolog);
        // test and set
        update_state = UpdateTupleAndUndoLink(
            txn_mgr, child_rd, new_undolink, table_info_->table_.get(), txn, new_tpmeta, new_tuple,
            [txn, base_ts](const TupleMeta &o_meta, const Tuple &o_tuple, RID rid, std::optional<UndoLink> undolink) {
              if (base_ts != o_meta.ts_) {
                return false;
              }
              return o_meta.ts_ <= txn->GetReadTs() || o_meta.ts_ == txn->GetTransactionTempTs();
            });
      }

      if (!update_state) {
        txn->SetTainted();
        throw ExecutionException("Update op failed detected in UpdateExecutor!");
      }

      // 其余索引更新
      for (auto &index_info : indexs_info_) {
        auto old_index_key =
            old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_index_key, child_rd, exec_ctx_->GetTransaction());

        auto new_index_key =
            new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_index_key, child_rd, exec_ctx_->GetTransaction());
      }
      // 事务写入集更新
      txn->AppendWriteSet(plan_->GetTableOid(), child_rd);
      update_count += 1;
    }
  }

  if (exist_prim_key && !tuples.empty()) {
    for (auto &new_tuple : tuples) {
      PmKeyInsertTuple(txn, txn_mgr, new_tuple, indexs_info_[0], table_info_);
      update_count += 1;
    }
  }

  std::vector<Value> res_val;
  res_val.emplace_back(TypeId::INTEGER, update_count);
  *tuple = {res_val, &plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub