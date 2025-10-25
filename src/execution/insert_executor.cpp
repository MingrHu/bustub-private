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
#include <iostream>
#include <memory>
#include <optional>
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  is_finished_ = false;
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  insert_count_ = 0;
  BUSTUB_ENSURE(table_info_, "table_info is empty!");
}

auto InsertExecutor::KeyExsitInPmKey(const std::shared_ptr<IndexInfo> &pm_key_info, const Tuple &tuple,
                                     RID &pm_key_rd) const -> bool {
  // 先根据当前元组获取组成pmkey的键
  auto tp_pm_key =
      tuple.KeyFromTuple(table_info_->schema_, pm_key_info->key_schema_, pm_key_info->index_->GetKeyAttrs());
  // 检查这个键是否存在于主键索引中
  std::vector<RID> check;
  pm_key_info->index_->ScanKey(tp_pm_key, &check, exec_ctx_->GetTransaction());
  if (!check.empty()) {
    pm_key_rd = check[0];
    BUSTUB_ENSURE(pm_key_rd.GetPageId() != INVALID_PAGE_ID, "You should check the reason pm_key is INVALID!\n");
  }
  return !check.empty();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果已经执行过获取
  if (is_finished_) {
    return false;
  }

  // 直接拿所有的索引信息
  auto indexs_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  bool exist_prim_key = false;
  Tuple tp{};
  RID rd{};
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  while (child_executor_->Next(&tp, &rd)) {
    // 由于期望的是删除元组不删除索引 为了避免非重复插入
    bool insert_into_delete = false;
    std::optional<RID> insert_rid = {};
    // 主键索引如果存在
    if (!indexs_info.empty() && indexs_info[0]->is_primary_key_) {
      exist_prim_key = true;
      // 检查这个元组的键在主键索引是否存在
      RID pm_key_rd = {};
      // RID存在
      if (KeyExsitInPmKey(indexs_info[0], tp, pm_key_rd)) {
        auto cur_meta = table_info_->table_->GetTupleMeta(pm_key_rd);
        bool is_confilct = true;
        // 1 发现被删除了
        if (cur_meta.is_deleted_) {
          // 1.1 别的已经提交事务删的或 需要更新日志
          // 1.2 当前事务删除后提交的 无需更新日志
          // 1.3 其余情况就是写冲突
          // bool lk_state = txn_mgr->LockTuple(pm_key_rd);
          if (cur_meta.ts_ <= txn->GetTransactionTempTs()) {
            is_confilct = false;
            insert_into_delete = true;
            insert_rid = pm_key_rd;
            auto link = txn_mgr->GetUndoLink(pm_key_rd);
            auto log = link == std::nullopt ? std::nullopt : txn_mgr->GetUndoLogOptional(link.value());
            // 如果是当前事务是不会进入日志生成的
            if (cur_meta.ts_ != txn->GetTransactionTempTs() && log.has_value()) {
              auto new_undolog = GenerateNewUndoLog(&table_info_->schema_, nullptr, &tp, cur_meta.ts_, UndoLink{});
              new_undolog.prev_version_ = link.value();
              auto new_undolink = txn->AppendUndoLog(new_undolog);
              txn_mgr->UpdateUndoLink(pm_key_rd, new_undolink);
            }
          }
        }
        // 2 发生写写冲突或主键索引不唯一 不继续进行
        if (is_confilct) {
          txn->SetTainted();
          throw ExecutionException("Primary key unique violation detected in InsertExecutor!");
        }
      }
    }

    TupleMeta meta = {txn->GetTransactionTempTs(), false};
    // 主键索引对应的RID不会变 因此插入删除的元组需要原地更新
    if (exist_prim_key && insert_into_delete) {
      // table_info_->table_->UpdateTupleInPlace(meta, tp, insert_rid.value());
      auto status = table_info_->table_->UpdateTupleInPlace(
          meta, tp, insert_rid.value(), [txn](const TupleMeta &meta, const Tuple &table, RID rid) -> bool {
            return meta.ts_ <= txn->GetReadTs() || meta.ts_ == txn->GetTransactionTempTs();
          });
      if (!status) {
        txn->SetTainted();
        throw ExecutionException("Primary key unique violation detected in InsertExecutor!");
      }
    } else {
      insert_rid = table_info_->table_->InsertTuple(meta, tp);
    }

    // 插入成功
    if (insert_rid.has_value()) {
      insert_count_ += 1;
      txn->AppendWriteSet(plan_->GetTableOid(), insert_rid.value());
      // 先更新主键索引
      if (exist_prim_key) {
        auto pm_key =
            tp.KeyFromTuple(table_info_->schema_, indexs_info[0]->key_schema_, indexs_info[0]->index_->GetKeyAttrs());
        // printf("pm_key = %s\n",pm_key.ToString(&indexs_info[0]->key_schema_).c_str());
        // 更新索引的时候是在事务锁保护下进行的 如果发现此时被其余事务占用了 则失败
        if (!insert_into_delete && !indexs_info[0]->index_->InsertEntry(pm_key, insert_rid.value(), txn)) {
          txn->SetTainted();
          throw ExecutionException("Write-write confilct detected in InsertExecutor!");
        }
      }
      // 更新其余索引
      for (auto &index_info : indexs_info) {
        if (exist_prim_key) {
          exist_prim_key = false;
          continue;
        }
        // 更新索引先必须获得索引的键 然后是插入的信息和事物上下文
        // 索引的键需要从元组中获取 构造方式一般是元组的几个属性列
        // 因此这里传入的是元组的模式schema 键的模式key_schema 键对应属性列下标位置attrs
        auto index_key =
            tp.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // 更新索引
        index_info->index_->InsertEntry(index_key, insert_rid.value(), exec_ctx_->GetTransaction());
      }
    }
  }
  // 返回插入的行数信息 等待全部插入完成后统计信息
  // 必须是Value类型
  std::vector<Value> val;
  val.emplace_back(TypeId::INTEGER, insert_count_);
  *tuple = {val, &plan_->OutputSchema()};
  is_finished_ = true;
  return true;
}

}  // namespace bustub
