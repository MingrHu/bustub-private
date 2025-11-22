//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <cstdint>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // 读时间戳是当前提交事务的最大时间戳
  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = last_commit_ts_.load() + 1;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  // 更新事务所涉及到的所有元组元数据信息
  // 1.这里思考一下别的事务是否会修改元组？这里能直接用update吗？
  // 2.Bustub的快照隔离 当前事务不应该看到别的事务修改的内容 这是如何做到的？
  for (const auto &table : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table.first);
    for (const auto &rid : table.second) {
      TupleMeta meta = {commit_ts, table_info->table_->GetTupleMeta(rid).is_deleted_};
      // 看看这一页的undolink集合是否存在
      // RID: page_id + slot_offset可唯一表示
      if (version_info_.find(rid.GetPageId()) != version_info_.end()) {
        auto ver_info = version_info_[rid.GetPageId()];
        auto &link_info = version_info_[rid.GetPageId()]->prev_link_;
        if (link_info.find(rid.GetSlotNum()) != link_info.end()) {
          // 更新最新元组在undolink的提交时间
          ver_info->base_tuple_ts_[rid.GetSlotNum()] = commit_ts;
        }
      }
      // 必须把版本链对应的元组提交时间更新了再更新元组的时间戳 否则中途可能被其余事务删除了
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }

  // 提交事务 更新last_commit_ts
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->commit_ts_ = commit_ts;
  last_commit_ts_.fetch_add(1);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  // 事务的原子性在于整个事务对所有产生修改的元组进行全部回滚
  // 而事务的并发保证了事务对一个元组的操作要么成功修改 要么不修改
  // 因此回滚的核心在于事务涉及成功修改的所有元组进行回滚 没修改的不需要回滚
  for (auto &[table_oid, rids] : txn->write_set_) {
    const auto table_heap = catalog_->GetTable(table_oid)->table_.get();
    for (const auto &rid : rids) {
      auto [meta, tuple, undolink] = GetTupleAndUndoLink(this, table_heap, rid);
      // 如果有版本链 说明是更新或者删除操作
      if (undolink.has_value() && undolink->IsValid()) {
        auto undolog = GetUndoLog(undolink.value());
        auto old_tuple = ReconstructTuple(&catalog_->GetTable(table_oid)->schema_, tuple, meta, {undolog});
        auto base_ts = meta.ts_;
        // 回滚事务的核心不是未修改的元组 而是已经成功修改的元组
        if (base_ts == txn->GetTransactionId()) {
          meta.ts_ = undolog.ts_;
          // 如果旧的元组存在
          if (old_tuple.has_value()) {
            meta.is_deleted_ = false;
            UpdateTupleAndUndoLink(this, rid, undolog.prev_version_, table_heap, txn, meta, *old_tuple);
          } else {
            // 不存在直接标记这个rid对应的元组是删除的即可
            meta.is_deleted_ = true;
            UpdateTupleAndUndoLink(this, rid, undolog.prev_version_, table_heap, txn, meta, tuple);
          }
        }
      } else {
        // 说明是插入 直接修改元数据为删除即可
        meta.is_deleted_ = true;
        meta.ts_ = 0;
        UpdateTupleAndUndoLink(this, rid, undolink, table_heap, txn, meta, tuple);
      }
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // 本质上希望删除的事务有两种：
  // 1.undolog不被大于等于watermark的活跃事务直接连接
  // 2.事务已经提交了但是本身没有Undolog
  auto water_mark = running_txns_.GetWatermark();
  std::unordered_map<txn_id_t, uint32_t> txn_useless_log;

  for (const auto &ver_info : version_info_) {
    // 实际上存储的是最新元组的偏移和版本链
    // 等价于 元组 + 其版本链
    for (const auto &[tuple_offset, prev_link] : ver_info.second->prev_link_) {
      auto undolink = prev_link;
      // 获取base_tuple的undolink 没有就跳过 说明该事务在这个元组没有Undolog
      if (undolink.prev_txn_ != INVALID_TXN_ID) {
        auto first_undolog_opt = GetUndoLogOptional(undolink);
        // 1.找到第一个小于水位线的undolog
        // 2.看这个undolog是第几个 如果是第一个 假设base_tuple都小于水位线 那显然这个undolog可以去除
        // 否则只要是后面小于水位线的 都可以被去除
        // BUSTUB_ENSURE(undolink.base_tuple_ts_ != INVALID_TS, "Base_tuple uncommited!\n");
        int32_t base_pos = ver_info.second->base_tuple_ts_[tuple_offset] <= water_mark ? -1 : 0;
        int32_t cur_pos = 0;
        for (auto undolog_opt = first_undolog_opt; undolink.prev_txn_ != INVALID_TXN_ID;
             undolink = undolog_opt->prev_version_) {
          // 获取当前的undolog
          undolog_opt = GetUndoLogOptional(undolink);
          // 某个事务已经被清理了 需要更新undolink
          if (!undolog_opt.has_value()) {
            undolink.prev_txn_ = INVALID_TXN_ID;
            break;
          }
          if (undolog_opt->ts_ < water_mark && cur_pos - base_pos > 0) {
            txn_useless_log[undolink.prev_txn_] += 1;
          }
          cur_pos += 1;
        }
      }
    }
  }

  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto txn = it->second;
    if (txn->state_ == TransactionState::COMMITTED &&
        (txn->GetUndoLogNum() == 0 || txn->GetUndoLogNum() == txn_useless_log[txn->GetTransactionId()])) {
      it = txn_map_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace bustub
