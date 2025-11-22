//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <tuple>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}
// order_bys的组成
// std::vector<OrderBy> order_bys = {
//     {0, OrderByType::ASC},   // 第0列（age）升序
//     {1, OrderByType::DESC}   // 第1列（name）降序
// };
// 在 C++ 排序规则中：
// 如果 comp(a, b) == false 且 comp(b, a) == false 则 a 和 b 被视为等价（equivalent） 排序算法不会交换它们
// 在序列中后一个的元素会被放在比较函数前一个位置 前一个元素会被放在比较函数的后一个位置
// 如果 operator()(a, b) == true，表示 a 应该排在 b 前面（即 a < b）。
// 如果 operator()(a, b) == false，表示 a 应该排在 b 后面 或 顺序不变
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  auto keya = entry_a.first;
  auto keyb = entry_b.first;
  uint32_t idx = 0;
  for (const auto &exp : order_bys_) {
    BUSTUB_ENSURE(idx < order_bys_.size(), "Order by idx out of range!\n");
    switch (exp.first) {
      case OrderByType::INVALID:
        BUSTUB_ENSURE(exp.first != OrderByType::INVALID, "Order by rule is invalid!\n");
      case OrderByType::DEFAULT:
      case OrderByType::ASC:
        // 如果是升序但a < b 则返回true
        if (keya[idx].CompareLessThan(keyb[idx]) == CmpBool::CmpTrue) {
          return true;
        } else if (keya[idx].CompareGreaterThan(keyb[idx]) == CmpBool::CmpTrue) {
          return false;
        }
        break;
      case OrderByType::DESC:
        // 如果是降序但a < b 则返回false
        if (keya[idx].CompareLessThan(keyb[idx]) == CmpBool::CmpTrue) {
          return false;
        } else if (keya[idx].CompareGreaterThan(keyb[idx]) == CmpBool::CmpTrue) {
          return true;
        }
        break;
    }
    idx += 1;
  }
  return false;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sort_key{};
  for (const auto &expr : order_bys) {
    sort_key.emplace_back(expr.second->Evaluate(&tuple, schema));
  }
  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // 如果当前元组本身是被删除的或者根本没有重构日志
  // 1.如果是被删除的且没有日志或者最后的结果是被删除的
  if ((base_meta.is_deleted_ && undo_logs.empty()) || (!undo_logs.empty() && undo_logs.back().is_deleted_)) {
    return std::nullopt;
  }
  // 2.如果没被删除 但是没有日志
  if (undo_logs.empty()) {
    return base_tuple;
  }

  // 先获取最新的完整元组所有列值
  std::vector<Value> base_val;
  base_val.reserve(schema->GetColumnCount());
  for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
    base_val.emplace_back(base_tuple.GetValue(schema, idx));
  }

  // 注意：undo_logs的顺序是最新的放最前面
  // 这里只需要构建base_val作为结果返回
  for (const auto &log : undo_logs) {
    // 有删除的直接跳过即可
    if (log.is_deleted_) {
      continue;
    }
    uint32_t ptr = 0;
    // 这里提取修改值时用的scheam必须新创建 需要获取指定的新列
    auto log_schema = GetUndoLogSchema(schema, log);
    for (uint32_t col = 0; col < log.modified_fields_.size(); col++) {
      if (log.modified_fields_[col]) {
        base_val[col] = log.tuple_.GetValue(&log_schema, ptr++);
      }
    }
  }

  return std::optional<Tuple>{Tuple{base_val, schema}};
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  auto read_ts = txn->GetReadTs();
  // 1.当前元组没有日志
  // 2.当前访问的时间戳是大于等于最新的元组
  // 3.当前元组被当前事务处理但未提交

  // 返回nullopt的关键条件是指定的日志元组状态是被删除的
  if (base_meta.ts_ <= read_ts || txn->GetTransactionTempTs() == base_meta.ts_) {
    return (base_meta.is_deleted_ ? std::nullopt : std::make_optional(std::vector<UndoLog>{}));
  }
  std::vector<UndoLog> undo_logs;
  // 必须找到符合要求的才返回 否则返回空
  if (undo_link.has_value()) {
    auto log = txn_mgr->GetUndoLogOptional(undo_link.value());
    while (log.has_value()) {
      undo_logs.emplace_back(log.value());
      if (log->ts_ <= read_ts) {
        return undo_logs;
      }
      log = txn_mgr->GetUndoLogOptional(log->prev_version_);
    }
  }
  return std::nullopt;
}

// 根据输入的相关信息获取表堆元组的事务指定访问历史版本
// 如果最终的返回的元组被删除 则返回std::nullopt
auto AcquireSpecTuple(const Schema &schema, const RID &rid, TableHeap *table_heap, Transaction *txn,
                      TransactionManager *txn_manager) -> std::optional<Tuple> {
  // std::tuple<TupleMeta, Tuple, std::optional<UndoLink>>
  auto [tp_meta, tp, UndoLink] = GetTupleAndUndoLink(txn_manager, table_heap, rid);
  // 元组重建和日志收集已经默认帮我们解决了std::nullopt的问题
  auto undo_logs_opt = CollectUndoLogs(rid, tp_meta, tp, UndoLink, txn, txn_manager);
  if (!undo_logs_opt.has_value()) {
    return std::nullopt;
  }
  auto res_tp = ReconstructTuple(&schema, tp, tp_meta, undo_logs_opt.value());
  return res_tp;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // target_tuple其实就是最新的元组 而base_tuple就是后续需要还原回去的元组
  // Undolog的tuple就是记录差异的tuple 本方法可以直接根据最原始的元组进行差异日志
  // 当前的delete = true可以认为当前元组通过删除得到的base_tuple
  std::vector<bool> modfileds(schema->GetColumnCount(), true);
  // case1 当前元组被删除 当前不记录
  if (target_tuple == nullptr) {
    return UndoLog{false, modfileds, *base_tuple, ts, prev_version};
  }
  // case2 当前对过去是一个删除操作
  if (base_tuple == nullptr) {
    return UndoLog{true, {}, {}, ts, prev_version};
  }
  // case3
  std::vector<Value> val;
  std::vector<uint32_t> colums;
  for (uint32_t col = 0; col < schema->GetColumnCount(); col++) {
    auto base_val = base_tuple->GetValue(schema, col);
    auto tar_val = target_tuple->GetValue(schema, col);
    if (!base_val.CompareExactlyEquals(tar_val)) {
      val.emplace_back(base_val);
      colums.emplace_back(col);
    } else {
      modfileds[col] = false;
    }
  }
  Schema sc{Schema::CopySchema(schema, colums)};
  Tuple res_tuple = {std::move(val), &sc};
  return UndoLog{false, std::move(modfileds), res_tuple, ts, prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // 如果base_tuple == nullptr说明其日志是一个全部修改的
  // 不需要考虑这个中间状态 直接返回即可
  if (log.is_deleted_ || base_tuple == nullptr) {
    return log;
  }

  // 如果需要更新的日志是一个删除操作
  if (base_tuple == nullptr) {
    std::vector<bool> modified_fields(schema->GetColumnCount(), true);
    std::vector<Value> values;
    for (const auto &col : schema->GetColumns()) {
      auto null_value = ValueFactory::GetNullValueByType(col.GetType());
      values.emplace_back(null_value);
    }
    Tuple tuple{values, schema};
    tuple.SetRid(target_tuple->GetRid());
    return {true, modified_fields, tuple, log.ts_, log.prev_version_};
  }

  uint32_t colcnt = schema->GetColumnCount();
  std::vector<Value> values;
  values.reserve(colcnt);
  std::vector<uint32_t> colums;
  colums.reserve(colcnt);
  std::vector<bool> modfileds(colcnt, true);

  // 获取上一个修改字段元组的schema
  auto org_log_schema = GetUndoLogSchema(schema, log);

  // 如果当前的操作是删除操作
  if (target_tuple == nullptr) {
    uint32_t ptr = 0;
    for (uint32_t col = 0; col < colcnt; col++) {
      // 这里分两种情况 由于重建需要根据删除的元组去重建过去的
      // 1.如果过去的日志中某列是修改得到的 我们的重建元组需要这个修改值而不是base_tuple的值
      // 2.否则直接沿用base_tuple的值
      // 例如：[1,2,3] -> base_tuple:[1,4,3] -> target = nullptr delmarker
      // base_tuple的log_tuple:index = 0,tuple = [2] 显然当前需要记录的是这个2而不是4
      auto val =
          log.modified_fields_[col] ? log.tuple_.GetValue(&org_log_schema, ptr++) : base_tuple->GetValue(schema, col);
      values.emplace_back(val);
      colums.emplace_back(col);
    }
  } else {
    uint32_t ptr = 0;
    for (uint32_t col = 0; col < colcnt; col++) {
      // base_tuple记录到过去的修改 差异来源于base_tuple
      if (log.modified_fields_[col]) {
        // 只要当前和undolog里面的mf对应的bool不同就要置位
        values.emplace_back(log.tuple_.GetValue(&org_log_schema, ptr++));
        colums.emplace_back(col);
        // 否则直接假设base_tuple就是最原始的值 差异来源于现在
      } else {
        auto tar_val = target_tuple->GetValue(schema, col);
        auto base_val = base_tuple->GetValue(schema, col);
        // 和base有差异
        if (!tar_val.CompareExactlyEquals(base_val)) {
          values.emplace_back(base_val);
          colums.emplace_back(col);
        } else {
          modfileds[col] = false;
        }
      }
    }
  }
  Schema sc{Schema::CopySchema(schema, colums)};
  Tuple res_tuple{std::move(values), &sc};
  return UndoLog{log.is_deleted_, std::move(modfileds), res_tuple, log.ts_, log.prev_version_};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  std::stringstream ss;
  fmt::println(stderr, "debug_hook: {}", info);
  ss << "TUPLE UNDO-LOG INFO:\n";
  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    ss << "--------------Cur tuple infomation-----------\n";
    auto rid = iter.GetRID();
    auto [metadata, tuple] = iter.GetTuple();
    ss << fmt::format("RID={}/{}", rid.GetPageId(), rid.GetSlotNum());
    // 当前元组属于未提交的元组
    if ((metadata.ts_ & TXN_START_ID) != 0) {
      ss << fmt::format(" ts={}* ", metadata.ts_ ^ TXN_START_ID);
    } else {
      ss << fmt::format(" ts={}  ", metadata.ts_);
    }
    if (metadata.is_deleted_) {
      ss << "<del marker>\n";
    } else {
      ss << tuple.ToString(&table_info->schema_) << "\n";
    }
    ss << "Tuple undo-log below:\n";
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      auto undo_log = txn_mgr->GetUndoLogOptional(*undo_link);
      while (undo_log.has_value()) {
        // 打印日志的事务ID和这个事务保存的上一个日志ID
        // 等价于当前事务和上一个事务日志 next指针作用
        ss << fmt::format("  txn{}@{} ", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_)
           << UndoLogToString(&table_info->schema_, *undo_log) << "\n";
        undo_link = undo_log->prev_version_;
        undo_log = txn_mgr->GetUndoLogOptional(undo_log->prev_version_);
      }
    }
    ++iter;
    ss << "\n";
  }
  fmt::println(stderr, "{}", ss.str());

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto UndoLogToString(const Schema *schema, const UndoLog &log) -> std::string {
  std::stringstream ss;
  ss << fmt::format("ts={} ", log.ts_);
  if (log.is_deleted_) {
    ss << "<del marker>";
  } else {
    const auto schema_log = GetUndoLogSchema(schema, log);
    ss << "(";
    for (uint32_t i = 0, j = 0, n = schema->GetColumnCount(); i < n; ++i) {
      if (i != 0U) {
        ss << " ";
      }
      if (log.modified_fields_[i]) {
        auto val = log.tuple_.GetValue(&schema_log, j++);
        if (val.IsNull()) {
          ss << "<NULL>";
        } else {
          ss << fmt::format("{}", val.ToString());
        }
      } else {
        ss << fmt::format("_");
      }
      if (i + 1 < n) {
        ss << ",";
      }
    }
    ss << ")";
  }
  return ss.str();
}

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log) -> Schema {
  std::vector<Column> schema_val;
  for (uint32_t i = 0; i < log.modified_fields_.size(); i++) {
    if (log.modified_fields_[i]) {
      schema_val.emplace_back(schema->GetColumn(i));
    }
  }
  return Schema{schema_val};
}

auto KeyExsitInPmKey(const std::shared_ptr<IndexInfo> &pm_key_info, const Tuple &tuple, RID &pm_key_rd,
                     std::shared_ptr<TableInfo> &table_info, Transaction *txn) -> bool {
  // 先根据当前元组获取组成pmkey的键
  auto tp_pm_key =
      tuple.KeyFromTuple(table_info->schema_, pm_key_info->key_schema_, pm_key_info->index_->GetKeyAttrs());
  // 检查这个键是否存在于主键索引中
  std::vector<RID> check;
  pm_key_info->index_->ScanKey(tp_pm_key, &check, txn);
  if (!check.empty()) {
    pm_key_rd = check[0];
    BUSTUB_ENSURE(pm_key_rd.GetPageId() != INVALID_PAGE_ID, "You should check the reason pm_key is INVALID!\n");
  }
  return !check.empty();
}

auto PmKeyInsertTuple(Transaction *txn, TransactionManager *txn_mgr, const Tuple &new_tuple,
                      std::shared_ptr<IndexInfo> &pm_key_index, std::shared_ptr<TableInfo> &table_info) -> void {
  bool insert_into_delete = false;
  std::optional<RID> insert_rid = {};
  // 检查这个元组的键在主键索引是否存在
  RID pm_key_rd = {};
  // RID存在
  if (KeyExsitInPmKey(pm_key_index, new_tuple, pm_key_rd, table_info, txn)) {
    auto [old_meta, old_tuple] = table_info->table_->GetTuple(pm_key_rd);
    bool is_confilct = true;
    // 1 发现被删除了
    if (old_meta.is_deleted_) {
      // 1.1 别的已经提交事务删的 需要更新日志
      // 1.2 当前事务删除后提交的 无需更新日志
      // 1.3 其余情况就是写冲突
      if (old_meta.ts_ <= txn->GetTransactionTempTs()) {
        is_confilct = false;
        insert_into_delete = true;
        insert_rid = pm_key_rd;
        auto link = txn_mgr->GetUndoLink(pm_key_rd);
        auto log = link == std::nullopt ? std::nullopt : txn_mgr->GetUndoLogOptional(link.value());
        if (old_meta.ts_ != txn->GetTransactionTempTs() && log.has_value()) {
          auto new_undolog = GenerateNewUndoLog(&table_info->schema_, nullptr, &new_tuple, old_meta.ts_, UndoLink{});
          new_undolog.prev_version_ = link.value();
          auto new_undolink = txn->AppendUndoLog(new_undolog);
          txn_mgr->UpdateUndoLink(pm_key_rd, new_undolink);
        }
      }
    }
    // 2 发生写写冲突或主键索引不唯一 不继续进行
    if (is_confilct) {
      txn->SetTainted();
      throw ExecutionException("Primary key unique violation detected in InsertTuple!");
    }
  }

  TupleMeta meta = {txn->GetTransactionTempTs(), false};
  // 插入删除的元组需要原地更新
  if (insert_into_delete) {
    // table_info_->table_->UpdateTupleInPlace(meta, tp, insert_rid.value());
    auto status = table_info->table_->UpdateTupleInPlace(
        meta, new_tuple, insert_rid.value(), [txn](const TupleMeta &meta, const Tuple &table, RID rid) -> bool {
          return meta.ts_ <= txn->GetReadTs() || meta.ts_ == txn->GetTransactionTempTs();
        });
    if (!status) {
      txn->SetTainted();
      throw ExecutionException("Write-write confict detected in InsertTuple!");
    }
  } else {
    insert_rid = table_info->table_->InsertTuple(meta, new_tuple);
  }

  txn->AppendWriteSet(table_info->oid_, insert_rid.value());
  // 更新主键索引
  auto pm_key =
      new_tuple.KeyFromTuple(table_info->schema_, pm_key_index->key_schema_, pm_key_index->index_->GetKeyAttrs());
  // printf("pm_key = %s\n",pm_key.ToString(&indexs_info[0]->key_schema_).c_str());
  // 更新索引的时候是在事务锁保护下进行的 如果发现此时被其余事务占用了 则失败
  if (!insert_into_delete && !pm_key_index->index_->InsertEntry(pm_key, insert_rid.value(), txn)) {
    txn->SetTainted();
    throw ExecutionException("Write-write confilct detected in InsertTuple!");
  }
}

auto PmKeyDeleteTuple(Transaction *txn, TransactionManager *txn_mgr, const Tuple &old_tuple, RID rd,
                      std::shared_ptr<TableInfo> &table_info) -> void {
  auto old_meta = table_info->table_->GetTupleMeta(rd);
  // 1.检查是否有写入冲突
  // bool lk_state = txn_mgr->LockTuple(child_rd);
  if (old_meta.ts_ > txn->GetReadTs() && txn->GetTransactionId() != old_meta.ts_) {
    txn->SetTainted();
    throw ExecutionException("Write-write conflict detected in PmKeyDelete.");
  }

  // 2.预定删除信息和版本链
  auto old_undolink_opt = txn_mgr->GetUndoLink(rd);
  TupleMeta new_meta = {txn->GetTransactionTempTs(), true};
  txn->AppendWriteSet(table_info->oid_, rd);

  bool update_state = false;
  auto base_ts = old_meta.ts_;
  // 如果是被当前的事务删除
  if (base_ts == txn->GetTransactionTempTs()) {
    // 如果原本有undolog 那么就更新 如果是当前事务插入然后现在删除的 因此没有undolog就无需生成
    if (old_undolink_opt.has_value() && old_undolink_opt->IsValid()) {
      auto old_undolog = txn->GetUndoLog(old_undolink_opt->prev_log_idx_);
      auto undolog = GenerateUpdatedUndoLog(&table_info->schema_, &old_tuple, nullptr, old_undolog);
      txn->ModifyUndoLog(old_undolink_opt->prev_log_idx_, undolog);
    }
    // 原子操作 test and set
    update_state = table_info->table_->UpdateTupleInPlace(
        new_meta, old_tuple, rd, [txn, base_ts](const TupleMeta &o_meta, const Tuple &o_tuple, RID rid) -> bool {
          if (base_ts != o_meta.ts_) {
            return false;
          }
          return o_meta.ts_ <= txn->GetReadTs() || o_meta.ts_ == txn->GetTransactionTempTs();
        });
  } else {
    auto new_undolog = GenerateNewUndoLog(&table_info->schema_, &old_tuple, nullptr, old_meta.ts_, UndoLink{});
    if (old_undolink_opt.has_value()) {
      new_undolog.prev_version_ = old_undolink_opt.value();
    }
    auto new_undolink = txn->AppendUndoLog(new_undolog);
    // 更新undolink
    update_state = UpdateTupleAndUndoLink(
        txn_mgr, rd, new_undolink, table_info->table_.get(), txn, new_meta, old_tuple,
        [base_ts, txn](const TupleMeta &o_meta, const Tuple &o_tp, RID rid, std::optional<UndoLink> undolink) -> bool {
          if (base_ts != o_meta.ts_) {
            return false;
          }
          return o_meta.ts_ <= txn->GetReadTs() || o_meta.ts_ == txn->GetTransactionTempTs();
        });
  }

  if (!update_state) {
    txn->SetTainted();
    throw ExecutionException("Write-write confilct detected in DeleteTuple!");
  }
}

}  // namespace bustub
