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
#include <optional>
#include <sstream>
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
  if((base_meta.is_deleted_ && undo_logs.empty()) || 
    (!undo_logs.empty() && undo_logs.back().is_deleted_)){
    return std::nullopt;
  }
  // 2.如果没被删除 但是没有日志
  if(undo_logs.empty()){
    return base_tuple;
  }

  // 先获取最新的完整元组所有列值
  std::vector<Value> base_val;
  base_val.reserve(schema->GetColumnCount());
  for(uint32_t idx = 0;idx < schema->GetColumnCount();idx++){
    base_val.emplace_back(base_tuple.GetValue(schema, idx));
  }

  // 注意：undo_logs的顺序是最新的放最前面
  // 这里只需要构建base_val作为结果返回
  for(uint32_t idx = 0;idx < undo_logs.size();idx++){
    auto log = undo_logs[idx];
    // 有删除的直接跳过即可
    if(log.is_deleted_){
      continue;
    }
    uint32_t ptr = 0;
    // 这里提取修改值时用的scheam必须新创建 需要获取指定的新列    
    auto log_schema = GetUndoLogSchema(schema,log);
    for(uint32_t col = 0;col < log.modified_fields_.size();col++){
      if(log.modified_fields_[col]){
        base_val[col] = log.tuple_.GetValue(&log_schema, ptr++);
      }
    }
  }

  return std::optional<Tuple> {Tuple{base_val,schema}};
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
  if(base_meta.ts_ <= read_ts || txn->GetTransactionTempTs() == base_meta.ts_){
    return (base_meta.is_deleted_ ? std::nullopt : 
      std::make_optional(std::vector<UndoLog>{}));
  }
  std::vector<UndoLog> undo_logs;
  // 必须找到符合要求的才返回 否则返回空
  if(undo_link.has_value()){
    auto log = txn_mgr->GetUndoLogOptional(undo_link.value());
    while(log.has_value()){
      undo_logs.emplace_back(log.value());
      if(log->ts_ <= read_ts){
        return undo_logs;
      }
      log = txn_mgr->GetUndoLogOptional(log->prev_version_);
    }
  }
  return std::nullopt;
}

// 根据输入的相关信息获取表堆元组的事务指定访问历史版本
// 如果最终的返回的元组被删除 则返回std::nullopt
auto AcquireSpecTuple(const Schema& schema,const RID& rid,TableHeap* table_heap,
  Transaction* txn,TransactionManager* txn_manager)->std::optional<Tuple>{
  // std::tuple<TupleMeta, Tuple, std::optional<UndoLink>> 
  auto [tp_meta,tp,UndoLink] =GetTupleAndUndoLink(txn_manager, table_heap, rid);
  // 元组重建和日志收集已经默认帮我们解决了std::nullopt的问题
  auto undo_logs_opt = CollectUndoLogs(rid, tp_meta, tp, UndoLink, txn, txn_manager);
  if(!undo_logs_opt.has_value()){
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
  UNIMPLEMENTED("not implemented");
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
  UNIMPLEMENTED("not implemented");
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
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    ss << "--------------Cur tuple infomation-----------\n";
    auto rid = iter.GetRID();
    auto [metadata, tuple] = iter.GetTuple();
    ss << fmt::format("RID={}/{}", rid.GetPageId(), rid.GetSlotNum());
    // 当前元组属于未提交的元组
    if (metadata.ts_ & TXN_START_ID) {
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

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log) -> Schema{
  std::vector<Column> schema_val;
  for(uint32_t i = 0;i < log.modified_fields_.size();i++){
    if(log.modified_fields_[i]){
      schema_val.emplace_back(schema->GetColumn(i));
    }
  }
  return Schema{std::move(schema_val)};
}

}  // namespace bustub
