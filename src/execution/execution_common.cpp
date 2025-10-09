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

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

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
  UNIMPLEMENTED("not implemented");
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
  UNIMPLEMENTED("not implemented");
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
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

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

}  // namespace bustub
