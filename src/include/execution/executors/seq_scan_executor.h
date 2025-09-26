//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
   // 顺序全表扫
   // 类似于yeild操作
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  // 顺序查询的计划节点 包含一些必备的资源比如查询所需要的表oid
  // 本质上也是属于查询计划的一部分
  const SeqScanPlanNode *plan_;
  // 表堆 存储当前表堆的所有信息
  std::shared_ptr<TableInfo> table_info_;
  // 本质上是一个cursor 指向当前的元组 并可以按顺序访问下一个元组 无论是否在一个页上
  std::shared_ptr<TableIterator> table_iter_;

};
}  // namespace bustub
