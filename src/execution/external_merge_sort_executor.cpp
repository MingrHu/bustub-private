//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <sstream>
#include <vector>
#include "catalog/schema.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "fmt/core.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {

  child_executor_->Init();
  debug_ss_.clear();
  debug_ss_<<"----------------BASE INFO--------------\n";
  runs_idx_ = 0;
  runs_.clear();
  CreateInitRuns();
  TwoWaysMerge();
  if (!runs_.empty()) {
    iter_ = runs_[0].Begin();
  }
  // FOR TEST
  debug_ss_<<fmt::format("buffer_pool_size = {}\n",exec_ctx_->GetBufferPoolManager()->Size());
  size_t pages = 0;
  for(const auto&run:runs_){
    pages += run.GetPageCount();
  }
  debug_ss_<<fmt::format("Actual all pages num = {}\n", pages);
  LOG_DEBUG("%s",debug_ss_.str().c_str());
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (runs_idx_ < runs_.size()) {
    while (iter_ != runs_[runs_idx_].End()) {
      *tuple = *iter_;
      *rid = {};
      ++iter_;
      return true;
    }
    runs_idx_ += 1;
    if (runs_idx_ < runs_.size()) {
      iter_ = runs_[runs_idx_].Begin();
    }
  }
  return false;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::CreateInitRuns() {
  // 单个runs在初始化阶段可容纳的SortPage个数
  static const int max_pages = 4;
  Tuple tuple{};
  RID rid{};
  while (true) {
    std::vector<page_id_t> sort_pages;
    std::vector<SortEntry> entries;
    uint32_t tuple_idx = 0;

    page_id_t new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
    WritePageGuard write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
    auto sort_page = write_guard.AsMut<SortPage>();
    auto size = child_executor_->GetOutputSchema().GetInlinedStorageSize();
    sort_page->Init(size);
    sort_pages.push_back(new_pgid);

    // 一个run包含的元组数量为sort_page->GetMaxSize() * max_pages
    // 从子执行器里面获取最多一个run的元组 多的就下一轮
    for (; tuple_idx < sort_page->GetMaxSize() * max_pages; tuple_idx++) {
      if (child_executor_->Next(&tuple, &rid)) {
        entries.emplace_back(GenerateSortKey(tuple, plan_->GetOrderBy(), GetOutputSchema()), tuple);
      } else {
        break;
      }
    }

    // 当前没有元组了 需要清空刚分配的页面
    if (tuple_idx == 0) {
      write_guard.Drop();
      BUSTUB_ENSURE(exec_ctx_->GetBufferPoolManager()->DeletePage(new_pgid), "Clean empty sort_page failed!\n");
      break;
    }

    // 执行排序操作 对所有的元组先排序再按顺序放入run里面
    // run里面的SortPage都是排好序的
    std::sort(entries.begin(), entries.end(), cmp_);

    // for(const auto& p:entries){
    //   std::string str = p.second.ToString(&child_executor_->GetOutputSchema());
    //   std::cout<< str<<std::endl;
    // }

    // 把run里面排序好的元组写入磁盘 最终将这些磁盘页号记录到runs里面
    for (uint32_t idx = 0, cnt = 0; idx < entries.size(); idx++, cnt++) {
      // 当一个页面满了的时候切换下一个页面
      if (cnt == sort_page->GetMaxSize()) {
        page_id_t pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
        sort_pages.push_back(pgid);
        write_guard = exec_ctx_->GetBufferPoolManager()->WritePage(pgid);
        sort_page = write_guard.AsMut<SortPage>();
        sort_page->Init(size);
        cnt = 0;
      }
      // 顺序插入元组
      sort_page->InsertTuple(entries[idx].second);
    }
    runs_.emplace_back(sort_pages, exec_ctx_->GetBufferPoolManager());
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::TwoWaysMerge() {
  // multi ways merge test
  #define KWAYS 0
  #if KWAYS
    KWayMerge();
  #else
  // for testing delete op
  // int delete_cnt = 0;
  while (runs_.size() > 1) {
    std::vector<MergeSortRun> new_runs;
    // runs存储的是所有的vector<page_id>
    uint32_t size = runs_.size();
    for (uint32_t idx = 0; idx < size; idx += 2) {
      if (idx == size - 1) {
        new_runs.emplace_back(std::move(runs_[idx]));
        break;
      }

      // 先拿到需要比较的两个runs
      // 后续的runs会越来越大 但始终只用三个页面
      // 存储页面store 比较页面pre和next
      auto iter_pre = runs_[idx].Begin();
      auto iter_next = runs_[idx + 1].Begin();

      std::vector<page_id_t> pages;
      // 定义新的存储页面
      auto new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
      pages.emplace_back(new_pgid);
      auto new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
      auto sort_page_store = new_page_guard.AsMut<SortPage>();
      sort_page_store->Init(child_executor_->GetOutputSchema().GetInlinedStorageSize());
      uint32_t res_nums = 0;

      while (iter_pre != runs_[idx].End() && iter_next != runs_[idx + 1].End()) {
        if (res_nums == sort_page_store->GetMaxSize()) {
          new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
          pages.emplace_back(new_pgid);
          new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
          sort_page_store = new_page_guard.AsMut<SortPage>();
          sort_page_store->Init(child_executor_->GetOutputSchema().GetInlinedStorageSize());
          res_nums = 0;
        }
        SortEntry entry_pre{GenerateSortKey(*iter_pre, plan_->GetOrderBy(), GetOutputSchema()), *iter_pre};
        SortEntry entry_next{GenerateSortKey(*iter_next, plan_->GetOrderBy(), GetOutputSchema()), *iter_next};

        // 左小于右 为true
        if (cmp_(entry_pre, entry_next)) {
          sort_page_store->InsertTuple(*iter_pre);
          ++iter_pre;
        } else {
          sort_page_store->InsertTuple(*iter_next);
          ++iter_next;
        }
        res_nums += 1;
      }
      // 处理pre和next可能的剩余
      SolveRemain(iter_pre, runs_[idx].End(), sort_page_store, res_nums, pages, new_page_guard);
      SolveRemain(iter_next, runs_[idx + 1].End(), sort_page_store, res_nums, pages, new_page_guard);

      for (const auto &id : runs_[idx].GetPages()) {
        bool is_delete = exec_ctx_->GetBufferPoolManager()->DeletePage(id);
        BUSTUB_ENSURE(is_delete, "Please check why page not deleted?\n")
        // delete_cnt += 1;
      }
      for (const auto &id : runs_[idx + 1].GetPages()) {
        bool is_delete = exec_ctx_->GetBufferPoolManager()->DeletePage(id);
        BUSTUB_ENSURE(is_delete, "Please check why page not deleted?\n")
        // delete_cnt += 1;
      }
      new_runs.emplace_back(pages, exec_ctx_->GetBufferPoolManager());
    }
    runs_ = std::move(new_runs);
  }
  // for testing delete op
  // std::cout<<"delete_cnt = "<<delete_cnt<<std::endl;
  #endif
}

template <size_t K>
void ExternalMergeSortExecutor<K>::SolveRemain(MergeSortRun::Iterator &iter, const MergeSortRun::Iterator &iter_end,
                                               SortPage *&sort_page_store, uint32_t cur_idx,
                                               std::vector<page_id_t> &pages, WritePageGuard &new_page_guard) {
  // 处理剩下的
  while (iter != iter_end) {
    if (cur_idx == sort_page_store->GetMaxSize()) {
      auto new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
      pages.emplace_back(new_pgid);
      new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
      sort_page_store = new_page_guard.AsMut<SortPage>();
      sort_page_store->Init(child_executor_->GetOutputSchema().GetInlinedStorageSize());
      cur_idx = 0;
    }
    sort_page_store->InsertTuple(*iter);
    ++iter;
    cur_idx += 1;
  }
}

// K路归并排序
template <size_t K>
void ExternalMergeSortExecutor<K>::KWayMerge() {
  size_t debug_k = K * 4;
  while (runs_.size() > 1) {
    std::vector<MergeSortRun> new_runs;
    uint32_t size = runs_.size();
    for (uint32_t idx = 0; idx < size; idx += debug_k) {
      // collect up to K runs [idx, idx+K)
      uint32_t end_idx = std::min<uint32_t>(idx + debug_k, size);
      uint32_t group_cnt = end_idx - idx;
      if (group_cnt == 1) {  // 单个剩余 run 直接搬过
        new_runs.emplace_back(std::move(runs_[idx]));
        continue;
      }

      struct HeapNode {
        SortEntry entry_;
        uint32_t run_idx_; 
      };

      auto cmp_heap = [&](const HeapNode &a, const HeapNode &b) {
        return cmp_(b.entry_, a.entry_); 
      };
      std::priority_queue<HeapNode, std::vector<HeapNode>, decltype(cmp_heap)> heap(cmp_heap);

      // 一些必要的变量
      std::vector<page_id_t> pages;
      page_id_t new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
      pages.push_back(new_pgid);
      auto new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
      auto sort_page_store = new_page_guard.AsMut<SortPage>();
      sort_page_store->Init(child_executor_->GetOutputSchema().GetInlinedStorageSize());
      uint32_t cur_nums = 0;

      // 每个对应run的迭代器
      std::vector<MergeSortRun::Iterator> iters(group_cnt);
      std::vector<MergeSortRun::Iterator> ends(group_cnt);
      for (uint32_t r = 0; r < group_cnt; ++r) {
        iters[r] = runs_[idx + r].Begin();
        ends[r] = runs_[idx + r].End();
        if (iters[r] != ends[r]) {
          SortEntry e{GenerateSortKey(*iters[r], plan_->GetOrderBy(), GetOutputSchema()), *iters[r]};
          heap.push(HeapNode{e, r});
          ++iters[r];
        }
      }

      while (!heap.empty()) {
        // 达到一个页最大的容量
        if (cur_nums == sort_page_store->GetMaxSize()) {
          new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
          pages.push_back(new_pgid);
          new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
          sort_page_store = new_page_guard.AsMut<SortPage>();
          sort_page_store->Init(child_executor_->GetOutputSchema().GetInlinedStorageSize());
          cur_nums = 0;
        }

        HeapNode top = heap.top();
        heap.pop();
        sort_page_store->InsertTuple(top.entry_.second); // 写出最小元素
        cur_nums += 1;

        uint32_t src = top.run_idx_;
        // 如果该 run 还有剩余元素，把下一个元素推入堆
        if (iters[src] != ends[src]) {
          SortEntry e{GenerateSortKey(*iters[src], plan_->GetOrderBy(), GetOutputSchema()), *iters[src]};
          heap.push(HeapNode{e, src});
          ++iters[src];
        }
      }

      // 删除被合并的旧 pages
      for (uint32_t r = 0; r < group_cnt; ++r) {
        for (const auto &pid : runs_[idx + r].GetPages()) {
          bool is_delete = exec_ctx_->GetBufferPoolManager()->DeletePage(pid);
          BUSTUB_ENSURE(is_delete, "Please check why page not deleted?\n");
        }
      }

      new_runs.emplace_back(pages, exec_ctx_->GetBufferPoolManager());
    } // end for idx
    runs_ = std::move(new_runs);
  } // end while
}


template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
