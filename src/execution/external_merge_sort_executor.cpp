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
#include <cstdint>
#include <iostream>
#include <optional>
#include <vector>
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),cmp_(plan->GetOrderBy()),
    child_executor_(std::move(child_executor)){}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  runs_idx_ = 0;
  runs_.clear();
  CreateInitRuns();
  TwoWaysMerge();
  if(!runs_.empty()){
    iter_ = runs_[runs_idx_].Begin();
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  
  if(runs_idx_ < runs_.size()){
    while(iter_ != runs_[runs_idx_].End()){
      *tuple = *iter_;
      *rid = {};
      ++iter_;
      return true;
    }
    runs_idx_ += 1;
    if(runs_idx_ < runs_.size()){
      iter_ = runs_[runs_idx_].Begin();
    }
  }
  return false;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::CreateInitRuns(){
  // 单个runs在初始化阶段可容纳的SortPage个数
  static const int max_pages = 4;
  Tuple tuple{};
  RID rid{};
  while(true){
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
    for( ;tuple_idx < sort_page->GetMaxSize() * max_pages;tuple_idx++){
      if(child_executor_->Next(&tuple, &rid)){
        entries.emplace_back(GenerateSortKey(tuple, plan_->GetOrderBy(), 
        child_executor_->GetOutputSchema()),tuple);
      }
      else{
        break;
      }
    }

    // 当前没有元组了 需要清空刚分配的页面
    if(tuple_idx == 0){
      write_guard.Drop();
      BUSTUB_ENSURE(exec_ctx_->GetBufferPoolManager()->DeletePage(new_pgid), "Clean empty sort_page failed!\n");
      break;
    }

    // 执行排序操作 对所有的元组先排序再按顺序放入run里面
    // run里面的SortPage都是排好序的
    std::sort(entries.begin(),entries.end(),cmp_);
    for(uint32_t idx = 0,cnt = 0;idx < entries.size();idx++,cnt++){
      // 当一个页面满了的时候切换下一个页面
      if(cnt == sort_page->GetMaxSize()){
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
    runs_.emplace_back(sort_pages,exec_ctx_->GetBufferPoolManager());
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::TwoWaysMerge(){
  while(runs_.size() > 1){
    std::vector<MergeSortRun> new_runs;
    // runs存储的是所有的vector<page_id>
    uint32_t size = runs_.size();
    for(uint32_t idx = 0;idx < size;idx += 2){
      if(idx == size - 1){
        new_runs.emplace_back(new_runs[idx]);
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

      while(iter_pre!=runs_[idx].End() && iter_next!=runs_[idx+1].End()){
        if(res_nums == sort_page_store->GetMaxSize()){
          new_pgid = exec_ctx_->GetBufferPoolManager()->NewPage();
          pages.emplace_back(new_pgid);
          new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(new_pgid);
          sort_page_store = new_page_guard.AsMut<SortPage>();
          sort_page_store->Init(child_executor_->GetOutputSchema().GetInlinedStorageSize());
          res_nums = 0;          
        }
        SortEntry entry_pre{GenerateSortKey(*iter_pre, plan_->GetOrderBy(), GetOutputSchema()),*iter_pre};
        SortEntry entry_next{GenerateSortKey(*iter_next, plan_->GetOrderBy(), GetOutputSchema()),*iter_next};

        // 左小于右 为true
        if(cmp_(entry_pre,entry_next)){
          sort_page_store->InsertTuple(*iter_pre);
          ++iter_pre;
        }
        else{
          sort_page_store->InsertTuple(*iter_next);
          ++iter_next;
        }
        res_nums += 1;
      }
      // 处理pre和next可能的剩余
      SolveRemain(iter_pre,runs_[idx].End(),sort_page_store,res_nums,pages,new_page_guard);
      SolveRemain(iter_next,runs_[idx + 1].End(),sort_page_store,res_nums,pages,new_page_guard);

      new_page_guard.Drop();
      for(const auto& id:runs_[idx].GetPages()){
        exec_ctx_->GetBufferPoolManager()->DeletePage(id);
      }
      for(const auto& id:runs_[idx + 1].GetPages()){
        exec_ctx_->GetBufferPoolManager()->DeletePage(id);
      }
      new_runs.emplace_back(pages,exec_ctx_->GetBufferPoolManager());
    }
    runs_ = std::move(new_runs);
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::SolveRemain(MergeSortRun::Iterator& iter,const MergeSortRun::Iterator &iter_end,
  SortPage* &sort_page_store,uint32_t cur_idx,std::vector<page_id_t>& pages,WritePageGuard& new_page_guard){
  // 处理剩下的
  while(iter != iter_end){
    if(cur_idx == sort_page_store->GetMaxSize()){
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

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
