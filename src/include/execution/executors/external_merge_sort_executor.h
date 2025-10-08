//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 *
 * Sort Page Format:
 *    12
 * ----------
 * | HEADER |
 * ----------
 * ----------------------------------------------
 * |  Tuple(1)  |  Tuple(2)  | ... |  Tuple(n)  |
 * ----------------------------------------------
 *
 * HEADER Format:
 * ----------------------------------
 * | size_ | maxsize_ | tuple_size_ |
 * ----------------------------------
 *
 * Tuple Format (after serialization):
 *    4    schema.GetInlinedStorageSize()
 * --------------------------------------
 * | size |            data             |
 * --------------------------------------
 *
 */
// !!!考虑到后续希望设计变长的数据 那么需要在每个数据块前面加入size元数据 
// 当前只需要实现一个定长的即可 SortPage的内存由bpm分配
// 主要流程如下：
// Page* page = buffer_pool_manager_->NewPage();
// WritePageGuard guard(page);
// SortPage* sort_page = guard.AsMut<SortPage>();
// sort_page->Init(row_num, row_size); // 初始化元数据和 data_
#define SORTPAGE_HEADER_SIZE 12
#define META_SIZE 4
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  // 底层资源需要删除拷贝构造和复制等操作 从而保证内存安全
  // 只能通过引用、指针或者移动访问获取
  SortPage() = delete;

  SortPage(const SortPage& other) = delete;

  ~SortPage() = delete;

  // 初始化
  void Init(uint32_t row_num,uint32_t row_size){
    size_ = 0;
    tuple_size_ = row_num * row_size + META_SIZE;
    max_size_ = (BUSTUB_PAGE_SIZE - SORTPAGE_HEADER_SIZE) / tuple_size_;
  };
  
  // 需要反序列化 返回其自带的反序列化方式
  auto GetTupleAt(uint32_t pos_idx)const->Tuple{
    BUSTUB_ENSURE(pos_idx < size_, "Your idx out of range!\n");
    uint32_t offset = pos_idx * tuple_size_;
    Tuple tuple{};
    tuple.DeserializeFrom(&data_[offset]);
    return tuple;
  }
  
  // 需要序列化 使用其自带的序列化方式
  auto InsertTuple(const Tuple &tuple)->void{
    uint32_t offset = (size_++) * tuple_size_;
    BUSTUB_ENSURE(offset + tuple_size_ <= BUSTUB_PAGE_SIZE, "Page size is not enough!\n");
    tuple.SerializeTo(&data_[offset]);
  }
  
  // 返回当前元组数量
  auto GetSize()const->uint32_t{ return size_; }

  // 返回最大元组数量
  auto GetMaxSize()const->uint32_t{ return max_size_; }
  
  // 判空
  auto IsEmpty()const->bool{ return size_ == 0; }
  
  // 当前如果满了就需要创建新的页
  auto IsFull()const->bool{ return size_ == max_size_; }

 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  // 当前页面存储的所有元组数量
  uint32_t size_;
  // 当前页面最多能存储的元组大小
  uint32_t max_size_;
  // 一个元组的大小
  uint32_t tuple_size_;
  // 存储实际数据的地方 
  char data_[];
  // 用vector必须单独处理内存
  // std::vector<char> data_;
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;

  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}
  
  MergeSortRun(MergeSortRun &&that):pages_(std::move(that.pages_)),bpm_(that.bpm_){
    that.bpm_ = nullptr;
  }

  MergeSortRun(const MergeSortRun &that) = delete;

  MergeSortRun &operator=(MergeSortRun &&that) noexcept {
    if (this != &that) {
      pages_ = std::move(that.pages_);
      bpm_ = that.bpm_;
      that.bpm_ = nullptr;
    }
    return *this;
  }

  MergeSortRun &operator=(const MergeSortRun &) = delete;

  auto GetPageCount() -> size_t { return pages_.size(); }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    // 使用noexcept保证异常安全 要么全成功要么全失败
    Iterator& operator=(Iterator &&that)noexcept{
      if(&that != this){
        cur_page_ = std::move(that.cur_page_);
        page_idx_ = that.page_idx_;
        is_valid_ = that.is_valid_;
        that.is_valid_ = false;
        run_ = std::move(that.run_);
        that.run_ = nullptr;
        tuple_idx_ = that.tuple_idx_;
      }
      return *this;
    }

    Iterator(Iterator&& that)noexcept{
      cur_page_ = std::move(that.cur_page_);
      page_idx_ = that.page_idx_;
      is_valid_ = that.is_valid_;
      that.is_valid_ = false;
      run_ = std::move(that.run_);
      that.run_ = nullptr;
      tuple_idx_ = that.tuple_idx_;
    }

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    auto operator++() -> Iterator & {
      if(!is_valid_){
        return *this;
      }
      // cosnt对象只能访问const成员函数
      // 保证不能修改对象的成员
      const SortPage* sort_page = cur_page_.As<SortPage>();
      tuple_idx_ += 1;
      // 超过最大页面位置
      if(tuple_idx_ == sort_page->GetMaxSize()){
        page_idx_ += 1;
        if(page_idx_ < run_->pages_.size()){
          tuple_idx_ = 0;
          cur_page_ = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
        }
        else{
          is_valid_ = false;
          page_idx_ = -1;
          tuple_idx_ = -1;
          cur_page_.Drop();
        }
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    auto operator*() -> Tuple {
      BUSTUB_ENSURE(is_valid_, "Iterator out of range!\n");
      const SortPage* sort_page = cur_page_.As<SortPage>();
      return sort_page->GetTupleAt(tuple_idx_);
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    auto operator==(const Iterator &other) const -> bool { 
      return (other.is_valid_ == is_valid_ && other.page_idx_ == page_idx_ 
        && other.tuple_idx_ == tuple_idx_ && other.run_ == run_);
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool { 
      return (other.is_valid_ != is_valid_ || other.page_idx_ != page_idx_ 
        || other.tuple_idx_ != tuple_idx_ || other.run_ != run_);
    }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run),page_idx_(0),tuple_idx_(0),is_valid_(true){
      cur_page_ = run_->bpm_->ReadPage(run_->pages_[0]);
    }

    Iterator(const MergeSortRun *run,bool is_valid):run_(run),page_idx_(-1),tuple_idx_(-1),is_valid_(is_valid){}

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_;

    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    // 当前访问的page
    ReadPageGuard cur_page_;
    // 当前的SortPage对应的下标位置
    uint32_t page_idx_;
    // 当前访问的SortPage中的元组位置
    uint32_t tuple_idx_;

    bool is_valid_;
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() -> Iterator { return Iterator(this);}

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() -> Iterator { return Iterator(this,false); }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the external merge sort */
  void Init() override;

  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  /** TODO: You will want to add your own private members here. */
};

}  // namespace bustub
