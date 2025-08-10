//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <atomic>
#include <future>
#include <memory>
#include <utility>
#include "storage/disk/disk_scheduler.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : PageGuard(page_id, std::move(frame), 
    std::move(replacer), std::move(bpm_latch), std::move(disk_scheduler))
{
  frame_->rwlatch_.lock_shared();
  // 获取读操作的共享锁
  is_readGuard_ = true;
  frame_->pin_count_.fetch_add(1,std::memory_order_relaxed);
  replacer_->RecordAccess(frame_->frame_id_);
  replacer_->SetEvictable(frame_->frame_id_, false);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept 
{
  // 内置类型是没有移动的 等价于拷贝
  if(that.is_valid_){
    this->page_id_ = that.page_id_;
    this->bpm_latch_ = std::move(that.bpm_latch_);
    this->disk_scheduler_ = std::move(that.disk_scheduler_);
    this->frame_ = std::move(that.frame_);
    this->replacer_ = std::move(that.replacer_);
    this->is_readGuard_ = true;
    this->is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & 
{ 
  if(this != &that){
    if(that.is_valid_){
      // 智能指针的好处体现出来了 无须手动释放当前资源
      // 在移动构造赋值的时候自动释放或减少了引用计数
      this->page_id_ = that.page_id_;
      this->bpm_latch_ = std::move(that.bpm_latch_);
      this->disk_scheduler_ = std::move(that.disk_scheduler_);
      this->frame_ = std::move(that.frame_);
      this->replacer_ = std::move(that.replacer_);
      this->is_readGuard_ = true;
      this->is_valid_ = that.is_valid_;
      that.is_valid_ = false;
    }
  }
  return *this; 
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto PageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto PageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto PageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_.load();
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void PageGuard::Flush() 
{ 
  bpm_latch_->lock();
  if(is_valid_){
    frame_->is_dirty_ = false;
    // 构造一个临时的拷贝对象 防止把内存中的数据转移了
    DiskRequest r{true,frame_->GetDataMut(),
      page_id_,std::promise<bool>{}};
    this->disk_scheduler_->Schedule(std::move(r));
  }
  bpm_latch_->unlock();
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void PageGuard::Drop() 
{ 
  if(is_valid_){
    if(is_readGuard_){
      frame_->pin_count_.fetch_sub(1,std::memory_order_relaxed);
      if(frame_->pin_count_.load() == 0){
        // 设置该页对应的帧可逐出
        bpm_latch_->lock();
        replacer_->SetEvictable(frame_->frame_id_, true);
        frame_->rwlatch_.unlock_shared();
        // 必须设置为false 防止重复释放
        is_valid_ = false;
        bpm_latch_->unlock();
      }
    }
    else{
      frame_->pin_count_.fetch_sub(1,std::memory_order_relaxed);
      if(frame_->pin_count_.load() == 0){
        bpm_latch_->lock();
        replacer_->SetEvictable(frame_->frame_id_, true);
        frame_->rwlatch_.unlock();
        is_valid_ = false;
        bpm_latch_->unlock();    
      }  
            
    }
  }
}


/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : PageGuard(page_id, std::move(frame), std::move(replacer), 
    std::move(bpm_latch), std::move(disk_scheduler))
{
  frame_->rwlatch_.lock();
  is_readGuard_ = false;
  is_valid_ = true;
  frame_->pin_count_.fetch_add(1,std::memory_order_relaxed);
  replacer_->RecordAccess(frame_->frame_id_);
  replacer_->SetEvictable(frame_->frame_id_, false);
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept 
{
  if(that.is_valid_){
    this->is_valid_ = that.is_valid_;
    this->is_readGuard_ = false;
    that.is_valid_ = false;
    this->page_id_ = that.page_id_;
    this->replacer_ = std::move(that.replacer_);
    this->frame_ = std::move(that.frame_);
    this->bpm_latch_ = std::move(that.bpm_latch_);
    this->disk_scheduler_ = std::move(that.disk_scheduler_);
  }
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & 
{ 
  if(this == &that){
    if(that.is_valid_){
      this->is_valid_ = that.is_valid_;
      this->is_readGuard_ = false;
      that.is_valid_ = false;
      this->page_id_ = that.page_id_;
      this->replacer_ = std::move(that.replacer_);
      this->frame_ = std::move(that.frame_);
      this->bpm_latch_ = std::move(that.bpm_latch_);
      this->disk_scheduler_ = std::move(that.disk_scheduler_);
    }
  }
  return *this; 
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_.store(true);
  return frame_->GetDataMut();
}

}  // namespace bustub
