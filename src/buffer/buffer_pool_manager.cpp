//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pgid_ = INVALID_PAGE_ID;
  pin_count_.store(0);
  isloading_ = false, iswriting_ = false;
  is_dirty_.store(false);
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }

  disk_manger_proxy_ = std::make_shared<DiskManagerProxy>(disk_scheduler_);
  // LOG_DEBUG("Start init buffer_pool_manager!\n");
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {  // 只负责分配新的页id 不绑定帧
  page_id_t newpage_id = next_page_id_.fetch_add(1);
  disk_scheduler_->IncreaseDiskSpace(newpage_id);
  return newpage_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // LOG_DEBUG("Call function:DeletePage! pageid = %d\n", page_id);
  // 检查内存中是否存在且被占用
  // 否则是内存中不存在但可能存在于磁盘上的
  std::scoped_lock latch(*bpm_latch_);
  frame_id_t fid = INVALID_FRAME_ID;
  if (page_table_.find(page_id) != page_table_.end()) {
    fid = page_table_[page_id];
    auto curframe = frames_[fid];
    // 有线程占用
    if (curframe->pin_count_.load() != 0) {
      return false;
    }
    if (curframe->pgid_ == page_id) {
      // 清除该页面在缓存中的所有相关信息
      disk_manger_proxy_->Deletepagecache(curframe);
      replacer_->Remove(fid);
      // 重置绑定的帧信息
      curframe->Reset();
      free_frames_.push_back(fid);
    }
    // 内存页表中删除
    page_table_.erase(page_id);
  }
  // 删除磁盘上的物理页
  disk_scheduler_->DeallocatePage(page_id);

  // LOG_DEBUG("Call function:Success DeletePage! pageid = %d\n", page_id);
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::optional<WritePageGuard> res = std::nullopt;
  frame_id_t fid = INVALID_FRAME_ID;
  // 防止被另外的线程给删了
  // 全局缓冲池锁不应该被任何等待析构的行为阻塞
  bpm_latch_->lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    fid = page_table_[page_id];
    auto cur_frame = frames_[fid];
    WritePageGuard wguard(page_id, cur_frame, replacer_, bpm_latch_, disk_scheduler_);
    // 等待当前帧 确保没有进行同步或者读取操作
    // 写页面必须当两者都没进行的时候才能返回给上层
    // 使用条件变量能有效避免忙等待
    std::unique_lock lock(cur_frame->io_latch_);
    while (cur_frame->isloading_ || cur_frame->iswriting_) {
      cur_frame->frame_cv_.wait(lock);
    }
    res = std::move(wguard);
  } else {
    // 尝试获取空闲的新帧
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
    }

    // 如果获取到了空闲帧 pin_count = 0
    // 空闲帧就是没有任何数据以及被映射的帧
    // 这里的空闲帧获取保证不会有别的线程
    // 破坏数据 因此不必加全局锁
    if (fid != INVALID_FRAME_ID) {
      // 对帧元数据进行操作
      auto newframe = frames_[fid];
      // 从磁盘尝试加载当前页的内容
      if (newframe->pgid_ != page_id) {
        // 记录新拿到的pgid
        newframe->pgid_ = page_id;
        Loaddatafromdisk(newframe);
      }
      // 创建写保护对象 会自动对帧引用计数 自动记录访问
      // 映射页表
      page_table_[page_id] = fid;
      // 这里面发生解锁
      WritePageGuard wguard(page_id, newframe, replacer_, bpm_latch_, disk_scheduler_);

      std::unique_lock lock(newframe->io_latch_);
      while (newframe->isloading_ || newframe->iswriting_) {
        newframe->frame_cv_.wait(lock);
      }
      res = std::move(wguard);
    } else {
      // Evict已经是线程安全的 保证被驱逐的不会被线程占用
      auto evid = replacer_->Evict();
      if (!evid.has_value()) {
        // 没找到直接解锁
        bpm_latch_->unlock();
      } else {
        // 对帧元数据进行操作
        fid = evid.value();
        auto curframe = frames_[fid];
        // 清除页表 保证别的线程即使拿到了pgid也无法立马访问帧
        page_table_.erase(curframe->pgid_);

        // 先同步帧上的旧页面数据
        // 非脏不同步
        // 脏页必须使用脏页的pageID
        bool expected = true;
        if (curframe->is_dirty_.compare_exchange_strong(expected, false)) {
          Cleandirtyframe(curframe, curframe->pgid_);
        }
        // 然后再更新帧和页的绑定
        // 从磁盘加载页的内容
        // 如果pgid相同说明本身就有数据 无需加载
        if (curframe->pgid_ != page_id) {
          // 这里再更新pgid就不会影响到清理脏页面的pgid
          curframe->pgid_ = page_id;
          Loaddatafromdisk(curframe);
        }

        // 新建映射
        page_table_[page_id] = fid;
        // 确保别的操作执行完成后才能开始接下来的操作
        // 且由于当前操作有帧锁 因此后续对帧操作是安全的
        WritePageGuard wguard(page_id, curframe, replacer_, bpm_latch_, disk_scheduler_);

        std::unique_lock lock(curframe->io_latch_);
        while (curframe->isloading_ || curframe->iswriting_) {
          curframe->frame_cv_.wait(lock);
        }
        res = std::move(wguard);
      }
    }
  }
  return res;
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::optional<ReadPageGuard> res = std::nullopt;
  frame_id_t fid = INVALID_FRAME_ID;
  bpm_latch_->lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    fid = page_table_[page_id];
    auto cur_frame = frames_[fid];
    ReadPageGuard rguard{page_id, cur_frame, replacer_, bpm_latch_, disk_scheduler_};
    // 读页面由于不能修改数据因此
    // 只需要保证页面数据加载完成即可
    std::unique_lock lock(cur_frame->io_latch_);
    while (cur_frame->isloading_) {
      cur_frame->frame_cv_.wait(lock);
    }
    res = std::move(rguard);
  } else {
    if (!free_frames_.empty()) {
      fid = free_frames_.front();
      free_frames_.pop_front();
    }

    // 找到合适的空闲帧
    if (fid != INVALID_FRAME_ID) {
      auto newframe = frames_[fid];
      if (newframe->pgid_ != page_id) {
        // 记录新拿到的pgid
        newframe->pgid_ = page_id;
        Loaddatafromdisk(newframe);
      }
      page_table_[page_id] = fid;
      ReadPageGuard rguard{page_id, newframe, replacer_, bpm_latch_, disk_scheduler_};
      std::unique_lock lock(newframe->io_latch_);
      while (newframe->isloading_) {
        newframe->frame_cv_.wait(lock);
      }
      res = std::move(rguard);
    } else {
      // 淘汰的策略是只有不被占用的帧才能被淘汰
      auto evid = replacer_->Evict();
      if (!evid.has_value()) {
        bpm_latch_->unlock();
      } else {
        fid = evid.value();
        auto curframe = frames_[fid];
        // 清除页表
        page_table_.erase(curframe->pgid_);

        // 先同步帧上的旧页面数据
        // 非脏不同步
        bool expected = true;
        if (curframe->is_dirty_.compare_exchange_strong(expected, false)) {
          Cleandirtyframe(curframe, curframe->pgid_);
        }
        // 然后再更新帧和页的绑定
        // 从磁盘加载页的内容
        // 如果pgid相同说明本身就有数据 无需加载
        if (curframe->pgid_ != page_id) {
          curframe->pgid_ = page_id;
          Loaddatafromdisk(curframe);
        }
        // 新建页表
        page_table_[page_id] = fid;
        ReadPageGuard rguard{page_id, curframe, replacer_, bpm_latch_, disk_scheduler_};
        std::unique_lock lock(curframe->io_latch_);
        while (curframe->isloading_) {
          curframe->frame_cv_.wait(lock);
        }
        res = std::move(rguard);
      }
    }
  }
  return res;
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  bool is_find = (page_table_.find(page_id) != page_table_.end());
  if (!is_find) {
    return false;
  }
  frame_id_t fid = page_table_[page_id];
  auto curframe = frames_[fid];
  // 不管该页是否dirty 都需要刷新到磁盘上
  std::promise<bool> p = disk_scheduler_->CreatePromise();
  std::future<bool> ft = p.get_future();
  DiskRequest write{true, curframe->GetDataMut(), curframe->pgid_, std::move(p)};
  disk_scheduler_->Schedule(std::move(write));
  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock latch(*bpm_latch_);
  frame_id_t fid = INVALID_FRAME_ID;
  bool is_find = (page_table_.find(page_id) != page_table_.end());
  if (is_find) {
    fid = page_table_[page_id];
  }

  if (fid == INVALID_FRAME_ID) {
    return false;
  }
  auto curframe = frames_[fid];
  // 不管该页是否dirty 都需要刷新到磁盘上
  std::promise<bool> p = disk_scheduler_->CreatePromise();
  std::future<bool> ft = p.get_future();
  DiskRequest write{true, curframe->GetDataMut(), curframe->pgid_, std::move(p)};
  disk_scheduler_->Schedule(std::move(write));
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  for (const auto &p : page_table_) {
    FlushPageUnsafe(p.first);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  for (const auto &p : page_table_) {
    FlushPage(p.first);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::optional<size_t> res = std::nullopt;
  std::scoped_lock latch(*bpm_latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t fid = page_table_[page_id];
    auto curframe = frames_[fid];
    res = curframe->pin_count_.load();
  }
  return res;
}

// 使用这个函数默认这个帧是需要加载的 也就是该帧数据不同步
void BufferPoolManager::Loaddatafromdisk(std::shared_ptr<FrameHeader> &curframe) const {
  // 标记此帧正在从磁盘加载数据
  // 这个标记是不会发生数据冲突的
  curframe->isloading_ = true;
  disk_manger_proxy_->ScheduleProxy(curframe, false, curframe->pgid_, std::vector<char>{});
}

// 使用这个函数默认这个帧是脏的 需要进行磁盘同步
void BufferPoolManager::Cleandirtyframe(std::shared_ptr<FrameHeader> &curframe, page_id_t oldpgid) {
  // 标记此帧正在往磁盘写入数据
  curframe->iswriting_ = true;
  disk_manger_proxy_->ScheduleProxy(curframe, true, oldpgid, curframe->data_);
}

// 单例的定义方式 不需要再加一次static了
std::shared_ptr<ThreadPool> ThreadPool::pool = nullptr;

DiskManagerProxy::DiskManagerProxy(std::shared_ptr<DiskScheduler> &disk_scheduler, size_t init_size)
    : disk_scheduler_(disk_scheduler) {
  page_cache_.reserve(init_size);
  page_cache_valid_.reserve(init_size);
  page_mtx_.reserve(init_size);
  // 初始化线程池
  thread_pool_ = ThreadPool::GetInstance();
}

DiskManagerProxy::ProxyFrame::ProxyFrame(std::shared_ptr<FrameHeader> &frame, bool iswrite, page_id_t oldpgid)
    : iswrite_(iswrite), frame_(frame), oldpgid_(oldpgid) {}

DiskManagerProxy::ProxyFrame::ProxyFrame(ProxyFrame &&that) noexcept {
  iswrite_ = that.iswrite_;
  frame_ = std::move(that.frame_);
  oldpgid_ = that.oldpgid_;
}

auto DiskManagerProxy::ProxyFrame::operator=(ProxyFrame &&that) noexcept -> DiskManagerProxy::ProxyFrame & {
  if (this != &that) {
    iswrite_ = that.iswrite_;
    frame_ = std::move(that.frame_);
    oldpgid_ = that.oldpgid_;
  }
  return *this;
}

void DiskManagerProxy::ScheduleProxy(std::shared_ptr<FrameHeader> &frame, bool iswrite, page_id_t oldpgid,
                                     const std::vector<char> &dirty_data) {
  // 这里是一个对于frame的拷贝
  // 只有pf的oldpgid才是真正的操作pgid
  page_mtx_[oldpgid].lock();
  // 写入磁盘
  if (iswrite) {
    // 加入队列
    page_cache_valid_[oldpgid] = false;
    // 对于写操作考虑先同步缓存
    page_cache_[oldpgid] = dirty_data;
    page_cache_valid_[oldpgid] = true;
    page_mtx_[oldpgid].unlock();

    // 加入线程
    // 值传入一个dirty_data
    thread_pool_->PushtTask([this, iswrite, oldpgid, dirty_data, frame] {
      std::promise<bool> p = disk_scheduler_->CreatePromise();
      std::future<bool> ft = p.get_future();
      {
        std::lock_guard latch(frame->io_latch_);
        frame->iswriting_ = false;
        frame->frame_cv_.notify_all();
      }

      DiskRequest r{iswrite, const_cast<char*>(dirty_data.data()), oldpgid, std::move(p)};
      disk_scheduler_->Schedule(std::move(r));
      ft.get();
    });

  } else {
    // 否则如果是从磁盘进行的加载操作 则直接读取合法缓存即可
    if (page_cache_valid_[oldpgid]) {
      // 从缓存读取
      frame->data_ = page_cache_[oldpgid];
      {
        std::lock_guard latch(frame->io_latch_);
        frame->isloading_ = false;
        frame->frame_cv_.notify_all();
      }
      page_mtx_[oldpgid].unlock();
    } else {
      page_mtx_[oldpgid].unlock();

      thread_pool_->PushtTask([this, iswrite, oldpgid, frame] {
        std::promise<bool> p = disk_scheduler_->CreatePromise();
        std::future<bool> ft = p.get_future();
        DiskRequest r{iswrite, frame->GetDataMut(), oldpgid, std::move(p)};
        disk_scheduler_->Schedule(std::move(r));
        ft.get();
        {
          std::lock_guard latch(frame->io_latch_);
          frame->isloading_ = false;
          frame->frame_cv_.notify_all();
        }
      });
    }
  }
}

void DiskManagerProxy::Deletepagecache(std::shared_ptr<FrameHeader> &frame) {
  std::lock_guard latch(page_mtx_[frame->pgid_]);
  page_cache_.erase(frame->pgid_);
  page_cache_valid_.erase(frame->pgid_);
}

}  // namespace bustub
