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
#include <atomic>
#include <cstddef>
#include <future>
#include <optional>
#include <thread>
#include <utility>
#include "common/config.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

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
  pin_count_.store(0);
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
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
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
auto BufferPoolManager::NewPage() -> page_id_t 
{ // 只负责分配新的页id 不绑定帧
  return next_page_id_.fetch_add(1, std::memory_order_relaxed);
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
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { UNIMPLEMENTED("TODO(P1): Add implementation."); }

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
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> 
{
  std::optional<WritePageGuard> res = std::nullopt;
  frame_id_t fid = INVALID_FRAME_ID;
  // 防止被另外的线程给删了
  bpm_latch_->lock();
  bool find_fid = (page_table_.find(page_id) != page_table_.end());
  if(find_fid){
    fid = page_table_[page_id];
  }
  bpm_latch_->unlock();

  // 如果这个页面ID对应的帧存在于内存中
  if(fid != INVALID_FRAME_ID){
    auto cur_frame = frames_[fid];
    WritePageGuard wguard(page_id,cur_frame,replacer_,bpm_latch_,
    disk_scheduler_);
    res = std::move(wguard);
  }
  else{
    bpm_latch_->lock();
    // 尝试获取空闲的新帧
    if(free_frames_.size() > 0){
      fid = free_frames_.front();
      free_frames_.pop_front();
    }
    bpm_latch_->unlock();
    // 如果获取到了空闲帧 pin_count = 0
    if(fid != INVALID_FRAME_ID){
      // 对帧元数据进行操作
      auto newframe = frames_[fid];
      // 记录一下新拿到的帧对应的当前页号
      newframe->pgid_ = page_id;
      // 从磁盘加载当前页的内容
      load_datafromdisk(fid);

      // 帧更新成功后才能新建映射
      bpm_latch_->lock();
      page_table_[page_id] = fid; 
      bpm_latch_->unlock();

      // 创建写保护对象 会自动对帧引用计数 自动记录访问
      WritePageGuard wguard(page_id,newframe,replacer_,bpm_latch_,
    disk_scheduler_);
      res = std::move(wguard);
    }
    // 否则去寻找可驱逐的帧
    else {
      // Evict已经是线程安全的
      auto evid = replacer_->Evict();
      if(evid.has_value()){
        // 对帧元数据进行操作
        fid = evid.value();
        auto curframe = frames_[fid];
        // 开始清理脏页 
        FlushPage(page_id);

        bpm_latch_->lock();
        page_table_.erase(curframe->pgid_);
        bpm_latch_->unlock();

        // 确保别的操作执行完成后才能开始接下来的操作
        // 且由于当前操作独占 因此后续操作是安全的
        WritePageGuard wguard(page_id,curframe,replacer_,bpm_latch_,
          disk_scheduler_);
        
        // 更新帧和页的绑定
        curframe->pgid_ = page_id;
        // 从磁盘加载页的内容
        load_datafromdisk(fid);

        // 内容同步成功后新建映射
        bpm_latch_->lock();
        page_table_[page_id] = fid;
        bpm_latch_->unlock();

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
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> 
{
  std::optional<ReadPageGuard> res = std::nullopt;
  frame_id_t fid = INVALID_FRAME_ID;
  bpm_latch_->lock();
  bool find_fid = (page_table_.find(page_id)!=page_table_.end());
  if(find_fid){
    fid = page_table_[page_id];
  }
  bpm_latch_->unlock();

  if(fid != INVALID_FRAME_ID){
    auto cur_frame = frames_[fid];
    ReadPageGuard rguard{page_id,cur_frame,replacer_,
      bpm_latch_,disk_scheduler_};
    res =std::move(rguard);
  }
  else{
    bpm_latch_->lock();
    if(free_frames_.size() > 0){
      fid = free_frames_.front();
      free_frames_.pop_front();
    }
    bpm_latch_->unlock();
    // 找到合适的空闲帧
    if(fid != INVALID_FRAME_ID){
      auto newframe = frames_[fid];
      newframe->pgid_ = page_id;
      ReadPageGuard rguard{page_id,newframe,replacer_,
        bpm_latch_,disk_scheduler_};

      load_datafromdisk(fid);

      bpm_latch_->lock();
      page_table_[page_id] = fid;
      bpm_latch_->unlock();
      res = std::move(rguard);
    }
    else{
      auto evid = replacer_->Evict();
      if(evid.has_value()){
        fid = evid.value();
        auto curframe = frames_[fid];
        bpm_latch_->lock();
        page_table_.erase(curframe->pgid_);
        bpm_latch_->unlock();

        // 开始清理脏页 
        FlushPage(page_id);

        // 这里加独占锁的原因是有可能别的线程正在访问
        // 当前的这个帧并进行保护页的读操作 但当前线程又不能真的创建一个pageguard
        // 因此需要模仿writepageguard的步骤进行加锁
        curframe->rwlatch_.lock();
        // 更新帧和页的绑定
        curframe->pgid_ = page_id;
        // 设置当前帧
        // 从磁盘加载页的内容
        load_datafromdisk(fid);
        curframe->rwlatch_.unlock();

        // 绑定帧头和页面保护
        ReadPageGuard rguard{page_id,curframe,replacer_,
        bpm_latch_,disk_scheduler_};
        // 内容同步成功后新建映射
        bpm_latch_->lock();
        page_table_[page_id] = fid;
        bpm_latch_->unlock();

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
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
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
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool 
{ 
  bool is_find = (page_table_.find(page_id)!=page_table_.end());
  if(!is_find)
    return false;
  auto curframe = frames_[page_id];
  // 开始清理脏页 
  // CAS操作 防止重复写入
  bool expected = true;
  if(curframe->is_dirty_.compare_exchange_strong(expected,false)){
    std::promise<bool> p = disk_scheduler_->CreatePromise();
    std::future<bool> ft = p.get_future();
    DiskRequest write{true,curframe->GetDataMut(),curframe->pgid_,std::move(p)};
    disk_scheduler_->Schedule(std::move(write));
    while(!ft.get()){
      std::this_thread::yield();
    }
  }
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
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool 
{ 
  bpm_latch_->lock();
  bool is_find = (page_table_.find(page_id)!=page_table_.end());
  if(!is_find)
    return false;
  bpm_latch_->unlock();
  auto curframe = frames_[page_id];
  curframe->rwlatch_.lock();
  // 开始清理脏页 
  // CAS操作 防止重复写入
  bool expected = true;
  if(curframe->is_dirty_.compare_exchange_strong(expected,false)){
    std::promise<bool> p = disk_scheduler_->CreatePromise();
    std::future<bool> ft = p.get_future();
    DiskRequest write{true,curframe->GetDataMut(),curframe->pgid_,std::move(p)};
    disk_scheduler_->Schedule(std::move(write));
    while(!ft.get()){
      std::this_thread::yield();
    }
  }
  curframe->rwlatch_.unlock();
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
void BufferPoolManager::FlushAllPagesUnsafe() { UNIMPLEMENTED("TODO(P1): Add implementation."); }

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
void BufferPoolManager::FlushAllPages() { UNIMPLEMENTED("TODO(P1): Add implementation."); }

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
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> 
{
  std::optional<size_t> res = std::nullopt;
  

  return res;
}

void BufferPoolManager::load_datafromdisk(frame_id_t fid)const{
  auto newframe = frames_[fid];
  // 从磁盘加载当前页的内容
  std::promise<bool> p = disk_scheduler_->CreatePromise();
  std::future<bool> ft = p.get_future();
  DiskRequest read{false,newframe->GetDataMut(),
    newframe->pgid_,std::move(p)};
  disk_scheduler_->Schedule(std::move(read));
  // 循环等待写完
  while(!ft.get()){
    std::this_thread::yield();
  }
}

}  // namespace bustub
