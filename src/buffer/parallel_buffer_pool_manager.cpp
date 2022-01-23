//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

// so many exercises!  damn it!

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances

  /*
  BufferPoolManagerInstance(size_t pool_size, 
  uint32_t num_instances, uint32_t instance_index,
   DiskManager *disk_manager, LogManager *log_manager)

  */
  num_instances_ = num_instances;
  parallel_pool_size_ = num_instances*pool_size;
  each_pool_size_ = pool_size;
  search_num_ = 0;

  // init of all protected things
  for(size_t i=0; i<num_instances; i++) {
    BufferPoolManagerInstance* tmp = new BufferPoolManagerInstance( \
      pool_size, num_instances, i, disk_manager, log_manager);
    bufpoolIns_vector_.push_back(tmp);

    // not a same namespace
    // all of them are pointer here
    std::mutex* related_mutex = new std::mutex();
    latch_vector_.push_back(related_mutex);

  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  // empty all spaces
  for(size_t i=0; i<num_instances_; i++) {
    delete(latch_vector_[i]);
    delete(bufpoolIns_vector_[i]);
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return parallel_pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t target_loc = page_id % num_instances_;

  return bufpoolIns_vector_[target_loc];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* select_bufferpool_ins = GetBufferPoolManager(page_id);
  Page* res = select_bufferpool_ins->FetchPgImp(page_id);

  return res;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  BufferPoolManager* select_bufferpool_ins = GetBufferPoolManager(page_id);
  bool res = select_bufferpool_ins->UnpinPgImp(page_id, is_dirty);
  return res;
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* select_bufferpool_ins = GetBufferPoolManager(page_id);
  bool res = select_bufferpool_ins->FlushPgImp(page_id);
  return res;
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  // this should be derectly right.

  // NULL
  latch_for_robin_.lock();
  size_t search_times = 0;
  Page* res = nullptr;

  for(search_times = 0; search_times < num_instances_; search_times++) {
    BufferPoolManager* select_bufferpool_ins = bufpoolIns_vector_[search_num_];
    res = select_bufferpool_ins->NewPgImp(page_id);

    search_times++;
    search_num_ = (search_num_ + 1)%num_instances_; // next number

    if(res!=nullptr) {
      // find it!
      latch_for_robin_.unlock();
      return res;
    }
  }

  latch_for_robin_.unlock();
  // unable to find it?
  return res;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager* select_bufferpool_ins = GetBufferPoolManager(page_id);

  bool res = select_bufferpool_ins->DeletePgImp(page_id);

  return res;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  // TRY to flush all things
  // one page inner
  // inner add the mutex and latches
  for(size_t i=0; i<num_instances_; i++) {
    bufpoolIns_vector_[i]->FlushAllPgsImp();
  }
}
}  // namespace bustub

// buffer_pool_manager_instance_test