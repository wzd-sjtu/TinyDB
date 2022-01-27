//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capa_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// find the least used frame_id
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  // empty frame page's id is in here
  if (lru_map_.empty()) {
    latch_.unlock();
    return false;
  }

  // min_use page which is not my thing
  frame_id_t lru_least_id = lru_list_.back();
  lru_map_.erase(lru_least_id);
  lru_list_.pop_back();
  *frame_id = lru_least_id;
  latch_.unlock();  // bottom lock is here

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // show that the frame is pinned by a process
  latch_.lock();

  // update the newest thing here
  // which is always LRU algorithm
  if (lru_map_.count(frame_id) != 0) {
    lru_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }

  latch_.unlock();
  return;
}

// debug loser! damn it!
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // a page is unpinned by process
  latch_.lock();
  // whether it is in the replacer?
  // page in free list is always free
  if (lru_map_.count(frame_id) != 0) {
    latch_.unlock();
    return;
  }

  while (Size() >= capa_) {
    // del the oldest thing here
    /*
    frame_id_t del_frame_id = lru_list.back();
    lru_list.pop_back();
    lru_map.erase(del_frame_id);
    */
    
    frame_id_t del_frame_id = lru_list_.front();
    lru_list_.pop_front();
    lru_map_.erase(del_frame_id);
    
  }

  // del the min thing? yes
  lru_list_.push_front(frame_id);
  // iterator
  lru_map_[frame_id] = lru_list_.begin();
  latch_.unlock();
  return;
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
