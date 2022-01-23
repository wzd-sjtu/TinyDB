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

LRUReplacer::LRUReplacer(size_t num_pages) { capa = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// find the least used frame_id
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch.lock();
  if (lru_map.empty()) {
    latch.unlock();
    return false;
  }

  frame_id_t lru_least_id = lru_list.back();
  lru_map.erase(lru_least_id);

  lru_list.pop_back();
  *frame_id = lru_least_id;
  latch.unlock();  // bottom lock is here

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  // show that the frame is pinned by a process
  latch.lock();

  // update the newest thing here
  // which is always LRU algorithm
  if (lru_map.count(frame_id) != 0) {
    lru_list.erase(lru_map[frame_id]);
    lru_map.erase(frame_id);
  }

  latch.unlock();
  return;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // a page is unpinned by process
  latch.lock();
  // whether it is in the replacer?
  if (lru_map.count(frame_id) != 0) {
    latch.unlock();
    return;
  }

  while (capa <= Size()) {
    // del the oldest thing here

    frame_id_t del_frame_id = lru_list.back();
    lru_list.pop_back();
    lru_map.erase(del_frame_id);

    /*
    frame_id_t del_frame_id = lru_list.front();
    lru_list.pop_front();
    lru_map.erase(del_frame_id);
    */
  }

  // del the min thing? yes
  lru_list.push_front(frame_id);
  // iterator
  lru_map[frame_id] = lru_list.begin();
  latch.unlock();
  return;
}

size_t LRUReplacer::Size() { return lru_list.size(); }

}  // namespace bustub
