/*
 * pf_hashtable.cc
 *
 *  Created on: Apr 7, 2018
 *      Author: zyh
 */

#include <pf_hashtable.h>

namespace toydb {

HashTable::HashTable(int num_buckets) {
  num_buckets_ = num_buckets;
  hash_entry_ = new HashEntry* [num_buckets];
  for (int i=0; i < num_buckets ;i++) hash_entry_[i] = nullptr;
}

HashTable::~HashTable() {
  for (int i = 0; i < num_buckets_; i++) {
    HashEntry *current = hash_entry_[i];
    while (current != nullptr) {
      HashEntry *next = current->next;
      delete current;
      current = next;
    }
  }
  delete[] hash_entry_;
}

void HashTable::Find(int fd,PageNum num, int &slot) {
  int bucket_num = Hash(fd, num);
  HashEntry *entry;
  for (entry = hash_entry_[bucket_num];
       entry != nullptr;
       entry = entry->next) {
    if (entry->fd == fd && entry->page_num == num) {
      slot = entry->slot;
      return;
    }
  }
  slot = -1;
}

Status HashTable::Insert(int fd, PageNum num, int slot) {
  // check entry does not exist in bucket
  int bucket_num = Hash(fd, num);
  for (HashEntry *entry = hash_entry_[bucket_num];
       entry != nullptr;
       entry = entry->next){
    if (entry->fd == fd && entry->page_num == num) {
      return Status(ErrorCode::kPF,"Can not insert.The page is already in list");
    }
  }

  HashEntry *entry = new HashEntry;
  entry->fd = fd;
  entry->page_num = num;
  entry->slot = slot;
  entry->prev = nullptr;
  if (hash_entry_[bucket_num] != nullptr) hash_entry_[bucket_num]->prev = entry;
  entry->next = hash_entry_[bucket_num];
  hash_entry_[bucket_num] = entry;
  return Status::OK();
}

Status HashTable::Delete(int fd,PageNum num) {
  int bucket_num = Hash(fd, num);
  HashEntry *entry;
  for (entry = hash_entry_[bucket_num];
       entry != nullptr;
       entry = entry->next) {
    if (entry->fd == fd && entry->page_num == num) {
      break;
    }
  }
  if (entry == nullptr) {
    return Status(ErrorCode::kPF, "Can not delete.the entry is not in the bucket");
  }
  if (entry == hash_entry_[bucket_num]) hash_entry_[bucket_num] = entry->next;
  if (entry->prev != nullptr) entry->prev->next = entry->next;
  if (entry->next != nullptr) entry->next->prev = entry->prev;
  delete entry;
  return Status::OK();
}

}  //namespace toydb

