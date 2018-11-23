#include "pf_hashtablewal.h"

namespace toydb {

HashTableWal::HashTableWal(int num_buckets)
    : num_buckets_(num_buckets) {
  hash_entry_ = new HashEntryWal* [num_buckets];
  for (int i = 0; i < num_buckets; i++) hash_entry_[i] = nullptr;
}

HashTableWal::~HashTableWal() {
  for (int i = 0; i < num_buckets_; i++) {
    HashEntryWal *current = hash_entry_[i];
    while (current != nullptr) {
      HashEntryWal *next = current->next;
      delete current;
      current = next;
    }
  }
  delete[] hash_entry_;
}

int HashTableWal::Hash(const PageInfo &page_info) {
  int slot = (page_info.rel_id * 16381 + page_info.type * 21841
      + page_info.page_num * 29123) % num_buckets_;
  if (slot < 0) slot += num_buckets_;
  return slot;
}

int HashTableWal::Find(const PageInfo &page_info) {
  int slot = -1;
  int bucket_num = Hash(page_info);
  HashEntryWal *entry;
  for (entry = hash_entry_[bucket_num];
       entry != nullptr;
       entry = entry->next) {
    if (entry->page_info == page_info) {
      slot = entry->slot;
      break;
    }
  }
  return slot;
}

int HashTableWal::Insert(const PageInfo &page_info, int slot) {
  // check entry does not exist in bucket
  int bucket_num = Hash(page_info);
  for (HashEntryWal *entry = hash_entry_[bucket_num];
       entry != nullptr;
       entry = entry->next){
    assert(!(entry->page_info == page_info));
  }

  HashEntryWal *entry = new HashEntryWal();
  entry->page_info = page_info;
  entry->slot = slot;
  entry->prev = nullptr;
  if (hash_entry_[bucket_num] != nullptr) hash_entry_[bucket_num]->prev = entry;
  entry->next = hash_entry_[bucket_num];
  hash_entry_[bucket_num] = entry;
  return 0;
}

int HashTableWal::Delete(const PageInfo &page_info) {
  int bucket_num = Hash(page_info);
  HashEntryWal *entry;
  for (entry = hash_entry_[bucket_num];
       entry != nullptr;
       entry = entry->next) {
    if (entry->page_info == page_info) {
      break;
    }
  }
  assert(entry != nullptr);
  if (entry == hash_entry_[bucket_num]) hash_entry_[bucket_num] = entry->next;
  if (entry->prev != nullptr) entry->prev->next = entry->next;
  if (entry->next != nullptr) entry->next->prev = entry->prev;
  delete entry;
  return 0;
}

}  // namespace toydb

