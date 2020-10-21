#ifndef CONCURRENT_WARMUP_H
#define CONCURRENT_WARMUP_H

#include <unistd.h>

#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ctpl.h"

#ifndef LOG_DEBUG
#include <iostream>
#define LOG_DEBUG std::cout << std::endl
#endif

#ifndef LOG_INFO
#include <iostream>
#define LOG_INFO std::cout << std::endl
#endif

#ifndef LOG_ERROR
#include <iostream>
#define LOG_ERROR std::cout << std::endl
#endif

namespace ww {

template <typename K, typename V>
using CacheType = std::unordered_map<K, V>;

template <typename K, typename V>
using SerializeFunc = std::function<int(const std::unordered_map<K, V> &)>;

template <typename K, typename V>
std::vector<CacheType<K, V>> SplitUnMap(const CacheType<K, V> &un_map, int n);

using DeserializeFunc = std::function<int(const std::string &)>;
using QuickDeserializeFunc = std::function<int()>;
using WriteTaskFunc = std::function<int()>;

using ThreadPool = ctpl::thread_pool;

int DoWriteTask(WriteTaskFunc write_task_func, const std::string &file_name);

template <typename K, typename V>
int ConcurrentDump(const CacheType<K, V> &dest_map,
                   SerializeFunc<K, V> serialize_func,
                   const std::string &file_name, int thread_cnt = 0);

int ConcurrentWarmup(DeserializeFunc deserialize_func,
                     const std::string &file_name, int thread_cnt = 0);
int ConcurrentWarmup(QuickDeserializeFunc deserialize_func,
                     const std::string &file_name, int thread_cnt = 0);

int WriteToBuf(const void *buf, size_t bytes, bool immediate = false);
int ReadFromBuf(void *target, size_t bytes);

template <typename K, typename V>
std::vector<CacheType<K, V>> SplitUnMap(const CacheType<K, V> &un_map, int n) {
  std::vector<std::unordered_map<K, V>> res;

  if (un_map.size() <= n || n <= 0) {
    res.emplace_back(un_map);
    return res;
  }

  int i = 1;
  int single_cnt = 1 + un_map.size() / n;
  if (un_map.size() == n) {
    --single_cnt;
  }

  std::unordered_map<K, V> tmp;
  for (auto &entry : un_map) {
    tmp[entry.first] = entry.second;
    if (i++ % single_cnt == 0) {
      res.emplace_back(std::move(tmp));
    }
  }

  if (i % n != 0) {
    res.emplace_back(std::move(tmp));
  }

  return res;
}

template <typename K, typename V>
int ConcurrentDump(const CacheType<K, V> &dest_map,
                   SerializeFunc<K, V> serialize_func,
                   const std::string &file_name, int thread_cnt) {
  if (thread_cnt <= 0) {
    thread_cnt = 2 * std::thread::hardware_concurrency();
  }
  if (thread_cnt > 100) {
    thread_cnt = 100;
  }

  auto caches = SplitUnMap(dest_map, thread_cnt);
  ThreadPool thread_pool(thread_cnt);
  std::vector<std::future<int>> futures;

  int i = 0;
  for (auto &cache : caches) {
    auto write_task_func = [&, serialize_func, cache]() {
      return serialize_func(cache);
    };
    futures.emplace_back(thread_pool.push(&DoWriteTask, write_task_func,
                                          file_name + std::to_string(i++)));
  }

  int res = 0;
  for (auto& future : futures) {
    if (future.get() != 0) {
      res = -1;
    }
  }

  return res;
}

}  // namespace ww

#endif  // CONCURRENT_WARMUP_H
