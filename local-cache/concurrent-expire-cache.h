#ifndef CONCURRENT_EXPIRE_CACHE_H
#define CONCURRENT_EXPIRE_CACHE_H

#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

namespace ww {

template <class TKey, class TValue, class THash = tbb::tbb_hash_compare<TKey>>
struct ConcurrentExpireCache {
 private:
  struct ListNode {
    ListNode() : m_timestamp(0), m_prev(nullptr), m_next(nullptr) {}

    explicit ListNode(const TKey &key, int64_t timestamp)
        : m_key(key),
          m_timestamp(timestamp),
          m_prev(nullptr),
          m_next(nullptr) {}

    TKey m_key;
    int64_t m_timestamp;
    std::shared_ptr<ListNode> m_prev;
    std::shared_ptr<ListNode> m_next;

    bool isInList() const { return m_prev != OutOfListMarker; }
  };

  static std::shared_ptr<ListNode> const OutOfListMarker;

  /**
   * The value that we store in the hashtable. The list node is allocated from
   * an internal object_pool. The ListNode* is owned by the list.
   */
  struct HashMapValue {
    HashMapValue() : m_listNode(nullptr) {}

    HashMapValue(const TValue &value, std::shared_ptr<ListNode> node)
        : m_value(value), m_listNode(node) {}

    TValue m_value;
    std::shared_ptr<ListNode> m_listNode;
  };

  typedef tbb::concurrent_hash_map<TKey, HashMapValue, THash> HashMap;
  typedef typename HashMap::const_accessor HashMapConstAccessor;
  typedef typename HashMap::accessor HashMapAccessor;
  typedef typename HashMap::value_type HashMapValuePair;

 public:
  /**
   * The proxy object for TBB::CHM::const_accessor. Provides direct access to
   * the user's value by dereferencing, thus hiding our implementation
   * details.
   */
  struct ConstAccessor {
    ConstAccessor() {}

    const TValue &operator*() const { return *get(); }

    const TValue *operator->() const { return get(); }

    const TValue *get() const { return &m_hashAccessor->second.m_value; }

    bool empty() const { return m_hashAccessor.empty(); }

   private:
    friend struct ConcurrentExpireCache;
    HashMapConstAccessor m_hashAccessor;
  };

  struct Accessor {
    Accessor() {}

    TValue &operator*() const { return *get(); }

    TValue *operator->() const { return get(); }

    TValue *get() const { return &m_hashAccessor->second.m_value; }

    bool empty() const { return m_hashAccessor.empty(); }

   private:
    friend struct ConcurrentExpireCache;
    HashMapAccessor m_hashAccessor;
  };

  explicit ConcurrentExpireCache(size_t maxSize, int64_t ttlSecond);

  ConcurrentExpireCache(const ConcurrentExpireCache &other) = delete;

  ConcurrentExpireCache &operator=(const ConcurrentExpireCache &) = delete;

  ~ConcurrentExpireCache() {
    evict_thread_stop_ = true;
    evict_thread_->join();
    clear();
  }

  bool remove(const TKey &key);

  bool remove(const std::vector<TKey> &keys);

  /**
   * Find a value by key, and return it by filling the ConstAccessor, which
   * can be default-constructed. Returns true if the element was found, false
   * otherwise.
   */
  bool find(ConstAccessor &ac, const TKey &key);

  bool find(Accessor &ac, const TKey &key);

  /**
   * Insert a value into the container. Both the key and value will be copied.
   * The new element will put into the eviction list as the most-recently
   * used.
   *
   * If there was already an element in the container with the same key, it
   * will not be updated, and false will be returned. Otherwise, true will be
   * returned.
   */
  bool insert(const TKey &key, const TValue &value);

  bool warmup(const TKey &key, const TValue &value, uint64_t extra_sec);

  /**
   * Clear the container. NOT THREAD SAFE -- do not use while other threads
   * are accessing the container.
   */
  void clear();

  /**
   * Get the approximate size of the container. May be slightly too low when
   * insertion is in progress.
   */
  size_t size() const { return m_size.load(); }

  /**
   * need to clone map, be careful.
   * @param dest_map
   */
  void getSnapshot(std::unordered_map<TKey, TValue> &dest_map) {
    HashMap clone(m_map);
    for (auto itr = clone.begin(); itr != clone.end(); ++itr) {
      dest_map.insert(std::make_pair(itr->first, itr->second.m_value));
    }
  }

  std::vector<TKey> keys();

 private:
  /**
   * Unlink a node from the list. The caller must lock the list mutex while
   * this is called.
   */
  void delink(std::shared_ptr<ListNode> node);

  /**
   * Add a new node to the list in the most-recently used position. The caller
   * must lock the list mutex while this is called.
   */
  void pushFront(std::shared_ptr<ListNode> node);

  /**
   * Evict the least-recently used item from the container. This function does
   * its own locking.
   */
  void evict();

  void evictTask();

  /**
   * The maximum number of elements in the container.
   */
  size_t m_maxSize;

  /**
   * This atomic variable is used to signal to all threads whether or not
   * eviction should be done on insert. It is approximately equal to the
   * number of elements in the container.
   */
  std::atomic<size_t> m_size;

  /**
   * The underlying TBB hash map.
   */
  HashMap m_map;

  /**
   * The linked list. The "head" is the most-recently used node, and the
   * "tail" is the least-recently used node. The list mutex must be held
   * during both read and write.
   */
  // first element of list does not store data, and it is just a head that
  // always in list
  std::shared_ptr<ListNode> m_head;
  // last element of list does not store data, and it is just a tail that always
  // in list
  std::shared_ptr<ListNode> m_tail;
  typedef std::mutex ListMutex;
  ListMutex m_listMutex;

  int64_t m_ttlSecond;
  std::shared_ptr<std::thread> evict_thread_;
  int32_t evict_thread_duration_second_;
  std::atomic<bool> evict_thread_stop_;
};

template <class TKey, class TValue, class THash>
std::shared_ptr<
    typename ConcurrentExpireCache<TKey, TValue, THash>::ListNode> const
    ConcurrentExpireCache<TKey, TValue, THash>::OutOfListMarker =
        std::make_shared<ListNode>();

template <class TKey, class TValue, class THash>
ConcurrentExpireCache<TKey, TValue, THash>::ConcurrentExpireCache(
    size_t maxSize, int64_t ttlSecond)
    : m_maxSize(maxSize),
      m_size(0),
      m_ttlSecond(ttlSecond),
      evict_thread_duration_second_(1),
      evict_thread_stop_(false),
      m_map(std::thread::hardware_concurrency() *
            4)  // it will automatically grow
{
  m_head = std::make_shared<ListNode>();
  m_tail = std::make_shared<ListNode>();
  m_head->m_next = m_tail;
  m_tail->m_prev = m_head;
  evict_thread_.reset(new std::thread(
      &ConcurrentExpireCache<TKey, TValue, THash>::evictTask, this));
}

template <class TKey, class TValue, class THash>
bool ConcurrentExpireCache<TKey, TValue, THash>::remove(const TKey &key) {
  bool res = false;
  HashMapAccessor hashAccessor;
  if (m_map.find(hashAccessor, key)) {
    res = m_map.erase(hashAccessor);
    // Acquire the lock, block if it is already held
    std::unique_lock<ListMutex> lock(m_listMutex);
    if (lock) {
      std::shared_ptr<ListNode> node = hashAccessor->second.m_listNode;
      if (node->isInList()) {
        delink(node);
        --m_size;
      } else {
        std::cerr << "local cache removing key, node does not in list: "
                  << node->m_key << std::endl;
      }
      lock.unlock();
    }
  }
  return res;
}

template <class TKey, class TValue, class THash>
bool ConcurrentExpireCache<TKey, TValue, THash>::remove(
    const std::vector<TKey> &keys) {
  bool res = true;
  if (!keys.empty()) {
    std::vector<std::shared_ptr<ListNode>> nodeList;
    size_t removedCnt = 0;
    for (const TKey &key : keys) {
      HashMapAccessor hashAccessor;
      if (m_map.find(hashAccessor, key)) {
        nodeList.emplace_back(hashAccessor->second.m_listNode);
        res &= m_map.erase(hashAccessor);
        ++removedCnt;
      }
    }

    if (!nodeList.empty()) {
      // Acquire the lock, block if it is already held
      std::unique_lock<ListMutex> lock(m_listMutex);
      for (auto &node : nodeList) {
        if (node->isInList()) {
          delink(node);
        } else {
          std::cerr << "local cache removing keys, node does not in list: "
                    << node->m_key << std::endl;
        }
      }
      lock.unlock();
      m_size -= removedCnt;
    }
  }
  return res;
}

template <class TKey, class TValue, class THash>
bool ConcurrentExpireCache<TKey, TValue, THash>::find(ConstAccessor &ac,
                                                      const TKey &key) {
  HashMapConstAccessor &hashAccessor = ac.m_hashAccessor;
  return m_map.find(hashAccessor, key);
}

template <class TKey, class TValue, class THash>
bool ConcurrentExpireCache<TKey, TValue, THash>::find(Accessor &ac,
                                                      const TKey &key) {
  HashMapAccessor &hashAccessor = ac.m_hashAccessor;
  return m_map.find(hashAccessor, key);
}

template <class TKey, class TValue, class THash>
bool ConcurrentExpireCache<TKey, TValue, THash>::insert(const TKey &key,
                                                        const TValue &value) {
  if (m_size >= m_maxSize) {
    return false;
  }

  std::time_t timestamp = std::time(NULL);
  std::shared_ptr<ListNode> node = std::make_shared<ListNode>(key, timestamp);
  HashMapAccessor hashAccessor;
  HashMapValuePair hashMapValue(key, HashMapValue(value, node));
  if (!m_map.insert(hashAccessor, hashMapValue)) {
    //            delete node;
    //            node = nullptr;
    return false;
  }

  // increase before insert
  ++m_size;
  std::unique_lock<ListMutex> lock(m_listMutex);
  pushFront(node);
  lock.unlock();
  return true;
}

template <class TKey, class TValue, class THash>
bool ConcurrentExpireCache<TKey, TValue, THash>::warmup(const TKey &key,
                                                        const TValue &value,
                                                        uint64_t extra_sec) {
  static uint64_t extra_seconds =
      extra_sec <= m_ttlSecond / 2 ? extra_sec : m_ttlSecond / 2;
  static std::mt19937 engine(std::time(NULL));
  static std::uniform_int_distribution<int> distribution(-extra_seconds,
                                                         extra_seconds);

  if (m_size >= m_maxSize) {
    return false;
  }

  std::time_t timestamp = std::time(NULL);
  std::shared_ptr<ListNode> node = std::make_shared<ListNode>(key, timestamp);
  HashMapAccessor hashAccessor;
  HashMapValuePair hashMapValue(key, HashMapValue(value, node));
  if (!m_map.insert(hashAccessor, hashMapValue)) {
    //            delete node;
    //            node = nullptr;
    return false;
  }

  // increase before insert
  ++m_size;
  std::unique_lock<ListMutex> lock(m_listMutex);
  pushFront(node);
  lock.unlock();
  return true;
}

template <class TKey, class TValue, class THash>
void ConcurrentExpireCache<TKey, TValue, THash>::clear() {
  m_map.clear();
  m_head->m_next = m_tail;
  m_tail->m_prev = m_head;
  m_size = 0;
}

template <class TKey, class TValue, class THash>
std::vector<TKey> ConcurrentExpireCache<TKey, TValue, THash>::keys() {
  std::vector<TKey> keys;
  {
    std::unique_lock<ListMutex> lock(m_listMutex);
    for (auto curNode = m_head->m_next; curNode != m_tail;
         curNode = curNode->m_next) {
      keys.push_back(curNode->m_key);
    }
  }
  return keys;
}

template <class TKey, class TValue, class THash>
inline void ConcurrentExpireCache<TKey, TValue, THash>::delink(
    std::shared_ptr<ListNode> node) {
  std::shared_ptr<ListNode> prev = node->m_prev;
  std::shared_ptr<ListNode> next = node->m_next;
  prev->m_next = next;
  next->m_prev = prev;
  node->m_prev = OutOfListMarker;
}

template <class TKey, class TValue, class THash>
inline void ConcurrentExpireCache<TKey, TValue, THash>::pushFront(
    std::shared_ptr<ListNode> node) {
  std::shared_ptr<ListNode> oldRealHead = m_head->m_next;
  node->m_prev = m_head;
  node->m_next = oldRealHead;
  oldRealHead->m_prev = node;
  m_head->m_next = node;
}

template <class TKey, class TValue, class THash>
void ConcurrentExpireCache<TKey, TValue, THash>::evict() {
  std::vector<std::shared_ptr<ListNode>> expiredNodes;
  std::time_t timestamp = std::time(NULL);
  timestamp -= m_ttlSecond;
  std::unique_lock<ListMutex> lock(m_listMutex);
  std::shared_ptr<ListNode> moribund = m_tail->m_prev;
  while (moribund != m_head && moribund->m_timestamp < timestamp) {
    if (moribund->isInList()) {
      delink(moribund);
    } else {
      std::cerr << "local cache evict, node does not in list: "
                << moribund->m_key << std::endl;
    }
    expiredNodes.push_back(moribund);
    moribund = m_tail->m_prev;
  }
  lock.unlock();

  if (expiredNodes.empty()) {
    return;
  }

  size_t evictedCnt = 0;
  for (auto &expiredNode : expiredNodes) {
    HashMapAccessor hashAccessor;
    if (!m_map.find(hashAccessor, expiredNode->m_key)) {
      // Presumably unreachable
      continue;
    }
    m_map.erase(hashAccessor);
    ++evictedCnt;
  }

  m_size -= evictedCnt;
}

template <class TKey, class TValue, class THash>
void ConcurrentExpireCache<TKey, TValue, THash>::evictTask() {
  while (!evict_thread_stop_) {
    evict();
    std::this_thread::sleep_for(
        std::chrono::seconds(evict_thread_duration_second_));
  }
}

}  // namespace ww

#endif  // CONCURRENT_EXPIRE_CACHE_H
