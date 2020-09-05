#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

#include "concurrent-expire-cache.h"
#include "concurrent_warmup.h"
#include "ctpl.h"

using Mat = std::vector<std::vector<float>>;
using MatPtr = std::shared_ptr<Mat>;

const int ID_SIZE = sizeof(int64_t);
const int VALUE_SIZE = sizeof(float);

ww::ConcurrentExpireCache<int64_t, MatPtr> cache(100000, 600);

using namespace ww;

int Serialize(const std::unordered_map<int64_t, MatPtr>& dest_map) {
  LOG_INFO << "write cache to file begin...";
  // write key size
  WriteToBuf(&ID_SIZE, sizeof(int));
  // write value size
  WriteToBuf(&VALUE_SIZE, sizeof(int));

  int64_t line_cnt, col_cnt;

  for (auto& entry : dest_map) {
    // write key
    WriteToBuf(&(entry.first), ID_SIZE);
    // write line count
    line_cnt = entry.second->size();
    WriteToBuf(&line_cnt, sizeof(int64_t));
    for (auto& values : *(entry.second)) {
      col_cnt = values.size();
      // write col cnt
      WriteToBuf(&col_cnt, sizeof(int64_t));
      if (col_cnt > 0) {
        // write all values
        WriteToBuf(values.data(), col_cnt * VALUE_SIZE);
      }
    }
  }

  LOG_INFO << "write cache to file end, count: " << dest_map.size();
  return 0;
}

int Deserialize() {
  LOG_INFO << "read cache from file begin...";
  int id_size, value_size;

  ReadFromBuf(&id_size, sizeof(int));
  if (ID_SIZE != id_size) {
    LOG_ERROR << "origin key size: " << id_size << ", but now: " << ID_SIZE
              << ", skip warmup";
    return -1;
  }

  ReadFromBuf(&value_size, sizeof(int));
  if (VALUE_SIZE != value_size) {
    LOG_ERROR << "origin value size: " << value_size
              << ", but now: " << VALUE_SIZE << ", skip warmup";
    return -1;
  }

  int64_t id, line_cnt, col_cnt, res;
  float value;

  int count = 0;
  while (true) {
    line_cnt = col_cnt = 0;
    res = ReadFromBuf(&(id), ID_SIZE);
    if (res == 0) {
      ReadFromBuf(&(line_cnt), sizeof(int64_t));
      auto mat_ptr = std::make_shared<Mat>();
      if (line_cnt > 0) {
        mat_ptr->reserve(line_cnt);
        for (int j = 0; j < line_cnt; ++j) {
          ReadFromBuf(&(col_cnt), sizeof(int64_t));
          std::vector<float> line;
          if (col_cnt > 0) {
            line.resize(col_cnt);
            ReadFromBuf(line.data(), col_cnt * VALUE_SIZE);
          }
          mat_ptr->emplace_back(std::move(line));
        }
      }
      if (!mat_ptr->empty()) {
        cache.warmup(id, mat_ptr, 300);
        ++count;
      }
      continue;
    } else if (res != -1) {
      LOG_ERROR << "read cache from file error";
    }
    break;
  }

  LOG_INFO << "read cache from file end, count: " << count;
  return 0;
}

int Dump(const std::string& file_name) {
  LOG_INFO << "begin to dump cache...";
  std::unordered_map<int64_t, MatPtr> dest_map;
  cache.getSnapshot(dest_map);

  int res = ConcurrentDump<int64_t, MatPtr>(dest_map, &Serialize, file_name);

  LOG_INFO << "end to dump cache, count: " << cache.size();
  return res;
}

int Warmup(const std::string& file_name) {
  LOG_INFO << "begin to warmup cache...";
  int res = ConcurrentWarmup(&Deserialize, file_name);

  LOG_INFO << "end to warmup cache, count: " << cache.size();
  return res;
}

void InitCache(int64_t id) {
  auto ptr = std::make_shared<Mat>();
  float f = 0.1;
  for (int i = 0; i < 10; ++i) {
    std::vector<float> line;
    for (int j = 0; j <= i; ++j) {
      line.push_back(f);
      f += 0.1;
    }
    ptr->emplace_back(std::move(line));
  }
  cache.insert(id, ptr);
}

void PrintCache() {
  std::unordered_map<int64_t, MatPtr> dest_map;
  cache.getSnapshot(dest_map);
  for (auto& entry : dest_map) {
    std::cout << "key: " << entry.first << std::endl;
    for (auto& line : *entry.second) {
      for (auto& value : line) {
        std::cout << value << ",";
      }
      std::cout << std::endl;
    }
  }
}

int main() {
  int type;
  std::cin >> type;
  switch (type) {
    case 1: {
      InitCache(1);
      InitCache(2);
      PrintCache();
      Dump("cache_file");
      break;
    }
    case 2: {
      Warmup("cache_file");
      PrintCache();
      break;
    }
    default: {
      return 0;
    }
  }

  return 0;
}
