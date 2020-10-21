#include "concurrent_warmup.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <thread>

namespace ww {

static size_t max_size = 1024 * 1024;
static size_t total_size = max_size * 2;

std::string ErrStr(int err_no) {
  char *e = strerror(errno);
  return e ? e : "no error message";
}

size_t Size(const std::string &file_name) {
  FILE *fp = fopen(file_name.c_str(), "r");
  if (fp == NULL) {
    LOG_ERROR << "open file error";
    return -1;
  }

  fseek(fp, 0L, SEEK_END);
  size_t res = ftell(fp);

  fclose(fp);
  return res;
}

struct OutputBuffer {
  OutputBuffer() {
    ptr = nullptr;
    fd = -1;
    cur_pos = 0;
  };

  OutputBuffer(const std::string &file_name, size_t n) {
    cur_pos = 0;
    ptr = (char *)malloc(n);
    if (!ptr) {
      LOG_ERROR << "malloc " << n << ", error: " << ErrStr(errno);
    }

    fd = open(file_name.c_str(), O_CREAT | O_WRONLY | O_TRUNC, S_IRWXU);
    if (fd < 0) {
      LOG_ERROR << "open file error: " << file_name;
    }
  }

  ~OutputBuffer() {
    if (ptr) free(ptr);
    if (fd > 0) close(fd);
  }

  OutputBuffer(OutputBuffer &&other) {
    ptr = other.ptr;
    fd = other.fd;
    cur_pos = other.cur_pos;

    other.ptr = nullptr;
    other.fd = -1;
    other.cur_pos = 0;
  }

  OutputBuffer &operator=(OutputBuffer &&other) {
    ptr = other.ptr;
    fd = other.fd;
    cur_pos = other.cur_pos;

    other.ptr = nullptr;
    other.fd = -1;
    other.cur_pos = 0;
    return *this;
  }

  OutputBuffer(const OutputBuffer &) = delete;
  OutputBuffer &operator=(const OutputBuffer &) = delete;

  char *ptr;
  int fd;
  size_t cur_pos;
};

struct InputBuffer {
  InputBuffer() {
    ptr = nullptr;
    size = 0;
    cur_pos = 0;
  };

  InputBuffer(const std::string file_name) {
    ptr = nullptr;
    size = 0;
    cur_pos = 0;
    if (0 == access(file_name.c_str(), R_OK)) {
      int fd = open(file_name.c_str(), O_RDONLY);
      if (fd < 0) {
        LOG_ERROR << "open file failed: " << file_name;
        return;
      }
      size = Size(file_name.c_str());
      if (size == 0) {
        LOG_ERROR << "empty file: " << file_name;
        return;
      }
      ptr = (char *)mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
      if (ptr) {
        LOG_INFO << "mmap ok, ptr: " << (void *)ptr << ", size: " << size;
      } else {
        LOG_ERROR << "mmap error: " << ErrStr(errno);
      }
      close(fd);
    }
  }

  ~InputBuffer() {
    if (ptr) {
      if (0 == munmap(ptr, size)) {
        LOG_INFO << "munmap ok, ptr: " << (void *)ptr << ", size: " << size;
      } else {
        LOG_ERROR << "munmap error: " << ErrStr(errno)
                  << ", ptr: " << (void *)ptr << ", size: " << size;
        ;
      }
    }
  }

  InputBuffer(InputBuffer &&other) {
    ptr = other.ptr;
    cur_pos = other.cur_pos;
    size = other.size;

    other.ptr = nullptr;
    other.cur_pos = 0;
    other.size = 0;
  }

  InputBuffer &operator=(InputBuffer &&other) {
    ptr = other.ptr;
    cur_pos = other.cur_pos;
    size = other.size;

    other.ptr = nullptr;
    other.cur_pos = 0;
    other.size = 0;
    return *this;
  }

  InputBuffer(const OutputBuffer &) = delete;
  InputBuffer &operator=(const OutputBuffer &) = delete;

  char *ptr;
  size_t size;
  size_t cur_pos;
};

thread_local OutputBuffer wt_buf;
thread_local InputBuffer rd_buf;

int WriteToBuf(const void *buf, size_t bytes, bool immediate) {
  if (!wt_buf.ptr) {
    if (write(wt_buf.fd, buf, bytes) < 0) {
      LOG_ERROR << "write error: " << ErrStr(errno);
      return -1;
    }
    return 0;
  }

  if (wt_buf.cur_pos >= max_size || (immediate && wt_buf.cur_pos > 0)) {
    if (write(wt_buf.fd, wt_buf.ptr, wt_buf.cur_pos) < 0) {
      LOG_ERROR << "write error: " << ErrStr(errno);
      return -1;
    }
    wt_buf.cur_pos = 0;
  }

  if (buf) {
    if (bytes <= max_size) {
      memcpy(wt_buf.ptr + wt_buf.cur_pos, buf, bytes);
      wt_buf.cur_pos += bytes;
    } else {
      if (write(wt_buf.fd, buf, bytes) < 0) {
        LOG_ERROR << "write error: " << ErrStr(errno);
        return -1;
      }
    }
  }

  return 0;
}

int ReadFromBuf(void *target, size_t bytes) {
  if (!rd_buf.ptr) {
    LOG_ERROR << "null read buffer ptr";
    return -2;
  }

  size_t new_pos = rd_buf.cur_pos + bytes;
  if (new_pos > rd_buf.size) {
    return -1;  // end
  }

  memcpy(target, rd_buf.ptr + rd_buf.cur_pos, bytes);
  rd_buf.cur_pos = new_pos;

  return 0;
}

int DoWriteTask(WriteTaskFunc write_task_func, const std::string &file_name) {
  wt_buf = OutputBuffer(file_name, total_size);

  LOG_DEBUG << "wt buffer addr: " << (void *)wt_buf.ptr
            << ", write cur pos addr: " << &wt_buf.cur_pos
            << ", file name: " << file_name;

  int res0 = write_task_func();
  int res1 = WriteToBuf(NULL, 0, true);

  if (res0 != 0 || res1 != 0) {
    return -1;
  }
  return 0;
}

int ReadTask(QuickDeserializeFunc deserialize_func,
             const std::string &file_name) {
  rd_buf = InputBuffer(file_name);

  LOG_DEBUG << "rd buffer addr: " << (void *)rd_buf.ptr
            << ", read cur pos addr: " << &rd_buf.cur_pos
            << ", file name: " << file_name;

  return deserialize_func();
}

int ConcurrentWarmup(DeserializeFunc deserialize_func,
                     const std::string &file_name, int thread_cnt) {
  if (thread_cnt <= 0) {
    thread_cnt = 2 * std::thread::hardware_concurrency();
  }
  if (thread_cnt > 100) {
    thread_cnt = 100;
  }

  ThreadPool thread_pool(thread_cnt);
  std::vector<std::future<int>> futures;

  for (int i = 0; i < 1000; ++i) {
    auto split_name = file_name + std::to_string(i);
    if (0 == access(split_name.c_str(), R_OK)) {
      futures.emplace_back(thread_pool.push(deserialize_func, split_name));
    } else {
      break;
    }
  }

  int res = 0;
  for (auto& future : futures) {
    if (future.get() != 0) {
      res = -1;
    }
  }

  return res;
}

int ConcurrentWarmup(QuickDeserializeFunc deserialize_func,
                     const std::string &file_name, int thread_cnt) {
  if (thread_cnt <= 0) {
    thread_cnt = 2 * std::thread::hardware_concurrency();
  }
  if (thread_cnt > 100) {
    thread_cnt = 100;
  }

  ThreadPool thread_pool(thread_cnt);
  std::vector<std::future<int>> futures;

  for (int i = 0; i < 1000; ++i) {
    auto split_name = file_name + std::to_string(i);
    if (0 == access(split_name.c_str(), R_OK)) {
      futures.emplace_back(
          thread_pool.push(&ReadTask, deserialize_func, split_name));
    } else {
      break;
    }
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