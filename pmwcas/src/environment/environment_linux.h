// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <numa.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdint>
#include <iostream>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#ifdef PMDK
#include <libpmemobj.h>
#endif

#include "include/environment.h"
#include "include/allocator.h"
#include "include/status.h"
#include "util/auto_ptr.h"
#include "util/macros.h"

namespace pmwcas {

class LinuxSharedMemorySegment : public SharedMemorySegment {
 public:
  LinuxSharedMemorySegment();
  ~LinuxSharedMemorySegment();

  static Status Create(unique_ptr_t<SharedMemorySegment>& segment);

  virtual Status Initialize(const std::string& segname, uint64_t size, bool open_existing) override;

  virtual Status Attach(void* base_address = nullptr) override;

  virtual Status Detach() override;

  virtual void* GetMapAddress() override;

  //virtual DumpToFile(const std::string& filename) override;

 private:
  std::string segment_name_;
  uint64_t size_;
  int map_fd_;
  void* map_address_;
};

class LinuxEnvironment : public IEnvironment {
 public:
  LinuxEnvironment();
  virtual ~LinuxEnvironment() {}

  static Status Create(IEnvironment*& environment) {
	  environment = (IEnvironment*)ShmemAlloc(sizeof(LinuxEnvironment));
    new(environment)LinuxEnvironment();
    return Status::OK();
  }

  static void Destroy(IEnvironment* e) {
	  //LinuxEnvironment* environment = static_cast<LinuxEnvironment*>(e);
	  //environment->~LinuxEnvironment();
  }


  virtual uint64_t NowMicros() override;

  virtual uint64_t NowNanos() override;

  virtual uint32_t GetCoreCount() override;

  virtual void Sleep(uint32_t ms_to_sleep) override;

  virtual Status NewRandomReadWriteAsyncFile(const std::string& filename,
      const FileOptions& options, ThreadPool* threadpool, RandomReadWriteAsyncFile** file,
      bool* exists = nullptr) override ;

  virtual Status NewSharedMemorySegment(const std::string& segname, uint64_t size,
                                        bool open_existing, SharedMemorySegment** seg) override;

  virtual Status NewThreadPool(uint32_t max_threads,
                               ThreadPool** pool) override;

  virtual Status SetThreadAffinity(uint64_t core, AffinityPattern affinity_pattern) override;

  virtual Status GetWorkingDirectory(std::string& directory) override;

  virtual Status GetExecutableDirectory(std::string& directory) override;

 private:
  Status SetThreadAffinity(pthread_t thread, uint64_t core, AffinityPattern affinity_pattern);
};


extern "C" int bztree_mem_size;

/// NUMA oblivious allocotar for shared memory and multiprocess environment
class NumaAllocator : public pmwcas::IAllocator {
 public:
  static Status Create(IAllocator*& allocator) {
    allocator = new NumaAllocator();
    return Status::OK();
  }

  static void Destroy(IAllocator* a) {
	  delete a;
  }

  struct NumaSegment {
	  char*    memory;
	  uint64_t allocated;
  };
  size_t kNumaMemorySize;
  NumaSegment* numa_segment;

  // The hidden part of each allocated block of memory
  struct Header {
    uint64_t size;
    Header* next;
	  char padding[kCacheLineSize - sizeof(size) - sizeof(next)];
    Header() : size(0), next(nullptr) {}
    inline void* GetData() { return (void*)((char*)this + sizeof(*this)); }
  };

  // Chain of all memory blocks of the same size
  struct BlockList {
    Header* head;
    Header* tail;
    BlockList() : head(nullptr), tail(nullptr) {}
    BlockList(Header* h, Header* t) : head(h), tail(t) {}

    inline void* Get() {
      if(head) {
        Header* alloc = head;
        if(alloc == tail) {
          head = tail = nullptr;
        } else {
          head = head->next;
        }
        return alloc->GetData();
      }
      return nullptr;
    }

    inline void Put(Header* header) {
      if(!head) {
        DCHECK(!tail);
        head = tail = header;
      } else {
        Header* old_tail = tail;
        old_tail->next = header;
        tail = header;
        header->next = nullptr;
      }
      DCHECK(head->size == header->size);
    }
  };

  inline Header* ExtractHeader(void* pBytes) {
    return (Header*)((char*)pBytes - sizeof(Header));
  }

  inline std::unordered_map<size_t, BlockList>& GetNumaMap() {
    static std::unordered_map<size_t, BlockList> tls_blocks;
    return tls_blocks;
  }

  struct Slab {
    static const uint64_t kSlabSize = 1024 * 1024 * 1024;  // 1GB
    NumaAllocator* tls_allocator;
    uint64_t allocated;
    void* memory;
    Slab() : allocated(0), memory(nullptr) {}
    ~Slab() {}
    inline void* Allocate(size_t n) {
    retry:
      if(memory && allocated + n <= kSlabSize) {
        uint64_t off = allocated;
        allocated += n;
        return (void*)((char*)memory + off);
      } else {
        // Slab full or not initialized yet
        auto node = numa_node_of_cpu(sched_getcpu());
        uint64_t off = __atomic_fetch_add(&tls_allocator->numa_segment[node].allocated, kSlabSize, __ATOMIC_SEQ_CST);
        memory = tls_allocator->numa_segment[node].memory + off;
        ALWAYS_ASSERT(off < tls_allocator->kNumaMemorySize);
        allocated = 0;
        goto retry;
      }
      DCHECK(false);
      return nullptr;
    }
  };

  inline Slab& GetNumaSlab() {
    static Slab slab;
    if(!slab.tls_allocator) {
      slab.tls_allocator = this;
    }
    return slab;
  }

  /// Try to get something from the TLS set
  inline void* NumaAllocate(size_t nSize) {
    // Align to cache line size
    nSize = (nSize + sizeof(Header) + kCacheLineSize - 1) / kCacheLineSize * kCacheLineSize;
    auto& tls_map = GetNumaMap();
    auto block_list = tls_map.find(nSize - sizeof(Header));
    void* pBytes = nullptr;
    if(block_list != tls_map.end()) {
      pBytes = block_list->second.Get();
    }

    if(!pBytes) {
      // Nothing in the map, try my local memory
      auto& tls_slab = GetNumaSlab();
      pBytes = tls_slab.Allocate(nSize);
      if(pBytes) {
        ((Header*)pBytes)->size = nSize - sizeof(Header);
        ((Header*)pBytes)->next = nullptr;
        pBytes = (void*)((char*)pBytes + sizeof(Header));
      }
    }
    DCHECK(pBytes);
    return pBytes;
  }

 public:
  NumaAllocator() {
    int nodes = numa_max_node() + 1;
	int flags = MAP_ANONYMOUS | MAP_SHARED | MAP_POPULATE | MAP_HUGETLB;
	kNumaMemorySize = (size_t)bztree_mem_size*1024/nodes;
    numa_segment = (NumaSegment*)mmap(
		nullptr, sizeof(NumaSegment)*nodes, PROT_READ | PROT_WRITE,
		flags, -1, 0);
	if (numa_segment == MAP_FAILED)
	{
		flags &= ~MAP_HUGETLB;
		numa_segment = (NumaSegment*)mmap(
			nullptr, sizeof(NumaSegment)*nodes, PROT_READ | PROT_WRITE,
			flags, -1, 0);
	}
    for(int i = 0; i < nodes; ++i) {
      numa_set_preferred(i);
      numa_segment[i].memory = (char *)mmap(
          nullptr, kNumaMemorySize, PROT_READ | PROT_WRITE,
          flags, -1, 0);

      numa_segment[i].allocated = 0;
    }
  }

  void Allocate(void **mem, size_t nSize) override {
    *mem= NumaAllocate(nSize);
    DCHECK(*mem);
  }

  void CAlloc(void **mem, size_t count, size_t size) override {
    /// TODO(tzwang): not implemented yet
  }

  void Free(void* pBytes) override {
    auto& tls_map = GetNumaMap();
    // Extract the hidden size info
    Header* pHeader = ExtractHeader(pBytes);
    pHeader->next = nullptr;
    DCHECK(pHeader->size);
    auto block_list = tls_map.find(pHeader->size);
    if(block_list == tls_map.end()) {
      tls_map.emplace(pHeader->size, BlockList(pHeader, pHeader));
    } else {
      block_list->second.Put(pHeader);
    }
  }

  void AllocateAligned(void **mem, size_t nSize, uint32_t nAlignment) override {
    /// TODO(tzwang): take care of aligned allocations
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    Allocate(mem, nSize);
  }

  void FreeAligned(void* pBytes) override {
    /// TODO(tzwang): take care of aligned allocations
    return Free(pBytes);
  }

  void AllocateAlignedOffset(void **mem, size_t size, size_t alignment, size_t offset) override{
    /// TODO(tzwang): not implemented yet
  }

  void AllocateHuge(void **mem, size_t size) override {
    /// TODO(tzwang): not implemented yet
  }

  Status Validate(void* pBytes) override {
    /// TODO(tzwang): not implemented yet
    return Status::OK();
  }

  uint64_t GetAllocatedSize(void* pBytes) override {
    /// TODO(tzwang): not implemented yet
    return 0;
  }

  int64_t GetTotalAllocationCount() {
    /// TODO(tzwang): not implemented yet
    return 0;
  }
};


// A simple wrapper for posix_memalign
class DefaultAllocator : IAllocator {
 public:
  DefaultAllocator() {}
  ~DefaultAllocator() {}

  static Status Create(IAllocator*& allocator) {
    int n = posix_memalign(reinterpret_cast<void**>(&allocator), kCacheLineSize, sizeof(DefaultAllocator));
    if(n || !allocator) return Status::Corruption("Out of memory");
    new(allocator) DefaultAllocator();
    return Status::OK();
  }

  static void Destroy(IAllocator* a) {
    DefaultAllocator * allocator = static_cast<DefaultAllocator*>(a);
    allocator->~DefaultAllocator();
    free(allocator);
  }

  void Allocate(void **mem, size_t nSize) override {
    int n RAW_CHECK_ONLY = posix_memalign(mem, kCacheLineSize, nSize);
    RAW_CHECK(n == 0, "allocator error.");
  }

  void CAlloc(void **mem, size_t count, size_t size) override{
    /// TODO(tzwang): not implemented yet
    return;
  }

  void Free(void* pBytes) override {
    free(pBytes);
  }

  void AllocateAligned(void **mem, size_t nSize, uint32_t nAlignment) override {
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    return Allocate(mem, nSize);
  }

  void FreeAligned(void* pBytes) override {
    return Free(pBytes);
  }

  void AllocateAlignedOffset(void **mem, size_t size, size_t alignment, size_t offset) override {
    /// TODO(tzwang): not implemented yet
    return;
  }

  void AllocateHuge(void **mem, size_t size) override {
    /// TODO(tzwang): not implemented yet
    return;
  }

  Status Validate(void* pBytes) override {
    /// TODO(tzwang): not implemented yet
    return Status::OK();
  }

  uint64_t GetAllocatedSize(void* pBytes) override {
    /// TODO(tzwang): not implemented yet
    return 0;
  }

  int64_t GetTotalAllocationCount() {
    /// TODO(tzwang): not implemented yet
    return 0;
  }

};

#ifdef PMDK

#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)
POBJ_LAYOUT_BEGIN(allocator);
POBJ_LAYOUT_TOID(allocator, char)
POBJ_LAYOUT_END(allocator)

/// A wrapper for using PMDK allocator
class PMDKAllocator : IAllocator {
 public:
  PMDKAllocator(PMEMobjpool *pop, const char *file_name): pop(pop), file_name(file_name) {}
  ~PMDKAllocator() {
    pmemobj_close(pop);
  }

  static std::function<Status(IAllocator *&)> Create(const char *pool_name,
                                                     const char *layout_name,
                                                     uint64_t pool_size) {
    return [pool_name, layout_name, pool_size](IAllocator *&allocator) {
      int n = posix_memalign(reinterpret_cast<void **>(&allocator), kCacheLineSize, sizeof(DefaultAllocator));
      if (n || !allocator) return Status::Corruption("Out of memory");

      PMEMobjpool *tmp_pool;
      if (!FileExists(pool_name)) {
        tmp_pool = pmemobj_create(pool_name, layout_name, pool_size, CREATE_MODE_RW);
        LOG_ASSERT(tmp_pool != nullptr);
      } else {
        tmp_pool = pmemobj_open(pool_name, layout_name);
        LOG_ASSERT(tmp_pool != nullptr);
      }

      new(allocator) PMDKAllocator(tmp_pool, pool_name);
      return Status::OK();
    };
  }

  static bool FileExists(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
  }

  static void Destroy(IAllocator *a) {
    auto* allocator= static_cast<PMDKAllocator*>(a);
    allocator->~PMDKAllocator();
    free(allocator);
  }

  void Allocate(void **mem, size_t nSize) override {
    TX_BEGIN(pop) {
      if(*mem != nullptr) {
        pmemobj_tx_add_range_direct(mem, sizeof(uint64_t));
      }
      *mem = pmemobj_direct(pmemobj_tx_alloc(nSize, TOID_TYPE_NUM(char)));
    }
    TX_ONABORT { std::cout<<"Allocate: TXN Allocation Error: "<< nSize << std::endl; }
    TX_END
  }

  template<typename T>
  inline T *GetDirect(T *pmem_offset) {
    return reinterpret_cast<T *>(
        reinterpret_cast<uint64_t>(pmem_offset) + reinterpret_cast<char *>(GetPool()));
  }

  template<typename T>
  inline T *GetOffset(T *pmem_direct) {
    return reinterpret_cast<T *>(
        reinterpret_cast<char *>(pmem_direct) - reinterpret_cast<char *>(GetPool()));
  }

  void AllocateDirect(void **mem, size_t nSize) {
    Allocate(mem, nSize); 
  }

  void* AllocateOff(size_t nSize){
    PMEMoid ptr;
    int ret = pmemobj_zalloc(pop, &ptr, sizeof(char) * nSize, TOID_TYPE_NUM(char));
    if (ret) {
      LOG(FATAL) << "POBJ_ALLOC error";
      ALWAYS_ASSERT(ret == 0);
    }
    return reinterpret_cast<void*>(ptr.off);
  }


  void* GetRoot(size_t nSize) {
    return pmemobj_direct(pmemobj_root(pop, nSize));
  }

  PMEMobjpool *GetPool(){
    return pop;
  }

  void PersistPtr(const void *ptr, uint64_t size){
    pmemobj_persist(pop, ptr, size);
  }

  void CAlloc(void **mem, size_t count, size_t size) override {
    // not implemented
  }

  void Free(void* pBytes) override {
    auto oid_ptr = pmemobj_oid(pBytes);
    TOID(char) ptr_cpy;
    TOID_ASSIGN(ptr_cpy, oid_ptr);
    POBJ_FREE(&ptr_cpy);
  }

  void AllocateAligned(void **mem, size_t nSize, uint32_t nAlignment) override {
    RAW_CHECK(nAlignment == kCacheLineSize, "unsupported alignment.");
    return Allocate(mem, nSize);
  }

  void FreeAligned(void* pBytes) override {
    return Free(pBytes);
  }

  void AllocateAlignedOffset(void **mem, size_t size, size_t alignment, size_t offset) override {
    // not implemented
  }

  void AllocateHuge(void **mem, size_t size) override{
    // not implemented
  }

  Status Validate(void* pBytes) {
    return Status::OK();
  }

  uint64_t GetAllocatedSize(void* pBytes) {
    return 0;
  }

  int64_t GetTotalAllocationCount() {
    return 0;
  }

 private:
  PMEMobjpool *pop;
  const char *file_name;
};

#endif  // PMDK
}
