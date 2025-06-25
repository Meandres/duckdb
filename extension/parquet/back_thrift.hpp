//===----------------------------------------------------------------------===//
//                         DuckDB
//
// thrift_tools.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include <list>
#include <rte_ring.h>
#include <rte_malloc.h>
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include <osv/ucache.hh>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/allocator.hpp"
#endif

namespace duckdb {

class row_group_range {
public:
	uint64_t offset_b;
	uint64_t len;
	ucache::VMA* vma;

	row_group_range(ucache::VMA* vma_p, uint64_t start, uint64_t size): 
		offset_b(start), len(size), vma(vma_p){}

	void update_bounds(uint64_t start, uint64_t size){
		offset_b = start;
		len = size;
	}

	uintptr_t addr_at_offset(uint64_t pos){
		return reinterpret_cast<uint64_t>(vma->start)+pos;
	}

	uintptr_t first_addr(){
		return addr_at_offset(offset_b);
	}

	uintptr_t last_addr(){
		return addr_at_offset(offset_b+len);
	}
};

extern std::map<int, row_group_range*> currently_scanned_ranges;

//extern std::atomic<uint64_t> prefetch_count[64];
//extern uint64_t prefetch_per_core;
//extern rte_ring* prefetch_rings[64];
void parquet_prefetch(ucache::VMA* vma, void* addr, ucache::PrefetchList pl);
void parquet_account_prefetch(ucache::Buffer* buf);

class ThriftFileTransport : public duckdb_apache::thrift::transport::TVirtualTransport<ThriftFileTransport> {
public:
	static constexpr uint64_t PREFETCH_FALLBACK_BUFFERSIZE = 1000000;
	//static constexpr uint64_t PREFETCH_RING_SIZE = 2 << 14; // increase this ?
	ThriftFileTransport(ucache::VMA* vma_p, bool prefetch_mode_p)
	    : vma(vma_p), location(0), size(vma->file->size), prefetch_mode(prefetch_mode_p)
			{
				/*if(prefetch_rings[sched::cpu::current()->id] == NULL){
					prefetch_rings[sched::cpu::current()->id] = (rte_ring*)malloc(rte_ring_get_memsize(PREFETCH_RING_SIZE));
					ucache::assert_crash(rte_ring_init(prefetch_rings[sched::cpu::current()->id], "prefetch_ring", PREFETCH_RING_SIZE, 0) == 0);
					vma->callback_implems.prefetch_pol = parquet_prefetch;
					vma->callback_implems.post_ReadyToInsertToCached_callback_implem = parquet_account_prefetch;
					vma->callback_implems.misprediction_callback_implem = parquet_account_prefetch;
				}*/
			}

	uint32_t read(uint8_t *buf, uint32_t len) {
		memcpy(buf, reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(vma->start)+location), len);
		location += len;
		return len;
	}

	uint64_t computeNbBuffersToPrefetch(uint64_t len){
		uint64_t ret = len % vma->pageSize == 0 ? len/vma->pageSize : (len+vma->pageSize)/vma->pageSize;
		return ret;
	}

	// Prefetch a single buffer
	void Prefetch(idx_t pos, uint64_t len) {
		RegisterPrefetch(pos, len);
		/*u64 nbToPrefetch = computeNbBuffersToPrefetch(len);
		std::vector<ucache::Buffer*> pl;
		pl.reserve(nbToPrefetch);
		for(uint64_t i = 0; i < nbToPrefetch; i++){
			uint64_t id = (pos+i*vma->pageSize)/vma->pageSize;
			pl.push_back(vma->buffers[id]);
		}
		ucache::uCacheManager->prefetch(vma, pl);*/
	}

	// Register a buffer for prefixing
	void RegisterPrefetch(idx_t pos, uint64_t len, bool can_merge = true) {
		int coreID = sched::cpu::current()->id;
		row_group_range* group_range;
		if(currently_scanned_ranges.find(coreID) != currently_scanned_ranges.end()){
			group_range = currently_scanned_ranges.at(coreID);
			group_range->update_bounds(pos, len);
		}else{
			currently_scanned_ranges.insert({coreID, new row_group_range(vma, pos, len)});
		}
		/*u64 nbToPrefetch = computeNbBuffersToPrefetch(len);
		void** l = (void**)malloc(nbToPrefetch * sizeof(void*));
		for(uint64_t i = 0; i < nbToPrefetch; i++){
			uint64_t id = (pos+i*vma->pageSize)/vma->pageSize;
			l[i] = (void*)id;
		}
		ucache::assert_crash(rte_ring_enqueue_bulk(prefetch_rings[sched::cpu::current()->id], l, nbToPrefetch, NULL) == nbToPrefetch);
		*/
	}

	// Prevents any further merges, should be called before PrefetchRegistered
	void FinalizeRegistration() {}

	// Prefetch all previously registered ranges
	void PrefetchRegistered() {
		/*std::vector<ucache::Buffer*> pl;
		pl.reserve(ucache::uCacheManager->batch);
		int dequeued = rte_ring_dequeue_burst(prefetch_ring, (void**)pl.data(), ucache::uCacheManager->batch, NULL);
		if(dequeued > 0){
			ucache::uCacheManager->prefetch(vma, pl);
		}*/
	}

	void ClearPrefetch() {
		//rte_ring_reset(prefetch_rings[sched::cpu::current()->id]);
	}

	void Skip(idx_t skip_count) {
		location += skip_count;
	}

	bool HasPrefetch() const {
		return currently_scanned_ranges.find(sched::cpu::current()->id) != currently_scanned_ranges.end();
		//return !rte_ring_empty(prefetch_rings[sched::cpu::current()->id]);
	}

	void SetLocation(idx_t location_p) {
		location = location_p;
	}

	idx_t GetLocation() {
		return location;
	}
	idx_t GetSize() {
		return size;
	}

private:
	ucache::VMA* vma;
	idx_t location;
	idx_t size;

	// Whether the prefetch mode is enabled. In this mode the DirectIO flag of the handle will be set and the parquet
	// reader will manage the read buffering.
	bool prefetch_mode;
};

} // namespace duckdb
