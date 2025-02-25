//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark_configuration.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

enum class BenchmarkMetaType { NONE, INFO, QUERY };
enum class BenchmarkProfileInfo { NONE, NORMAL, DETAILED };

struct BenchmarkConfiguration {
public:
	constexpr static size_t DEFAULT_NRUNS = 5;
	constexpr static size_t DEFAULT_IDLE_TIME = 0;
	constexpr static size_t DEFAULT_TRANSITION_TIME = 0;
	constexpr static size_t DEFAULT_TIMEOUT = 30;

public:
	vector<string> name_patterns;
	vector<uint32_t> nruns;
	vector<uint32_t> idle_time;
	vector<uint32_t> transition_time;
	BenchmarkMetaType meta = BenchmarkMetaType::NONE;
	BenchmarkProfileInfo profile_info = BenchmarkProfileInfo::NONE;
	optional_idx timeout_duration = optional_idx(DEFAULT_TIMEOUT);
};

} // namespace duckdb
