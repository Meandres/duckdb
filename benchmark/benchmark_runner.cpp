#include "benchmark_runner.hpp"

#include "duckdb/common/profiler.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb.hpp"
#include "duckdb_benchmark.hpp"
#include "interpreted_benchmark.hpp"

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "re2/re2.h"

#include <fstream>
#include <sstream>
#include <thread>

using namespace duckdb;

void BenchmarkRunner::RegisterBenchmark(Benchmark *benchmark) {
	GetInstance().benchmarks.push_back(benchmark);
}

Benchmark::Benchmark(bool register_benchmark, string name, string group)
    : name(name), group(group), nruns(DEFAULT_NRUNS), idle_time(0) {
	if (register_benchmark) {
		BenchmarkRunner::RegisterBenchmark(this);
	}
}

static void listFiles(FileSystem &fs, const string &path, std::function<void(const string &)> cb) {
	fs.ListFiles(path, [&](const string &fname, bool is_dir) {
		string full_path = fs.JoinPath(path, fname);
		if (is_dir) {
			// recurse into directory
			listFiles(fs, full_path, cb);
		} else {
			cb(full_path);
		}
	});
}

static bool endsWith(const string &mainStr, const string &toMatch) {
	return (mainStr.size() >= toMatch.size() &&
	        mainStr.compare(mainStr.size() - toMatch.size(), toMatch.size(), toMatch) == 0);
}

BenchmarkRunner::BenchmarkRunner() {
}

void BenchmarkRunner::InitializeBenchmarkDirectory() {
	auto fs = FileSystem::CreateLocal();
	// check if the database directory exists; if not create it
	if (!fs->DirectoryExists(DUCKDB_BENCHMARK_DIRECTORY)) {
		fs->CreateDirectory(DUCKDB_BENCHMARK_DIRECTORY);
	}
}

atomic<bool> is_active;
atomic<bool> timeout;

void sleep_thread(Benchmark *benchmark, BenchmarkRunner *runner, BenchmarkState *state, bool hotrun,
                  const optional_idx &optional_timeout) {
	if (!optional_timeout.IsValid()) {
		return;
	}
	auto timeout_duration = optional_timeout.GetIndex();

	// timeout is given in seconds
	// we wait 10ms per iteration, so timeout * 100 gives us the amount of
	// iterations
	for (size_t i = 0; i < (size_t)(timeout_duration * 100) && is_active; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	if (is_active) {
		timeout = true;
		benchmark->Interrupt(state);

		// wait again after interrupting
		for (size_t i = 0; i < (size_t)(timeout_duration * 100) && is_active; i++) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		if (is_active) {
			// still active - we might be stuck in an infinite loop
			// our interrupt is not working
			if (!hotrun) {
				runner->Log(StringUtil::Format("%s\t%d\t", benchmark->name, 0));
			}
			runner->LogResult("KILLED");
			exit(1);
		}
	}
}

void BenchmarkRunner::Log(string message) {
	fprintf(stderr, "%s", message.c_str());
	fflush(stderr);
}

void BenchmarkRunner::LogLine(string message) {
	fprintf(stderr, "%s\n", message.c_str());
	fflush(stderr);
}

void BenchmarkRunner::LogResult(string message) {
	LogLine(message);
	if (out_file.good()) {
		out_file << message << endl;
		out_file.flush();
	}
}

void BenchmarkRunner::LogOutput(string message) {
	if (log_file.good()) {
		log_file << message << endl;
		log_file.flush();
	}
}

void BenchmarkRunner::RunBenchmark(Benchmark *benchmark) {
	Profiler profiler;
	auto display_name = benchmark->DisplayName();

	auto state = benchmark->Initialize(configuration);
	auto nruns = benchmark->NRuns();
	auto idle_time = benchmark->IdleTime();
	for (size_t i = 0; i < nruns + 1; i++) {
		bool hotrun = i > 0;
		if (hotrun) {
			Log(StringUtil::Format("%s\t%d\t", benchmark->name, i));
		}
		if (hotrun && benchmark->RequireReinit()) {
			state = benchmark->Initialize(configuration);
		}
		is_active = true;
		timeout = false;
		std::thread interrupt_thread(sleep_thread, benchmark, this, state.get(), hotrun,
		                             benchmark->Timeout(configuration));

		profiler.Start();
		benchmark->Run(state.get());
		profiler.End();

		is_active = false;
		interrupt_thread.join();
		if (hotrun) {
			LogOutput(benchmark->GetLogOutput(state.get()));
			if (timeout) {
				// write timeout
				LogResult("TIMEOUT");
				break;
			} else {
				// write time
				auto verify = benchmark->Verify(state.get());
				if (!verify.empty()) {
					LogResult("INCORRECT");
					LogLine("INCORRECT RESULT: " + verify);
					LogOutput("INCORRECT RESULT: " + verify);
					break;
				} else {
					LogResult(std::to_string(profiler.Elapsed()));
				}
			}
		}
		benchmark->Cleanup(state.get());
		if (i < nruns) {
			std::this_thread::sleep_for(std::chrono::milliseconds(idle_time));
		}
	}
	benchmark->Finalize();
}

void BenchmarkRunner::RunBenchmarks() {
	LogLine("Starting benchmark run.");
	LogLine("name\trun\ttiming");
	for (auto &benchmark : benchmarks) {
		RunBenchmark(benchmark);
	}
}

void print_help() {
	fprintf(stderr, "Usage: benchmark_runner\n");
	fprintf(stderr, "              --list                 Show a list of all benchmarks\n");
	fprintf(stderr, "              --profile              Prints the query profile information\n");
	fprintf(stderr, "              --detailed-profile     Prints detailed query profile information\n");
	fprintf(stderr, "              --threads=n            Sets the amount of threads to use during execution (default: "
	                "hardware concurrency)\n");
	fprintf(stderr, "              --out=[file]           Move benchmark output to file\n");
	fprintf(stderr, "              --log=[file]           Move log output to file\n");
	fprintf(stderr, "              --info                 Prints info about the benchmark\n");
	fprintf(stderr, "              --query                Prints query of the benchmark\n");
	fprintf(stderr, "              --root-dir             Sets the root directory for where to store temp data and "
	                "look for the 'benchmarks' directory\n");
	fprintf(stderr, "              --disable-timeout      Disables killing the run after a certain amount of time has "
	                "passed (30 seconds by default)\n");
	fprintf(stderr,
	        "              [name_pattern]         Run only the benchmark which names match the specified name pattern, "
	        "e.g., DS.* for TPC-DS benchmarks\n");
}

enum ConfigurationError { None, BenchmarkNotFound, InfoWithoutBenchmarkName };

void LoadInterpretedBenchmarks(FileSystem &fs) {
	// load interpreted benchmarks
	listFiles(fs, "benchmark", [](const string &path) {
		if (endsWith(path, ".benchmark")) {
			new InterpretedBenchmark(path);
		}
	});
}

string parse_root_dir_or_default(const int arg_counter, char const *const *arg_values, FileSystem &fs) {
	// check if the user specified a different root directory
	for (int arg_index = 1; arg_index < arg_counter; ++arg_index) {
		string arg = arg_values[arg_index];
		if (arg == "--root-dir") {
			if (arg_index + 1 >= arg_counter) {
				fprintf(stderr, "Missing argument for --root-dir\n");
				print_help();
				exit(1);
			}
			auto path = arg_values[arg_index + 1];
			if (fs.IsPathAbsolute(path)) {
				return path;
			} else {
				return fs.JoinPath(FileSystem::GetWorkingDirectory(), path);
			}
		}
	}
	// default root directory is the duckdb root directory
	return DUCKDB_ROOT_DIRECTORY;
}
/**
 * Builds a configuration based on the passed arguments.
 */
void parse_arguments(const int arg_counter, char const *const *arg_values) {
	auto &instance = BenchmarkRunner::GetInstance();
	auto &benchmarks = instance.benchmarks;
	for (int arg_index = 1; arg_index < arg_counter; ++arg_index) {
		string arg = arg_values[arg_index];
		if (arg == "--list") {
			// list names of all benchmarks
			for (auto &benchmark : benchmarks) {
				fprintf(stdout, "%s\n", benchmark->name.c_str());
			}
			exit(0);
		} else if (arg == "--info") {
			// write info of benchmark
			instance.configuration.meta = BenchmarkMetaType::INFO;
		} else if (arg == "--profile") {
			// write info of benchmark
			instance.configuration.profile_info = BenchmarkProfileInfo::NORMAL;
		} else if (arg == "--detailed-profile") {
			// write info of benchmark
			instance.configuration.profile_info = BenchmarkProfileInfo::DETAILED;
		} else if (StringUtil::StartsWith(arg, "--nruns=")) {
			// write info of benchmark
			auto splits = StringUtil::Split(arg, '=');
			for (auto values : StringUtil::Split(splits[1], ','))
				instance.configuration.nruns.emplace_back(
				    Value(values).DefaultCastAs(LogicalType::UINTEGER).GetValue<uint32_t>());
		} else if (StringUtil::StartsWith(arg, "--idle_time=")) {
			// write info of benchmark
			auto splits = StringUtil::Split(arg, '=');
			for (auto values : StringUtil::Split(splits[1], ','))
				instance.configuration.idle_time.emplace_back(
				    Value(values).DefaultCastAs(LogicalType::UINTEGER).GetValue<uint32_t>());
		} else if (StringUtil::StartsWith(arg, "--transition_time=")) {
			// write info of benchmark
			auto splits = StringUtil::Split(arg, '=');
			for (auto values : StringUtil::Split(splits[1], ','))
				instance.configuration.transition_time.emplace_back(
				    Value(values).DefaultCastAs(LogicalType::UINTEGER).GetValue<uint32_t>());
		} else if (StringUtil::StartsWith(arg, "--threads=")) {
			// write info of benchmark
			auto splits = StringUtil::Split(arg, '=');
			instance.threads = Value(splits[1]).DefaultCastAs(LogicalType::UINTEGER).GetValue<uint32_t>();
		} else if (arg == "--root-dir") {
			// We've already handled this, skip it
			arg_index++;
		} else if (arg == "--query") {
			// write group of benchmark
			instance.configuration.meta = BenchmarkMetaType::QUERY;
		} else if (arg == "--disable-timeout") {
			instance.configuration.timeout_duration = optional_idx();
		} else if (StringUtil::StartsWith(arg, "--out=") || StringUtil::StartsWith(arg, "--log=")) {
			auto splits = StringUtil::Split(arg, '=');
			if (splits.size() != 2) {
				print_help();
				exit(1);
			}
			auto &file = StringUtil::StartsWith(arg, "--out=") ? instance.out_file : instance.log_file;
			file.open(splits[1]);
			if (!file.good()) {
				fprintf(stderr, "Could not open file %s for writing\n", splits[1].c_str());
				exit(1);
			}
		} else {
			instance.configuration.name_patterns.emplace_back(arg);
		}
	}
}

/**
 * Runs the benchmarks specified by the configuration if possible.
 * Returns an configuration error code.
 */
ConfigurationError run_benchmarks() {
	BenchmarkRunner::InitializeBenchmarkDirectory();

	auto &instance = BenchmarkRunner::GetInstance();
	auto &benchmarks = instance.benchmarks;
	auto &nruns = instance.configuration.nruns;
	auto &idle_time = instance.configuration.idle_time;
	auto &transition_time = instance.configuration.transition_time;
	if (!instance.configuration.name_patterns.empty()) {
		// run only benchmarks which names matches the passed name pattern.
		std::vector<int> benchmark_indices {};
		benchmark_indices.reserve(benchmarks.size());

		for (const auto &name_pattern : instance.configuration.name_patterns) {
			for (idx_t index = 0; index < benchmarks.size(); ++index) {
				if (RE2::FullMatch(benchmarks[index]->name, name_pattern)) {
					benchmark_indices.emplace_back(index);
				} else if (RE2::FullMatch(benchmarks[index]->group, name_pattern)) {
					benchmark_indices.emplace_back(index);
				}
			}
		}
		benchmark_indices.shrink_to_fit();

		if (benchmark_indices.empty()) {
			return ConfigurationError::BenchmarkNotFound;
		}

		for (uint32_t i {0}; i < benchmark_indices.size(); ++i) {
			if (nruns.size() > i)
				benchmarks[benchmark_indices[i]]->setNRuns(nruns[i]);
			if (idle_time.size() > i)
				benchmarks[benchmark_indices[i]]->setIdleTime(idle_time[i]);
		}

		if (instance.configuration.meta == BenchmarkMetaType::INFO) {
			// print info of benchmarks
			for (const auto &benchmark_index : benchmark_indices) {
				auto display_name = benchmarks[benchmark_index]->DisplayName();
				auto display_group = benchmarks[benchmark_index]->Group();
				auto subgroup = benchmarks[benchmark_index]->Subgroup();
				fprintf(stdout, "display_name:%s\ngroup:%s\nsubgroup:%s\n", display_name.c_str(), display_group.c_str(),
				        subgroup.c_str());
			}
		} else if (instance.configuration.meta == BenchmarkMetaType::QUERY) {
			for (const auto &benchmark_index : benchmark_indices) {
				auto query = benchmarks[benchmark_index]->GetQuery();
				if (query.empty()) {
					continue;
				}
				fprintf(stdout, "%s\n", query.c_str());
			}
		} else {
			unsigned i = 0;
			instance.LogLine("name\trun\ttiming");
			for (const auto &benchmark_index : benchmark_indices) {
				instance.RunBenchmark(benchmarks[benchmark_index]);
				if (transition_time.size() > i)
					std::this_thread::sleep_for(std::chrono::milliseconds(transition_time[i++]));
			}
		}
	} else {
		return ConfigurationError::InfoWithoutBenchmarkName;
	}
	return ConfigurationError::None;
}

void print_error_message(const ConfigurationError &error) {
	switch (error) {
	case ConfigurationError::BenchmarkNotFound:
		fprintf(stderr, "Benchmark to run could not be found.\n");
		break;
	case ConfigurationError::InfoWithoutBenchmarkName:
		fprintf(stderr, "Info requires benchmark name pattern.\n");
		break;
	case ConfigurationError::None:
		break;
	}
	print_help();
}
//
// int main(int argc, char **argv) {
// 	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
// 	// Set the working directory. We need to scan this before loading the benchmarks or parsing the other arguments
// 	string root_dir = parse_root_dir_or_default(argc, argv, *fs);
// 	FileSystem::SetWorkingDirectory(root_dir);
// 	// load interpreted benchmarks before doing anything else
// 	LoadInterpretedBenchmarks(*fs);
// 	parse_arguments(argc, argv);
// 	const auto configuration_error = run_benchmarks();
// 	if (configuration_error != ConfigurationError::None) {
// 		print_error_message(configuration_error);
// 		exit(1);
// 	}
// 	return 0;
// }
//
namespace duckdb {
void setup(int argc, char **argv) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	// Set the working directory. We need to scan this before loading the benchmarks or parsing the other arguments
	string root_dir = parse_root_dir_or_default(argc, argv, *fs);
	FileSystem::SetWorkingDirectory(root_dir);
	// load interpreted benchmarks before doing anything else
	LoadInterpretedBenchmarks(*fs);
	parse_arguments(argc, argv);
}

int run() {
	const auto configuration_error = run_benchmarks();
	if (configuration_error != ConfigurationError::None) {
		print_error_message(configuration_error);
		exit(1);
	}
	return 0;
}
}
