/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

#include <algorithm>
#include <any>
#include <chrono>
#include <deque>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <vector>

// #include <execinfo.h>
// #include <signal.h>

#include "tbb/concurrent_hash_map.h"
#include "tbb/concurrent_queue.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_vector.h"
#include "tbb/parallel_sort.h"
#include "tbb/task_arena.h"
#include "tbb/task_group.h"
#include "tbb/task_scheduler_init.h"

using namespace std;
using namespace tbb;

#include "string_slice.cpp"
#include "istream_wrapper.cpp"
#include "source.cpp"
#include "node.cpp"
#include "pipeline.cpp"
#include "filters.cpp"
#include "string_scanner.cpp"
#include "sam_types.cpp"
#include "filter_pipeline.cpp"
#include "simple_filters.cpp"
#include "mark_duplicates.cpp"

template<typename F>
void timed_run (bool timed, const string& msg, const F& f) {
	if (timed) {
		cerr << msg;
		auto start = chrono::steady_clock::now();
		f();
		auto end = chrono::steady_clock::now();
		chrono::duration<double> diff = end-start;
		cerr << "Elapsed time: " << diff.count() << " s.\n";
	} else {
		f();
	}
}

void run_best_practices_pipeline_intermediate_sam (istream& input, ostream& output, const string_slice& sorting_order, const vector<header_filter>& filters, const vector<header_filter>& filters2, bool timed) {
	sam filtered_reads;
	chrono::duration<double> between;
	timed_run (timed, "Reading SAM into memory and applying filters.\n", [&](){
			stream_pipeline_input in(input);
			sam_pipeline_output out(filtered_reads);
			between = in.run_pipeline(out, filters, sorting_order);
		});
	if (timed) {
		cerr << "Time between phases: " << between.count() << "s.\n";
	}
	timed_run (timed, "Write to file.\n", [&](){
			sam_pipeline_input in(filtered_reads);
			stream_pipeline_output out(output);
			in.run_pipeline(out, filters2, (sorting_order == unsorted) ? unsorted : keep);
		});
}

void run_best_practices_pipeline (istream& input, ostream& output, const string_slice& sorting_order, const vector<header_filter>& filters, bool timed) {
	timed_run (timed, "Running pipeline.\n", [&](){
			stream_pipeline_input in(input);
			stream_pipeline_output out(output);
			in.run_pipeline(out, filters, sorting_order);
		});
}

void elprep_filter_script (list<string>& args) {
	auto sorting_order = keep;
	auto timed = false;
	header_filter replace_ref_seq_dict_filter = nullptr;
	header_filter remove_unmapped_reads_filter = nullptr;
	header_filter replace_read_group_filter = nullptr;
	header_filter mark_duplicates_filter = nullptr;
	header_filter remove_duplicates_filter = nullptr;
	auto input = args.front(); args.pop_front();
	auto output = args.front(); args.pop_front();
	while (!args.empty()) {
		auto entry = args.front(); args.pop_front();
		if (entry == "--replace-reference-sequences") {
			auto ref_seq_dict = args.front(); args.pop_front();
			replace_ref_seq_dict_filter = replace_reference_sequence_dictionary_from_sam_file(ref_seq_dict);
		} else if (entry == "--filter-unmapped-reads") {
			remove_unmapped_reads_filter = filter_unmapped_reads;
		} else if (entry == "--filter-unmapped-reads-strict") {
			remove_unmapped_reads_filter = filter_unmapped_reads_strict;
		} else if (entry == "--replace-read-group") {
			auto read_group_string = args.front(); args.pop_front();
			replace_read_group_filter = add_or_replace_read_group(string_scanner(read_group_string).parse_sam_header_line_from_string());
		} else if (entry == "--mark-duplicates") {
			mark_duplicates_filter = mark_duplicates(false);
		} else if (entry == "--mark-duplicates-deterministic") {
			mark_duplicates_filter = mark_duplicates(true);
		} else if (entry == "--remove-duplicates") {
			remove_duplicates_filter = filter_duplicate_reads;
		} else if (entry == "--sorting-order") {
			auto so = args.front(); args.pop_front();
			if (so == "keep") sorting_order = keep;
			else if (so == "unknown") sorting_order = unknown;
			else if (so == "unsorted") sorting_order = unsorted;
			else if (so == "queryname") sorting_order = queryname;
			else if (so == "coordinate") sorting_order = coordinate;
			else throw runtime_error("Unknown sorting order.");
		} else if (entry == "--nr-of-threads") {
			args.pop_front();
			// ignore
		} else if (entry == "--timed") {
			timed = true;
		} else if ((entry == "--filter-non-exact-mapping-reads") ||
							 (entry == "--filter-non-exact-mapping-reads-strict") ||
							 (entry == "--filter-non-overlapping-reads") ||
							 (entry == "--remove-optional-fields") ||
							 (entry == "--keep-optional-fields") ||
							 (entry == "--clean-sam") ||
							 (entry == "--profile") ||
							 (entry == "--reference-t") ||
							 (entry == "--reference-T") ||
							 (entry == "--rename-chromosomes")) {
			throw runtime_error(entry + " not supported");
		} else {
			throw runtime_error("unknown command line option");
		}
	}
	vector<header_filter> filters, filters2;
	if (input != "/dev/stdin") {throw runtime_error("filenames not supported yet.\n");}
	if (output != "/dev/stdout") {throw runtime_error("filenames not supported yet.\n");}
	if (remove_unmapped_reads_filter != nullptr) {filters.push_back(remove_unmapped_reads_filter);}
	if (replace_ref_seq_dict_filter != nullptr) {filters.push_back(replace_ref_seq_dict_filter);}
	if (replace_read_group_filter != nullptr) {filters.push_back(replace_read_group_filter);}
	if ((replace_ref_seq_dict_filter != nullptr) || (mark_duplicates_filter != nullptr) ||
			(sorting_order == coordinate) || (sorting_order == queryname)) {
		filters.push_back(add_refid);
	}
	if (mark_duplicates_filter != nullptr) {filters.push_back(mark_duplicates_filter);}
	filters.push_back(filter_optional_reads);
	if (remove_duplicates_filter != nullptr) {filters2.push_back(remove_duplicates_filter);}
	ifstream fin(input);
	ofstream fout(output);
	if ((mark_duplicates_filter != nullptr) || (sorting_order == coordinate) || (sorting_order == queryname) ||
			((replace_ref_seq_dict_filter != nullptr) && (sorting_order == keep))) {
		run_best_practices_pipeline_intermediate_sam(fin, fout, sorting_order, filters, filters2, timed);
	} else {
		run_best_practices_pipeline(fin, fout, sorting_order, filters, timed);
	}
}

/*
void handler(int sig) {
	void *array[10];
	size_t size;

	// get void*'s for all entries on the stack
	size = backtrace(array, 256);

	// print out all the frames to stderr
	fprintf(stderr, "Error: signal %d:\n", sig);
	backtrace_symbols_fd(array, size, STDERR_FILENO);
	exit(1);
}
*/

int main (int argc, char* argv[]) {
	//	signal(SIGSEGV, handler);

	list<string> args;
	for (auto i = 1; i < argc; ++i) {
		args.emplace_back(argv[i]);
	}

	cerr << "elPrep rudimentary C++ version.\n";
	if (args.size() < 1) {
		cerr << "Incorrect number of parameters.\n";
		return 1;
	} else {
		if (args.front() == "split") {
			cerr << "split command not implemented yet.\n";
			return 1;
		} else if (args.front() == "merge") {
			cerr << "merge comand not implemented yet.\n";
			return 1;
		} else if (args.front() == "filter") {
			args.pop_front();
			elprep_filter_script(args);
		}
	}
}
