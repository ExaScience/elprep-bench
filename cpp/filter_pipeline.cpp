// elprep-bench.
// Copyright (c) 2018-2023 imec vzw.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version, and Additional Terms
// (see below).

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License and Additional Terms along with this program. If not, see
// <https://github.com/ExaScience/elprep-bench/blob/master/LICENSE.txt>.

typedef function<bool(const shared_ptr<sam_alignment>&)> alignment_filter;
typedef function<alignment_filter(const shared_ptr<sam_header>&)> header_filter;

class pipeline_output {
public:
	virtual void add_nodes(pipeline& p, const shared_ptr<sam_header>& header, const string_slice& sorting_order) = 0;
	virtual ~pipeline_output() {}
};

class pipeline_input {
public:
	virtual chrono::duration<double> run_pipeline (pipeline_output& output, const vector<header_filter>& filters, const string_slice& sorting_order) = 0;
	virtual ~pipeline_input() {}
};

pair<receiver, finalizer> alignment_to_string(pipeline& p, node_kind kind, int& data_size) {
	return make_pair([](int seq_no, any data) -> any {
			try {
				auto alns = any_cast<shared_ptr<deque<shared_ptr<sam_alignment>>>>(data);
				auto result = make_shared<stringstream>();
				for (auto& aln: *alns) {
					aln->format(*result);
				}
				return result;
			} catch (bad_any_cast& ex) {
				throw runtime_error("unexpected type in alignment_to_string");
			}
		}, nullptr);
}

pair<receiver, finalizer> string_to_alignment(pipeline& p, node_kind kind, int& data_size) {
	return make_pair([](int seq_no, any data) -> any {
			try {
				auto strings = any_cast<shared_ptr<deque<string_slice>>>(data);
				auto alns = make_shared<deque<shared_ptr<sam_alignment>>>();
				for (auto& str: *strings) {
					alns->emplace_back(make_shared<sam_alignment>(str));
				}
				return alns;
			} catch (bad_any_cast& ex) {
				throw runtime_error("unexpected type in string_to_alignment");
			}
		}, nullptr);
}

class sam_pipeline_output : public pipeline_output {
public:
	sam& output;

	sam_pipeline_output(sam& output) : output(output) {}

	virtual ~sam_pipeline_output() {}

	virtual void add_nodes(pipeline& p, const shared_ptr<sam_header>& header, const string_slice& sorting_order) {
		output.header = header;
		if ((sorting_order == keep) || (sorting_order == unknown)) {
			p.nodes.emplace_back(make_shared<seqnode>(ordered, vector<filter>{to_deque(output.alignments)}));
		} else if (sorting_order == coordinate) {
			p.nodes.emplace_back(make_shared<seqnode>(sequential, vector<filter>{
						to_deque(output.alignments), finalize([this](){parallel_sort(output.alignments, coordinate_less);})
							}));
		} else if (sorting_order == queryname) {
			p.nodes.emplace_back(make_shared<seqnode>(sequential, vector<filter>{
						to_deque(output.alignments), finalize([this](){parallel_sort(output.alignments, queryname_less);})
							}));
		} else if (sorting_order == unsorted) {
			p.nodes.emplace_back(make_shared<seqnode>(sequential, vector<filter>{
						to_deque(output.alignments)
							}));
		} else {
			throw runtime_error("Unknown sorting order.");
		}
	}
};

class stream_pipeline_output : public pipeline_output {
public:
	ostream& output;

	stream_pipeline_output(ostream& output) : output(output) {}

	virtual ~stream_pipeline_output() {}

	virtual void add_nodes(pipeline& p, const shared_ptr<sam_header>& header, const string_slice& sorting_order) {
		header->format(output);
		node_kind kind;
		if ((sorting_order == keep) || (sorting_order == unknown)) {
			kind = ordered;
		} else if ((sorting_order == coordinate) || (sorting_order == queryname)) {
			throw runtime_error("Sorting on files not supported.");
		} else if (sorting_order == unsorted) {
			kind = sequential;
		} else {
			throw runtime_error("Unknown sorting order.");
		}
		p.nodes.emplace_back(make_shared<parnode>(vector<filter>{alignment_to_string}));
		p.nodes.emplace_back(make_shared<seqnode>(kind, vector<filter>{
					receive([this](int seq_no, any data) -> any {
							try {
								auto ss = any_cast<shared_ptr<stringstream>>(data);
								output << ss->rdbuf();
								return data;
							} catch (bad_any_cast& ex) {
								throw runtime_error("unexpected type in stream_pipeline_output");
							}
						})
						}));
	}
};

receiver compose_filters(const shared_ptr<sam_header>& header, const vector<header_filter>& hdr_filters) {
	vector<alignment_filter> aln_filters; aln_filters.reserve(hdr_filters.size());
	for (auto& f: hdr_filters) {
		auto aln_filter = f(header);
		if (aln_filter) {
			aln_filters.push_back(aln_filter);
		}
	}
	if (aln_filters.size() > 0) {
		return [aln_filters](int seq_no, any data) -> any {
			auto alns = any_cast<shared_ptr<deque<shared_ptr<sam_alignment>>>>(data);
			for (auto i = 0; i < alns->size(); i++) {
				auto& aln = alns->operator[](i);
				for (auto& f: aln_filters) {
					if (!f(aln)) {
						for (auto j = i+1; j < alns->size(); j++) {
							auto& aln = alns->operator[](j);
							auto keep = true;
							for (auto& f: aln_filters) {
								if (!f(aln)) {
									keep = false; break;
								}
							}
							if (keep) {
								alns->operator[](i) = aln;
								i++;
							}
						}
						alns->resize(i);
						return alns;
					}
				}
			}
			return data;
		};
	}
	return nullptr;
}

const string_slice& effective_sorting_order (const string_slice& sorting_order, const shared_ptr<sam_header>& header, const string_slice& original_sorting_order) {
	const string_slice& so = (sorting_order == keep) ? original_sorting_order : sorting_order;
	auto current_sorting_order = header->get_hd_so();
	if ((so == coordinate) || (so == queryname)) {
		if (current_sorting_order == so) {
			return keep;
		}
		header->set_hd_so(so);
	} else if ((so == unknown) || (so == unsorted)) {
		if (current_sorting_order != so) {
			header->set_hd_so(so);
		}
	}
	return so;
}

class sam_pipeline_input : public pipeline_input {
public:
	sam& input;

	sam_pipeline_input(sam& input) : input(input) {}

	virtual ~sam_pipeline_input() {}

	virtual chrono::duration<double> run_pipeline(pipeline_output& output, const vector<header_filter>& hdr_filters, const string_slice& so) {
		auto header = input.header;
		auto& alns = input.alignments;
		auto original_sorting_order = header->get_hd_so();
		auto aln_filter = compose_filters(header, hdr_filters);
		auto sorting_order = effective_sorting_order(so, header, original_sorting_order);
		auto out = dynamic_cast<sam_pipeline_output*>(&output);
		if ((out != nullptr) && (tbb::this_task_arena::max_concurrency() <= 3)) {
			auto start = chrono::steady_clock::now();
			out->output.header = header;
			out->output.alignments.swap(alns);
			if (aln_filter) {
				aln_filter(0, out->output.alignments);
			}
			if (sorting_order == coordinate) {
				sort(out->output.alignments.begin(), out->output.alignments.end(), coordinate_less);
			} else if (sorting_order == queryname) {
				sort(out->output.alignments.begin(), out->output.alignments.end(), queryname_less);
			} else if ((sorting_order == keep) ||
								 (sorting_order == unknown) ||
								 (sorting_order == unsorted)) {
				// nothing to do
			} else {
				throw runtime_error("Unknown sorting order.");
			}
			auto end = chrono::steady_clock::now();
			return end-start;
		}
		pipeline p;
		p.src = make_shared<deque_source<shared_ptr<sam_alignment>>>(alns);
		if (aln_filter) {
			p.nodes.emplace_back(make_shared<parnode>(vector<filter>{receive(aln_filter)}));
		}
		output.add_nodes(p, header, sorting_order);
		return run(p);
	}
};

class stream_pipeline_input : public pipeline_input {
public:
	istream_wrapper input;

	stream_pipeline_input(istream& input) : input(input) {}

	virtual ~stream_pipeline_input() {}

	virtual chrono::duration<double> run_pipeline(pipeline_output& output, const vector<header_filter>& hdr_filters, const string_slice& so) {
		auto header = make_shared<sam_header>(input);
		auto original_sorting_order = header->get_hd_so();
		auto aln_filter = compose_filters(header, hdr_filters);
		auto sorting_order = effective_sorting_order(so, header, original_sorting_order);
		pipeline p;
		p.src = make_shared<istream_source>(input);
		p.nodes.emplace_back(make_shared<parnode>(vector<filter>{string_to_alignment}));
		if (aln_filter) {
			p.nodes.emplace_back(make_shared<parnode>(vector<filter>{receive(aln_filter)}));
		}
		output.add_nodes(p, header, sorting_order);
		return run(p);
	}
};
