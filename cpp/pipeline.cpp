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

class pipeline {
public:
	shared_ptr<source> src;
	vector<shared_ptr<node>> nodes;
	int nof_batches;

	pipeline() : nof_batches(0) {}
};

int nof_batches(pipeline& p, int n) {
	if (n < 1) {
		auto nof_batches = p.nof_batches;
		if (nof_batches < 1) {
			nof_batches = 2 * task_scheduler_init::default_num_threads();
			p.nof_batches = nof_batches;
		}
		return nof_batches;
	} else {
		p.nof_batches = n;
		return n;
	}
}

const int batch_inc = 1024;
const int max_batch_size = 0x2000000;

int next_batch_size(int batch_size) {
	auto result = batch_size + batch_inc;
	if (result > max_batch_size) {
		result = max_batch_size;
	}
	return result;
}

chrono::duration<double> run(pipeline& p) {
	auto start = chrono::steady_clock::now();
	auto data_size = p.src->prepare();
	auto filtered_size = data_size;
	for (auto index = 0; index < p.nodes.size();) {
		if (p.nodes[index]->begin(p, index, filtered_size)) {
			index++;
		} else {
			p.nodes.erase(p.nodes.begin()+index);
		}
	}
	if (p.nodes.size() > 0) {
		for (auto index = 0; index < p.nodes.size()-1;) {
			if (p.nodes[index]->try_merge(p.nodes[index+1])) {
				p.nodes.erase(p.nodes.begin()+index+1);
			} else {
				index++;
			}
		}
		if (data_size < 0) {
			auto seq_no = 0;
			auto batch_size = batch_inc;
			while (p.src->fetch(batch_size) > 0) {
				p.nodes[0]->feed(p, 0, seq_no, p.src->data());
				seq_no++;
				batch_size = next_batch_size(batch_size);
			}
		} else {
			auto batch_size = ((data_size - 1) / nof_batches(p, 0)) + 1;
			if (batch_size == 0) {
				batch_size = 1;
			}
			for (auto seq_no = 0; p.src->fetch(batch_size) > 0; seq_no++) {
				p.nodes[0]->feed(p, 0, seq_no, p.src->data());
			}
		}
	}
	for (auto& node: p.nodes) {
		node->end();
	}
	auto end = chrono::steady_clock::now();
	return end-start;
}

void feed_forward(pipeline& p, int index, int seq_no, any data) {
	index++;
	if (index < p.nodes.size()) {
		p.nodes[index]->feed(p, index, seq_no, data);
	}
}
