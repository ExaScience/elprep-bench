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

class pipeline;

enum node_kind { ordered, sequential, parallel };

using receiver = function<any(int, any)>;
using finalizer = function<void()>;
using filter = function<pair<receiver, finalizer>(pipeline&, node_kind, int&)>;

pair<vector<receiver>, vector<finalizer>> compose_filters(pipeline& p, node_kind kind, int& data_size, const vector<filter>& filters) {
	vector<receiver> receivers;
	vector<finalizer> finalizers;
	for (auto& f: filters) {
		auto [receiver, finalizer] = f(p, kind, data_size);
		if (receiver != nullptr) {
			receivers.push_back(receiver);
		}
		if (finalizer != nullptr) {
			finalizers.push_back(finalizer);
		}
	}
	return make_pair(receivers, finalizers);
}

void feed_forward(pipeline& p, int index, int seq_no, any);

void _feed(pipeline& p, const vector<receiver>& receivers, int index, int seqno, any data) {
	for (auto& r: receivers) {
		data = r(seqno, data);
	}
	feed_forward(p, index, seqno, data);
}

class node {
public:
	virtual ~node() noexcept(false) {};
	virtual bool try_merge(shared_ptr<node> n) = 0;
	virtual bool begin(pipeline& p, int index, int& data_size) = 0;
	virtual void feed(pipeline& p, int index, int seqno, any data) = 0;
	virtual void end() = 0;
};

class parnode : public node {
public:
	task_group g;
	vector<filter> filters;
	vector<receiver> receivers;
	vector<finalizer> finalizers;

	parnode(const vector<filter>& filters) : filters(filters) {}

	virtual ~parnode() {}

	virtual bool try_merge(shared_ptr<node> n) {
		auto nxt = dynamic_pointer_cast<parnode>(n);
		if (nxt) {
			for (auto& f: nxt->filters) {
				filters.push_back(f);
			}
			for (auto& r: nxt->receivers) {
				receivers.push_back(r);
			}
			for (auto& f: nxt->finalizers) {
				finalizers.push_back(f);
			}
			return true;
		}
		return false;
	}

	virtual bool begin(pipeline& p, int index, int& data_size) {
		tie(receivers, finalizers) = compose_filters(p, parallel, data_size, filters);
		filters.clear();
		return (receivers.size() > 0) || (finalizers.size() > 0);
	}

	virtual void feed(pipeline& p, int index, int seqno, any data) {
		g.run([&p, this, index, seqno, data](){
				_feed(p, receivers, index, seqno, data);
				});
	}

	virtual void end() {
		auto st = g.wait();
		if (st != complete) {
			throw runtime_error("tbb::task_group state not complete after wait");
		}
		for (auto& f: finalizers) {
			f();
		}
		receivers.clear();
		finalizers.clear();
	}
};

class seqnode : public node {
public:
	node_kind kind;
	task_group g;
	concurrent_bounded_queue<pair<int, any>> channel;
	vector<filter> filters;
	vector<receiver> receivers;
	vector<finalizer> finalizers;

	seqnode(node_kind kind, const vector<filter>& filters) : kind(kind), filters(filters) {}

	virtual ~seqnode() {}

	virtual bool try_merge(shared_ptr<node> n) {
		auto nxt = dynamic_pointer_cast<seqnode>(n);
		if (nxt) {
			if (nxt->kind == ordered) {
				kind = ordered;
			}
			for (auto& f: nxt->filters) {
				filters.push_back(f);
			}
			for (auto& r: nxt->receivers) {
				receivers.push_back(r);
			}
			for (auto& f: nxt->finalizers) {
				finalizers.push_back(f);
			}
			return true;
		}
		return false;
	}

	virtual bool begin(pipeline& p, int index, int& data_size) {
		tie(receivers, finalizers) = compose_filters(p, kind, data_size, filters);
		filters.clear();
		auto keep = (receivers.size() > 0) || (finalizers.size() > 0);
		if (keep) {
			channel.set_capacity(2 * task_scheduler_init::default_num_threads());
			switch (kind) {
			case sequential:
				g.run([&p, this, index]() {
						pair<int, any> batch;
						while (true) {
							channel.pop(batch);
							if (batch.first < 0) {
								break;
							}
							_feed(p, receivers, index, batch.first, batch.second);
						}
					});
				break;
			case ordered:
				g.run([&p, this, index]() {
						unordered_map<int, any> stash;
						auto run = 0;
						pair<int, any> batch;
						while (true) {
							channel.pop(batch);
							if (batch.first < 0) {
								break;
							} else if (batch.first > run) {
								stash.insert(batch);
							} else {
								_feed(p, receivers, index,  batch.first, batch.second);
								while (true) {
									run++;
									auto entry = stash.find(run);
									if (entry == stash.end()) {
										break;
									}
									batch = *entry;
									stash.erase(entry);
									_feed(p, receivers, index, batch.first, batch.second);
								}
							}
						}
					});
				break;
			}
		}
		return keep;
	}

	virtual void feed(pipeline& p, int index, int seqno, any data) {
		channel.emplace(seqno, data);
	}

	virtual void end() {
		channel.emplace(-1, nullptr);
		auto st =	g.wait();
		if (st != complete) {
			throw runtime_error("tbb::task_group state not complete after wait");
		}
		for (auto& f: finalizers) {
			f();
		}
		receivers.clear();
		finalizers.clear();
	}
};
