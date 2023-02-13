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

ilter receive(receiver receive) {
	return [=](pipeline& p, node_kind kind, int& data_size) -> pair<receiver, finalizer> {
		return make_pair(receive, nullptr);
	};
}

filter finalize(finalizer finalize) {
	return [=](pipeline& p, node_kind kind, int& data_size) -> pair<receiver, finalizer> {
		return make_pair(nullptr, finalize);
	};
}

filter receive_and_finalize(receiver receive, finalizer finalize) {
	return [=](pipeline& p, node_kind kind, int& data_size) -> pair<receiver, finalizer> {
		return make_pair(receive, finalize);
	};
}

template<typename T> filter to_deque(deque<T>& result) {
	return [&](pipeline&, node_kind kind, int& data_size) -> pair<receiver, finalizer> {
		if (kind == parallel) {
			auto mut = make_shared<mutex>();
			return make_pair([&result, mut](int seq_no, any data) -> any {
					try {
						auto v = any_cast<shared_ptr<deque<T>>>(data);
						if (v->size() > 0) {
							scoped_lock lock(*mut);
							copy(v->begin(), v->end(), back_inserter(result));
						}
						return data;
					} catch (bad_any_cast& ex) {
						throw runtime_error("unexpected type in parallel to_deque");
					}
				}, nullptr);
		} else {
			return make_pair([&result](int seq_no, any data) -> any {
					try {
						auto v = any_cast<shared_ptr<deque<T>>>(data);
						if (v->size() > 0) {
							copy(v->begin(), v->end(), back_inserter(result));
						}
					} catch (bad_any_cast& ex) {
						throw runtime_error("unexpected type in sequential to_deque");
					}
					return data;
				}, nullptr);
		}
	};
}
