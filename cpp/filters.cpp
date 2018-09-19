/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

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
