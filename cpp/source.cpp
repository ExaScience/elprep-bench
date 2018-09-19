/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

class source {
public:
	virtual ~source() {}
	virtual int prepare() = 0;
	virtual int fetch(int size) = 0;
	virtual any data() = 0;
};

template<typename T> class deque_source : public source {
public:
	deque<T>& v;
	shared_ptr<deque<T>> d;

	deque_source(deque<T>& v) : v(v), d(nullptr) {}

	virtual ~deque_source() {}

	virtual int prepare() {
		return v.size();
	}

	virtual int fetch(int n) {
		auto begin = v.begin();
		auto end = min(begin+n, v.end());
		auto sz = end-begin;
		if (sz == 0) {
			d = nullptr;
			return 0;
		}
		auto w = make_shared<deque<T>>();
		move(begin, end, back_inserter(*w));
		v.erase(begin, end);
		d = w;
		return sz;
	}

	virtual any data() {
		return d;
	}
};

class istream_source : public source {
public:
	istream_wrapper& in;
	shared_ptr<deque<string_slice>> d;

	istream_source(istream_wrapper& in) : in(in), d(nullptr) {}

	virtual ~istream_source() {}

	virtual int prepare() {
		return -1;
	}

	virtual int fetch(int n) {
		auto result = make_shared<deque<string_slice>>();
		auto fetched = 0;
		for (; fetched < n; fetched++) {
			auto [line, ok] = in.getline();
			if (!ok) break;
			result->push_back(line);
		}
		d = (fetched == 0) ? nullptr : result;
		return fetched;
	}

	virtual any data() {
		return d;
	}
};
