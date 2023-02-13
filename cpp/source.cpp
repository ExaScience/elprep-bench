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
