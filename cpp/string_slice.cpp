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

class string_slice {
public:
	shared_ptr<string> storage;
	int index, count;

	string_slice () : storage(0), index(0), count(0) {}

	string_slice (const shared_ptr<string>& s, int index = 0, int count = -1) :
		storage(count == 0 ? 0 : s),
		index(index),
		count(count < 0 ? s->size()-index : count)
	{}

	string_slice (const string_slice& s, int index = 0, int count = -1) :
		storage(count == 0 ? 0 : s.storage),
		index(s.index+index),
		count(count < 0 ? s.size()-index : count)
	{}

	string_slice (const string& str, int index = 0, int count = -1) :
		storage(count == 0 ? nullptr : make_shared<string>(str)),
		index(index),
		count(count < 0 ? str.length() - index : count)
	{}

	string_slice& operator= (const string_slice& s) {
		storage = s.storage;
		index = s.index;
		count = s.count;
		return *this;
	}

	inline int size() const {return count;}

	inline string::value_type* begin() const {
		return &storage->operator[](index);
	}

	inline string::value_type* end () const {
		return &storage->operator[](index+count);
	}

	inline auto& operator[] (int n) {
		return storage->operator[](index+n);
	}

	inline const auto& operator[] (int n) const {
		return storage->operator[](index+n);
	}

	inline bool is_null () const {
		return (storage == 0) || (count == 0);
	}
};

inline ostream& operator<< (ostream& out, const string_slice& s) {
	out.write(s.begin(), s.size());
	return out;
}

inline bool starts_with(const string_slice& prefix, const string_slice& sequence) {
	auto len = prefix.size();
	return (len <= sequence.size()) &&
		(strncmp(prefix.begin(), sequence.begin(), len) == 0);
}

inline int compare (const string_slice& s1, const string_slice& s2) {
	auto len1 = s1.size();
	auto len2 = s2.size();
	auto res = strncmp(&s1[0], &s2[0], min(len1, len2));
	return (res == 0) ? len1-len2 : res;
}

inline bool operator== (const string_slice& s1, const string_slice& s2) {
	return compare(s1, s2) == 0;
}

inline bool operator!= (const string_slice& s1, const string_slice& s2) {
	return compare(s1, s2) != 0;
}

inline bool operator< (const string_slice& s1, const string_slice& s2) {
	return compare(s1, s2) < 0;
}

inline bool operator> (const string_slice& s1, const string_slice& s2) {
	return compare(s1, s2) > 0;
}

size_t tbb_hasher(const string_slice& s) {
	size_t result = 13;
	for (auto p = s.begin(); p != s.end(); ++p) {
		result = 29 * result + *p;
	}
	return result;
}

namespace std {
	template<> class hash<string_slice> {
	public:
		inline size_t operator()(const string_slice& s) const {
			return tbb_hasher(s);
		}
	};
}

using string_map = unordered_map<string_slice, string_slice>;

template<typename F> int find(const vector<string_map>& dict, const F& f) {
	for (auto index = 0; index < dict.size(); index++) {
		if (f(dict[index])) return index;
	}
	return -1;
}
