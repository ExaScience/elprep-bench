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

inline void set_unique_entry(string_map& record, const pair<string_slice, string_slice>& entry) {
	auto found = record.find(entry.first);
	if (found == record.end()) {
		record.insert(entry);
	} else {
		throw new runtime_error("Key not unique in a string_map.");
	}
}

class string_scanner {
public:
	const string_slice str;
	size_t index;

	string_scanner (const string_slice& str) : str(str), index(0) {}

	inline char peek () const {return str[index];}

	inline size_t length() const {return str.size()-index;}

	inline void skip_whitespace () {
		for (auto i = index; i < str.size(); ++i) {
			if (str[i] != ' ') {
				index = i;
				return;
			}
		}
	}

	pair<char, bool> read_byte_until (char c) {
		char result = str[index];
		++index;
		if (index < str.size()) {
			if (str[index] == c) {
				++index;
				return make_pair(result, true);
			} else {
				throw runtime_error("Unexpected character in string_scanner::read_byte_until.");
			}
		} else {
			return make_pair(result, false);
		}
	}

	string_slice read_until(char c) {
		auto begin = index;
		auto len = str.size();
		for (auto end = index; end < len; ++end) {
			if (str[end] == c) {
				index = end+1;
				return string_slice(str, begin, end-begin);
			}
		}
		index = len;
		return string_slice(str, begin, len-begin);
	}

	pair<string_slice, char> read_until (char c1, char c2) {
		auto begin = index;
		auto len = str.size();
		for (auto end = index; end < len; ++end) {
			auto c = str[end];
			if ((c == c1) || (c == c2)) {
				index = end+1;
				return make_pair(string_slice(str, begin, end-begin), c);
			}
		}
		index = len;
		return make_pair(string_slice(str, begin, len-begin), 0);
	}

	inline string_slice do_string() {
		auto slice = read_until('\t');
		if (slice.is_null()) {
			throw runtime_error("Missing tabulator in SAM alignment line.");
		}
		return slice;
	}

	inline int32_t do_int() {
		return atoi(do_string().begin());
	}

	pair<string_slice, string_slice> parse_sam_header_field() {
		auto tag = read_until(':');
		if (tag.is_null()) {
			throw runtime_error("Invalid field tag.");
		}
		auto value = read_until('\t');
		return make_pair(tag, value);
	}

	pair<string_slice, string_slice> parse_sam_header_field_from_string() {
		auto tag = read_until(':');
		if (tag.is_null()) {
			throw runtime_error("Invalid field tag.");
		}
		auto value = read_until(' ');
		skip_whitespace();
		return make_pair(tag, value);
	}

	string_map parse_sam_header_line() {
		string_map record;
		record.reserve(8);
		while (length() > 0) {
			set_unique_entry(record, parse_sam_header_field());
		}
		return record;
	}

	string_map parse_sam_header_line_from_string() {
		string_map record;
		record.reserve(8);
		while (length() > 0) {
			set_unique_entry(record, parse_sam_header_field_from_string());
		}
		return record;
	}
};
