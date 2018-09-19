/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

const auto istream_wrapper_buffer_size = 65536;

class istream_wrapper {
public:
	istream& input;
	size_t index;
	shared_ptr<string> buffer;

	istream_wrapper (istream& input) : input(input), index(0), buffer(make_shared<string>(istream_wrapper_buffer_size, 0)) {
		input.read(&buffer->operator[](0), istream_wrapper_buffer_size);
		buffer->resize(input.gcount());
	}

private:
	void fill () {
		auto new_buffer = make_shared<string>(istream_wrapper_buffer_size, 0);
		auto rest = buffer->size()-index;
		buffer->copy(&new_buffer->operator[](0), rest, index);
		input.read(&new_buffer->operator[](rest), index);
		new_buffer->resize(rest+input.gcount());
		index = 0;
		buffer = new_buffer;
	}

public:
	inline bool eof () {
		if (index < buffer->size()) {
			return false;
		} else {
			fill();
			return buffer->size() == 0;
		}
	}

	inline char peek () {
		if (index < buffer->size()) {
			return buffer->operator[](index);
		} else {
			fill();
			if (buffer->size() > 0) {
				return buffer->operator[](0);
			} else {
				throw runtime_error("peek after eof");
			}
		}
	}

	pair<string_slice, bool> getline () {
		auto start = index;
		for (auto end = index; end < buffer->size(); ++end) {
			if (buffer->operator[](end) == '\n') {
				index = end+1;
				return make_pair(string_slice(buffer, start, end-start), true);
			}
		}
		fill();
		for (auto end = 0; end < buffer->size(); ++end) {
			if (buffer->operator[](end) == '\n') {
				index = end+1;
				return make_pair(string_slice(buffer, 0, end), true);
			}
		}
		index = buffer->size();
		if (input.eof()) {
			return make_pair(string_slice(buffer, 0, index), index > 0);
		} else {
			throw runtime_error("istream_wrapper::getline: istream_wrapper_buffer_size too small.");
		}
	}

	void skipline () {
		for (auto end = index; end < buffer->size(); ++end) {
			if (buffer->operator[](end) == '\n') {
				index = end+1;
				return;
			}
		}
		fill();
		for (auto end = 0; end < buffer->size(); ++end) {
			if (buffer->operator[](end) == '\n') {
				index = end+1;
				return;
			}
		}
		index = buffer->size();
		if (!input.eof()) {
			throw runtime_error("istream_wrapper::skipline: istream_wrapper_buffer_size too small.");
		}
	}
};
