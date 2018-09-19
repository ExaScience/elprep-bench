/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

const string_slice sam_file_format_version("1.5");
const string_slice sam_file_format_date("1 June 2017");

bool is_header_user_tag(const string_slice& code) {
	for (auto c: code) {
		if (('a' <= c) && (c <= 'z')) {
			return true;
		}
	}
	return false;
}

inline void format_sam_string (ostream& out, const pair<string_slice, const string_slice>& entry) {
	out << '\t' << entry.first << ':' << entry.second;
}

inline void format_sam_comment (ostream& out, const string_slice& code, const string_slice& comment) {
	out << code << '\t' << comment << '\n';
}

inline void format_sam_header_line (ostream& out, const string_slice& code, const string_map& record) {
	out << code;
	for (auto& entry: record) format_sam_string(out, entry);
	out << '\n';
}

const string_slice keep("keep");
const string_slice unknown("unknown");
const string_slice unsorted("unsorted");
const string_slice queryname("queryname");
const string_slice coordinate("coordinate");

const string_slice none("none");
const string_slice query("query");
const string_slice reference("reference");

const string_slice VN("VN");
const string_slice SO("SO");
const string_slice GO("GO");

const string_slice at_hd("@HD");
const string_slice at_sq("@SQ");
const string_slice at_rg("@RG");
const string_slice at_pg("@PG");
const string_slice at_co("@CO");

const string_slice at_hd_t("@HD\t");
const string_slice at_sq_t("@SQ\t");
const string_slice at_rg_t("@RG\t");
const string_slice at_pg_t("@PG\t");
const string_slice at_co_t("@CO\t");

class sam_header {
public:
	string_map hd;
	vector<string_map> sq, rg, pg;
	vector<string_slice> co;
	unordered_map<string_slice, vector<string_map>> user_records;

	sam_header(istream_wrapper& reader) {
		hd.insert({VN, sam_file_format_version});
		sq.reserve(32);
		pg.reserve(2);
		for (auto first = true; !reader.eof(); first = false) {
			auto c = reader.peek();
			if (c != '@') {
				return;
			} else {
				auto [line, ok] = reader.getline();
				if (!ok) break;
				string_scanner sc(string_slice(line, 4));
				if (starts_with(at_hd_t, line)) {
					if (!first) {
						throw runtime_error("@HD line not in first line when parsing a SAM header.");
					} else {
						hd = sc.parse_sam_header_line();
					}
				} else if (starts_with(at_sq_t, line)) {
					sq.push_back(sc.parse_sam_header_line());
				} else if (starts_with(at_rg_t, line)) {
					rg.push_back(sc.parse_sam_header_line());
				} else if (starts_with(at_pg_t, line)) {
					pg.push_back(sc.parse_sam_header_line());
				} else if (starts_with(at_co_t, line)) {
					co.push_back(string_slice(line, 4));
				} else if (starts_with(at_co, line)) {
					co.push_back(string_slice(line, 3));
				} else {
					string_slice code(line, 0, 3);
					if (is_header_user_tag(code)) {
						if (line[3] != '\t') {
							throw runtime_error("Header code not followed by a tab when parsing a SAM header.");
						} else {
							add_user_record(code, sc.parse_sam_header_line());
						}
					} else {
						throw runtime_error("Unknown SAM record type code.");
					}
				}
			}
		}
	}

	inline string_slice get_hd_so () const {
		auto entry = hd.find(SO);
		if (entry == hd.end()) {
			return unknown;
		} else {
			return entry->second;
		}
	}

	inline void set_hd_so(const string_slice& value) {
		hd.erase(GO);
		hd.insert({SO, value});
	}

	inline string_slice get_hd_go () const {
		auto entry = hd.find(GO);
		if (entry == hd.end()) {
			return unknown;
		} else {
			return entry->second;
		}
	}

	inline void set_hd_go(const string_slice& value) {
		hd.erase(SO);
		hd.insert({GO, value});
	}

	inline void add_user_record(const string_slice& code, const string_map& record) {
		user_records[code].push_back(record);
	}

	void format(ostream& out) const {
		if (!hd.empty()) format_sam_header_line(out, at_hd, hd);
		for (auto& record: sq) format_sam_header_line(out, at_sq, record);
		for (auto& record: rg) format_sam_header_line(out, at_rg, record);
		for (auto& record: pg) format_sam_header_line(out, at_pg, record);
		for (auto& record: co) format_sam_comment(out, at_co, record);
		for (auto& entry: user_records) {
			for (auto& record: entry.second) {
				format_sam_header_line(out, entry.first, record);
			}
		}
	}
};

const string_slice LN("LN");

inline int32_t get_sq_ln(const string_map& record) {
	auto it = record.find(LN);
	if (it == record.end()) {
		return 0x7FFFFFFF;
	} else {
		return atoi(&it->second[0]);
	}
}

const unordered_map<std::type_index, function<void(ostream& out, const any& value)>> optional_field_output_table({
		{type_index(typeid(char)), [](ostream& out, const any& value){out << ":A:" << any_cast<char>(value);}},
		{type_index(typeid(int32_t)), [](ostream& out, const any& value){out << ":i:" << any_cast<int32_t>(value);}},
		{type_index(typeid(float)), [](ostream& out, const any& value){out << ":f:" << any_cast<float>(value);}},
		{type_index(typeid(string_slice)), [](ostream& out, const any& value){out << ":Z:" << any_cast<string_slice>(value);}},
		{type_index(typeid(deque<uint8_t>)), [](ostream& out, const any& value){
				out << ":H:" << hex;
				for (auto b: any_cast<deque<uint8_t>>(value)) {
					if (b < 16) {
						out << '0';
					}
					out << b;
				}
			}},
		{type_index(typeid(vector<int8_t>)), [](ostream& out, const any& value){
				out << ":B:c" << dec;
				for (auto v: any_cast<vector<int8_t>>(value)) {
					out << ',' << v;
				}
			}},
		{type_index(typeid(vector<uint8_t>)), [](ostream& out, const any& value){
				out << ":B:C" << dec;
				for (auto v: any_cast<vector<uint8_t>>(value)) {
					out << ',' << v;
				}
			}},
		{type_index(typeid(vector<int16_t>)), [](ostream& out, const any& value){
				out << ":B:s" << dec;
				for (auto v: any_cast<vector<int16_t>>(value)) {
					out << ',' << v;
				}
			}},
		{type_index(typeid(vector<uint16_t>)), [](ostream& out, const any& value){
				out << ":B:S" << dec;
				for (auto v: any_cast<vector<uint16_t>>(value)) {
					out << ',' << v;
				}
			}},
		{type_index(typeid(vector<int32_t>)), [](ostream& out, const any& value){
				out << ":B:i" << dec;
				for (auto v: any_cast<vector<int32_t>>(value)) {
					out << ',' << v;
				}
			}},
		{type_index(typeid(vector<uint32_t>)), [](ostream& out, const any& value){
				out << ":B:I" << dec;
				for (auto v: any_cast<vector<uint32_t>>(value)) {
					out << ',' << v;
				}
			}},
		{type_index(typeid(vector<float>)), [](ostream& out, const any& value){
				out << ":B:f" << dec;
				for (auto v: any_cast<vector<float>>(value)) {
					out << ',' << v;
				}
			}},
	});

class sam_value {
public:
	string_slice tag;
	/* It would be more natural to use std::variant over the various possible 12 types.
		 However, std::variant is not allowed to allocate "big" objects on the heap, so always
		 uses memory in the size of the largest object. Most sam_values are using one of
		 the "small" types (char, int, float), so this would be a waste of space.
		 std::any, on the other hand, is allowed to allocate memory on the heap for
		 "big" objects, and store "small" objects as immediate values. So this is
		 the more efficient choice here. */
	any value;

	template<typename T> sam_value(const string_slice& tag, const T& value) : tag(tag), value(value) {}

	void format (ostream& out) const {
		out << '\t' << tag;
		optional_field_output_table.at(type_index(value.type()))(out, value);
	}
};

sam_value parse_sam_char(const string_slice& tag, string_scanner& sc) {
	return sam_value(tag, sc.read_byte_until('\t').first);
}

sam_value parse_sam_integer(const string_slice& tag, string_scanner& sc) {
	return sam_value(tag, atoi(sc.read_until('\t').begin()));
}

sam_value parse_sam_float(const string_slice& tag, string_scanner& sc) {
	return sam_value(tag, float(atof(sc.read_until('\t').begin())));
}

sam_value parse_sam_string(const string_slice& tag, string_scanner& sc) {
	return sam_value(tag, sc.read_until('\t'));
}

sam_value parse_sam_byte_array(const string_slice& tag, string_scanner& sc) {
	auto slice = sc.read_until('\t');
	deque<uint8_t> result;
	for (auto p = slice.begin(); p != slice.end(); p += 2) {
		result.push_back(stoi(string(p, 2), nullptr, 16));
	}
	return sam_value(tag, result);
}

template<typename T, typename F> sam_value _parse_sam_numeric_array(const string_slice& tag, string_scanner& sc, F f) {
	vector<T> result; result.reserve(8);
	while (true) {
		auto res = sc.read_until(',', '\t');
		result.push_back(f(res.first.begin()));
		if (res.second != ',') break;
	}
	return sam_value(tag, result);
}

sam_value parse_sam_numeric_array(const string_slice& tag, string_scanner& sc) {
	auto [ntype, success] = sc.read_byte_until(',');
	if (!success) {
		throw runtime_error("Missing entry in numeric array.");
	}
	switch (ntype) {
		case 'c': return _parse_sam_numeric_array<int8_t>(tag, sc, atoi);
		case 'C': return _parse_sam_numeric_array<uint8_t>(tag, sc, atoi);
		case 's': return _parse_sam_numeric_array<int16_t>(tag, sc, atoi);
		case 'S': return _parse_sam_numeric_array<uint16_t>(tag, sc, atoi);
		case 'i': return _parse_sam_numeric_array<int32_t>(tag, sc, atoi);
		case 'I': return _parse_sam_numeric_array<uint32_t>(tag, sc, atoi);
		case 'f': return _parse_sam_numeric_array<float>(tag, sc, atof);
		default:
			throw runtime_error("Invalid numeric array type.");
	}
}

const unordered_map<char, function<sam_value(const string_slice&, string_scanner&)>> optional_field_parse_table({
		{'A', parse_sam_char},
		{'i', parse_sam_integer},
		{'f', parse_sam_float},
		{'Z', parse_sam_string},
		{'H', parse_sam_byte_array},
		{'B', parse_sam_numeric_array}
	});

sam_value parse_sam_alignment_field (string_scanner& sc) {
	auto tag = sc.read_until(':');
	if (tag.size() != 2) {
		throw runtime_error("Invalid field tag in SAM alignment line.");
	}
	auto [typebyte, success] = sc.read_byte_until(':');
	if (!success) {
		throw runtime_error("Invalid field type in SAM alignment line.");
	}
	return optional_field_parse_table.at(typebyte)(tag, sc);
}

using sam_vector = vector<sam_value>;

inline sam_vector::iterator assoc(sam_vector& v, const string_slice& tag) {
	for (auto it = v.begin(); it != v.end(); ++it) {
		if (it->tag == tag) return it;
	}
	return v.end();
}

const string_slice rg("RG");
const string_slice ID("ID");
const string_slice LB("LB");
const string_slice refid("REFID");
const string_slice libid("LIBID");

const auto multiple      =   0x1;
const auto proper        =   0x2;
const auto unmapped      =   0x4;
const auto next_unmapped =   0x8;
const auto reversed      =  0x10;
const auto next_reversed =  0x20;
const auto first         =  0x40;
const auto last          =  0x80;
const auto secondary     = 0x100;
const auto qcfailed      = 0x200;
const auto duplicate     = 0x400;
const auto supplementary = 0x800;

class sam_alignment {
public:
	string_slice qname;
	uint16_t flag;
	string_slice rname;
	int32_t pos;
	uint8_t mapq;
	string_slice cigar;
	string_slice rnext;
	int32_t pnext;
	int32_t tlen;
	string_slice seq;
	string_slice qual;
	vector<sam_value> tags;
	vector<sam_value> temps;

	sam_alignment(const string_slice& line) {
		tags.reserve(16);
		temps.reserve(4);

		string_scanner sc(line);

		qname = sc.do_string();
		flag = sc.do_int();
		rname = sc.do_string();
		pos = sc.do_int();
		mapq = sc.do_int();
		cigar = sc.do_string();
		rnext = sc.do_string();
		pnext = sc.do_int();
		tlen = sc.do_int();
		seq = sc.do_string();
		qual = sc.read_until('\t');

		while (sc.length() > 0) {
			tags.push_back(parse_sam_alignment_field(sc));
		}
	}

	inline string_slice get_rg() {
		return any_cast<string_slice>(assoc(tags, rg)->value);
	}

	inline void set_rg (const string_slice& value) {
		auto it = assoc(tags, rg);
		if (it == tags.end()) {
			tags.push_back(sam_value(rg, value));
		} else {
			it->value = value;
		}
	}

	inline int32_t get_refid () {
		return any_cast<int32_t>(assoc(temps, refid)->value);
	}

	inline void set_refid (int32_t value) {
		auto it = assoc(temps, refid);
		if (it == temps.end()) {
			temps.push_back(sam_value(refid, value));
		} else {
			it->value = value;
		}
	}

	inline string_slice get_libid() {
		return any_cast<string_slice>(assoc(temps, libid)->value);
	}

	inline void set_libid(const string_slice& value) {
		auto it = assoc(temps, libid);
		if (it == temps.end()) {
			temps.push_back(sam_value(libid, value));
		} else {
			it->value = value;
		}
	}

	inline bool is_multiple() const      {return (flag & multiple)      != 0;}
	inline bool is_proper() const        {return (flag & proper)        != 0;}
	inline bool is_unmapped() const      {return (flag & unmapped)      != 0;}
	inline bool is_next_unmapped() const {return (flag & next_unmapped) != 0;}
	inline bool is_reversed() const      {return (flag & reversed)      != 0;}
	inline bool is_next_reversed() const {return (flag & next_reversed) != 0;}
	inline bool is_first() const         {return (flag & first)         != 0;}
	inline bool is_last() const          {return (flag & last)          != 0;}
	inline bool is_secondary() const     {return (flag & secondary)     != 0;}
	inline bool is_qcfailed() const      {return (flag & qcfailed)      != 0;}
	inline bool is_duplicate() const     {return (flag & duplicate)     != 0;}
	inline bool is_supplementary() const {return (flag & supplementary) != 0;}

	inline bool flag_every     (uint16_t f) const {return (flag & f) == f;}
	inline bool flag_some      (uint16_t f) const {return (flag & f) != 0;}
	inline bool flag_not_every (uint16_t f) const {return (flag & f) != f;}
	inline bool flag_not_any   (uint16_t f) const {return (flag & f) == 0;}

	void format (ostream& out) const {
		out << dec
				<< qname << '\t'
				<< flag << '\t'
				<< rname << '\t'
				<< pos << '\t'
				<< uint32_t(mapq) << '\t'
				<< cigar << '\t'
				<< rnext << '\t'
				<< pnext << '\t'
				<< tlen << '\t'
				<< seq << '\t'
				<< qual;

		for (auto& entry: tags) {
			entry.format(out);
		}

		out << '\n';
	}
};

bool coordinate_less (const shared_ptr<sam_alignment>& aln1, const shared_ptr<sam_alignment>& aln2) {
	auto refid1 = aln1->get_refid();
	auto refid2 = aln2->get_refid();
	if      (refid1 < refid2) return refid1 >= 0;
	else if (refid2 < refid1) return refid2 < 0;
	else                      return aln1->pos < aln2->pos;
}

bool queryname_less (const shared_ptr<sam_alignment>& aln1, const shared_ptr<sam_alignment>& aln2) {
	return aln1->qname < aln2->qname;
}

class sam {
public:
	shared_ptr<sam_header> header;
	deque<shared_ptr<sam_alignment>> alignments;
};

const unordered_map<char,char> cigar_operations({
		{'M', 'M'},
		{'m', 'M'},
		{'I', 'I'},
		{'i', 'I'},
		{'D', 'D'},
		{'d', 'D'},
		{'N', 'N'},
		{'n', 'N'},
		{'S', 'S'},
		{'s', 'S'},
		{'H', 'H'},
		{'h', 'H'},
		{'P', 'P'},
		{'p', 'P'},
		{'X', 'X'},
		{'x', 'X'},
		{'=', '='}
	});

inline bool is_digit (char c) {return ('0' <= c) && (c <= '9');}

class cigar_operation {
public:
	int32_t length;
	char operation;
	cigar_operation (int32_t length, char operation) : length(length), operation(operation) {}
};

cigar_operation make_cigar_operation (const string_slice& cigar, int& i) {
	for (auto j = i; true; ++j) {
		auto c = cigar[j];
		if (!is_digit(c)) {
			auto length = atoi(&cigar[i]);
			auto operation = cigar_operations.at(c);
			i = j+1;
			return cigar_operation(length, operation);
		}
	}
}

const string_slice star("*");

concurrent_unordered_map<string_slice, vector<cigar_operation>> cigar_cache({{star, vector<cigar_operation>()}});

const vector<cigar_operation>& scan_cigar_string(const string_slice& cigar) {
	auto it = cigar_cache.find(cigar);
	if (it == cigar_cache.end()) {
		vector<cigar_operation> result; result.reserve(8);
		for (auto i = 0; i < cigar.size();) {
			result.push_back(make_cigar_operation(cigar, i));
		}
		tie(it, ignore) = cigar_cache.emplace(cigar, result);
	}
	return it->second;
}
