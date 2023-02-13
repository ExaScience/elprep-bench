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

const string_slice SN("SN");

header_filter replace_reference_sequence_dictionary (const vector<string_map>& dict) {
	return [dict](const shared_ptr<sam_header>& header) -> alignment_filter {
		if (header->get_hd_so() == coordinate) {
			auto previous_pos = -1;
			auto& old_dict = header->sq;
			for (auto& entry: dict) {
				auto it = entry.find(SN);
				if (it == entry.end()) {
					throw runtime_error("SN not found.");
				}
				auto sn = it->second;
				auto pos = find(old_dict, [&sn](const string_map& entry){
						auto it = entry.find(SN);
						return (it != entry.end()) && (it->second == SN);
					});
				if (pos >= 0) {
					if (pos > previous_pos) {
						previous_pos = pos;
					} else {
						header->set_hd_so(unknown);
						break;
					}
				}
			}
		}
		auto dict_table = make_shared<multiset<string_slice>>();
		for (auto& entry: dict) {
			auto it = entry.find(SN);
			if (it == entry.end()) {
				throw runtime_error("SN not found.");
			}
			dict_table->insert(it->second);
		}
		header->sq = dict;
		return [dict_table](const shared_ptr<sam_alignment>& aln) -> bool {
			return dict_table->count(aln->rname) > 0;
		};
	};
}

header_filter replace_reference_sequence_dictionary_from_sam_file (const string& sam_file) {
	ifstream input(sam_file);
	istream_wrapper wrapper(input);
	sam_header header(wrapper);
	return replace_reference_sequence_dictionary(header.sq);
}

alignment_filter filter_unmapped_reads (const shared_ptr<sam_header>&) {
	return [](const shared_ptr<sam_alignment>& aln){
		return (aln->flag & unmapped) == 0;
	};
}

alignment_filter filter_unmapped_reads_strict (const shared_ptr<sam_header>&) {
	return [](const shared_ptr<sam_alignment>& aln){
		return ((aln->flag & unmapped) == 0) && (aln->pos != 0) && (aln->rname != star);
	};
}

alignment_filter filter_duplicate_reads (const shared_ptr<sam_header>&) {
	return [](const shared_ptr<sam_alignment>& aln){
		return (aln->flag & duplicate) == 0;
	};
}

const string_slice sr("sr");
const string_slice at_sr("@sr");

alignment_filter filter_optional_reads (const shared_ptr<sam_header>& header) {
	auto record = header->user_records.find(at_sr);
	if (record == header->user_records.end()) {
		return nullptr;
	} else {
		header->user_records.erase(record);
		return [](const shared_ptr<sam_alignment>& aln){
			return assoc(aln->tags, sr) == aln->tags.end();
		};
	}
}

header_filter add_or_replace_read_group (const string_map& read_group) {
	return [read_group](const shared_ptr<sam_header>& header) -> alignment_filter {
		header->rg = {read_group};
		auto it = read_group.find(ID);
		if (it == read_group.end()) {
			throw runtime_error("ID not found.");
		}
		auto id = it->second;
		return [id](const shared_ptr<sam_alignment>& aln) -> bool {
			aln->set_rg(id); return true;
		};
	};
}

uniform_int_distribution<int> pg_id_dist(0, 0x10000);
random_device rd;

const string_slice PP("PP");

header_filter add_pg_line (string_map& new_pg) {
	return [&new_pg](const shared_ptr<sam_header>& header) -> alignment_filter {
		auto it = new_pg.find(ID);
		if (it == new_pg.end()) {
			throw runtime_error("ID not found.");
		}
		auto id = it->second;
		if (find(header->pg, [&id](const string_map& entry){
					auto it = entry.find(ID);
					return (it != entry.end()) && (it->second == id);
				}) >= 0) {
			ostringstream out; out << id << hex;
			do {
				out << pg_id_dist(rd);
				id = out.str();
			} while (find(header->pg, [&out, &id](const string_map& entry){
						auto it = entry.find(ID);
						return (it != entry.end()) && (it->second == id);
					}) >= 0);
		}
		new_pg.find(ID)->second = id;
		for (auto& pg: header->pg) {
			auto it = pg.find(ID);
			if (it == pg.end()) {
				throw runtime_error("ID not found.");
			}
			auto next_id = it->second;
			auto pos = find(header->pg, [&next_id](const string_map& entry){
					auto it = entry.find(PP);
					return (it != entry.end()) && (it->second == next_id);
				});
			if (pos < 0) {
				new_pg.insert({PP, next_id}); break;
			}
		}
		header->pg.push_back(new_pg);
		return nullptr;
	};
}

alignment_filter add_refid (const shared_ptr<sam_header>& header) {
	auto dict_table = make_shared<unordered_map<string_slice, int32_t>>();
	for (int32_t index = 0; index < header->sq.size(); index++) {
		auto& sq = header->sq[index];
		auto it = sq.find(SN);
		if (it == sq.end()) {
			throw runtime_error("SN not found.");
		}
		dict_table->insert({it->second, index});
	}
	return [dict_table](const shared_ptr<sam_alignment>& aln) -> bool {
		auto it = dict_table->find(aln->rname);
		aln->set_refid((it == dict_table->end()) ? -1 : it->second);
		return true;
	};
}
