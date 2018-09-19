/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

const auto phred_score_table = []() -> array<uint8_t,512> {
	array<uint8_t,512> table;
	for (auto c = 0; c < 256; ++c) {
		auto pos = c << 1;
		if ((c < 33) || (c > 126)) {
			table[pos] = 0;
			table[pos+1] = 1;
		} else {
			auto qual = c - 33;
			table[pos] = (qual >= 15) ? qual : 0;
			table[pos+1] = 0;
		}
	}
	return table;
}();

int32_t compute_phred_score (const shared_ptr<sam_alignment>& aln) {
	int32_t score = 0;
	int32_t error = 0;
	for (auto p = aln->qual.begin(); p != aln->qual.end(); ++p) {
		auto pos = *p << 1;
		score += phred_score_table[pos];
		error |= phred_score_table[pos+1];
	}
	if (error != 0) {
		throw runtime_error("Invalid QUAL character.");
	}
	return score;
}

const set<char> clipped_table({'S', 'H'});
const set<char> reference_table({'M', 'D', 'N', '=', 'X'});

int32_t compute_unclipped_position(const shared_ptr<sam_alignment>& aln) {
	auto& cigar = scan_cigar_string(aln->cigar);
	if (cigar.size() == 0) {return aln->pos;}

	if (aln->is_reversed()) {
		int32_t clipped = 1;
		auto result = aln->pos - 1;
		for (int i = cigar.size()-1; i >= 0; --i) {
			auto op = cigar[i];
			auto p = op.operation;
			auto c = clipped_table.count(p);
			auto r = reference_table.count(p);
			clipped *= c;
			result += (r|clipped)*op.length;
		}
		return result;
	} else {
		auto result = aln->pos;
		for (auto op: cigar) {
			auto p = op.operation;
			if (clipped_table.count(p) == 0) {break;}
			result -= op.length;
		}
		return result;
	}
}

const string_slice pos("pos");
const string_slice score("score");

inline int32_t get_adapted_pos (const shared_ptr<sam_alignment>& aln) {
	return any_cast<int32_t>(assoc(aln->temps, pos)->value);
}

inline void set_adapted_pos (const shared_ptr<sam_alignment>& aln, int32_t value) {
	auto it = assoc(aln->temps, pos);
	if (it == aln->temps.end()) {
		aln->temps.push_back(sam_value(pos, value));
	} else {
		it->value = value;
	}
}

inline int32_t get_adapted_score (const shared_ptr<sam_alignment>& aln) {
	return any_cast<int32_t>(assoc(aln->temps, score)->value);
}

inline void set_adapted_score (const shared_ptr<sam_alignment>& aln, int32_t value) {
	auto it = assoc(aln->temps, score);
	if (it == aln->temps.end()) {
		aln->temps.push_back(sam_value(score, value));
	} else {
		it->value = value;
	}
}

void adapt_alignment (const shared_ptr<sam_alignment>& aln, const string_map& lb_table) {
	auto rg = aln->get_rg();
	auto it = lb_table.find(rg);
	if (it != lb_table.end()) {
		aln->set_libid(it->second);
	}
	set_adapted_pos(aln, compute_unclipped_position(aln));
	set_adapted_score(aln, compute_phred_score(aln));
}

template<typename T>
class handle {
public:
	T object;

	handle (const T& object) : object(object) {}

	bool compare_exchange (T& expected, const T& desired) {
		return atomic_compare_exchange_strong(&object, &expected, desired);
	}
};

inline bool is_true_fragment (const shared_ptr<sam_alignment>& aln) {
	return (aln->flag & (multiple | next_unmapped)) != multiple;
}

inline bool is_true_pair (const shared_ptr<sam_alignment>& aln) {
	return (aln->flag & (multiple | next_unmapped)) == multiple;
}

class fragment_hash {
public:
	size_t operator() (const shared_ptr<sam_alignment>& aln) const {
		return 29 * (tbb_hasher(aln->get_libid()) ^
								 tbb_hasher(aln->get_refid()) ^
								 tbb_hasher(get_adapted_pos(aln)) ^
								 tbb_hasher(aln->is_reversed()));
	}
};

class fragment_equal {
public:
	bool operator() (const shared_ptr<sam_alignment>& aln1, const shared_ptr<sam_alignment>& aln2) const {
		return (aln1->get_libid() == aln2->get_libid()) &&
			(aln1->get_refid() == aln2->get_refid()) &&
			(get_adapted_pos(aln1) == get_adapted_pos(aln2)) &&
			(aln1->is_reversed() == aln2->is_reversed());
	}
};

typedef concurrent_unordered_map<shared_ptr<sam_alignment>, shared_ptr<handle<shared_ptr<sam_alignment>>>, fragment_hash, fragment_equal> fragment_map;

void classify_fragment (const shared_ptr<sam_alignment>& aln, fragment_map& fragments, bool deterministic) {
	auto p = fragments.emplace(aln, make_shared<handle<shared_ptr<sam_alignment>>>(aln));
	if (p.second) return;
	auto best_handle = p.first->second;
	if (is_true_fragment(aln)) {
		auto aln_score = get_adapted_score(aln);
		while (true) {
			auto best = best_handle->object;
			if (is_true_pair(best)) {
				aln->flag |= duplicate; break;
			} else {
				auto best_aln_score = get_adapted_score(best);
				if (best_aln_score > aln_score) {
					aln->flag |= duplicate; break;
				} else if (best_aln_score == aln_score) {
					if (deterministic) {
						if (aln->qname > best->qname) {
							aln->flag |= duplicate; break;
						} else if (best_handle->compare_exchange(best, aln)) {
							best->flag |= duplicate; break;
						}
					} else {
						aln->flag |= duplicate; break;
					}
				} else if (best_handle->compare_exchange(best, aln)) {
					best->flag |= duplicate; break;
				}
			}
		}
	} else {
		while (true) {
			auto best = best_handle->object;
			if (is_true_pair(best)) {
				break;
			} else if (best_handle->compare_exchange(best, aln)) {
				best->flag |= duplicate; break;
			}
		}
	}
}

class alignment_pair_hash {
public:
	static size_t hash (const shared_ptr<sam_alignment>& aln) {
		return 29 * (tbb_hasher(aln->get_libid()) ^ tbb_hasher(aln->qname));
	}

	static bool equal (const shared_ptr<sam_alignment>& aln1, const shared_ptr<sam_alignment>& aln2) {
		return (aln1->get_libid() == aln2->get_libid()) && (aln1->qname == aln2->qname);
	}
};

typedef concurrent_hash_map<shared_ptr<sam_alignment>, shared_ptr<sam_alignment>, alignment_pair_hash> pair_fragment_map;

class pair_handle {
public:
	int32_t score;
	shared_ptr<sam_alignment> aln1;
	shared_ptr<sam_alignment> aln2;

	pair_handle (int32_t score, const shared_ptr<sam_alignment>& aln1, const shared_ptr<sam_alignment>& aln2) : score(score), aln1(aln1), aln2(aln2) {}
	pair_handle (const pair_handle& p) : score(p.score), aln1(p.aln1), aln2(p.aln2) {}
};

class pair_hash {
public:
	size_t operator() (const pair_handle& p) const {
		return 29 * (tbb_hasher(p.aln1->get_libid()) ^
								 tbb_hasher(p.aln1->get_refid()) ^
								 tbb_hasher(p.aln2->get_refid()) ^
								 tbb_hasher((int64_t(get_adapted_pos(p.aln1)) << 32) + int64_t(get_adapted_pos(p.aln2))) ^
								 tbb_hasher(p.aln1->is_reversed()) ^
								 tbb_hasher(p.aln2->is_reversed()));
	}
};

class pair_equal {
public:
	bool operator() (const pair_handle& p1, const pair_handle& p2) const {
		return (p1.aln1->get_libid() == p2.aln1->get_libid()) &&
			(p1.aln1->get_refid() == p2.aln1->get_refid()) &&
			(get_adapted_pos(p1.aln1) == get_adapted_pos(p2.aln1)) &&
			(p1.aln1->is_reversed() == p2.aln1->is_reversed()) &&
			(p1.aln2->get_refid() == p2.aln2->get_refid()) &&
			(get_adapted_pos(p1.aln2) == get_adapted_pos(p2.aln2)) &&
			(p1.aln2->is_reversed() == p2.aln2->is_reversed());
	}
};

typedef concurrent_unordered_map<pair_handle, shared_ptr<handle<shared_ptr<pair_handle>>>, pair_hash, pair_equal> pair_map;

void classify_pair (const shared_ptr<sam_alignment>& aln, pair_fragment_map& fragments, pair_map& pairs, bool deterministic) {
	if (!is_true_pair(aln)) return;

	auto aln1 = aln;
	auto aln2 = [&]() -> shared_ptr<sam_alignment> {
		pair_fragment_map::accessor acc;
		if (fragments.insert(acc, aln)) {
			acc->second = aln;
			return nullptr;
		} else {
			auto aln2 = acc->second;
			fragments.erase(acc);
			return aln2;
		}
	}();

	if (aln2 == nullptr) return;

	auto score = get_adapted_score(aln1) + get_adapted_score(aln2);
	auto aln1pos = get_adapted_pos(aln1);
	auto aln2pos = get_adapted_pos(aln2);
	if (aln1pos > aln2pos) {
		swap(aln1, aln2);
		swap(aln1pos, aln2pos);
	}

	pair_handle pair_key(score, aln1, aln2);

	auto p = pairs.emplace(pair_key, make_shared<handle<shared_ptr<pair_handle>>>(make_shared<pair_handle>(pair_key)));
	if (p.second) return;
	auto best_handle = p.first->second;
	shared_ptr<pair_handle> entry = nullptr;
	auto get_entry = [&](){
		if (entry == nullptr) {
			entry = make_shared<pair_handle>(pair_key);
		}
		return entry;
	};
	while (true) {
		auto best = best_handle->object;
		if (best->score > score) {
			aln1->flag |= duplicate;
			aln2->flag |= duplicate;
			break;
		} else if (best->score == score) {
			if (deterministic) {
				if (aln1->qname > best->aln1->qname) {
					aln1->flag |= duplicate;
					aln2->flag |= duplicate;
					break;
				} else if (best_handle->compare_exchange(best, get_entry())) {
					best->aln1->flag |= duplicate;
					best->aln2->flag |= duplicate;
					break;
				}
			} else {
				aln1->flag |= duplicate;
				aln2->flag |= duplicate;
				break;
			}
		} else if (best_handle->compare_exchange(best, get_entry())) {
			best->aln1->flag |= duplicate;
			best->aln2->flag |= duplicate;
			break;
		}
	}
}

header_filter mark_duplicates (bool deterministic) {
	return [deterministic](const shared_ptr<sam_header>& header) -> alignment_filter {
		auto fragments = make_shared<fragment_map>(1000000);
		auto pair_fragments = make_shared<pair_fragment_map>(1000000);
		auto pairs = make_shared<pair_map>(1000000);
		string_map lb_table;
		for (auto& rg_entry: header->rg) {
			auto lb_it = rg_entry.find(LB);
			if (lb_it != rg_entry.end()) {
				auto id_it = rg_entry.find(ID);
				if (id_it == rg_entry.end()) {
					throw runtime_error("Missing mandatory ID entry in an @RG line in a SAM file header.");
				}
				lb_table.emplace(id_it->second, lb_it->second);
			}
		}
		return [lb_table, fragments, pair_fragments, pairs, deterministic](const shared_ptr<sam_alignment>& aln) -> bool {
			if (aln->flag_not_any(unmapped | secondary | duplicate | supplementary)) {
				adapt_alignment(aln, lb_table);
				classify_fragment(aln, *fragments, deterministic);
				classify_pair(aln, *pair_fragments, *pairs, deterministic);
			}
			return true;
		};
	};
}
