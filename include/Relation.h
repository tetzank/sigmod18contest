#ifndef RELATION_H_
#define RELATION_H_

#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <vector>
#include <variant>
#include <chrono>

#include "MultiArrayTable.h"
#include "ArrayTable.h"
#include "BitsetTable.h"


//FIXME: hardcoded type
//using HT_t = MultiArrayTable<uint32_t>;
using HT_t = MultiArrayTable<uint64_t>;

//using HTu_t = ArrayTable<uint32_t>;
using HTu_t = ArrayTable<uint64_t>;


using hashtable_t = std::variant<std::monostate,HT_t,HTu_t>;
using column_t = std::variant<uint64_t*,uint32_t*,uint16_t*>;


class Relation{
private:
	char *mapped_addr; // address returned by mmap()
	uint64_t fsize;
	uint64_t size; // number of tuples
	std::vector<column_t> columns;
	std::vector<BitsetTable> BTs;
	std::vector<hashtable_t> HTs;

public:
	Relation(const char *fname);
	~Relation();
	Relation(const Relation&)=delete;
	Relation(Relation &&o);

	inline uint64_t getNumberOfTuples() const{
		return size;
	}
	inline const column_t &getColumn(int col) const{
		return columns[col];
	}
	inline size_t getNumberOfColumns() const{
		return columns.size();
	}

	void stats_init(){
		BTs.resize(getNumberOfColumns());
		HTs.resize(getNumberOfColumns());
	}
	// precalculate column, used in multi-threaded case
	void stats(int column);

	const hashtable_t *getHT(int col) const{
		return &HTs[col];
	}
	const BitsetTable *getBT(int col) const{
		return &BTs[col];
	}
};


#endif
