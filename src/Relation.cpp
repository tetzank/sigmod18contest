#include "Relation.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>


Relation::Relation(const char *fname){
	int fd = open(fname, O_RDONLY);
	if(fd == -1){
		perror("failed to open relation");
		exit(EXIT_FAILURE);
	}
	// get file size
	struct stat s;
	if(fstat(fd, &s) == -1){
		perror("failed to fstat file");
		exit(EXIT_FAILURE);
	}
	fsize = s.st_size;
	// no magic number to indicate file type, needs at least header size
	if(fsize < 16u){
		fprintf(stderr, "relation file %s does not contain a valid header\n", fname);
		exit(EXIT_FAILURE);
	}
	// map file into memory
	mapped_addr = reinterpret_cast<char*>(mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0));
	if(mapped_addr == MAP_FAILED){
		perror("failed to fstat file");
		exit(EXIT_FAILURE);
	}
	// read header
	char *addr = mapped_addr;
	size = *reinterpret_cast<uint64_t*>(addr);
	addr += sizeof(uint64_t);
	size_t num_columns = *reinterpret_cast<size_t*>(addr);
	addr += sizeof(size_t);
	for(size_t i=0; i<num_columns; ++i){
		// initially 64-bit values
		columns.emplace_back(reinterpret_cast<uint64_t*>(addr));
		addr += size*sizeof(uint64_t);
	}
	// close file, undo open(), file still open because of mmap()
	close(fd);
}

Relation::~Relation(){
	munmap(mapped_addr, fsize);

	for(const auto &col : columns){
		switch(col.index()){
			case 0: /* nothing to do, memory was mmap'ed */ break;
			case 1: delete[] std::get<uint32_t*>(col); break;
			case 2: delete[] std::get<uint16_t*>(col); break;
		}
	}
}

Relation::Relation(Relation &&o){
	mapped_addr = o.mapped_addr;
	fsize = o.fsize;
	size = o.size;
	columns = std::move(o.columns);
	HTs = std::move(o.HTs);
	o.mapped_addr = nullptr;
	o.fsize = 0;
	o.size = 0;
}

void Relation::stats(int column){
#ifndef QUIET
	auto t_start = std::chrono::high_resolution_clock::now();
#endif
	// min, max on column
	uint64_t *col = std::get<uint64_t*>(columns[column]);
	uint64_t max=0, min=std::numeric_limits<uint64_t>::max();
	for(uint64_t idx=0; idx<size; ++idx){
		const auto val = col[idx];
		if(min > val) min = val;
		if(max < val) max = val;
	}
#ifndef QUIET
	auto t_minmax = std::chrono::high_resolution_clock::now();
#endif
#ifdef MINIMIZECOL
	// represent column with smaller type if possible
	uint64_t bits = 64 - __builtin_clzl(max);
#ifndef QUIET
	uint64_t bits2= 64 - __builtin_clzl(max - min);
	printf("r?c%i: %lu - %lu (min: %lu; max: %lu)\n", column, bits2, bits, min, max);
#endif
	if(bits <= 16){
		uint16_t *newcol = new uint16_t[size];
		for(uint64_t idx=0; idx<size; ++idx){
			newcol[idx] = col[idx];
		}
		columns[column] = newcol;
	}else if(bits <= 32){
		uint32_t *newcol = new uint32_t[size];
		for(uint64_t idx=0; idx<size; ++idx){
			newcol[idx] = col[idx];
		}
		columns[column] = newcol;
	} // stays at 64-bit values
#endif
#ifndef QUIET
	auto t_minimize = std::chrono::high_resolution_clock::now();
#endif
	// precalc BitsetTable for column, in case we want to have a semijoin
	BTs[column].init(min, max, col, size);
#ifndef QUIET
	auto t_bt = std::chrono::high_resolution_clock::now();
#endif
	//TODO: check uniqueness (with Bitset)
	//HACK: assumes first column is unique
	if(column!=0){
		// precalc MultiArrayTable
		HTs[column].emplace<HT_t>(min, max, col, size);
	}else{
		// precalc ArrayTable because it only contains unique elements
		HTs[column].emplace<HTu_t>(min, max, col, size);
	}

#ifndef QUIET
	auto t_ht = std::chrono::high_resolution_clock::now();
	printf("column: %i; size: %lu\n  minmax: %12.2f us\nminimize: %12.2f us\n  bitset: %12.2f us\n      HT: %12.2f us\n   total: %12.2f us\n---\n",
		column, size,
		std::chrono::duration<double, std::micro>(t_minmax - t_start).count(),
		std::chrono::duration<double, std::micro>( t_minimize - t_minmax ).count(),
		std::chrono::duration<double, std::micro>( t_bt - t_minimize ).count(),
		std::chrono::duration<double, std::micro>( t_ht - t_bt ).count(),
		std::chrono::duration<double, std::micro>( t_ht - t_start).count()
	);
#endif
}
