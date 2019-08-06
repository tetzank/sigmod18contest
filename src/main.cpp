#include <cstdio>

#ifndef DISABLE_OPENMP
#include <omp.h>
#endif

#include "Relation.h"


static std::vector<Relation> parseInit(const char *fname){
	std::vector<Relation> relations;

	FILE *fd = fopen(fname, "r");
	if(!fd){
		perror("fopen failed");
		exit(EXIT_FAILURE);
	}
	// line buffer, allocated and reallocated by getline()
	char *line=nullptr;
	size_t len=0;

	ssize_t nread;
	while((nread=getline(&line, &len, fd)) != -1){
		// remove included newline
		line[nread-1] = '\0';
		//Relation rel(line);
		//printf("relation %s; number of tuples: %lu\n", line, rel.getNumberOfTuples());
		relations.emplace_back(line);
		//FIXME: we just assume that relation x is in on line x in init file
		//       otherwise we would need a mapping
	}

	free(line); // allocated by getline()
	fclose(fd);

	return relations;
}

static void precalc(std::vector<Relation> &relations){
#ifndef DISABLE_OPENMP
	// parallelize per column in relation as well
	size_t work = 0;
	for(auto &r : relations){
		work += r.getNumberOfColumns();
		r.stats_init();
	}
#ifndef QUIET
	printf("precalculating %lu indices, %i threads\n", work, omp_get_max_threads());
#endif
	size_t threads = omp_get_max_threads();
	if(work < threads) threads = work; //TODO: in this case one should also disable parallel sort
	#pragma omp parallel for schedule(static,1) num_threads(threads)
	for(size_t i=0; i<work; ++i){
		//HACK: a lot of sequential work to get to the right column/work item
		size_t j = i;
		for(auto &r : relations){
			if(j < r.getNumberOfColumns()){
				r.stats(j);
				break;
			}
			j -= r.getNumberOfColumns();
		}
	}
#else
	// precalculate sequentially
	for(auto &r : relations){
		r.stats_init();
		for(size_t c=0; c<r.getNumberOfColumns(); ++c){
			r.stats(c);
		}
	}
#endif
}


int main(int argc, char *argv[]){
	if(argc < 4){
		puts("./program -[t|a|l] init work");
		return -1;
	}

#ifndef DISABLE_OPENMP
	// setting for OpenMP
	omp_set_nested(true);
	omp_set_max_active_levels(2);
#endif

	auto t_start = std::chrono::high_resolution_clock::now();

	std::vector<Relation> relations = parseInit(argv[2]);
	precalc(relations);

	return 0;
}
