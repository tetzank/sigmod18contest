#include <cstdio>
#include <vector>

#ifndef DISABLE_OPENMP
#include <omp.h>
#endif

#include <coat/Function.h>
#include <coat/ControlFlow.h>

#include "Relation.h"
#include "Query.h"
#include "ScanOperator.h"
#include "ProjectionOperator.h"


#ifdef MEASURE_TIME
double prepare_time=0.0;
double compilation_time=0.0;
double exec_time=0.0;
#endif


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


void printResult(uint64_t amount, const uint64_t *results, uint64_t rsize, FILE *fd_out){
	if(amount != 0){
		printf("%lu", results[0]);
		fprintf(fd_out, "%lu", results[0]);
		for(size_t r=1; r<rsize; ++r){
			printf(" %lu", results[r]);
			fprintf(fd_out, " %lu", results[r]);
		}
	}else{
		// no result records, print NULL
		printf("NULL");
		fprintf(fd_out, "NULL");
		for(size_t r=1; r<rsize; ++r){
			printf(" NULL");
			fprintf(fd_out, " NULL");
		}
	}
	printf("\n");
	fprintf(fd_out, "\n");
}
void printResult(ProjectionOperator *proj, FILE *fd_out){
	// extract result from projection operator
	const auto &results = proj->getResults();
	printResult(proj->getAmount(), results.data(), results.size(), fd_out);
}


using executeFunc = void (*)(const Query &q, ScanOperator *scan, ProjectionOperator *proj, FILE *fd_out, void *data, size_t query);

void parseWork(const char *fname, std::vector<Relation> &relations, executeFunc execfn, void *data){
	FILE *fd_out = fopen("output.res", "w");
	if(!fd_out){
		perror("fopen failed");
		exit(EXIT_FAILURE);
	}

	FILE *fd = fopen(fname, "r");
	if(!fd){
		perror("fopen failed");
		exit(EXIT_FAILURE);
	}
	// line buffer, allocated and reallocated by getline()
	char *line=nullptr;
	size_t len=0;

	Query q;
	ssize_t nread;
	size_t query=1;
	while((nread=getline(&line, &len, fd)) != -1){
		if(*line == 'F') continue;
#ifndef QUIET
		printf("%lu: %s", query, line);
#endif
#ifdef MEASURE_TIME
		auto t_start = std::chrono::high_resolution_clock::now();
#endif
		// remove included newline
		line[nread-1] = '\0';
		// parse query string
		q.parse(line);

#ifndef QUIET
		if(q.filters.empty()){
			printf("no filter :: ");
		}else{
			printf("filter(");
			for(const auto &f : q.filters){
				printf("%u,", f.sel.relationId);
			}
			printf(") :: ");
		}
		for(unsigned relid : q.relationIds){
			printf("r%u: %lu; ", relid, relations[relid].getNumberOfTuples());
		}
		printf("\n");
#endif

		q.rewrite(relations);
		auto [scan,proj] = q.constructPipeline(relations);

#ifdef MEASURE_TIME
		auto t_prepare = std::chrono::high_resolution_clock::now();
#endif

		execfn(q, scan, proj, fd_out, data, query);

		// deallocate pipeline
		delete scan;
#ifdef MEASURE_TIME
		auto t_end = std::chrono::high_resolution_clock::now();
#ifndef QUIET
		printf("prepare: %11.2f us\n  query: %11.2f us\n  total: %11.2f us\n",
			std::chrono::duration<double, std::micro>( t_prepare - t_start).count(),
			std::chrono::duration<double, std::micro>( t_end - t_prepare).count(),
			std::chrono::duration<double, std::micro>( t_end - t_start).count()
		);
#endif

		prepare_time += std::chrono::duration<double, std::micro>( t_prepare - t_start).count();
#endif
		// clear query
		q.clear();
		++query;
	}

	free(line); // allocated by getline()
	fclose(fd);

	fclose(fd_out);
}


void tupleByTuple(const Query &q, ScanOperator *scan, ProjectionOperator *proj, FILE *fd_out, void*, size_t){
#ifdef MEASURE_TIME
	auto t_start = std::chrono::high_resolution_clock::now();
#endif
	// execute non-codegen
	Context ctx(q.relationIds.size());
	scan->execute(&ctx);

#ifdef MEASURE_TIME
	auto t_end = std::chrono::high_resolution_clock::now();
#ifndef QUIET
	printf("query: %11.2f us\n",
		std::chrono::duration<double, std::micro>( t_end - t_start).count()
	);
#endif

	exec_time += std::chrono::duration<double, std::micro>( t_end - t_start).count();
#endif

	printResult(proj, fd_out);
}

#ifdef MORSELS
// returns amount, writes to res
uint64_t morsel_execution(codegen_func_type fnptr, uint64_t tuples, uint64_t *res, size_t rsize){
	uint64_t amount = 0;
	for(size_t i=0; i<rsize; ++i){
		res[i] = 0;
	}
	const uint64_t morsel_size = 1024;
	const uint64_t morsel_count = (tuples / morsel_size) +1;
	#pragma omp parallel
	{
		uint64_t privres[rsize];
		uint64_t privaggr[rsize];
		for(size_t i=0; i<rsize; ++i){
			privaggr[i] = 0;
		}
		uint64_t privcollectedamount=0;
		#pragma omp for schedule(dynamic,1)
		for(uint64_t m=0; m<morsel_count; m++){
			uint64_t begin = m * morsel_size;
			uint64_t end = begin + morsel_size;
			if(end > tuples){
				// last one, gets the rest, that way we don't have to care about rounding
				end = tuples;
			}
			// execute
			uint64_t privamount = fnptr(begin, end, privres);
			for(size_t i=0; i<rsize; ++i){
				privaggr[i] += privres[i];
			}
			privcollectedamount += privamount;
		}

		#pragma omp critical
		{
			for(size_t i=0; i<rsize; ++i){
				res[i] += privaggr[i];
			}
			amount += privcollectedamount;
		}
	}
	return amount;
}
#endif

void codegenAsmjit(const Query &q, ScanOperator *scan, ProjectionOperator *proj, FILE *fd_out, void *data, size_t /*query*/){
#ifdef MEASURE_TIME
	auto t_start = std::chrono::high_resolution_clock::now();
#endif
	coat::Function<coat::runtimeasmjit,codegen_func_type> fn((coat::runtimeasmjit*)data);
	{
		CodegenContext ctx(fn, q.relationIds.size(), q.selections.size());
		scan->codegen(fn, ctx);
		proj->codegen_save(fn, ctx);
		coat::ret(fn, ctx.amount);
	}
	// finalize function
	codegen_func_type fnptr = fn.finalize((coat::runtimeasmjit*)data);
#ifdef MEASURE_TIME
	auto t_compile = std::chrono::high_resolution_clock::now();
#endif

	const size_t rsize = q.selections.size();
	uint64_t res[rsize];
	const uint64_t tuples = scan->getTuples();
#ifdef MORSELS
	uint64_t amount = morsel_execution(fnptr, tuples, res, rsize);
#else
	// execute generated function
	uint64_t amount = fnptr(0, tuples, res);
#endif

#ifdef MEASURE_TIME
	auto t_end = std::chrono::high_resolution_clock::now();
#ifndef QUIET
	printf("compile: %11.2f us\n  query: %11.2f us\n",
		std::chrono::duration<double, std::micro>( t_compile - t_start).count(),
		std::chrono::duration<double, std::micro>( t_end - t_compile).count()
	);
#endif

	compilation_time += std::chrono::duration<double, std::micro>( t_compile - t_start).count();
	exec_time += std::chrono::duration<double, std::micro>( t_end - t_compile).count();
#endif

	printResult(amount, res, q.selections.size(), fd_out);
}
void codegenLLVMjit(const Query &q, ScanOperator *scan, ProjectionOperator *proj, FILE *fd_out, void *data, size_t /*query*/){
	coat::runtimellvmjit *llvmrt = (coat::runtimellvmjit*) data;
#ifdef MEASURE_TIME
	auto t_start = std::chrono::high_resolution_clock::now();
#endif
	coat::Function<coat::runtimellvmjit,codegen_func_type> fn(*llvmrt);
	{
		CodegenContext ctx(fn, q.relationIds.size(), q.selections.size());
		scan->codegen(fn, ctx);
		proj->codegen_save(fn, ctx);
		coat::ret(fn, ctx.amount);
	}
	llvmrt->print("last.ll");
	if(!llvmrt->verifyFunctions()){
		puts("verification failed. aborting.");
		exit(EXIT_FAILURE); //FIXME: better error handling
	}
	if(llvmrt->getOptLevel() > 0){
		llvmrt->optimize();
		llvmrt->print("last_opt.ll");
		if(!llvmrt->verifyFunctions()){
			puts("verification after optimization failed. aborting.");
			exit(EXIT_FAILURE); //FIXME: better error handling
		}
	}
	// finalize function
	codegen_func_type fnptr = fn.finalize(*llvmrt);
#ifdef MEASURE_TIME
	auto t_compile = std::chrono::high_resolution_clock::now();
#endif

	const size_t rsize = q.selections.size();
	uint64_t res[rsize];
	const uint64_t tuples = scan->getTuples();
#ifdef MORSELS
	uint64_t amount = morsel_execution(fnptr, tuples, res, rsize);
#else
	// execute generated function
	uint64_t amount = fnptr(0, tuples, res);
#endif

#ifdef MEASURE_TIME
	auto t_end = std::chrono::high_resolution_clock::now();
#ifndef QUIET
	printf("compile: %11.2f us\n  query: %11.2f us\n",
		std::chrono::duration<double, std::micro>( t_compile - t_start).count(),
		std::chrono::duration<double, std::micro>( t_end - t_compile).count()
	);
#endif

	compilation_time += std::chrono::duration<double, std::micro>( t_compile - t_start).count();
	exec_time += std::chrono::duration<double, std::micro>( t_end - t_compile).count();
#endif

	printResult(amount, res, q.selections.size(), fd_out);
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

	auto t_init = std::chrono::high_resolution_clock::now();

	// init JIT engines
#ifdef ENABLE_ASMJIT
	coat::runtimeasmjit asmrt;
#endif
#ifdef ENABLE_LLVMJIT
	coat::runtimellvmjit::initTarget();
	coat::runtimellvmjit llvmjit;
#endif

	char *p = &argv[1][1];
	while(*p){
		if(*p == 't'){
			parseWork(argv[3], relations, tupleByTuple, nullptr);
#if ENABLE_ASMJIT
		}else if(*p == 'a'){
			parseWork(argv[3], relations, codegenAsmjit, &asmrt);
#endif
#ifdef ENABLE_LLVMJIT
		}else if(*p == 'l'){
			switch(p[1]){
				case '0': llvmjit.setOptLevel(0); break;
				case '1': llvmjit.setOptLevel(1); break;
				case '2': llvmjit.setOptLevel(2); break;
				case '3': llvmjit.setOptLevel(3); break;
				default: break;
			}
			parseWork(argv[3], relations, codegenLLVMjit, &llvmjit);
#endif
		}
		++p;
	}

	auto t_end = std::chrono::high_resolution_clock::now();
	printf(" init: %12.2f us\n work: %12.2f us\ntotal: %12.2f us\n",
		std::chrono::duration<double, std::micro>(t_init - t_start).count(),
		std::chrono::duration<double, std::micro>( t_end - t_init ).count(),
		std::chrono::duration<double, std::micro>( t_end - t_start).count()
	);

#ifdef MEASURE_TIME
	printf("\nbreakdown of time working on queries from query preparation, compilation latency to execution time\n"
			"accumulated from all queries in the workload, disable QUIET in cmake to see details for each query\n"
			"prepare: %12.2f us\ncompile: %12.2f us\nexecute: %12.2f us\n",
		prepare_time, compilation_time, exec_time
	);
#endif

	return 0;
}
