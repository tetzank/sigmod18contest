#ifndef OPERATOR_H_
#define OPERATOR_H_

#include <vector>

#include <coat/Function.h>


/*
operators:
 - "Scan": just loop through all rowids of starting relation
 - Filter: check against literal, eq/gt/lt
 - Join: check against "hash" table, carry rowid (might be needed in projection)
         multiple variants depending on joined column
		  - JoinOperator:       using MultiArrayTable if non-unique
		  - JoinUniqueOperator: using ArrayTable      if unique
		  - SemiJoinOperator:   using BitsetTable     if unique     and not used later
 - "self join": left and right relation are both already joined/scanned, could be the same relation, just filter
*/

// function signature of generated function
// parameters: lower, upper (morsel), ptr to buffer of projection entries
// returns number of results
using codegen_func_type = uint64_t (*)(uint64_t,uint64_t,uint64_t*);
using Fn_asmjit = coat::Function<coat::runtimeasmjit,codegen_func_type>;
using Fn_llvmjit = coat::Function<coat::runtimellvmjit,codegen_func_type>;


struct Context{
	// current rowids in order as relations are defined in query
	std::vector<uint64_t> rowids;

	Context(size_t size){
		rowids.resize(size);
	}
};


inline uint64_t loadValue(const column_t &col, uint64_t idx){
	// load value from column which can have different type sizes
	switch(col.index()){
		case 0: return std::get<uint64_t*>(col)[idx];
		case 1: return std::get<uint32_t*>(col)[idx];
		case 2: return std::get<uint16_t*>(col)[idx];

		default:
			fprintf(stderr, "unknown type in column_t: %lu\n", col.index());
			abort();
	}
}


template<class Fn>
struct CodegenContext {
	using CC = typename Fn::F;

	std::tuple<coat::Value<CC,uint64_t>,coat::Value<CC,uint64_t>,coat::Ptr<CC,coat::Value<CC,uint64_t>>> arguments;
	std::vector<coat::Value<CC,uint64_t>> rowids;
	std::vector<coat::Value<CC,uint64_t>> results;
	coat::Value<CC,uint64_t> amount;

	CodegenContext(Fn &fn, size_t numberOfRelations, size_t numberOfProjections)
		: arguments(fn.getArguments("lower", "upper", "proj_addr"))
		, amount(fn, 0UL, "amount")
	{
		// emplace_back() in a loop to avoid copies
		rowids.reserve(numberOfRelations);
		for(size_t i=0; i<numberOfRelations; ++i){
			rowids.emplace_back(fn);
		}
		results.reserve(numberOfProjections);
		for(size_t i=0; i<numberOfProjections; ++i){
			results.emplace_back(fn, 0UL);
		}
	}
};


// loadValue with COAT
template<class Fn, class CC>
coat::Value<CC,uint64_t> loadValue(Fn &fn, const column_t &col, coat::Value<CC,uint64_t> &idx){
	coat::Value<CC, uint64_t> loaded(fn, "loaded");
	switch(col.index()){
		case 0: {
			auto vr_col = fn.embedValue(std::get<uint64_t*>(col), "col");
			// fetch 64 bit value from column
			loaded = vr_col[idx];
			break;
		}
		case 1: {
			auto vr_col = fn.embedValue(std::get<uint32_t*>(col), "col");
			// fetch 32 bit value from column and extend to 64 bit
			loaded.widen(vr_col[idx]);
			break;
		}
		case 2: {
			auto vr_col = fn.embedValue(std::get<uint16_t*>(col), "col");
			// fetch 16 bit value from column and extend to 64 bit
			loaded.widen(vr_col[idx]);
			break;
		}

		default:
			fprintf(stderr, "unknown type in column_t: %lu\n", col.index());
			abort();
	}
	return loaded;
}


class Operator{
protected:
	Operator *next=nullptr;

public:
	Operator(){}
	virtual ~Operator(){
		delete next;
	}

	void setNext(Operator *next){
		this->next = next;
	}

	// tuple-by-tuple execution
	virtual void execute(Context*)=0;

	// code generation with coat, for each backend, chosen at runtime
	virtual void codegen(Fn_asmjit&, CodegenContext<Fn_asmjit>&)=0;
	virtual void codegen(Fn_llvmjit&, CodegenContext<Fn_llvmjit>&)=0;
};

#endif
