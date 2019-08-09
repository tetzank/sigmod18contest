#ifndef RELATIONALOPERATORS_H_
#define RELATIONALOPERATORS_H_

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

#include "Relation.h"
#include "Query.h"

#include "coat/ControlFlow.h"


// function signature of generated function
// parameters: lower, upper (morsel), ptr to buffer of projection entries
// returns number of results
//using codegen_func_type = uint64_t (*)(uint64_t,uint64_t,uint64_t*);
using codegen_func_type = uint64_t (*)(uint64_t*);
#ifdef ENABLE_ASMJIT
using Fn_asmjit = coat::Function<coat::runtimeasmjit,codegen_func_type>;
#endif
#ifdef ENABLE_LLVMJIT
using Fn_llvmjit = coat::Function<coat::runtimellvmjit,codegen_func_type>;
#endif


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


#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)

template<class Fn>
struct CodegenContext {
	using CC = typename Fn::F;

	std::vector<coat::Value<CC,uint64_t>> rowids;
	std::vector<coat::Value<CC,uint64_t>> results;
	coat::Value<CC,uint64_t> amount;
	std::tuple<coat::Ptr<CC,coat::Value<CC,uint64_t>>> arguments;

	CodegenContext(Fn &fn, size_t numberOfRelations, size_t numberOfProjections)
		: rowids(numberOfRelations, coat::Value<CC,uint64_t>(fn))
		, results(numberOfProjections, coat::Value<CC,uint64_t>(fn, 0UL))
		, amount(fn, 0UL, "amount")
		, arguments(fn.getArguments("proj_addr"))
	{
		
	}
};


// loadValue with COAT
template<class Fn, class CC>
coat::Value<CC,uint64_t> loadValue(Fn &fn, const column_t &col, coat::Value<CC,uint64_t> &idx){
	coat::Value<CC, uint64_t> loaded(fn, "loaded");
	switch(col.index()){
		case 0: {
			uint64_t *c = std::get<uint64_t*>(col);
			coat::Ptr<CC,coat::Value<CC,uint64_t>> vr_col(fn, "col");
			vr_col = c;
			loaded = vr_col[idx];
			break;
		}
		case 1: {
			uint32_t *c = std::get<uint32_t*>(col);
			coat::Ptr<CC,coat::Value<CC,uint32_t>> vr_col(fn, "col");
			vr_col = c;
			loaded.widen(vr_col[idx]);
			break;
		}
		case 2: {
			uint16_t *c = std::get<uint16_t*>(col);
			coat::Ptr<CC,coat::Value<CC,uint16_t>> vr_col(fn, "col");
			vr_col = c;
			loaded.widen(vr_col[idx]);
			break;
		}

		default:
			fprintf(stderr, "unknown type in column_t: %lu\n", col.index());
			abort();
	}
	
	return loaded;
}
#endif


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
#ifdef ENABLE_ASMJIT
	virtual void codegen(Fn_asmjit&, CodegenContext<Fn_asmjit>&)=0;
#endif
#ifdef ENABLE_LLVMJIT
	virtual void codegen(Fn_llvmjit&, CodegenContext<Fn_llvmjit>&)=0;
#endif
};


class ScanOperator final : public Operator{
private:
	uint64_t tuples;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("ScanOperator");
#endif
		coat::Value vr_tuples(fn, tuples, "tuples");
		ctx.rowids[0] = 0;
		coat::do_while(fn, [&]{
			next->codegen(fn, ctx);
			++ctx.rowids[0];
		}, ctx.rowids[0] < vr_tuples);
	}
#endif

public:
	ScanOperator(const Relation &relation) : tuples(relation.getNumberOfTuples()) {}

	void execute(Context *ctx) override {
		for(uint64_t idx=0; idx<tuples; ++idx){
			// pass tuple by tuple (very bad performance without codegen)
			ctx->rowids[0] = idx;
			next->execute(ctx);
		}
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif

	uint64_t getTuples() const {
		return tuples;
	}
};


class FilterOperator final : public Operator{
private:
	const column_t &column;
	uint64_t constant;
	unsigned relid;
	Filter::Comparison comparison;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("FilterOperator");
#endif
		// read from column, depends on column type
		auto val = loadValue(fn, column, ctx.rowids[relid]);
		switch(comparison){
			case Filter::Comparison::Less: {
				coat::if_then(fn, val < constant, [&]{
					next->codegen(fn, ctx);
				});
				break;
			}
			case Filter::Comparison::Greater: {
				coat::if_then(fn, val > constant, [&]{
					next->codegen(fn, ctx);
				});
				break;
			}
			case Filter::Comparison::Equal: {
				coat::if_then(fn, val == constant, [&]{
					next->codegen(fn, ctx);
				});
				break;
			}
		}
	}
#endif

public:
	FilterOperator(const Relation &relation, const Filter &filter)
		: column(relation.getColumn(filter.sel.columnId))
		, constant(filter.constant)
		, relid(filter.sel.relationId)
		, comparison(filter.comparison)
	{}

	void execute(Context *ctx) override{
		uint64_t val = loadValue(column, ctx->rowids[relid]);
		switch(comparison){
			case Filter::Comparison::Less: {
				if(val < constant){
					next->execute(ctx);
				}
				break;
			}
			case Filter::Comparison::Greater: {
				if(val > constant){
					next->execute(ctx);
				}
				break;
			}
			case Filter::Comparison::Equal: {
				if(val == constant){
					next->execute(ctx);
				}
				break;
			}
		}
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
};


// relations on both sides of join already bound -> simple filter instead of join
// it is not limited to the same relation on both sides, can be different relations
class SelfJoinOperator final : public Operator{
private:
	const column_t &leftColumn;
	const column_t &rightColumn;
	unsigned leftBinding;
	unsigned rightBinding;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("SelfJoinOperator");
#endif
		auto lval = loadValue(fn, leftColumn, ctx.rowids[leftBinding]);
		auto rval = loadValue(fn, rightColumn, ctx.rowids[rightBinding]);
		if_then(fn, lval == rval, [&]{
			next->codegen(fn, ctx);
		});
	}
#endif

public:
	SelfJoinOperator(
		const Relation &leftRelation,
		const Relation &rightRelation,
		const Selection &leftSide,
		const Selection &rightSide
	)
		: leftColumn(leftRelation.getColumn(leftSide.columnId))
		, rightColumn(rightRelation.getColumn(rightSide.columnId))
		, leftBinding(leftSide.relationId)
		, rightBinding(rightSide.relationId)
	{}

	void execute(Context *ctx) override{
		uint64_t lval = loadValue(leftColumn, ctx->rowids[leftBinding]);
		uint64_t rval = loadValue(rightColumn, ctx->rowids[rightBinding]);
		if(lval == rval){
			next->execute(ctx);
		}
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
};


// join on column with non-unique elements, using precalculated MultiArrayTable
class JoinOperator final : public Operator{
private:
	const column_t &probeColumn;
	const HT_t *hashtable;
	unsigned probeRelation;
	unsigned buildRelation;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		printf("JoinOperator: probe=%u; build=%u\n", probeRelation, buildRelation);
#endif
		auto val = loadValue(fn, probeColumn, ctx.rowids[probeRelation]);
		coat::Struct<typename Fn::F,HT_t> ht(fn, "hashtable");
		//FIXME: const structures currently not supported
		ht = const_cast<HT_t*>(hashtable); // load address of hash table as immediate/constant in the generated code
		// iterate over all join partners
		ht.iterate(val, [&](auto &ele){
			// set rowid of joined relation
			ctx.rowids[buildRelation] = ele;
			next->codegen(fn, ctx);
		});
	}
#endif

public:
	JoinOperator(
		const Relation &relation,
		const Selection &probeSide,
		const Selection &buildSide,
		const HT_t *hashtable
	)
		: probeColumn(relation.getColumn(probeSide.columnId))
		, hashtable(hashtable)
		, probeRelation(probeSide.relationId)
		, buildRelation(buildSide.relationId)
	{}

	void execute(Context *ctx) override{
		uint64_t val = loadValue(probeColumn, ctx->rowids[probeRelation]);
		auto [itpos,itend] = hashtable->lookupIterators(val);
		// if val is outside of domain of hashtable, lookup() returns nullptr -> check
		if(itpos && itend){
			for(; itpos != itend; ++itpos){
				// set rowid of joined relation
				ctx->rowids[buildRelation] = *itpos;
				next->execute(ctx);
			}
		}
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
};


// join on column with unique elements, using precalculated ArrayTable
class JoinUniqueOperator final : public Operator{
private:
	const column_t &probeColumn;
	const HTu_t *hashtable;
	unsigned probeRelation;
	unsigned buildRelation;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("JoinUniqueOperator");
#endif
		auto val = loadValue(fn, probeColumn, ctx.rowids[probeRelation]);
		coat::Struct<typename Fn::F,HTu_t> ht(fn, "hashtable_unique");
		//FIXME: const structures currently not supported
		ht = const_cast<HTu_t*>(hashtable); // load address of hash table as immediate/constant in the generated code
		// lookup join partner, if there is one
		ht.lookup(val, [&](auto &ele){
			// set rowid of joined relation
			ctx.rowids[buildRelation] = ele;
			next->codegen(fn, ctx);
		});
	}
#endif

public:
	JoinUniqueOperator(
		const Relation &relation,
		const Selection &probeSide,
		const Selection &buildSide,
		const HTu_t *hashtable
	)
		: probeColumn(relation.getColumn(probeSide.columnId))
		, hashtable(hashtable)
		, probeRelation(probeSide.relationId)
		, buildRelation(buildSide.relationId)
	{}

	void execute(Context *ctx) override{
		uint64_t val = loadValue(probeColumn, ctx->rowids[probeRelation]);
		auto it = hashtable->lookup(val);
		if(it != hashtable->end()){
			// set rowid of joined relation
			ctx->rowids[buildRelation] = it;
			next->execute(ctx);
		}
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
};


// join on column with unique elements and relation is not used afterwards, using precalculated BitsetTable
class SemiJoinOperator final : public Operator{
private:
	const column_t &probeColumn;
	const BitsetTable *hashtable;
	unsigned probeRelation;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("SemiJoinOperator");
#endif
		auto val = loadValue(fn, probeColumn, ctx.rowids[probeRelation]);
		coat::Struct<typename Fn::F,BitsetTable> bt(fn, "bitsettable");
		//FIXME: const structures currently not supported
		bt = const_cast<BitsetTable*>(hashtable); // load address of hash table as immediate/constant in the generated code
		bt.check(val, [&]{
			next->codegen(fn, ctx);
		});
	}
#endif

public:
	SemiJoinOperator(
		const Relation &relation,
		const Selection &probeSide,
		const BitsetTable *hashtable
	)
		: probeColumn(relation.getColumn(probeSide.columnId))
		, hashtable(hashtable)
		, probeRelation(probeSide.relationId)
	{}

	void execute(Context *ctx) override{
		uint64_t val = loadValue(probeColumn, ctx->rowids[probeRelation]);
		if(hashtable->lookup(val)){
			next->execute(ctx);
		}
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
#endif
};


class ProjectionOperator final : public Operator{
private:
	// list of columns and relationIds
	std::vector<std::pair<const column_t*,unsigned>> projections;
	// resulting sums
	std::vector<uint64_t> results;
	// number of tuples reached projection, to distinguish sum==0 and NULL because of no tuples
	uint64_t amount = 0;
	uint64_t size;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("ProjectionOperator");
#endif
		for(size_t i=0; i<size; ++i){
			auto [column, relid] = projections[i];
			auto val = loadValue(fn, *column, ctx.rowids[relid]);
			ctx.results[i] += val;
		}
		++ctx.amount;
	}

	template<class Fn>
	void codegen_save_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("ProjectionOperator save");
#endif
		for(size_t i=0; i<size; ++i){
			auto &projaddr = std::get<0>(ctx.arguments);
			projaddr[i] = ctx.results[i];
		}
	}
#endif

public:
	ProjectionOperator(
		const std::vector<Relation> &relations,
		const std::vector<unsigned> &bindings,
		const std::vector<Selection> &selections
	){
		for(const auto &s : selections){
			unsigned relid = bindings[s.relationId];
			projections.emplace_back(&relations[relid].getColumn(s.columnId), s.relationId);
		}
		// initialize to 0
		results.resize(selections.size(), 0);

		size = selections.size();
	}

	void execute(Context *ctx) override{
		for(size_t i=0; i<size; ++i){
			auto [column, relid] = projections[i];
			uint64_t val = loadValue(*column, ctx->rowids[relid]);
			results[i] += val;
		}
		++amount;
	}

#ifdef ENABLE_ASMJIT
	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
	void codegen_save(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx){ codegen_save_impl(fn, ctx); }
#endif
#ifdef ENABLE_LLVMJIT
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
	void codegen_save(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx){ codegen_save_impl(fn, ctx); }
#endif

	const std::vector<uint64_t> &getResults() const {
		return results;
	}
	uint64_t getAmount() const {
		return amount;
	}
};


#endif
