#ifndef JOINUNIQUEOPERATOR_H_
#define JOINUNIQUEOPERATOR_H_

#include "Operator.h"
#include "Relation.h"

#include "coat/ControlFlow.h"


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

#endif
