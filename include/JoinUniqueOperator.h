#ifndef JOINUNIQUEOPERATOR_H_
#define JOINUNIQUEOPERATOR_H_

#include "Operator.h"
#include "Relation.h"


// join on column with unique elements, using precalculated ArrayTable
class JoinUniqueOperator final : public Operator{
private:
	const column_t &probeColumn;
	const HTu_t *hashtable;
	unsigned probeRelation;
	unsigned buildRelation;

	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
		// fetch value from probed column
		auto val = loadValue(fn, probeColumn, ctx.rowids[probeRelation]);
		// embed pointer to hashtable in the generated code
		auto ht = fn.embedValue(hashtable, "hashtable_unique");
		// lookup join partner, if there is one
		ht.lookup(val, [&](auto &ele){
			// set rowid of joined relation
			ctx.rowids[buildRelation] = ele;
			next->codegen(fn, ctx);
		});
	}

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

	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
};

#endif
