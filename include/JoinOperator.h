#ifndef JOINOPERATOR_H_
#define JOINOPERATOR_H_

#include "Operator.h"
#include "Relation.h"


// join on column with non-unique elements, using precalculated MultiArrayTable
class JoinOperator final : public Operator{
private:
	const column_t &probeColumn;
	const HT_t *hashtable;
	unsigned probeRelation;
	unsigned buildRelation;

	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
		// fetch value from probed column
		auto val = loadValue(fn, probeColumn, ctx.rowids[probeRelation]);
		// embed pointer to hashtable in the generated code
		auto ht = fn.embedValue(hashtable, "hashtable");
		// iterate over all join partners
		ht.iterate(val, [&](auto &ele){
			// set rowid of joined relation
			ctx.rowids[buildRelation] = ele;
			next->codegen(fn, ctx);
		});
	}

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

	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
};

#endif
