#ifndef SEMIJOINOPERATOR_H_
#define SEMIJOINOPERATOR_H_

#include "Operator.h"
#include "Relation.h"
#include "BitsetTable.h"


// join on column with unique elements and relation is not used afterwards, using precalculated BitsetTable
class SemiJoinOperator final : public Operator{
private:
	const column_t &probeColumn;
	const BitsetTable *hashtable;
	unsigned probeRelation;

	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
		auto val = loadValue(fn, probeColumn, ctx.rowids[probeRelation]);
		coat::Struct<typename Fn::F,BitsetTable> bt(fn, "bitsettable");
		//FIXME: const structures currently not supported
		bt = const_cast<BitsetTable*>(hashtable); // load address of hash table as immediate/constant in the generated code
		bt.check(val, [&]{
			next->codegen(fn, ctx);
		});
	}

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

	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
};

#endif
