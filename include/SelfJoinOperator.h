#ifndef SELFJOINOPERATOR_H_
#define SELFJOINOPERATOR_H_

#include "Operator.h"
#include "Relation.h"

#include "coat/ControlFlow.h"


// relations on both sides of join already bound -> simple filter instead of join
// it is not limited to the same relation on both sides, can be different relations
class SelfJoinOperator final : public Operator{
private:
	const column_t &leftColumn;
	const column_t &rightColumn;
	unsigned leftBinding;
	unsigned rightBinding;

	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
		auto lval = loadValue(fn, leftColumn, ctx.rowids[leftBinding]);
		auto rval = loadValue(fn, rightColumn, ctx.rowids[rightBinding]);
		if_then(fn, lval == rval, [&]{
			next->codegen(fn, ctx);
		});
	}

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

	void codegen(Fn_asmjit &fn, CodegenContext<Fn_asmjit> &ctx) override { codegen_impl(fn, ctx); }
	void codegen(Fn_llvmjit &fn, CodegenContext<Fn_llvmjit> &ctx) override { codegen_impl(fn, ctx); }
};

#endif
