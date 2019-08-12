#ifndef SCANOPERATOR_H_
#define SCANOPERATOR_H_

#include "Operator.h"

#include "coat/ControlFlow.h"


class ScanOperator final : public Operator{
private:
	uint64_t tuples;

#if defined(ENABLE_ASMJIT) || defined(ENABLE_LLVMJIT)
	template<class Fn>
	void codegen_impl(Fn &fn, CodegenContext<Fn> &ctx){
#ifndef QUIET
		puts("ScanOperator");
#endif
		// do not make a copy, just take the virtual register from arguments
		ctx.rowids[0] = std::move(std::get<0>(ctx.arguments));
		auto &upper = std::get<1>(ctx.arguments);
		coat::do_while(fn, [&]{
			next->codegen(fn, ctx);
			++ctx.rowids[0];
		}, ctx.rowids[0] < upper);
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

#endif
