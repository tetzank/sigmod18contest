#ifndef FILTEROPERATOR_H_
#define FILTEROPERATOR_H_

#include "Operator.h"

#include "Query.h"


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

#endif
