#ifndef QUERY_H_
#define QUERY_H_

#include <cstdint>
#include <vector>

#include "Relation.h"
//#include "RelationalOperators.h"
// just forward declare
class ScanOperator;
class ProjectionOperator;


struct Selection{
	unsigned relationId; // binding in query, not relation id in database
	unsigned columnId;

	Selection(unsigned r, unsigned c) : relationId(r), columnId(c) {}

	void operator=(const Selection &other){
		relationId = other.relationId;
		columnId = other.columnId;
	}
	bool operator==(const Selection &other) const {
		return relationId==other.relationId && columnId==other.columnId;
	}
};

// join predicates, can be self-join, always equi-join
struct Predicate{
	Selection left, right;

	Predicate(unsigned r1, unsigned c1, unsigned r2, unsigned c2)
		: left(r1, c1), right(r2, c2) {}

	bool operator==(const Predicate &other) const {
		return left==other.left && right==other.right;
	}
};

// predicate with comparison against constant
struct Filter{
	Selection sel;
	uint64_t constant;

	enum Comparison : char { Less='<', Greater='>', Equal='=' };
	Comparison comparison;

	Filter(unsigned r, unsigned c, uint64_t constant, Comparison cmp)
		: sel(r, c), constant(constant), comparison(cmp) {}
};

struct Query{
	std::vector<unsigned> relationIds;
	std::vector<Predicate> predicates;
	std::vector<Filter> filters;
	std::vector<Selection> selections;

	void parse(char *line);
	void rewrite(const std::vector<Relation> &relations);
	std::pair<ScanOperator*,ProjectionOperator*> constructPipeline(const std::vector<Relation> &relations);
	void clear();
};


#endif
