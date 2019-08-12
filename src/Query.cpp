#include "Query.h"

#include <algorithm>

#include "ScanOperator.h"
#include "FilterOperator.h"
#include "SelfJoinOperator.h"
#include "JoinOperator.h"
#include "JoinUniqueOperator.h"
#include "SemiJoinOperator.h"
#include "ProjectionOperator.h"


void Query::parse(char *line){
	char *rels  = strtok(line, "|");
	char *preds = strtok(nullptr, "|");
	char *sels  = strtok(nullptr, "|");

#ifdef VERBOSE_PARSE
	printf("rels: '%s'\npreds: '%s'\nsels: '%s'\n", rels, preds, sels);
#endif

	// parse relation list
	{
		char *r = strtok(rels, " ");
		while(r){
			relationIds.push_back(strtoul(r, nullptr, 10));
			r = strtok(nullptr, " ");
		}
#ifdef VERBOSE_PARSE
		puts("relations");
		for(unsigned i : relationIds){
			printf("%u\n", i);
		}
#endif
	}

	// parse predicates
	{
		char *p = strtok(preds, "&");
		while(p){
			// assumes at most 10 relations in one query
			unsigned relid = *p - '0';
			// left side always relation with column
			// move past dot as well
			p += 2;
			unsigned colid = *p - '0';
			while(*++p <= '9'){ //HACK: no checks at all
				colid = colid*10 + (*p - '0');
			}
			if(*p == '=' && p[2]=='.'){
				// equi-join predicate
				unsigned r2 = p[1] - '0';
				p += 3;
				unsigned c2 = *p - '0';
				while(*++p != '\0'){ //HACK: no checks at all
					c2 = c2*10 + (*p - '0');
				}
				predicates.emplace_back(relid, colid, r2, c2);
			}else{
				// filter
				uint64_t constant = strtoul(p+1, nullptr, 10);
				filters.emplace_back(relid, colid, constant, Filter::Comparison(*p));
			}
			
			p = strtok(nullptr, "&");
		}
#ifdef VERBOSE_PARSE
		puts("predicates");
		for(const Predicate &p : predicates){
			printf("r%u.c%u = r%u.c%u\n",
				p.left.relationId, p.left.columnId,
				p.right.relationId, p.right.columnId);
		}
		puts("filters");
		for(const Filter &f : filters){
			printf("r%u.c%u %c %lu\n",
				f.sel.relationId, f.sel.columnId, f.comparison, f.constant);
		}
#endif
	}

	// parse selections
	{
		char *r = strtok(sels, " ");
		while(r){
			// assumes at most 10 relations in one query
			unsigned relid = *r - '0';
			unsigned colid = strtoul(r+2, nullptr, 10);
			selections.emplace_back(relid, colid);
			r = strtok(nullptr, " ");
		}
#ifdef VERBOSE_PARSE
		puts("selections");
		for(const auto &ele : selections){
			printf("r%u.c%u\n", ele.relationId, ele.columnId);
		}
#endif
	}
}

void Query::rewrite(const std::vector<Relation> &relations){
#ifdef REWRITE_ORDER
	// reorder relations according to their "selectivity", most selective first
	// poor mans approach: filter with equality first, relations size, filter on relation
	std::vector<std::pair<unsigned,unsigned>> newOrder;
	newOrder.reserve(relationIds.size());
	for(size_t i=0; i<relationIds.size(); ++i){
		newOrder.emplace_back(relationIds[i], i);
	}
	//HACK
	uint64_t filterOnBinding=0, equalityFilterOnBinding=0;
	for(const Filter &f : filters){
		filterOnBinding |= 1ULL << f.sel.relationId;
		if(f.comparison == Filter::Comparison::Equal){
			equalityFilterOnBinding |= 1ULL << f.sel.relationId;
		}
	}
	std::sort(newOrder.begin(), newOrder.end(),
		[&relations,filterOnBinding,equalityFilterOnBinding](const auto &f, const auto &s){
			bool f_eq = equalityFilterOnBinding & (1ULL << f.second);
			bool s_eq = equalityFilterOnBinding & (1ULL << s.second);
			if(f_eq == s_eq){
				auto f_size = relations[f.first].getNumberOfTuples();
				auto s_size = relations[s.first].getNumberOfTuples();
				if(f_size == s_size){
					bool f_filter = filterOnBinding & (1ULL << f.second);
					bool s_filter = filterOnBinding & (1ULL << s.second);
					if(f_filter == s_filter){
						return true; // don't care about order, leave it
					}else{
						return f_filter;
					}
				}else{
					return f_size < s_size;
				}
			}else{
				return f_eq;
			}
		}
	);
	// rewrite map for reordering, old binding to new binding
	std::vector<unsigned> rewritemap(relationIds.size());
	for(size_t i=0; i<newOrder.size(); ++i){
		rewritemap[newOrder[i].second] = i;
		relationIds[i] = newOrder[i].first; // replace old order
	}
#ifndef QUIET
	for(size_t i=0; i<rewritemap.size(); ++i){
		printf("%lu -> %u, ", i, rewritemap[i]);
	}
	printf("\n");
#endif
	// rewrite every predicate
	for(Predicate &p : predicates){
		p.left.relationId = rewritemap[p.left.relationId];
		p.right.relationId = rewritemap[p.right.relationId];
		if(p.left.relationId > p.right.relationId){
			std::swap(p.left, p.right);
		}
	}
	// reorder predicates
	std::sort(predicates.begin(), predicates.end(), [](const Predicate &f, const Predicate &s){
#if 1
		if(f.left.relationId == s.left.relationId){
			return f.right.relationId < s.right.relationId;
		}else
#endif
			return f.left.relationId < s.left.relationId;
	});
	// check connectivity
	unsigned usedRelations = 1U << predicates[0].left.relationId; // scanned
	//for(Predicate &p : predicates){
	for(size_t i=0,size=predicates.size(); i<size; ++i){
		Predicate &p = predicates[i];
		bool left  = usedRelations & (1u << p.left.relationId);
		bool right = usedRelations & (1u << p.right.relationId);
		if(!left && !right){
			std::swap(predicates[i], predicates[i+1]); // we are always connected, last predicates cannot fail
			--i;
			continue;
		}
		if(!left && right){
			std::swap(p.left, p.right);
		}
		usedRelations |= 1U << p.right.relationId;
	}
	// rewrite filters and projections
	for(Filter &f : filters){
		f.sel.relationId = rewritemap[f.sel.relationId];
	}
	for(Selection &s : selections){
		s.relationId = rewritemap[s.relationId];
	}
#ifndef QUIET
	for(unsigned r : relationIds){
		printf("%u ", r);
	}
	printf("| ");
	for(const auto &p : predicates){
		printf("%u.%u=%u.%u, ", p.left.relationId, p.left.columnId, p.right.relationId, p.right.columnId);
	}
	printf("| ");
	for(auto &f : filters){
		printf("%u.%u%c%lu, ", f.sel.relationId, f.sel.columnId, f.comparison, f.constant);
	}
	printf("| ");
	for(auto &s : selections){
		printf("%u.%u, ", s.relationId, s.columnId);
	}
	printf("\n");
#endif
#endif

	//  - replace equivalent columns with one column to reduce read columns
	//  - remove identical joins or filter (0.0=1.0 & 0.0=1.0 & 1.0=0.0)
	//    => effect in total very small
	//TODO: other rewrites:
	//  - remove filter which are pointless (0.0<10 & 0.0<12 or 0.1=10 & 0.1<12)
	//    => only a few queries with very low runtime -> only tiny gain possible
	//  - pointless self joins (0.0 = 0.0)
	//    => because of reorder it does not happen anymore

#ifdef REWRITE_EQUIVALENCE
	// rewrite joins
	for(size_t pred=1,predsize=predicates.size(); pred<predsize; ++pred){
		auto &predicate = predicates[pred];
		for(size_t prev=0; prev<pred; ++prev){
			const auto &prevpredicate = predicates[prev];
			if(predicate.left == prevpredicate.right){
				predicate.left = prevpredicate.left;
			}
		}
	}
#ifndef QUIET
	for(const auto &p : predicates){
		printf("%u.%u=%u.%u, ", p.left.relationId, p.left.columnId, p.right.relationId, p.right.columnId);
	}
	printf("\n");
#endif
	// rewrite filter
	for(auto &f : filters){
		for(size_t pred=0,predsize=predicates.size(); pred<predsize; ++pred){
			auto &predicate = predicates[pred];
			if(f.sel == predicate.right){
				f.sel = predicate.left;
			}
		}
	}
#ifndef QUIET
	for(auto &f : filters){
		printf("%u.%u%c%lu, ", f.sel.relationId, f.sel.columnId, f.comparison, f.constant);
	}
	printf("\n");
#endif
	// rewrite selections
	for(auto &s : selections){
		for(size_t pred=0,predsize=predicates.size(); pred<predsize; ++pred){
			auto &predicate = predicates[pred];
			if(s == predicate.right){
				s = predicate.left;
			}
		}
	}
#ifndef QUIET
	for(auto &s : selections){
		printf("%u.%u, ", s.relationId, s.columnId);
	}
	printf("\n");
#endif
#endif

#ifdef REWRITE_IDENTICALJOINS
	for(size_t pred=1,predsize=predicates.size(); pred<predsize; ++pred){
		auto &predicate = predicates[pred];
		for(size_t prev=0; prev<pred; ++prev){
			const auto &prevpredicate = predicates[prev];
			if(predicate == prevpredicate){
				predicates.erase(predicates.begin()+pred);
				--pred; --predsize;
				break;
			}
		}
	}
#ifndef QUIET
	for(const auto &p : predicates){
		printf("%u.%u=%u.%u, ", p.left.relationId, p.left.columnId, p.right.relationId, p.right.columnId);
	}
	printf("\n");
#endif
#endif
}

std::pair<ScanOperator*,ProjectionOperator*> Query::constructPipeline(const std::vector<Relation> &relations){
	// create pipeline, left-deep in order of predicates
	unsigned binding = predicates[0].left.relationId;
	// get relation id in database instead of binding in query
	const unsigned relid = relationIds[binding];
	// keep track of which relations are scanned/joined already
	unsigned usedRelations = 1u << binding; // bitset, assumes small number of relations
	ScanOperator *scan = new ScanOperator(relations[relid]);
	Operator *lastop = scan;
	// find filters for the scanned relation
	for(const auto &f : filters){
		if(f.sel.relationId == binding){
			FilterOperator *filter = new FilterOperator(relations[relid], f);
			lastop->setNext(filter);
			lastop = filter;
		}
	}
	// joins for every join predicate
	//  - next join, check if joined table has filter -> create hash table or use precalced
	for(size_t pred=0, predsize=predicates.size(); pred<predsize; ++pred){
		const auto &p = predicates[pred];
		//FIXME: only works if left side is already joined or scanned
		if(usedRelations & (1u << p.left.relationId)){
			unsigned relid_right = relationIds[p.right.relationId];
			unsigned relid_left = relationIds[p.left.relationId];
			// if right side is also known, circular join graph, kind of filter, does not join new relation
			if(usedRelations & (1u << p.right.relationId)){
				// no new relation, similar to filter
				SelfJoinOperator *selfjoin = new SelfJoinOperator(relations[relid_left], relations[relid_right], p.left, p.right);
				lastop->setNext(selfjoin);
				lastop = selfjoin;
			}else{
				// check if right-side relation is used later or just semijoin
				bool used=false;
				// check following predicates
				for(size_t pred2=pred+1; pred2<predsize; ++pred2){
					const auto &p2 = predicates[pred2];
					if(p2.left.relationId == p.right.relationId || p2.right.relationId == p.right.relationId){
						used = true;
						break;
					}
				}
				// check filters
				for(const auto &f : filters){
					if(f.sel.relationId == p.right.relationId/*binding*/){
						used = true;
						break;
					}
				}
				// check selections
				for(const auto &s : selections){
					if(s.relationId == p.right.relationId){
						used = true;
						break;
					}
				}
				//TODO: semijoin with a plain bitset only works if the column is unique
				//      otherwise we have to "multiply" with the number of occurences
				//      very specific to programming contest as we sum every projected column
#ifndef QUIET
				if(!used && p.right.columnId!=0){
					puts("semijoin possible on non-unique column");
				}
#endif
				if(used || p.right.columnId!=0){ //HACK
					usedRelations |= 1u << p.right.relationId;
					// get relation id in database instead of binding in query
					const auto *ht = relations[relid_right].getHT(p.right.columnId);
					Operator *join;
					switch(ht->index()){
						case 0:
							fprintf(stderr, "monostate variant\n");
							exit(1);
							break;
						case 1:
							join = new JoinOperator(relations[relid_left], p.left, p.right, std::get_if<HT_t>(ht));
							break;
						case 2:
							join = new JoinUniqueOperator(relations[relid_left], p.left, p.right, std::get_if<HTu_t>(ht));
							break;

						default:
							fprintf(stderr, "unexpected index in variant of hashtables: %lu\n", ht->index());
							exit(1);
					}
					lastop->setNext(join);
					lastop = join;
					//HACK: handle filter predicates on joined relations
					// find filters for newly joined relation
					for(const auto &f : filters){
						if(f.sel.relationId == p.right.relationId/*binding*/){
							FilterOperator *filter = new FilterOperator(relations[relid_right], f);
							lastop->setNext(filter);
							lastop = filter;
						}
					}
				}else{
					// semijoin
					const auto *bt = relations[relid_right].getBT(p.right.columnId);
					SemiJoinOperator *semijoin = new SemiJoinOperator(relations[relid_left], p.left, bt);
					lastop->setNext(semijoin);
					lastop = semijoin;
#ifndef QUIET
					puts("semijoin used");
#endif
				}
			}
		}else{
			//TODO: retry later, not needed it workload it seems
			fprintf(stderr, "not implemented retry\n");
		}
	}

	// aggregation at the end
	ProjectionOperator *proj = new ProjectionOperator(relations, relationIds, selections);
	lastop->setNext(proj);

	return {scan, proj};
}

void Query::clear(){
	relationIds.clear();
	predicates.clear();
	filters.clear();
	selections.clear();
}
