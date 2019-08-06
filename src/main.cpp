#include <cstdio>

#include "Relation.h"


int main(int argc, char *argv[]){
	Relation table(argv[1]);
	printf("columns: %lu; tuples: %lu\n", table.getNumberOfColumns(), table.getNumberOfTuples());

	return 0;
}
