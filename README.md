SIGMOD Programming Contest 2018: Code Generation
===

This repository contains a prototype for the [SIGMOD Programming Contest 2018](http://sigmod18contest.db.in.tum.de/task.shtml).
It is a showcase for code generation in databases and for [COAT](https://github.com/tetzank/coat), an EDSL for C++ which makes code generation easier.

More details are explained in a [blog post](https://tetzank.github.io/posts/codegen-in-databases/).


## Build Instructions

Fetch the JIT engines LLVM and Asmjit, and build them (use more or less cores by changing `-j`, LLVM can take a while...):
```
$ ./buildDependencies.sh -j8
```

Then, build the prototype with cmake:
```
$ mkdir build
$ cd build
$ cmake ..
$ make -j8
```

## Run Instructions

Download and unpack the test workload from the contest:
```
$ cd workloads
$ ./download.sh
```

Afterwards, run the test workload:
```
$ cd public
$ ../../build/sig18 -t public.{init,work}
```

This runs the naive baseline with a tuple-at-a-time execution engine without code generation.

For Asmjit, run:
```
$ ../../build/sig18 -a public.{init,work}
```

For LLVM, run:
```
$ ../../build/sig18 -l3 public.{init,work}
```
You can pick an optimization level from 0 to 3.

The expected results of each query are in public.res.
Use `diff` to compare the output for correctness.

Here are some measurements from my workstation.

Back End | Compilation Latency | Execution Time |
 ------- | -------------------:| --------------:|
AsmJit   |                5 ms |         550 ms |
LLVM -O0 |              341 ms |         768 ms |
LLVM -O1 |              650 ms |         513 ms |
LLVM -O2 |              663 ms |         521 ms |
LLVM -O3 |              660 ms |         567 ms |
