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
