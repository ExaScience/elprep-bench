# elprep-bench
## elPrep reimplementations in C++ and Java, only for benchmark comparisons

---

**This is not the official elPrep repository. If you want to use elPrep in production, please go to [the elPrep repository](https://github.com/exascience/elprep "elPrep repository").**

---

All code in the elprep-bench repository is copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserved.

This repository contains rudimentary reimplementations of elPrep in C++17 and Java, with the sole purpose to evaluate their runtime performance and memory use. We have used these benchmarks alongside with another reimplementation in Go, and based on the results, settled on Go for the production-ready elPrep version, which you can find in [the main elPrep repository](https://github.com/exascience/elprep "elPrep repository") under an open-source license.

You can find a detailed discussion of the benchmarks and their results in our paper ["A comparison of three programming languages for a full-fledged next-generation sequencing tool"](https://doi.org/10.1186/s12859-019-2903-5).

We currently do not intend to accept any pull requests or make any other modifications to the code in this repository. The reason for this is that the repository is referenced in our paper, so we need to keep that reference and especially the code stable.

However, feel free to fork the repository and create your own versions. If you have interesting results, we will gladly link to them from this README. Please ensure that you keep the copyright statement in place for versions that are derived from our code.
