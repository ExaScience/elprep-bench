# elprep-bench
## elPrep reimplementations in C++ and Java, only for benchmark comparisons

---

**This is not the official elPrep repository. If you want to use elPrep in production, please go to [the elPrep repository](https://github.com/exascience/elprep "elPrep repository").**

---

This repository contains rudimentary reimplementations of elPrep in C++17 and Java, with the sole purpose to evaluate their runtime performance and memory use. We have used these benchmarks alongside with another reimplementation in Go, and based on the results, settled on Go for the production-ready elPrep version, which you can find in [the main elPrep repository](https://github.com/exascience/elprep "elPrep repository") under an open-source license.

You can find a detailed discussion of the benchmarks and their results in our paper ["A comparison of three programming languages for a full-fledged next-generation sequencing tool"](https://doi.org/10.1186/s12859-019-2903-5).

We currently do not intend to accept any pull requests or make any other modifications to the code in this repository. The reason for this is that the repository is referenced in our paper, so we need to keep that reference and especially the code stable.

However, feel free to fork the repository and create your own versions. If you have interesting results, we will gladly link to them from this README. Please ensure that you keep the copyright statement in place for versions that are derived from our code.

---

### License 

elprep-bench is released as an open-source project under the 
terms of the GNU Affero General Public License version 3 as published by the Free Software Foundation, with Additional 
Terms. Please see the file LICENSE.txt for a copy of the GNU Affero Public License and the Additional Terms. 

### Citing elprep-bench

Please cite the following articles:

Herzeel C, Costanza P, Decap D, Fostier J, Wuyts R, Verachtert W (2021) Multithreaded variant calling in elPrep 5. PLoS ONE 16(2): e0244471. https://doi.org/10.1371/journal.pone.0244471

Herzeel C, Costanza P, Decap D, Fostier J, Verachtert W (2019) elPrep 4: A multithreaded framework for sequence analysis. PLoS ONE 14(2): e0209523. https://doi.org/10.1371/journal.pone.0209523

Herzeel C, Costanza P, Decap D, Fostier J, Reumers J (2015) elPrep: High-Performance Preparation of Sequence Alignment/Map Files for Variant Calling. PLoS ONE 10(7): e0132868. https://doi.org/10.1371/journal.pone.0132868

Costanza P, Herzeel C, Verachter W (2019) A comparison of three programming languages for a full-fledged next-generation sequencing tool. BMC Bioinformatics 2019 20:301. https://doi.org/10.1186/s12859-019-2903-5

### Instructions for benchmarking the code

The benchmark that we used in the paper is available in the [elPrep repository](https://github.com/exascience/elprep/tree/v3.04/demo). More specifically, it's the [run-wes-gatk.sh](https://github.com/ExaScience/elprep/blob/v3.04/demo/run-wes-gatk.sh) script.

The script downloads a (pretty large) BAM file that is then processed by elPrep. For testing, you can also run with smaller BAM files. For example, [run-large-gatk.sh](https://github.com/ExaScience/elprep/blob/v3.04/demo/run-large-gatk.sh) uses a somewhat smaller BAM file. Keep in mind, though, that elPrep only makes sense if it can also handle large BAM files.

There are some things to watch out for:

- The scripts are for the older version 3 of elPrep and need to be adapted for newer versions (in Go) and for the bench versions (in C++ and Java from this repository). More specifically:

- The run-wes-gatk.sh script uses a Python wrapper to split up the input file into smaller chunks and then processes them separately before merging the partial results again. We don't support that Python wrapper anymore (elPrep version 4 has a better solution for this), and the benchmark in the paper does not run in that mode anyway but runs elPrep directly on the full input file. Instead of executing the Python wrapper, you should just call `elprep filter` (like in run-large-gatk.sh).

- The scripts pass a --nr-of-threads parameter to elPrep. This is a parameter that came from the original Common Lisp version of elPrep. It is not necessary anymore to pass it to the Go version, and the C++ and Java bench versions simply ignore this parameter. C++, Go, Java and their libraries can figure out the optimal number of worker threads on their own without that parameter. (The Go version still recognizes the parameter, which may end up in a suboptimal configuration, so it's better to drop it from the invocation.)

- Finally, the C++ and Java versions only process the (ASCII-based) SAM format, but not the binary BAM format directly. Instead, you have to use [samtools](https://github.com/samtools/samtools) to convert from BAM to SAM, pipe the result into elPrep, and then pipe the output of elPrep into samtools to convert back into BAM format. The full invocation looks similar to this:

  `
  samtools view -h -@$threads $input.bam | elprep filter /dev/stdin /dev/stdout --filter-unmapped-reads --replace-reference-sequences ucsc.hg19.dict --replace-read-group "ID:grou\
p1 LB:lib1 PL:illumina PU:unit1 SM:sample1" --mark-duplicates --sorting-order coordinate | samtools view -b -@$threads -o $input.result.bam -                                      
  `
  
  See  the [samtools manual page](https://www.htslib.org/doc/samtools.html) for details on samtools parameters.

- elPrep version 3 and lower internally calls samtools and sets up the proper piping. If you use samtools to pipe into / out of your version of elPrep, please don't compare the performance to elPrep 4 or higher without explicit pipes. By default, elPrep 4 or higher processes BAM files itself without any 'outside help', which is more efficient, so the comparison would not be fair.
