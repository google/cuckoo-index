**NOTE** This is not an officially supported Google product.

# Cuckoo Index

## Overview

[Cuckoo Index](https://www.vldb.org/pvldb/vol13/p3559-kipf.pdf) (CI) is a lightweight secondary index structure that represents the many-to-many relationship between keys and partitions of columns in a highly space-efficient way. At its core, CI associates variable-sized fingerprints in a [Cuckoo filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf) with compressed bitmaps indicating qualifying partitions.

## What Problem Does It Solve?

The problem of finding all partitions that possibly contain a given lookup key is traditionally solved by maintaining one filter (e.g., a Bloom filter) per partition that indexes all unique key values contained in this partition:

```
Partition 0:
A, B => Bloom filter 0

Partition 1:
B, C => Bloom filter 1
...
```

To identify all partitions containing a key, we need to probe all per-partition filters (which could be many). Since a Bloom filter may return false positives, there is a chance (of e.g. 1%) that we accidentally identify a negative partition as positive. In the above example, a lookup for key A may return Partition 0 (true positive) and 1 (false positive). Depending on the storage medium, a false positive partition can be very expensive (e.g., many milliseconds on disk).

Furthermore, secondary columns typically contain many duplicates (also across partitions). With the per-partition filter design, these duplicates may be indexed in multiple filters (in the worst case, in all filters). In the above example, the key B is redundantly indexed in Bloom filter 0 and 1.

Cuckoo Index addresses both of these drawbacks of per-partition filters.

## Features

*   100% correct results for lookups with occurring keys (as opposed to per-partition filters).
*   Configurable scan rate (ratio of false positive partitions) for lookups with non-occurring keys.
*   Much smaller footprint size than full-fledged indexes that store full-sized keys.
*   Smaller footprint size than per-partition filters for low-to-medium cardinality columns.

## Limitations

*   Requires access to all keys at build time.
*   Relatively high build time (in O(n) but with a high constant factor) compared to e.g. per-partition Bloom filters.
*   Once built, CI is immutable but fast to query (it uses a [rank support structure](https://www.cs.cmu.edu/~dga/papers/zhou-sea2013.pdf) for efficient rank calls).

## Running Experiments

Prepare a dataset in a CSV format that you are going to use. One of the datasets we used was DMV [Vehicle, Snowmobile, and Boat Registrations](https://catalog.data.gov/dataset/vehicle-snowmobile-and-boat-registrations).

```
wget -c https://data.ny.gov/api/views/w4pv-hbkt/rows.csv -O Vehicle__Snowmobile__and_Boat_Registrations.csv
```

Add the file to the `data` dependencies in the `BUILD.bazel` file.

```
data = [
    # Put your csv files here
    "Vehicle__Snowmobile__and_Boat_Registrations.csv"
],
```

For footprint experiments, run the following command, specifying the path to the data file, columns to test, and the tests to run.

```
bazel run -c opt --cxxopt="-std=c++17" :evaluate -- \
  --input_file_path="Vehicle__Snowmobile__and_Boat_Registrations.csv" \
  --columns_to_test="City,Zip,Color" \
  --test_cases="positive_uniform,positive_distinct,positive_zipf,negative,mixed" \
  --output_csv_path="results.csv"
```

For lookup performance experiments, run the following command, specifying the path to the data file, and columns to test.

**NOTE** You might want to use fewer rows for lookup experiments as the benchmarks are quite time-consuming.

```
bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- \
  --input_file_path="Vehicle__Snowmobile__and_Boat_Registrations.csv" \
  --columns_to_test="City,Zip,Color"
```

## Code Organization

#### Evaluation Framework

*   Evaluate (evaluate.h): *Entry point (binary) into our evaluation framework with instantiations of all indexes.*
*   Evaluator (evaluator.h): *Evaluation framework.*
*   Table/Column (data.h): *Integer columns that we run the benchmarks on (string columns are dict-encoded).*
*   IndexStructure (index_structure.h): *Interface shared among all indexes.*

#### Cuckoo Index

*   CuckooIndex (cuckoo_index.h): *Main class of Cuckoo Index.*
*   CuckooKicker (cuckoo_kicker.h): *A heuristic that finds a close-to-optimal assignment of keys to buckets (in terms of the ratio of items residing in primary buckets).*
*   FingerprintStore (fingerprint_store.h): *Stores variable-sized fingerprints in bitpacket format.*
*   RleBitmap (rle_bitmap.h): *An RLE-based (bitwise, unaligned) bitmap representation (for sparse bitmaps we use position lists).*
*   BitPackedReader (bit_packing.h): *A helper class for storing & retrieving bitpacked data.*

## Cite

Please cite our [VLDB 2020 paper](https://www.vldb.org/pvldb/vol13/p3559-kipf.pdf) if you use this code in your own work:

```
@article{cuckoo-index,
author = {Kipf, Andreas and Chromejko, Damian and Hall, Alexander and Boncz, Peter and Andersen, David},
title = {Cuckoo Index: A Lightweight Secondary Index Structure},
year = {2020},
issue_date = {September 2020},
publisher = {VLDB Endowment},
volume = {13},
number = {13},
issn = {2150-8097},
url = {https://doi.org/10.14778/3424573.3424577},
doi = {10.14778/3424573.3424577},
journal = {Proc. VLDB Endow.},
month = sep,
pages = {3559-3572},
numpages = {14}
}
```
