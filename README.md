**NOTE** This is not an officially supported Google product.

# Cuckoo Index

## Overview

Cuckoo Index (CI), formerly Cuckoo Lookup Table (CLT), is a lightweight
secondary index structure that represents the many-to-many relationship between
keys and stripes (chunks of columns) in a highly space-efficient way. At its
core, CI associates variable-sized fingerprints in a Cuckoo filter [1] with
compressed bitmaps indicating qualifying stripes.

## What Problem Does It Solve?

The problem of finding all stripes that possibly contain a given lookup key is
traditionally solved by maintaining one filter (e.g., a Bloom filter) per stripe
that indexes all unique key values contained in this stripe:

```
Stripe 0:
Keys: A, B => Bloom filter 0 (1% false positive rate)

Stripe 1:
Keys: B, C => Bloom filter 1 (1% false positive rate)

...
```

To identify all stripes that contain a key, we probe all per-stripe filters
(which could be many!) to derive a bitmap of qualifying stripes. Since a Bloom
filter may return false positives, there is a chance (of e.g. 1%) that we
accidentally identify a stripe as a false positive. In the above example, a
lookup for key A may return Stripe 0 (true positive) and 1 (false positive).
Depending on the storage medium, a false positive stripe can be very expensive
(e.g., many milliseconds on disk).

Besides this problem of false positive stripes (even for occurring keys such as
A!), secondary columns typically contain many duplicates (even across stripes).
With the per-stripe filter design, these duplicates may be indexed in multiple
filters (in the worst case, in all filters!). In the above example, the key B is
redundantly indexed in Bloom filter 0 and 1.

Cuckoo Index addresses both of these drawbacks of per-stripe filters.

## Features

*   100% correct results for lookups with occurring keys (as opposed to
    traditional per-stripe filters)
*   Configurable scan rate (ratio of false positive stripes) for lookups with
    non-occurring keys
*   Much smaller footprint size than full-fledged indexes that store full-sized
    keys at the cost of false positive stripes for lookups with non-occurring
    keys
*   Smaller footprint size than per-stripe filters for low-to-medium cardinality
    columns

## Limitations

*   Requires access to all keys at build time
*   Relatively high build time (in O(n) but with a high constant factor)
    compared to e.g. per-stripe Bloom filters
*   Once built, CI is immutable and will be fast to query (the current
    implementation lacks a rank support structure [2] that is required for
    efficient lookups)

## Running experiments

Prepare a data set in a CSV format that you are going to use. One of the data
sets we used was the DMV
[Vehicle, Snowmobile, and Boat Registrations](https://catalog.data.gov/dataset/vehicle-snowmobile-and-boat-registrations).

For footprint experiments, run the following command, specifying the path to the
data file, columns to test and the tests to run.

```
bazel run -c opt --cxxopt="-std=c++17" :evaluate -- \
  --input_file_path="Vehicle__Snowmobile__and_Boat_Registrations.csv" \
  --columns_to_test="City,Zip,Color" \
  --test_cases="positive_uniform,positive_distinct,positive_zipf,negative,mixed" \
  --output_csv_path="results.csv"
```

For lookup performance experiments, run the following command, specifying the
path to the the data file and columns to test.

**NOTE** You might want to use fewer rows for lookup experiments as the
benchmarks are quite time-consuming.

```
bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark -- \
  --input_file_path="Vehicle__Snowmobile__and_Boat_Registrations.csv" \
  --columns_to_test="City,Zip,Color"
```

## Code Organization

#### Evaluation Framework

*   Evaluate (evaluate.h)

    Entry point (binary) into our evaluation framework with instantiations of
    all indexes

*   Evaluator (evaluator.h)

    Evaluation framework

*   Table/Column (data.h)

    Integer columns that we run the benchmarks on (string columns are
    dict-encoded)

*   IndexStructure (index_structure.h)

    Interface shared among all indexes

#### Cuckoo Index

*   CuckooIndex (cuckoo_index.h)

    Main class of Cuckoo Index

*   CuckooKicker (cuckoo_kicker.h)

    A heuristic that finds a close-to-optimal assignment of keys to buckets (in
    terms of the ratio of items residing in primary buckets)

*   FingerprintStore (fingerprint_store.h)

    Stores variable-sized fingerprints in bitpacket format

*   RleBitmap (rle_bitmap.h)

    An RLE-based (bitwise, unaligned) bitmap representation (for sparse bitmaps
    we use position lists)

*   BitPackedReader (bit_packing.h)

    A helper class for storing & retrieving bitpacked data

## References

[1]
[Fan et al., Cuckoo Filter: Practically Better Than Bloom, 2014](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)

[2] [Zhou et al., Space-Efficient, High-Performance Rank & Select Structures on
Uncompressed Bit Sequences,
2013](https://www.cs.cmu.edu/~dga/papers/zhou-sea2013.pdf)
