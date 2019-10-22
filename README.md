# elastic-freight

## Project and Rationale
Bulk loading Elasticsearch indexes has always been a cumbersome process.
While Elasticsearch has a bulk load API, making many requests to a live cluster in order to load the index can cause performance issues.
Elastic-freight is an attempt to alleviate these issues by creating an Elasticsearch index offline using Hadoop MapReduce and the snapshotting process which then can be loaded into a live cluster using the normal restore process.

## Overview
![es_index_generation](https://github.com/simon-data/elastic-freight/wiki/images/es_index_generation.png)

The above diagram details the general approach.  Each reducer within the MapReduce job creates an embedded Elasticsearch node which it loads the data into.  We then snapshot each of those embedded node and upload them to S3.  We then convert those combined snapshots into one master snapshot for the whole index (all shards) which can be used to restore the index to the entire live cluster.

This project is derived from the [purecloudlabs/elasticsearch-lambda](https://github.com/purecloudlabs/elasticsearch-lambda) project (Apache 2.0 lincese).  We have made significant changes to the project to allow for the following:
- Elasticsearch version 5.5 support
- Removed lambda overhead in preference of a simplified job that can be run using most any architecture
- Faster bulk indexing
- Support for Elasticsearch index mapping overrides

# Installation

This project uses maven.  To create the jar archive run:

`mvn package`

# Usage

`hadoop jar elasticsearch-indexer-1.1.0.jar esIndex [pipe separated input] [snapshot final destination] [snapshot repo name] [index name] [es mappings] [num shards] [document id]`

Fields marked in bold are required
- **[pipe separated input]**
   * The input JSON files (e.g. s3://your.repo/part-*)
- **[snapshot final destination]**
   * Output dir where the snapshot should be placed (e.g. s3://your.repo/snapshots/snapshot_name/)
   * s3/nfs/hdfs - primarily testest with S3
- **[snapshot repo name]**
   * Name of your snapshot repo that we are creating (e.g. repo_index_name)
- **[index name]**
   * Name of the index (e.g. index_name)
- **[es mappings]**
   * S3 location of where the (e.g. s3://your.repo/mappings/index_name.json)
   * In the format specified in the ES documentation here: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping.html#_example_mapping
- **[num shards]**
   * Number of shards for the index (e.g. 8)
- **[document id]**
   * The name of the JSON node to be extracted as your document id for determining the sharding of your indexed documents
- [batch size]
   * Bulk load batch size (e.g. 50000)
   * Defaults to 20000
   * Will flush to local ES if [batch size],[batch size mb], or [batch flush interval seconds] is reached first
- [batch size mb]
  * Bulk load batch size MB (e.g. 5)
  * Defaults to 10
  * Will flush to local ES if [batch size],[batch size mb], or [batch flush interval seconds] is reached first
- [batch flush interval seconds]
   * Bulk load flush interval (e.g. 30)
   * Defualts to 60 seconds
   * Will flush to local ES if [batch size],[batch size mb], or [batch flush interval seconds] is reached first
- [num processors]
   * Number of processors on each reducer task; used for bulk optimizations (e.g. 8)
   * Defaults to 8
- [use ramdisk]
   * Experimental, tries to create a ramdisk to back the embedded elasticsearch rather than disk (default).
   * Have not been able to get this to work reliably yet

## Example Job

This project provides and [example job](https://github.com/simon-data/elastic-freight/blob/master/src/main/java/com/simondata/example/IndexingJob.java) to serve as a framework for any custom jobs you may want to run.  However, you may be able to use the example job to create the index on your cluster without any modification.

There are two main peices of code which you may want to customize in the example job:
 - [documentId](https://github.com/simon-data/elastic-freight/blob/master/src/main/java/com/simondata/example/IndexingMapperImpl.java#L51)
 - [index template](https://github.com/simon-data/elastic-freight/blob/master/src/main/java/com/simondata/example/IndexingReducerImpl.java#L27)

# Contributing

To Run the tests run the following:

`mvn test`

See [our doc on contributing](https://github.com/simon-data/elastic-freight/blob/master/CONTRIBUTING.md)

# Questions

Questions, thoughts, and ideas can be opened as issues on this repo or can be sent to opensource@simondata.com.
