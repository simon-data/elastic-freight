/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.example;

import com.simondata.elasticfreight.job.BaseESReducer;
import com.simondata.elasticfreight.ConfigParams;
import com.simondata.elasticfreight.job.IndexingPostProcessor;
import com.simondata.elasticfreight.ShardConfig;
import com.simondata.elasticfreight.util.ShardPartitioner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class IndexingJob implements Tool {

    private static Configuration conf;

    private static transient Logger logger = LoggerFactory.getLogger(IndexingJob.class);

    public static int main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Invalid # arguments. EG: esIndex " +
                    "[pipe separated input] " +
                    "[snapshot final destination (s3/nfs/hdfs)] " +
                    "[snapshot repo name] " +
                    "[index name] " +
                    "[es mappings]" +
                    "[num shards] " +
                    "[document id] " +
                    "[batch size] " +
                    "[batch size mb] " +
                    "[batch flush interval seconds] " +
                    "[num processors]" +
                    "[user ramdisk]");
            return -1;
        }

        String inputPath = args[0];
        String snapshotFinalDestination = args[1];
        String snapshotRepoName = args[2];
        String indexName = args[3];
        String esMappings = args[4];
        Integer numReducers = new Integer(args[5]);
        String documentId = args[6];

        String bulkIndexBatchSize = "20000";
        if (args.length >= 8) {
            bulkIndexBatchSize = args[7];
        }
        String bulkIndexBatchSizeMb = "10";
        if (args.length >= 9) {
            bulkIndexBatchSizeMb = args[8];
        }
        String bulkIndexFlushInterval = "60";
        if (args.length >= 10) {
            bulkIndexFlushInterval = args[9];
        }
        String numProcessors = "8";
        if (args.length >= 11) {
            numProcessors = args[10];
        }
        String ramDisk = null;
        if (args.length >= 12) {
            ramDisk = args[11];
        }

        String manifestLocation = "hdfs:///tmp/" + snapshotRepoName + "_" + indexName + "/";
        String snapshotWorkingLocation = "/mnt/var/lib/hadoop/tmp/bulkload/";
        String esWorkingDir = "/mnt/var/lib/hadoop/tmp/esrawdata/";
        if (ramDisk != null) {
            snapshotWorkingLocation = "/dev/shm/bulkload/";
            esWorkingDir = "/dev/shm/esrawdata/";
        }

        // Remove trailing slashes from the destination
        snapshotFinalDestination = StringUtils.stripEnd(snapshotFinalDestination, "/");

        conf = new Configuration();
        conf.set(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString(), snapshotWorkingLocation);
        conf.set(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString(), snapshotFinalDestination);
        conf.set(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString(), snapshotRepoName);
        conf.set(ConfigParams.ES_WORKING_DIR.toString(), esWorkingDir);
        conf.set(ConfigParams.NUM_SHARDS.toString(), numReducers.toString());
        conf.set(ConfigParams.INDEX_NAME.toString(), indexName);
        conf.set(ConfigParams.ES_MAPPINGS.toString(), esMappings);
        conf.set(ConfigParams.DOCUMENT_ID.toString(), documentId);
        conf.setInt(ConfigParams.BULK_INDEX_BATCH_SIZE.toString(), Integer.parseInt(bulkIndexBatchSize));
        conf.setInt(ConfigParams.BULK_INDEX_BATCH_SIZE_MB.toString(), Integer.parseInt(bulkIndexBatchSizeMb));
        conf.setInt(ConfigParams.BULK_INDEX_FLUSH_INTERVAL.toString(), Integer.parseInt(bulkIndexFlushInterval));
        conf.setInt(ConfigParams.NUM_PROCESSORS.toString(), Integer.parseInt(numProcessors));

        Job job = Job.getInstance(conf, "Indexing job");
        job.setJarByClass(IndexingJob.class);
        job.setJobName("Elastic Search Offline Index Generator");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(IndexingMapperImpl.class);
        job.setReducerClass(IndexingReducerImpl.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setNumReduceTasks(numReducers);
        job.setPartitionerClass(ShardPartitioner.class);
        job.setSpeculativeExecution(false);
        job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);

        Path jobOutput = new Path(manifestLocation + "/raw/");
        Path manifestFile = new Path(manifestLocation + "manifest");

        FileOutputFormat.setOutputPath(job, jobOutput);

        // Set up inputs
        String[] inputFolders = StringUtils.split(inputPath, "|");
        for (String input : inputFolders) {
            FileInputFormat.addInputPath(job, new Path(input));
        }

        ShardConfig shardConfig = new ShardConfig(numReducers.longValue());
        IndexingPostProcessor postProcessor = new IndexingPostProcessor(job.getConfiguration(), IndexingReducerImpl.class);
        Set<String> indexes = new HashSet<>();
        indexes.add(indexName);
        postProcessor.createAllShards(indexes, shardConfig);
        Tuple<String, String> baseSnapshotInfo = postProcessor.getBaseSnapshotInfo(indexName);

        job.getConfiguration().set(ConfigParams.BASE_SNAPSHOT_INFO.toString(),
                baseSnapshotInfo.v1() + BaseESReducer.TUPLE_SEPARATOR + baseSnapshotInfo.v2());

        // Start the job
        job.waitForCompletion(true);

        postProcessor.execute(jobOutput, manifestFile, shardConfig);
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        return IndexingJob.main(args);
    }

}
