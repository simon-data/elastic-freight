/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simondata.elasticfreight.ConfigParams;
import com.simondata.elasticfreight.ESEmbededContainer;
import com.simondata.elasticfreight.ShardConfig;
import com.simondata.elasticfreight.transport.S3SnapshotTransport;
import com.simondata.elasticfreight.transport.SnapshotTransportStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static com.simondata.elasticfreight.job.BaseESReducer.DIR_SEPARATOR;
import static com.simondata.elasticfreight.ESEmbededContainer.makeSnapshotName;
import static com.simondata.elasticfreight.transport.BaseTransport.makeSnapshotFilename;

public class IndexingPostProcessor {
    private static transient Logger logger = LoggerFactory.getLogger(IndexingPostProcessor.class);

    private ESEmbededContainer esEmbededContainer;
    private Configuration conf;
    private String snapshotWorkingLocation;
    private String snapshotRepoName;
    private String snapshotFinalDestination;
    private String esWorkingDir;
    private String esMappingsFilename;
    private int numProcessors;
    private S3SnapshotTransport transport;

    public IndexingPostProcessor(Configuration conf, Class<? extends BaseESReducer> reducerClass) {
        this.conf = conf;

        Random rand = new Random();
        String rand_value = Integer.toString(rand.nextInt(Integer.MAX_VALUE));

        snapshotWorkingLocation = conf.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString()) + rand_value + DIR_SEPARATOR;
        snapshotRepoName = conf.get(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString());
        esMappingsFilename = conf.get(ConfigParams.ES_MAPPINGS.toString());
        snapshotFinalDestination = conf.get(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString());
        esWorkingDir = conf.get(ConfigParams.ES_WORKING_DIR.toString()) + rand_value + DIR_SEPARATOR;
        numProcessors = conf.getInt(ConfigParams.NUM_PROCESSORS.toString(), 8);

        BaseESReducer red;
        String templateName = null;
        String templateJson = null;
        try {
            red = reducerClass.newInstance();
            templateName = red.getTemplateName();
            templateJson = red.getTemplate(conf);
            red.cleanup(null);
        } catch (Exception ex) {
            logger.error("Error creating reducer: ", ex);
        }

        // Create an ESEmbededContainer configured for some local indexing
        ESEmbededContainer.Builder builder = new ESEmbededContainer.Builder()
                .withNodeName("embededESTempLoaderNode")
                .withWorkingDir(esWorkingDir)
                .withClusterName("bulkLoadPartition")
                .withSnapshotWorkingLocation(snapshotWorkingLocation)
                .withSnapshotRepoName(snapshotRepoName)
                .withNumProcessors(numProcessors);

        if (templateName != null && templateJson != null) {
            builder.withTemplate(templateName, templateJson);
        }

        esEmbededContainer = builder.build();

        transport = (S3SnapshotTransport) SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination);
        transport.init();
    }

    public Tuple<String, String> getBaseSnapshotInfo(String index) {
        return transport.getSnapshotInfo(index);
    }

    public SnapshotInfo createAllShards(Set<String> indices, ShardConfig shardConfig) {
        ObjectMapper mapper = new ObjectMapper();
        // Create all the indexes
        for (String index : indices) {
            CreateIndexRequestBuilder indexBuilder = esEmbededContainer.getNode().client().admin().indices()
                    .prepareCreate(index).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, shardConfig.getShardsForIndex(index)));

            if (esMappingsFilename != null) {
                S3SnapshotTransport transport = (S3SnapshotTransport)SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination);
                transport.init();
                try {
                    String mapping = transport.readFile(esMappingsFilename.replace("s3://", ""));
                    logger.info("Es mapping: " + mapping);
                    Map<String, Object> map = mapper.readValue(mapping, new TypeReference<Map<String, Object>>() {});
                    Map<String, Object> mappingKeys = (Map<String, Object>)map.get("customer");
                    indexBuilder.addMapping("customer", mappingKeys);
                } catch (Exception ex) {
                    logger.error("Unable to read es mapping: " + esMappingsFilename, ex);
                }
                transport.close();

            }
            indexBuilder.get();
        }

        // Snapshot it
        List<String> indexesToSnapshot = new ArrayList<>();
        indexesToSnapshot.addAll(indices);
        SnapshotInfo info = esEmbededContainer.snapshot(indexesToSnapshot, makeSnapshotName(indexesToSnapshot), snapshotRepoName, null);
        return info;
    }

    /**
     * The job output in HDFS is just a manifest of indices generated by the Job. Why? S3 is eventually consistent in some
     * zones. That means if you try to list the indices you just generated by this job, you might miss some. Instead, we
     * have the job spit out tiny manifests. This method merges them together, de-dupes them, and if there's any shards that
     * didn't get generated because they have no data it puts a placeholder empty shard in it's place to satisfy ElasticSearch.
     *
     * @param jobOutput
     * @param manifestFile
     * @param shardConfig
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void execute(Path jobOutput, Path manifestFile, ShardConfig shardConfig) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        boolean rootManifestUploaded = false;
        try {
            Set<String> indices = new HashSet<>();
            Map<String, Set<Tuple<String, String>>> indexNameToSnapshotIndexIds = new HashMap<>();
            Map<String, Integer> numShardsGenerated = new HashMap<>();

            // Each reducer spits out it's own manifest file, merge em all together into 1 file
            FileUtil.copyMerge(fs, jobOutput, fs, manifestFile, false, conf, "");

            // Read the merged file, de-duping entries as it reads
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(manifestFile)));
            String line;
            line = br.readLine();

            while (line != null) {
                String[] indexInfo = line.split(BaseESReducer.TUPLE_SPLIT);
                String indexName = indexInfo[0];

                indices.add(indexName);

                Set<Tuple<String, String>> snapshotIdexIds = indexNameToSnapshotIndexIds.getOrDefault(indexName, new HashSet<Tuple<String, String>>());
                snapshotIdexIds.add(new Tuple<>(indexInfo[1], indexInfo[2]));
                indexNameToSnapshotIndexIds.put(indexName, snapshotIdexIds);

                int count = numShardsGenerated.getOrDefault(indexName, 0);
                numShardsGenerated.put(indexName, count + 1);
                line = br.readLine();
            }

            File scratch = new File(esWorkingDir);
            if (!scratch.exists()) {
                // Make the dir if it doesn't exist
                scratch.mkdirs();
            } else {
                FileUtils.deleteDirectory(scratch);
                scratch.mkdirs();
            }

            String scratchFile = esWorkingDir + "manifest";
            PrintWriter writer = new PrintWriter(scratchFile, "UTF-8");

            for (String index : indices) {
                try {
                    Set<Tuple<String, String>> snapshotIndexIds = indexNameToSnapshotIndexIds.get(index);
                    Tuple<String, String> baseSnapshotInfo = getBaseSnapshotInfo(index);

                    String destination = transport.removeStorageSystemFromPath(transport.getSnapshotFinalDestination());

                    // Copy the snapshots we created for each shard to one snapshot folder to rule them all
                    if (baseSnapshotInfo.v1() != null && baseSnapshotInfo.v2() != null && !rootManifestUploaded) {
                        // Fill in any missing shards (and copy the metadata for the one snapshot to rule them all)
                        Set<Integer> missingShards = transport.placeMissingShards(index, snapshotIndexIds, shardConfig, !rootManifestUploaded);

                        String copySrc = destination + DIR_SEPARATOR + "indices" + DIR_SEPARATOR + baseSnapshotInfo.v2() + DIR_SEPARATOR;
                        Set<String> subFolders = transport.listSubFolders(copySrc);
                        String dest;
                        for (String subFolder : subFolders) {
                            if (!missingShards.contains(Integer.parseInt(subFolder))) {
                                for (Tuple<String, String> snapshotIndexId : snapshotIndexIds) {
                                    if (transport.checkExists(copySrc + subFolder + DIR_SEPARATOR, makeSnapshotFilename(snapshotIndexId.v1()))) {
                                        String src = copySrc + subFolder + DIR_SEPARATOR + makeSnapshotFilename(snapshotIndexId.v1());
                                        dest = copySrc + subFolder + DIR_SEPARATOR + makeSnapshotFilename(baseSnapshotInfo.v1());
                                        logger.info("Copying src: " + src + " to: " + dest);
                                        transport.copyS3File(src, dest);
                                        break;
                                    }
                                }
                            }
                        }

                    }

                    // The root level manifests are the same on each one, so it need only be uploaded once
                    rootManifestUploaded = true;
                } catch (FileNotFoundException | IllegalArgumentException e) {
                    logger.error("Unable to include index " + index + " in the manifest because missing shards could not be generated", e);
                    continue;
                }

                // Re-write the manifest to local disk
                writer.println(index);
            }
            transport.close();

            // Clean up index from embedded instance
            for (String index : indices) {
                esEmbededContainer.getNode().client().admin().indices().prepareDelete(index).execute();
            }

            writer.close();

            // Move the manifest onto HDFS
            fs.copyFromLocalFile(new Path(scratchFile), manifestFile);
        } finally {
            if (esEmbededContainer != null) {
                esEmbededContainer.getNode().close();
                while (!esEmbededContainer.getNode().isClosed()) ;
            }
            FileUtils.deleteDirectory(new File(snapshotWorkingLocation));
        }
    }
}
