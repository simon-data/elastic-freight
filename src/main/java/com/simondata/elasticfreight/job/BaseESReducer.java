/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.job;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.simondata.elasticfreight.ConfigParams;
import com.simondata.elasticfreight.ESEmbededContainer;
import com.simondata.elasticfreight.ShardConfig;
import com.simondata.elasticfreight.index.ElasticSearchIndexMetadata;
import com.simondata.elasticfreight.index.routing.ElasticsearchRoutingStrategyV5;
import com.simondata.elasticfreight.transport.S3SnapshotTransport;
import com.simondata.elasticfreight.transport.SnapshotTransportStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.simondata.elasticfreight.ESEmbededContainer.makeSnapshotName;

public abstract class BaseESReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static transient Logger logger = LoggerFactory.getLogger(ESEmbededContainer.class);

    public static final char TUPLE_SEPARATOR = '|';
    public static final String TUPLE_SPLIT = "\\|";
    public static final char DIR_SEPARATOR = '/';

    public static enum JOB_COUNTER {
        TIME_SPENT_INDEXING_MS, TIME_SPENT_FLUSHING_MS, TIME_SPENT_MERGING_MS, TIME_SPENT_SNAPSHOTTING_MS, TIME_SPENT_TRANSPORTING_SNAPSHOT_MS, INDEXING_DOC_FAIL, INDEX_DOC_CREATED, INDEX_DOC_NOT_CREATED
    }

    // The local filesystem location that ES will write the snapshot out to
    private String snapshotWorkingLocation;

    // Where the snapshot will be moved to. Typical use case would be to throw it onto S3
    private String snapshotFinalDestination;

    // The name of a snapshot repo. We'll enumerate that on each job run so that the repo names are unique across rebuilds
    private String snapshotRepoName;

    // Local filesystem location where index data is built
    private String esWorkingDir;

    // The partition of data this reducer is serving. Useful for making directories unique if running multiple reducers on a task tracker
    private String partition;

    private String esMappingsFilename;

    // The container handles spinning up our embedded elasticsearch instance
    private ESEmbededContainer esEmbededContainer;

    private ShardConfig shardConfig;

    private int bulkIndexBatchSize = 50000;
    private int bulkIndexBatchSizeMb = 15;
    private int bulkIndexFlushInterval = 60;
    private int numProcessors = 8;

    private Tuple<String, String> baseSnapshotInfo;

    private ElasticsearchRoutingStrategyV5 elasticsearchRoutingStrategy;

    // Hold onto some frequently generated objects to cut down on GC overhead
    private String indexType;
    private String docId;
    private String json;

    @Override
    public void setup(Context context) {

        partition = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
        String attemptId = String.valueOf(context.getTaskAttemptID().getId());

        Configuration conf = context.getConfiguration();

        // If running multiple reducers on a node, the node needs a unique name & data directory hence the random number we append
        Random rand = new Random();
        String rand_value = Integer.toString(rand.nextInt(Integer.MAX_VALUE));

        snapshotWorkingLocation = conf.get(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString()) + partition + attemptId + rand_value + DIR_SEPARATOR;
        snapshotFinalDestination = conf.get(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString()).trim();
        snapshotRepoName = conf.get(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString()).trim();
        esWorkingDir = conf.get(ConfigParams.ES_WORKING_DIR.toString()) + partition + attemptId + rand_value + DIR_SEPARATOR;
        bulkIndexBatchSize = conf.getInt(ConfigParams.BULK_INDEX_BATCH_SIZE.toString(), bulkIndexBatchSize);
        bulkIndexBatchSizeMb = conf.getInt(ConfigParams.BULK_INDEX_BATCH_SIZE_MB.toString(), bulkIndexBatchSizeMb);
        bulkIndexFlushInterval = conf.getInt(ConfigParams.BULK_INDEX_FLUSH_INTERVAL.toString(), bulkIndexFlushInterval);
        numProcessors = conf.getInt(ConfigParams.NUM_PROCESSORS.toString(), numProcessors);
        esMappingsFilename = conf.get(ConfigParams.ES_MAPPINGS.toString());

        String[] baseSnapshotInfoStr = conf.get(ConfigParams.BASE_SNAPSHOT_INFO.toString()).split(TUPLE_SPLIT);
        baseSnapshotInfo = new Tuple<>(baseSnapshotInfoStr[0], baseSnapshotInfoStr[1]);

        ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata(conf);
        elasticsearchRoutingStrategy = new ElasticsearchRoutingStrategyV5();
        elasticsearchRoutingStrategy.configure(indexMetadata);

        if (shardConfig == null) {
            shardConfig = getShardConfig(conf);
        }
    }

    public void setShardConfig(ShardConfig shardConfig) {
        this.shardConfig = shardConfig;
    }

    private void init(String index, Configuration configuration) throws IOException, UnsupportedEncodingException {
        String templateName = getTemplateName();
        String templateJson = getTemplate(configuration);

        ESEmbededContainer.Builder builder = new ESEmbededContainer.Builder()
                .withNodeName("embededESTempLoaderNode" + partition)
                .withWorkingDir(esWorkingDir)
                .withClusterName("bulkLoadPartition:" + partition)
                .withSnapshotWorkingLocation(snapshotWorkingLocation)
                .withSnapshotRepoName(snapshotRepoName)
                .withNumProcessors(numProcessors);

        if (templateName != null && templateJson != null) {
            builder.withTemplate(templateName, templateJson);
        }

        if (esEmbededContainer == null) {
            esEmbededContainer = builder.build();
        }

        // Create index
        CreateIndexRequestBuilder indexBuilder = esEmbededContainer.getNode().client().admin().indices().prepareCreate(index);
        if (esMappingsFilename != null) {
            S3SnapshotTransport transport = (S3SnapshotTransport)SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination);
            transport.init();
            try {
                // Refer to the documentation here for format of mappings file: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping.html#_example_mapping
                String mapping = transport.readFile(esMappingsFilename.replace("s3://", ""));
                logger.info("Es mapping: " + mapping);
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> map = mapper.readValue(mapping, new TypeReference<Map<String, Object>>() {});
                Map<String, Object> mappingKeys = (Map<String, Object>)map.get("mappings");
                for (Map.Entry<String, Object> entry : mappingKeys.entrySet()) {
                    indexBuilder.addMapping(entry.getKey(), entry.getValue());
                }
            } catch (Exception ex) {
                logger.error("Unable to read es mapping: " + esMappingsFilename, ex);
            }
            transport.close();
        }
        indexBuilder.get();
    }

    /**
     * Provide the JSON contents of the index template. This is your hook for configuring ElasticSearch.
     * <p>
     * https://www.elastic.co/guide/en/elasticsearch/reference/5.5/indices-templates.html
     *
     * @return String
     */
    public abstract String getTemplate(Configuration configuration);


    /**
     * Provide a ShardConfig which provides the number of shards per index and the number of
     * shards to split organizations across. The number can be uniform across indices or a mapping
     * can be provided to enable per-index configuration values.
     *
     * @param config
     * @return ShardConfig
     */
    public abstract ShardConfig getShardConfig(Configuration config);

    /**
     * Provide an all lower case template name
     *
     * @return String
     */
    public abstract String getTemplateName();

    @Override
    protected void reduce(Text docMetaData, Iterable<Text> documentPayloads, Context context) throws IOException, InterruptedException {
        String[] pieces = docMetaData.toString().split(TUPLE_SPLIT);
        String indexName = pieces[0];
        String routing = pieces[1];
        init(indexName, context.getConfiguration());

        long start = System.currentTimeMillis();

        Client esClient = esEmbededContainer.getNode().client();

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
                esClient,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {

                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        if (response.hasFailures()) {
                            for (BulkItemResponse resp : response.getItems()) {
                                if (resp.isFailed()) {
                                    IndexRequest indexRequest = (IndexRequest)request.requests().get(resp.getItemId());
                                    logger.error("Failed to write record: " + indexRequest.source().utf8ToString());
                                    logger.error(resp.getFailure().getMessage(), resp.getFailure().getCause());
                                    context.getCounter(JOB_COUNTER.INDEX_DOC_NOT_CREATED).increment(1l);
                                }
                            }
                        } else {
                            context.getCounter(JOB_COUNTER.INDEX_DOC_CREATED).increment(request.numberOfActions());
                        }
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        context.getCounter(JOB_COUNTER.INDEX_DOC_NOT_CREATED).increment(request.numberOfActions());
                    }
                })
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3));

        if (bulkIndexBatchSize != 0) {
            bulkProcessorBuilder.setBulkActions(bulkIndexBatchSize);
        }
        if (bulkIndexBatchSizeMb != 0) {
            bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkIndexBatchSizeMb, ByteSizeUnit.MB));
        }
        if (bulkIndexFlushInterval != 0) {
            bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueSeconds(bulkIndexFlushInterval));
        }
        // Concurrency is numprocessors * 2
        bulkProcessorBuilder.setConcurrentRequests(numProcessors * 2);

        BulkProcessor bulkProcessor = bulkProcessorBuilder.build();

        for (Text line : documentPayloads) {
            if (line == null) {
                continue;
            }

            pieces = line.toString().split(TUPLE_SPLIT);
            indexType = pieces[0];
            docId = pieces[1];
            json = pieces[2];

            logger.debug(json);
            for(int i=3; i<pieces.length; i++) {
                json = json + TUPLE_SEPARATOR + pieces[i];
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode record = mapper.readTree(json);

            // Add the doc to the bulkProcessor for bulk indexing
            bulkProcessor.add(
                    esClient.prepareIndex(indexName, indexType)
                            .setType("customer")
                            .setId(docId)
                            .setRouting(routing)
                            .setSource(mapper.writeValueAsString(record), XContentType.JSON)
                            .request()
            );
        }
        bulkProcessor.flush();

        boolean awaitClose = bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        logger.info("Bulk processor await close: " + Boolean.toString(awaitClose));
        context.getCounter(JOB_COUNTER.TIME_SPENT_INDEXING_MS).increment(System.currentTimeMillis() - start);

        // The shardId of this reducer.  This represents the shard that we will write to in S3.
        // The shardId that the ES embedded container writes to is different and is essentially random so creating a
        // shardId for this reducer is required so that we don't overwrite shards in S3 with the same (random) ES shardId
        // TODO: This could be improved to more easily support other routing strategies
        String reducerShard = Integer.toString(
                elasticsearchRoutingStrategy.getDocIdHash(docId, shardConfig.getShardsForIndex(indexName).intValue()));

        // Snapshot the ES container
        Tuple<SnapshotInfo, String> snapshotAndIndexInfo = snapshot(indexName, reducerShard, context);

        SnapshotInfo snapshotInfo = snapshotAndIndexInfo.v1();
        String indexId = snapshotAndIndexInfo.v2();

        // Write metadata about the snapshot to the output of the reducer
        context.write(NullWritable.get(), new Text(
                indexName + TUPLE_SEPARATOR + snapshotInfo.snapshotId().getUUID() + TUPLE_SEPARATOR + indexId
        ));
    }

    @Override
    public void cleanup(Context context) throws IOException {
        if (esEmbededContainer != null) {
            esEmbededContainer.getNode().close();
            while (!esEmbededContainer.getNode().isClosed()) ;
            FileUtils.deleteDirectory(new File(snapshotWorkingLocation));
        }
    }

    /**
     * Creates the snapshot
     *
     * @param indexName
     * @param reducerShard
     * @param context
     * @return Tuple<SnapshotInfo ,   String> containing info about the snapshot and the indexId of index written
     * @throws IOException
     */
    public Tuple<SnapshotInfo, String> snapshot(String indexName, String reducerShard, Context context) throws IOException {
        // Create the snapshot
        List<String> indexesToSnapshot = Arrays.asList(indexName);
        String snapshotName = makeSnapshotName(indexesToSnapshot);
        SnapshotInfo info = esEmbededContainer.snapshot(indexesToSnapshot, snapshotName, snapshotRepoName, context);

        // Delete the index to free up that space
        ActionFuture<DeleteIndexResponse> response = esEmbededContainer.getNode().client().admin().indices().delete(new DeleteIndexRequest(indexName));
        while (!response.isDone()) ;

        // Upload the shard snapshot to the destination in S3
        long start = System.currentTimeMillis();
        String indexId = SnapshotTransportStrategy.get(snapshotWorkingLocation, snapshotFinalDestination, baseSnapshotInfo).execute(info, reducerShard);
        context.getCounter(JOB_COUNTER.TIME_SPENT_TRANSPORTING_SNAPSHOT_MS).increment(System.currentTimeMillis() - start);

        esEmbededContainer.deleteSnapshot(snapshotName, snapshotRepoName);
        return new Tuple<>(info, indexId);
    }
}
