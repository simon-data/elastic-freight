/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.example;

import com.simondata.elasticfreight.job.BaseESReducer;
import com.simondata.elasticfreight.ConfigParams;
import com.simondata.elasticfreight.ShardConfig;
import org.apache.hadoop.conf.Configuration;

public class IndexingReducerImpl extends BaseESReducer {

    private static Integer MAX_MERGED_SEGMENT_SIZE_MB = 256;

    /**
     * Provide the JSON contents of the index template. This should match the configuration setting on your
     * production cluster.  See the link below for details on how to dump this information from your cluster.
     * <p>
     * https://www.elastic.co/guide/en/elasticsearch/reference/5.5/indices-templates.html
     */
    public String getTemplate(Configuration configuration) {
        Long numShardsPerIndex = configuration.getLong(ConfigParams.NUM_SHARDS.name(), 1l);
        return "{ \"template\" : \"*\", " +
                "\"settings\": { " +
                    "\"number_of_shards\": " + numShardsPerIndex + ", " +
                    "\"refresh_interval\": -1, " +
                    "\"number_of_replicas\": 0, " +
                    "\"load_fixed_bitset_filters_eagerly\": false, " +
                    "\"translog.flush_threshold_size\": \"2048mb\", " +
                    "\"merge.policy.max_merged_segment\": \"" + MAX_MERGED_SEGMENT_SIZE_MB + "mb\", " +
                    "\"merge.policy.segments_per_tier\": 4, " +
                    "\"merge.scheduler.max_thread_count\": 4, " +
                    "\"mapping.total_fields.limit\": 5000" +
                "}}";
    }

    /**
     * Return a name for the index template.
     */
    @Override
    public String getTemplateName() {
        return "baseindex";
    }

    @Override
    public ShardConfig getShardConfig(Configuration configuration) {
        Long numShardsPerIndex = configuration.getLong(ConfigParams.NUM_SHARDS.name(), 1l);
        return new ShardConfig(numShardsPerIndex);
    }

}