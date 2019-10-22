/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.util;

import com.simondata.elasticfreight.job.BaseESReducer;
import com.simondata.elasticfreight.index.ElasticSearchIndexMetadata;
import com.simondata.elasticfreight.index.routing.ElasticsearchRoutingStrategyV5;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class ShardPartitioner<K2, V2> extends Partitioner<K2, V2> implements Configurable {
    private Configuration conf;
    private ElasticsearchRoutingStrategyV5 strategy;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata(conf);
        strategy = new ElasticsearchRoutingStrategyV5();
        strategy.configure(indexMetadata);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int getPartition(K2 key, V2 value, int numReduceTasks) {
        // getDocIdHash produces a number between 0 and numReduceTasks for each docId
        String docId = value.toString().split(BaseESReducer.TUPLE_SPLIT)[1];
        return strategy.getDocIdHash(docId, strategy.getNumShards());
    }
}