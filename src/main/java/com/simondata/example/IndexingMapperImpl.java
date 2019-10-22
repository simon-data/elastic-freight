/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simondata.elasticfreight.job.BaseESMapper;
import com.simondata.elasticfreight.job.BaseESReducer;
import com.simondata.elasticfreight.ConfigParams;
import com.simondata.elasticfreight.index.ElasticSearchIndexMetadata;
import com.simondata.elasticfreight.index.routing.ElasticsearchRoutingStrategy;
import com.simondata.elasticfreight.index.routing.ElasticsearchRoutingStrategyV5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

public class IndexingMapperImpl extends BaseESMapper {
    private ElasticsearchRoutingStrategy elasticsearchRoutingStrategy;
    private String indexName;
    private String documentIdName;
    private String indexNameSep;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.indexName = conf.get(ConfigParams.INDEX_NAME.toString(), "");
        this.documentIdName = conf.get(ConfigParams.DOCUMENT_ID.toString(), "");
        this.indexNameSep = this.indexName + BaseESReducer.TUPLE_SEPARATOR;

        ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata(conf);
        elasticsearchRoutingStrategy = new ElasticsearchRoutingStrategyV5();
        elasticsearchRoutingStrategy.configure(indexMetadata);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String json = value.toString();

        Map<String, Object> map = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});

        // Replace this with your primary document id field
        String documentId = (String) map.get(this.documentIdName);
        String routingHash = elasticsearchRoutingStrategy.getRoutingHash(documentId);

        // The routing hash is a hash representing the reducerId
        Text outputKey = new Text(indexNameSep + routingHash);
        Text outputValue = new Text(indexNameSep + documentId + BaseESReducer.TUPLE_SEPARATOR + json);
        context.write(outputKey, outputValue);
    }
}