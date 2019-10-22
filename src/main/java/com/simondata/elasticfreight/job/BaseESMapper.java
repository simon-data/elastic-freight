/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class acts as a passthrough routing data to the reducer. Your input should be keyed off of
 * [index name] | [index type]
 * <p>
 * The values should be the raw json payloads to send to ES.
 *
 */
public class BaseESMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString()), value);
    }
}