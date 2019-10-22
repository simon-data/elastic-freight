/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.index;

import com.simondata.elasticfreight.ConfigParams;
import org.apache.hadoop.conf.Configuration;

/**
 * This class holds onto some metadata about the ES index.
 *
 */
public class ElasticSearchIndexMetadata {

	/**
	 * Number of shards in the index
	 */
	private int numShards;

	public ElasticSearchIndexMetadata() {
		this.numShards = 1;
	}

	public ElasticSearchIndexMetadata(Configuration conf) {
		this.numShards = conf.getInt(ConfigParams.NUM_SHARDS.toString(), 1);
	}

	public int getNumShards() {
		return numShards;
	}
	public void setNumShards(int numShards) {
		this.numShards = numShards;
	}

	@Override
	public String toString() {
		return "ElasticSearchIndexMetadata [numShards=" + numShards + "]";
	}
}
