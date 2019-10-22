/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.index.routing;

import com.simondata.elasticfreight.index.ElasticSearchIndexMetadata;

public interface ElasticsearchRoutingStrategy extends java.io.Serializable {
	String getRoutingHash(String orgId);
	String[] getPossibleRoutingHashes(String orgId);
	void configure(ElasticSearchIndexMetadata indexMetadata);
	int getNumShards();
}
