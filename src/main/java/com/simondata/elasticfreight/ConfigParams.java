/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight;

public enum ConfigParams {
	SNAPSHOT_WORKING_LOCATION_CONFIG_KEY,
	SNAPSHOT_REPO_NAME_CONFIG_KEY,
	SNAPSHOT_FINAL_DESTINATION,
	ES_WORKING_DIR,
	NUM_SHARDS,
	INDEX_NAME,
	BASE_SNAPSHOT_INFO,
	BULK_INDEX_BATCH_SIZE,
	BULK_INDEX_BATCH_SIZE_MB,
	BULK_INDEX_FLUSH_INTERVAL,
	NUM_PROCESSORS,
	ES_MAPPINGS,
	DOCUMENT_ID
}
