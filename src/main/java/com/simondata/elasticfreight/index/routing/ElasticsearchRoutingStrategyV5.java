/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.index.routing;

import com.google.common.base.Preconditions;
import com.simondata.elasticfreight.index.ElasticSearchIndexMetadata;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;

import java.util.HashMap;
import java.util.Map;


/**
 * This routing strategy for elasticsearch. Read up on what routing does here
 * http://www.elastic.co/blog/customizing-your-document-routing/
 * 
 * Perhaps you have 10 shards per index and you don't wish to query every shard 
 * every time you do a search against an org. A simple sharding strategy would
 * put all the data for 1 org on 1 shard using consistant hashing on orgId. However that 
 * has the potential to hotspot some shards if an org with a lot of data comes
 * through.
 * 
 * This attempts to alleviate that by making the # subset of shards configurable. EG
 * numShards = 10, numShardsPerOrg = 3, all of an org's data will be split to one of 
 * 3 shards. Which one of the 3 is determined by hashing the conversationId.
 *  
 * Note: DO NOT CHANGE THIS CLASS. It's immutable once it's been used to generate ES indexes
 * so changing it affects data routing and will make data appear unavailable b/c its looking
 * in the wrong shard. The correct thing to do is to make a newer version of this class,
 * say ElasticsearchRoutingStrategyV5 and see to it that the hadoop jobs to rebuild
 * the ES indexes not only use it, but update zookeeper with which implementation
 * indexes were built with. That way you can evolve the routing strategy without breaking
 * anything.
 *
 */
public class ElasticsearchRoutingStrategyV5 implements ElasticsearchRoutingStrategy, java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private int numShards;
	private Map<Integer, Integer> shardToRout = new HashMap<>();

	public void init() {
		Integer x = 0;
		while(shardToRout.size() < numShards) {
			Integer hash = Murmur3HashFunction.hash(x.toString());
			if(shardToRout.get(x) == null) {
				shardToRout.put(x, hash);
			}
			x++;
		}
	}

	public ElasticsearchRoutingStrategyV5() {

	}

	@Override
	public void configure(ElasticSearchIndexMetadata indexMetadata) {
		Preconditions.checkNotNull(indexMetadata.getNumShards(), "Num shards must not be null with " + this.getClass().getSimpleName());
		this.numShards = indexMetadata.getNumShards();
		init();
	}

	@Override
	public int getNumShards() {
		return numShards;
	}
	

	public Map<Integer, Integer> getShardToRout() {
		return shardToRout;
	}

	/**
	 * For an docId, get the shard routing for a document.
	 * 
	 * Note: ES re-hashes routing values so shard 1 wont necessarily mean 
	 * your data ends up in shard 1. However, if you realize that 
	 * then you're in a bad place. 
	 * 
	 * 
	 * @param docId
	 * @return
	 */

	@Override
	public String getRoutingHash(String docId) {
		int shard = getDocIdHash(docId, numShards);

		return shardToRout.get(shard).toString();
	}

	/**
	 * When searching data for an Org, you may desire to only search the shards
	 * which hold data for that Org. This gives you a list of possible shard routings.
	 *
	 * @param docId
	 * @return
	 */
	@Override
	public String[] getPossibleRoutingHashes(String docId) {
		int docIdHash = getDocIdHash(docId, numShards);
		String[] possibleShards = new String[1];
		for(int x = 0; x < 1; x ++) {
			int shard = docIdHash + x;
			possibleShards[x] = shardToRout.get(shard).toString();
		}
		return possibleShards;
	}

	public int getDocIdHash(String docId, int numShards) {
		int hash = Murmur3HashFunction.hash(docId);
		return Math.floorMod(hash, numShards);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + numShards;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ElasticsearchRoutingStrategyV5 other = (ElasticsearchRoutingStrategyV5) obj;
		if (numShards != other.numShards)
			return false;
		return true;
	}

    @Override
    public String toString() {
        return "ElasticsearchRoutingStrategyV5 [numShards=" + numShards
                + ", shardToRout=" + shardToRout + "]";
    }

	
}
