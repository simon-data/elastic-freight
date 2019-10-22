/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight;

import com.google.common.base.Preconditions;
import com.simondata.elasticfreight.job.BaseESReducer;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds an embedded elasticsearch instance and configures it for you
 *
 */
public class ESEmbededContainer {
	private Node node;
	private long DEFAULT_TIMEOUT_MS = 60 * 30 * 1000;
	private static transient Logger logger = LoggerFactory.getLogger(ESEmbededContainer.class);
	
	public SnapshotInfo snapshot(List<String> index, String snapshotName, String snapshotRepoName, Reducer.Context context) {
		return snapshot(index, snapshotName, snapshotRepoName, DEFAULT_TIMEOUT_MS, context);
	}
	
	/**
	 * Flush, optimize, and snapshot an index. Block until complete.
	 * 
	 * @param indices
	 * @param snapshotName
	 * @param snapshotRepoName
	 * @param timeoutMS
	 * @param context
	 */
	public SnapshotInfo snapshot(List<String> indices, String snapshotName, String snapshotRepoName, long timeoutMS, Reducer.Context context) {
		/* Flush & optimize before the snapshot.
		 *  
		 * TODO: Long operations could block longer that the container allows an operation to go
		 * unresponsive b/f killing. We need to issue the request and poll the future waiting on the
		 * operation to succeed, but update a counter or something to let the hadoop framework
		 * know the process is still alive. 
		 */  
		TimeValue v = new TimeValue(timeoutMS);
		for(String index : indices) {
			long start = System.currentTimeMillis();

			// Flush
			node.client().admin().indices().prepareFlush(index).get(v);
			if(context != null) {
				logger.info(BaseESReducer.JOB_COUNTER.TIME_SPENT_FLUSHING_MS.name() + ": " + Long.toString(System.currentTimeMillis() - start));
				context.getCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_FLUSHING_MS).increment(System.currentTimeMillis() - start);
			}

			// Merge
			start = System.currentTimeMillis();
			node.client().admin().indices().prepareForceMerge(index).get(v);
			if(context != null) {
				logger.info(BaseESReducer.JOB_COUNTER.TIME_SPENT_MERGING_MS.name() + ": " + Long.toString(System.currentTimeMillis() - start));
				context.getCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_MERGING_MS).increment( System.currentTimeMillis() - start);
			}
		}

		// Snapshot
		long start = System.currentTimeMillis();
		node.client().admin().cluster()
				.prepareCreateSnapshot(snapshotRepoName, snapshotName)
				.setIndices((String[]) indices.toArray(new String[0])).get();

		// ES snapshot restore ignores timers and will block no more than 30s :( You have to block & poll to make sure it's done
		SnapshotInfo info = blockForSnapshot(snapshotRepoName, indices, timeoutMS, context);
		
		if(context != null) {
			logger.info(BaseESReducer.JOB_COUNTER.TIME_SPENT_SNAPSHOTTING_MS.name() + ": " + Long.toString(System.currentTimeMillis() - start));
			context.getCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_SNAPSHOTTING_MS)
					.increment(System.currentTimeMillis() - start);
		}
		return info;
	}

	/**
	 * Block for index snapshots to be complete
	 * 
	 * @param snapshotRepoName
	 * @param indices
	 * @param timeoutMS
     * @param context
	 */
	private SnapshotInfo blockForSnapshot(String snapshotRepoName, List<String> indices, long timeoutMS, Reducer.Context context) {
		long start = System.currentTimeMillis();
		int waitTimes = 0;
		while(System.currentTimeMillis() - start < timeoutMS) {
		    if (context != null) {
                context.progress();
                context.setStatus("Waiting for snapshot to complete: " + Integer.toString(waitTimes) + "seconds");
            }

			GetSnapshotsResponse repos = node.client().admin()
					.cluster().getSnapshots(new GetSnapshotsRequest(snapshotRepoName)).actionGet();

			for(SnapshotInfo i : repos.getSnapshots()) {
				if (i.failedShards() > 0) {
					logger.error("Failed shards: " + Integer.toString(i.failedShards()));
					for (SnapshotShardFailure failure : i.shardFailures()) {
						logger.error("Snapshot failed for: " + failure.reason());
					}
					return null;
				}
				if(i.state().completed() && i.successfulShards() == i.totalShards() && i.totalShards() >= indices.size()) {
					logger.info("Snapshot completed {} out of {} shards. Snapshot state {}. ",
							i.successfulShards(), i.totalShards(), i.state().completed());
					return i;
				} else {
					logger.info("Snapshotted {} out of {} shards, polling for completion. Snapshot state {}.",
							i.successfulShards(), i.totalShards(), i.state().completed());
				}
			}

			try {
				// Don't slam ES with snapshot status requests in a tight loop
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			waitTimes++;
		}
		return null;
	}
	
	public void deleteSnapshot(String snapshotName, String snapshotRepoName) {
		node.client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).execute().actionGet();
	}

	public static String makeSnapshotName(Collection<String> indices) {
		String snapshotName = "snapshot_";
		for (String index : indices) {
			snapshotName = snapshotName + index + "_";
		}
		return snapshotName.substring(0, snapshotName.length()-1);
	}

	public static class Builder {
		private ESEmbededContainer container;
		private String nodeName;
		private String workingDir;
		private String clusterName;
		private String templateName;
		private String templateSource;
		private String snapshotWorkingLocation;
		private String snapshotRepoName;
		private int numProcessors;

		public ESEmbededContainer build() {
			Preconditions.checkNotNull(nodeName);
			Preconditions.checkNotNull(workingDir);
			Preconditions.checkNotNull(clusterName);

			Settings.Builder builder = Settings.builder()
				.put("http.enabled", false) // Disable HTTP transport, we'll communicate inner-jvm
				.put("processors", numProcessors) // We could experiment ramping this up to match # cores - num reducers per node
				.put("cluster.name", clusterName)
				.put("node.name", nodeName)
				.put("path.home", "/")
				.put("path.data", workingDir)
				.put("path.repo", snapshotWorkingLocation)
				.put("transport.type", "local")
				//.put("plugins.load_classpath_plugins", true) // Allow plugins if they're bundled in with the uuberjar
				.put("bootstrap.memory_lock", true)
				.put("cluster.routing.allocation.disk.watermark.low", "99%") // Nodes don't form a cluster, so routing allocations don't matter
				.put("cluster.routing.allocation.disk.watermark.high", "99%")
				.put("indices.store.throttle.type", "none") // Allow indexing to max out disk IO
				.put("indices.memory.index_buffer_size", "40%") // The default 10% is a bit large b/c it's calculated against JVM heap size & not Yarn container allocation. Choosing a good value here could be made smarter.
				.put("indices.fielddata.cache.size", "0%");

			Settings nodeSettings = builder.build();

			// Create the node
			Node node = new Node(nodeSettings);
			container.setNode(node);

			// Start ES
			try {
				container.getNode().start();
			} catch (NodeValidationException ex) {
				logger.error("Node validation exception unable to start node", ex);
				throw new RuntimeException(ex);
			}

			// Configure the cluster with an index template mapping
			if(templateName != null && templateSource != null) {
				container.getNode().client().admin().indices().preparePutTemplate(templateName).setSource(templateSource).get();	
			}

			// Create the snapshot repo
			if(snapshotWorkingLocation != null && snapshotRepoName != null) {
				Map<String, Object> settings = new HashMap<>();
				settings.put("location", snapshotWorkingLocation);
				settings.put("compress", true);
				settings.put("max_snapshot_bytes_per_sec", "2048mb"); // The default 20mb/sec is very slow for a local disk to disk snapshot
				PutRepositoryResponse response =
						container.getNode().client().admin().cluster().preparePutRepository(snapshotRepoName).setType("fs").setSettings(settings).get();
			}

			return container;
		}

		public Builder() {
			container = new ESEmbededContainer();
		}

		/**
		 * @param nodeName
		 * @return Builder
		 */
		public Builder withNodeName(String nodeName) {
			this.nodeName = nodeName;
			return this;
		}

		/**
		 * 
		 * @param workingDir
		 * @return Builder
		 */
		public Builder withWorkingDir(String workingDir) {
			this.workingDir = workingDir;
			return this;
		}
		
		/**
		 * 
		 * @param clusterName
		 * @return Builder
		 */
		public Builder withClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}

		/**
		 * 
		 * @param templateName
		 * @param templateSource
		 * @return Builder
		 */
		public Builder withTemplate(String templateName, String templateSource) {
			this.templateName = templateName;
			this.templateSource = templateSource;
			return this;
		}

		/**
		 * 
		 * @param snapshotWorkingLocation
		 * @return Builder
		 */
		public Builder withSnapshotWorkingLocation(String snapshotWorkingLocation) {
			this.snapshotWorkingLocation = snapshotWorkingLocation;
			return this;
		}

		/**
		 * 
		 * @param snapshotRepoName
		 * @return Builder
		 */
		public Builder withSnapshotRepoName(String snapshotRepoName) {
			this.snapshotRepoName = snapshotRepoName;
			return this;
		}

		public Builder withNumProcessors(int numProcessors) {
			this.numProcessors = numProcessors;
			return this;
		}

	}

	/**
	 * 
	 * @return Node
	 */
	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

}
