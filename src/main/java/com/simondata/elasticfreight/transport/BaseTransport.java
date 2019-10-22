/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.transport;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.simondata.elasticfreight.job.BaseESReducer;
import com.simondata.elasticfreight.ShardConfig;
import com.simondata.elasticfreight.transport.SnapshotTransportStrategy.STORAGE_SYSTEMS;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseTransport {
	private static transient Logger logger = LoggerFactory.getLogger(BaseTransport.class);
	protected String snapshotWorkingLocation;
	protected String snapshotFinalDestination;
	protected Tuple<String,String> baseSnapshotInfo;
	private DirectoryFilter directoryFilter = new DirectoryFilter();
	
	public BaseTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		this.snapshotWorkingLocation = snapshotWorkingLocation;
		this.snapshotFinalDestination = snapshotFinalDestination;
		Preconditions.checkNotNull(snapshotWorkingLocation);
		Preconditions.checkNotNull(snapshotFinalDestination);
	}

	public BaseTransport(String snapshotWorkingLocation, String snapshotFinalDestination, Tuple<String,String> baseSnapshotInfo) {
		this.snapshotWorkingLocation = snapshotWorkingLocation;
		this.snapshotFinalDestination = snapshotFinalDestination;
		this.baseSnapshotInfo = baseSnapshotInfo;
		Preconditions.checkNotNull(snapshotWorkingLocation);
		Preconditions.checkNotNull(snapshotFinalDestination);
		Preconditions.checkNotNull(baseSnapshotInfo);
	}

	public abstract void init();
	public abstract void close();
	public abstract void transferFile(boolean deleteSource, String destination, String filename, String localDirectory) throws IOException;
	public abstract void transferFile(boolean deleteSource, String destination, String filename, String localDirectory, String destRename) throws IOException;
	protected abstract void transferDir(String destination, String localShardPath, String shard) throws IOException;
	public abstract boolean checkExists(String destination, String shardNumber) throws IOException;

	/**
	 * Transport a snapshot sitting on the local filesystem to a remote repository. Snapshots are stiched together
	 * shard by shard because we're snapshotting 1 shard at a time. 
	 * 
	 * @param snapshotInfo
	 * @param reducerShard - shardId for the particular reducer
	 * @return the indexId for the index that was snapshotted
	 * @throws IOException
	 */
	public String execute(SnapshotInfo snapshotInfo, String reducerShard) throws IOException {
		init();
		// Figure out which shard has all the data
		String largestShard = getShardSource();

		// Cleanup path name
		String destination = removeStorageSystemFromPath(snapshotFinalDestination);

		// Upload top level manifests
		if (baseSnapshotInfo == null) {
			transferFile(false, destination, makeMetadataFilename(snapshotInfo.snapshotId().getUUID()), snapshotWorkingLocation);
			transferFile(false, destination, makeSnapshotFilename(snapshotInfo.snapshotId().getUUID()), snapshotWorkingLocation);
			transferFile(false, destination, getLatestIndexFilename(), snapshotWorkingLocation);
			transferFile(false, destination, "index.latest", snapshotWorkingLocation);
		}

		// Upload per-index manifests
		String indexManifestSource = getSnapshotLocation();
		logger.info("indexManifestSource: " + indexManifestSource);
		String indexManifestDestination;

		if (baseSnapshotInfo != null) {
			indexManifestDestination =
					destination + BaseESReducer.DIR_SEPARATOR + "indices" + BaseESReducer.DIR_SEPARATOR + baseSnapshotInfo.v2() + BaseESReducer.DIR_SEPARATOR;
		}else {
			indexManifestDestination = indexManifestSource.replace(
					snapshotWorkingLocation,destination + BaseESReducer.DIR_SEPARATOR);
		}
		
		transferFile(false,
				indexManifestDestination.substring(0, indexManifestDestination.length()-1),
				makeMetadataFilename(snapshotInfo.snapshotId().getUUID()), indexManifestSource,
				(baseSnapshotInfo != null) ? makeMetadataFilename(baseSnapshotInfo.v1()) : null);
		
		// Cleanup shard data
		cleanEmptyShards(largestShard);

		// Upload shard data but we need to rename it to the shardId assigned to the reducer
		// TODO: This could be improved to more easily support other routing strategies
		String shardSource = getSnapshotLocation() + largestShard;
		String shardDestination = indexManifestDestination;
		transferDir(shardDestination, shardSource, reducerShard);
		close();

		indexManifestSource = indexManifestSource.substring(0, indexManifestSource.length()-1);
		return indexManifestSource.substring(indexManifestSource.lastIndexOf('/')+1);
	}

	/**
	 * Reads the snapshot metadata file and returns a Tuple of snapshotId and indexId for the given snapshot
	 * @param indexName
	 * @return
	 */
	public Tuple<String,String> getSnapshotInfo(String indexName) {
		String destination = removeStorageSystemFromPath(snapshotWorkingLocation);

		File[] files = new File(destination).listFiles(new WildcardFileFilter("index-*"));
		Tuple<String,String> snapshotMetadata = null;
		if (files != null && files.length == 1) {
			snapshotMetadata = getSnapshotMetadata(files[0].getName(), indexName);
		}
		return snapshotMetadata;
	}

	/**
	 * Fills in blank shards for the final dataset (i.e. if shards were only create for 4 our of the 5 nodes
	 * it will create a blank shard for node 5).  If includeRootManifest is set it will copy the base snapshot and
	 * metadata for the snapshot.
	 *
	 * @param indexName
	 * @param snapshotIndexIds
	 * @param shardConfig
	 * @param includeRootManifest
	 * @throws IOException
	 */
	public Set<Integer> placeMissingShards(String indexName, Set<Tuple<String, String>> snapshotIndexIds, ShardConfig shardConfig, boolean includeRootManifest) throws IOException {
		String destination = removeStorageSystemFromPath(snapshotFinalDestination);

		if(includeRootManifest) {
			// Upload top level manifests
			transferFile(false, destination, makeMetadataFilename("*"), snapshotWorkingLocation);
			transferFile(false, destination, makeSnapshotFilename("*"), snapshotWorkingLocation);
			transferFile(false, destination, getLatestIndexFilename(), snapshotWorkingLocation);
			transferFile(false, destination, "index.latest", snapshotWorkingLocation);
		}

		Set<Integer> missingShards = new HashSet<>();
		for(int shard = 0; shard < shardConfig.getShardsForIndex(indexName); shard++) {
			String indexDestination = getSnapshotLocation().replace(snapshotWorkingLocation ,destination + BaseESReducer.DIR_SEPARATOR);
			if(!checkExists(indexDestination, Integer.toString(shard) + BaseESReducer.DIR_SEPARATOR)) {
				logger.info("Shard not found: " + indexDestination + " shard: " + Integer.toString(shard));
				// Upload shard data
				missingShards.add(shard);
				String shardSource = getSnapshotLocation() + shard;
				transferDir(indexDestination, shardSource, Integer.toString(shard));
			}
		}
		return missingShards;
	}

	private String getLatestIndexFilename() {
		File latestIndexFile = new File(snapshotWorkingLocation + "index.latest");
		try (InputStream blob = new FileInputStream(latestIndexFile)) {
			BytesStreamOutput out = new BytesStreamOutput();
			Streams.copy(blob, out);
			return "index-" + Long.toString(Numbers.bytesToLong(out.bytes().toBytesRef()));
		} catch (Exception ex) {
			logger.error("Unable to read file: " + latestIndexFile.getAbsolutePath(), ex);
		}
		return "index-0";
	}

	/**
	 * Read the snapshot metadata file to get the snapshotId and indexId out of it
	 * @param metadataFilename
	 * @param indexName
	 * @return Tuple<String,String> of snapshotId and indexId
	 */
	private Tuple<String,String> getSnapshotMetadata(String metadataFilename, String indexName) {
		File latestIndexFile = new File(snapshotWorkingLocation + metadataFilename);
		try (InputStream blob = new FileInputStream(latestIndexFile)) {
			Map<String,Object> map = new ObjectMapper().readValue(blob, new TypeReference<Map<String, Object>>() {});
			List<Map<String, Object>> snapshots = (List<Map<String, Object>>)map.get("snapshots");
			String uuid = (String)snapshots.get(0).get("uuid");
			Map<String, Map<String, Object>> indices = (Map<String, Map<String, Object>>)map.get("indices");
			Map<String, Object> indexInfo = indices.get(indexName);
			String indexId = (String)indexInfo.get("id");
			return new Tuple<String,String>(uuid, indexId);
		} catch (Exception ex) {
			logger.error("Unable to read file: " + latestIndexFile.getAbsolutePath(), ex);
		}
		return new Tuple<String,String>(null, null);
	}
	
	/**
	 * Rip out filesystem specific stuff off the path EG s3:// 
	 * @param s
	 * @return s
	 */
	public String removeStorageSystemFromPath(String s) {
		for(STORAGE_SYSTEMS storageSystem : SnapshotTransportStrategy.STORAGE_SYSTEMS.values()) {
			s = s.replaceFirst(storageSystem.name() + "://", "");			
		}

		return s;
	}

	/**
	 * Automatically figure out the folder where the snapshot is located.
	 * @return path to shapshot folder
	 */
	private String getSnapshotLocation() {
        String baseIndexLocation = snapshotWorkingLocation + "indices" + BaseESReducer.DIR_SEPARATOR;
        File file = new File(baseIndexLocation);
        String[] directories = file.list(directoryFilter);
        if (directories == null || directories.length != 1) {
            return null;
        }
        return new File(baseIndexLocation + directories[0]).getAbsolutePath() + BaseESReducer.DIR_SEPARATOR;
    }

	/**
	 * We've snapshotted an index with all data routed to a single shard (1 shard per reducer). Problem is 
	 * we don't know which shard # it routed all the data to. We can determine that by picking 
	 * out the largest shard folder.
	 */
	private String getShardSource() throws IOException {
		// Get a list of shards in the snapshot
		String baseIndexLocation = getSnapshotLocation();
		logger.info("baseIndexLocation: " + baseIndexLocation);
		File file = new File(baseIndexLocation);
		String[] shardDirectories = file.list(directoryFilter);

		// Figure out which shard has all the data in it. Since we've routed all data to it, there'll only be one
		Long biggestDirLength = null;
		String biggestDir = null;
		for(String directory : shardDirectories) {
			File curDir = new File(baseIndexLocation + directory);
			try {
                long curDirLength = FileUtils.sizeOfDirectory(curDir);
                if (biggestDirLength == null || biggestDirLength < curDirLength) {
                    biggestDir = directory;
                    biggestDirLength = curDirLength;
                }
            } catch (Exception ex) {
			    logger.error("Exception listing: " + baseIndexLocation + directory, ex);
            }
		}
		logger.info("Largest: " + biggestDir);
		return biggestDir;
	}
	
	/**
	 * We're building 1 shard at a time. Therefore each snapshot has a bunch of empty
	 * shards and 1 shard with all the data in it. This deletes all the empty shard folders
	 * for you.
	 *
	 * @param biggestDir
	 * @throws IOException
	 */
	private void cleanEmptyShards(String biggestDir) throws IOException {
		String baseIndexLocation = getSnapshotLocation();
		File file = new File(baseIndexLocation.trim());
		String[] shardDirectories = file.list(directoryFilter);
		
		// Remove the empty shards
		for(String directory : shardDirectories) {
			if(!directory.equals(biggestDir)) {
				FileUtils.deleteDirectory(new File(baseIndexLocation + directory));
			}
		}
	}
	
	private class DirectoryFilter implements FilenameFilter {
		@Override
		public boolean accept(File current, String name) {
			return new File(current, name).isDirectory();
		}
	}

	private class WildcardFileFilter implements FilenameFilter {

		private Pattern pattern;
		private String ESCAPE_CHARS = "<([{\\^-=$!|]})?*+.>";

		WildcardFileFilter(String pattern) {
			String escapedPattern = pattern;
			for (int i=0; i<ESCAPE_CHARS.length(); i++) {
				String c = Character.toString(ESCAPE_CHARS.charAt(i));
				escapedPattern = escapedPattern.replace(c, "\\"+c);
			}
			this.pattern = Pattern.compile(escapedPattern.replace("\\*", ".*"));
		}

		@Override
		public boolean accept(File current, String name) {
			Matcher m = this.pattern.matcher(name);
			if (m.matches()) {
				logger.info("Found file: " + name);
			}
			return m.matches();
		}
	}

	protected File findFileByPattern(String location, String pattern) {
		File[] files = new File(location).listFiles(new WildcardFileFilter(pattern));
		if (files != null) {
			return files[files.length-1];
		}
		return new File(location + BaseESReducer.DIR_SEPARATOR + pattern);
	}

	public String getSnapshotWorkingLocation() {
		return snapshotWorkingLocation;
	}

	public String getSnapshotFinalDestination() {
		return snapshotFinalDestination;
	}

	public static String makeMetadataFilename(String snapshotId) {
		return "meta-" + snapshotId + ".dat";
	}

	public static String makeSnapshotFilename(String snapshotId) {
		return "snap-" + snapshotId + ".dat";
	}
}
