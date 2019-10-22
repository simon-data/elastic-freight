/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.transport;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Move the snapshot to locally connected storage
 * 
 * @author drew
 *
 */
public class LocalFSSnapshotTransport extends BaseTransport {
	private static transient Logger logger = LoggerFactory.getLogger(LocalFSSnapshotTransport.class);

	public LocalFSSnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		super(snapshotWorkingLocation, snapshotFinalDestination);
	}

	public LocalFSSnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination, Tuple<String, String> baseSnapshotInfo) {
		super(snapshotWorkingLocation, snapshotFinalDestination, baseSnapshotInfo);
	}

	@Override
	public void init() {
		// no-op
	}

	@Override
	public void close() {
		// no-op
	}

	@Override
	public void transferFile(boolean deleteSource, String bucket, String filename, String localDirectory) throws IOException  {
		transferFile(deleteSource, bucket, filename, localDirectory, null);
	}

	@Override
	public void transferFile(boolean deleteSource, String destination, String filename, String localDirectory, String destRename) throws IOException {
		File source = this.findFileByPattern(localDirectory, filename);
		Preconditions.checkArgument(source.exists(), "Could not find source file: " + source.getAbsolutePath());

		File destinationDir = new File(destination);
		FileUtils.forceMkdir(destinationDir);
		if (destRename == null) {
			FileUtils.copyFileToDirectory(source, destinationDir);
		} else {
			File destFile = new File(destinationDir, destRename);
			FileUtils.copyFile(source, destFile);
		}
		if(deleteSource) {
			source.delete();
		}
	}

	@Override
	protected void transferDir(String destination, String source, String shard) throws IOException {
		File sourceDir = new File(source);
		Preconditions.checkArgument(sourceDir.exists(), "Could not find dir: " + source); 

		File destinationDir = new File(destination + shard);
		FileUtils.forceMkdir(destinationDir);
		FileUtils.copyDirectory(sourceDir, destinationDir);
	}

	@Override
	public boolean checkExists(String destination, String shardNumber) throws IOException {
		return new File(destination + shardNumber).exists();
	}

}
