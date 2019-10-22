/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.transport;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.collect.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Stitch together snapshots of ES shards as it pushes files to S3. If we could get the
 * snapshot gateway S3 plugin working we could potentially use that instead of using the aws sdk
 * directly, but there's some things holding that up. 
 * 
 * 1) That plugin needs some work to support MFA (at the time of writing it does not)
 * 2) We'll have to repackage ES with the plugin because installing plugins via the
 * embeded client isn't supported very well.
 * 3) This job is actually creating franken-snapshots. We're snapshotting each shard separately to reduce
 * the storage footprint per reducer task and then merging the snapshots together. To use the snapshot gateway
 * we would need a way to have the whole index on a single task tracker node. That means
 * beefy task trackers or mounting either some NFS or Ephemeral disks. Doing 1 shard at a time shrinks the problem.
 *
 */
public class S3SnapshotTransport extends BaseTransport {
	private static transient Logger logger = LoggerFactory.getLogger(S3SnapshotTransport.class);
	private static final int S3_TRANSFER_THREAD_COUNT = 128;
	private TransferManager tx;
	private ObjectMetadataProvider objectMetadataProvider;
	private int UPLOAD_RETRIES = 5;

	/**
	 * The default S3 thread pool in the aws sdk is 10 threads. ES Snapshots can be 100s of files, so parallelizing that
	 * is advised. 
	 * 
	 * @return
	 */
	public static ThreadPoolExecutor createDefaultExecutorService() {
		ThreadFactory threadFactory = new ThreadFactory() {
			private int threadCount = 1;

			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName("s3-transfer-manager-worker-" + threadCount++);
				return thread;
			}
		};
		return (ThreadPoolExecutor)Executors.newFixedThreadPool(S3_TRANSFER_THREAD_COUNT, threadFactory);
	}

	public S3SnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination) {
		super(snapshotWorkingLocation, snapshotFinalDestination);
	}

	public S3SnapshotTransport(String snapshotWorkingLocation, String snapshotFinalDestination, Tuple<String, String> baseSnapshotInfo) {
		super(snapshotWorkingLocation, snapshotFinalDestination, baseSnapshotInfo);
	}

	public static AmazonS3Client getS3Client() {
		return (Regions.getCurrentRegion() != null) ?
				Regions.getCurrentRegion().createClient(AmazonS3Client.class,
						new DefaultAWSCredentialsProviderChain(),
						new ClientConfiguration()) :
							new AmazonS3Client();
	}

	@Override
	public void init() {
		tx = new TransferManager(getS3Client(), createDefaultExecutorService());
		
		objectMetadataProvider = new ObjectMetadataProvider() {
			@Override
			public void provideObjectMetadata(File file, ObjectMetadata metadata) {
				metadata.setSSEAlgorithm("AES256");
				metadata.setContentLength(file.length());
			}
		};
	}
	
	@Override
	public void close() {
		tx.shutdownNow();	
	}

	@Override
	protected void transferDir(String shardDestinationBucket, String localShardPath, String shard) {
	    logger.info("Transferring: " + localShardPath + " to: " + shardDestinationBucket + shard);

	    boolean uploadSuccess = false;
	    for (int i=0; i<UPLOAD_RETRIES; i++) {
			MultipleFileUpload mfu = tx.uploadDirectory(shardDestinationBucket + shard, null, new File(localShardPath), true, objectMetadataProvider);

			double lastPercent = -1d;
			while (!mfu.isDone()) {
				double currentPercent = mfu.getProgress().getPercentTransferred();
				if (currentPercent == lastPercent && Double.compare(mfu.getProgress().getPercentTransferred(),100d) != 0) {
					logger.info("Upload stalled retrying...");
					break;
				}

				logger.info("Transferring to S3 completed %" + currentPercent);
				lastPercent = currentPercent;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					logger.info("Interrupted retrying...");
					break;
				}
			}


			if (mfu.getState().equals(TransferState.Completed) &&
					(mfu.isDone() || Double.compare(mfu.getProgress().getPercentTransferred(),100d) == 0)) {
				uploadSuccess = true;
				break;
			}
		}

		if (!uploadSuccess) {
			throw new IllegalStateException(
					"Directory upload " + localShardPath + " failed to upload after " + String.valueOf(UPLOAD_RETRIES) + " tries");
		}
	}

	@Override
	public void transferFile(boolean deleteSource, String bucket, String filename, String localDirectory) {
		transferFile(deleteSource, bucket, filename, localDirectory, null);
	}

	@Override
	public void transferFile(boolean deleteSource, String bucket, String filename, String localDirectory, String destRename) {
		File source = this.findFileByPattern(localDirectory, filename);
		filename = source.getName();
		if (destRename == null) {
			destRename = filename;
		}
		Preconditions.checkArgument(source.exists(), "Could not find source file: " + source.getAbsolutePath());
		logger.info("Transfering + " + source + " to " + bucket + " with key " + destRename);
		FileInputStream fis;
		try {
			fis = new FileInputStream(source);
			ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.setSSEAlgorithm("AES256");
			objectMetadata.setContentLength(source.length());

			boolean uploadSuccess = false;
			for (int i=0; i<UPLOAD_RETRIES; i++) {
				Upload upload = tx.upload(bucket, destRename, fis, objectMetadata);
				try {
					UploadResult result = upload.waitForUploadResult();
				} catch (RuntimeException | InterruptedException ex) {
					logger.error("Failed to upload: ", ex);
				}
				if (upload.getState().equals(TransferState.Completed)) {
					uploadSuccess = true;
					break;
				}
			}
			if (!uploadSuccess) {
				throw new IllegalStateException(
						"File " + filename + " failed to upload after " + String.valueOf(UPLOAD_RETRIES) + " tries");
			}

			if(deleteSource) {
				source.delete();
			}
		} catch (FileNotFoundException e) {
			// Exception should never be thrown because the precondition above has already validated existence of file
			logger.error("Filename could not be found " + filename, e);
		}
	}

	public static Tuple<String,String> getBucketAndKey(String destination) {
		String[] pieces = StringUtils.split(destination, "/");
		String bucket = pieces[0];
		String key = destination.substring(bucket.length() + 1);
		return new Tuple<>(bucket, key);
	}

	@Override
	public boolean checkExists(String destination, String shard) throws IOException {
		// Break that s3 path into bucket & key 
		Tuple<String,String> bucketAndKey = getBucketAndKey(destination);
		String bucket = bucketAndKey.v1();
		String key = bucketAndKey.v2();;
		
		// AWS SDK doesn't have an "exists" method so you have to list and check if the key is there. Thanks Obama
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(key);
		ListObjectsV2Result result;

		do {
			result = getS3Client().listObjectsV2(req);

			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				if (objectSummary.getKey().startsWith(key + shard)) {
					return true;
				}
			}
			String token = result.getNextContinuationToken();
			req.setContinuationToken(token);
		} while (result.isTruncated());
		return false;
	}

	public Set<String> listSubFolders(String src) throws IOException {
		// Break that s3 path into bucket & key
		Tuple<String,String> bucketAndKey = getBucketAndKey(src);
		String bucket = bucketAndKey.v1();
		String key = bucketAndKey.v2();

		Set<String> subFolders = new HashSet<>();

		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(key);
		ListObjectsV2Result result;

		do {
			result = getS3Client().listObjectsV2(req);

			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				String folderKey = objectSummary.getKey().replace(key, "");
				if (folderKey.charAt(0) == '/') {
					folderKey = folderKey.substring(1);
				}
				if (folderKey.contains("/")) {
					subFolders.add(folderKey.split("/")[0]);
				}
			}
			String token = result.getNextContinuationToken();
			req.setContinuationToken(token);
		} while (result.isTruncated());
		return subFolders;
	}

	public void copyAwsPathToAnother(String srcFolderPath, String destFolderPath) {
		Tuple<String,String> srcBucketAndKey = getBucketAndKey(srcFolderPath);
		String srcBucket = srcBucketAndKey.v1();
		String finalSrcFolderPath = srcBucketAndKey.v2();

		Tuple<String,String> destBucketAndKey = getBucketAndKey(destFolderPath);
		String destBucket = destBucketAndKey.v1();
		String finalDestFolderPath = destBucketAndKey.v2();

		List<String> failedKeys = new ArrayList<>();

		tx.getAmazonS3Client().listObjectsV2(srcBucket, finalSrcFolderPath)
				.getObjectSummaries().stream().map(S3ObjectSummary::getKey).forEach(key -> {
					String destKey = key.replace(finalSrcFolderPath, finalDestFolderPath);
					copyKey(srcBucket, key, destBucket, destKey, failedKeys);
				}
		);

		if (!failedKeys.isEmpty()) {
			logger.error("Some of the keys cannot be copied: " + failedKeys);
			throw new RuntimeException("Some of the keys cannot be copied: " + failedKeys);
		}

		logger.info("All objects copied successfully.");
	}

	public void copyS3File(String srcFile, String destFile) {
        Tuple<String,String> srcBucketAndKey = getBucketAndKey(srcFile);
        String srcBucket = srcBucketAndKey.v1();
        String srcKey = srcBucketAndKey.v2();

        Tuple<String,String> destBucketAndKey = getBucketAndKey(destFile);
        String destBucket = destBucketAndKey.v1();
        String destKey = destBucketAndKey.v2();

        copyKey(srcBucket, srcKey, destBucket, destKey, new ArrayList<>());
    }

	public String readFile(String srcFile) throws IOException {
		Tuple<String,String> srcBucketAndKey = getBucketAndKey(srcFile);
		String srcBucket = srcBucketAndKey.v1();
		String srcKey = srcBucketAndKey.v2();
		GetObjectRequest request = new GetObjectRequest(srcBucket, srcKey);
		S3Object object = tx.getAmazonS3Client().getObject(request);

		StringBuilder sb = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		String line = null;
		while ((line = reader.readLine()) != null) {
			sb.append(line);
		}
		return sb.toString();
	}

	public void deleteKey(String key) {
		Tuple<String,String> bucketAndKey = getBucketAndKey(key);
		String bucket = bucketAndKey.v1();
		String folderPath = bucketAndKey.v2();
		logger.info("Deleting object: " + bucket + folderPath);
		tx.getAmazonS3Client().deleteObject(new DeleteObjectRequest(bucket, folderPath));
	}

	private void copyKey(String srcBucket, String srcKey, String destBucket, String destKey, List<String> failedKeys) {
        logger.info("Copying: " + srcKey + " to: " + destKey);
	    CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcBucket, srcKey, destBucket, destKey);
		try {
			Copy xfer = tx.copy(copyObjectRequest);
            xfer.waitForCompletion();
			logger.info("Deleting src object: " + srcBucket + srcKey);
			tx.getAmazonS3Client().deleteObject(new DeleteObjectRequest(srcBucket, srcKey));
		} catch (Exception e) {
			logger.error("Xfer failed for: " + srcKey, e);
			failedKeys.add(srcKey);
		}
	}

}
