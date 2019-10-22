/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.transport;

import org.elasticsearch.common.collect.Tuple;

public class SnapshotTransportStrategy {

    public enum STORAGE_SYSTEMS {
        s3,
        hdfs
    }

    /**
     * Given a source & destination, return an appropriate transport implementation
     *
     * @param snapshotWorkingLocation
     * @param snapshotFinalDestination
     * @return BaseTransport
     */
    public static BaseTransport get(String snapshotWorkingLocation, String snapshotFinalDestination) {
        BaseTransport trasport;
        if (snapshotFinalDestination.startsWith(STORAGE_SYSTEMS.s3.name())) {
            trasport = new S3SnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination);
        } else if (snapshotFinalDestination.startsWith(STORAGE_SYSTEMS.hdfs.name())) {
            trasport = new HDFSSnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination);
        } else {
            trasport = new LocalFSSnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination);
        }
        return trasport;
    }


    /**
     * Given a source & destination, return an appropriate transport implementation
     *
     * @param snapshotWorkingLocation
     * @param snapshotFinalDestination
     * @param baseSnapshotInfo
     * @return BaseTransport
     */
    public static BaseTransport get(String snapshotWorkingLocation, String snapshotFinalDestination, Tuple<String, String> baseSnapshotInfo) {
        if (baseSnapshotInfo == null) {
            return get(snapshotWorkingLocation, snapshotFinalDestination);
        }

        BaseTransport trasport;
        if (snapshotFinalDestination.startsWith(STORAGE_SYSTEMS.s3.name())) {
            trasport = new S3SnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination, baseSnapshotInfo);
        } else if (snapshotFinalDestination.startsWith(STORAGE_SYSTEMS.hdfs.name())) {
            trasport = new HDFSSnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination, baseSnapshotInfo);
        } else {
            trasport = new LocalFSSnapshotTransport(snapshotWorkingLocation, snapshotFinalDestination, baseSnapshotInfo);
        }
        return trasport;
    }
}
