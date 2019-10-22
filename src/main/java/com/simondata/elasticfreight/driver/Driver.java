/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight.driver;

import com.simondata.example.IndexingJob;
import org.apache.hadoop.util.ProgramDriver;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;

public class Driver extends ProgramDriver {

    public Driver() throws Throwable {
        super();
        addClass(
                "esIndex",
                IndexingJob.class,
                "Example job for how to build elasticsearch indexes using Hadoop"
        );
    }

    public static void main(String[] args) throws Throwable {   	
        DateTimeZone.setDefault(DateTimeZone.UTC);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        Driver driver = new Driver();
        driver.driver(args);
        System.exit(0);
    }
}
