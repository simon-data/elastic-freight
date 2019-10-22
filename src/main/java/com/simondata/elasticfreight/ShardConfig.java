/*
 * Copyright 2019-present, Simon Data, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0
 * found in the LICENSE file in the root directory of this source tree.
 */

package com.simondata.elasticfreight;

import java.util.HashMap;
import java.util.Map;

public class ShardConfig {
    private Map<String, Long> shardsPerIndex = new HashMap<>();
    private Long defaultShardsPerIndex = 1l;
    
    public ShardConfig(Map<String, Long> shardsPerIndex) {
        this.shardsPerIndex = shardsPerIndex;
    }
        
    public ShardConfig(Map<String, Long> shardsPerIndex, Long defaultShardsPerIndex) {
        this.shardsPerIndex = shardsPerIndex;
        this.defaultShardsPerIndex = defaultShardsPerIndex;
    }
    
    public ShardConfig() {}
    
    public ShardConfig(Long defaultShardsPerIndex) {
        this.defaultShardsPerIndex = defaultShardsPerIndex;
    }

    public Long getShardsForIndex(String index) {
        if(shardsPerIndex.containsKey(index)) {
            return shardsPerIndex.get(index);
        } 
        
        return defaultShardsPerIndex;
    }
    

    @Override
    public String toString() {
        return "ShardConfig [shardsPerIndex=" + shardsPerIndex +
                ", defaultShardsPerIndex=" + defaultShardsPerIndex + "]";
    }

}
