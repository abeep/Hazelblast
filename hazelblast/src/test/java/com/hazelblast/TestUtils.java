package com.hazelblast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class TestUtils {
    public static HazelcastInstance newLiteInstance() {
        Config config = new XmlConfigBuilder().build();
        config.setLiteMember(true);
        return Hazelcast.newHazelcastInstance(config);
    }
}
