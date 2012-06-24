package com.hazelblast.client;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class LiteMemberTest {

    @Before
    public void before() {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    @Ignore
    public void test() throws InterruptedException {
        HazelcastInstance[] instances = new HazelcastInstance[4];
        instances[0] = newLiteInstance();
        for (int k = 1; k < instances.length; k++) {
            instances[k] = Hazelcast.newHazelcastInstance(null);
        }

            Thread.sleep(10000);


        for (int k = 0; k < 1000000; k++) {
            System.out.println(k);
            Partition p = instances[1].getPartitionService().getPartition(k);
            assertNotNull(p);
            assertNotNull(p.getOwner());
            assertFalse(p.getOwner().isLiteMember());
        }
    }

    public static HazelcastInstance newLiteInstance() {
        Config config = new XmlConfigBuilder().build();
        config.setLiteMember(true);
        return Hazelcast.newHazelcastInstance(config);
    }
}
