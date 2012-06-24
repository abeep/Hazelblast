package com.hazelblast.client.router;

import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.partition.Partition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import static com.hazelblast.TestUtils.newLiteInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionedRouterTest {

    private static int clusterSize = 3;
    private static List<HazelcastInstance> instances = new LinkedList<HazelcastInstance>();
    private static HazelcastInstance liteInstance;

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        Hazelcast.shutdownAll();
        for (int k = 0; k < clusterSize; k++) {
            instances.add(Hazelcast.newHazelcastInstance(null));
        }
        liteInstance = newLiteInstance();
        instances.add(liteInstance);

        Thread.sleep(10000);
    }

    @AfterClass
    public static void afterClass() {
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().shutdown();
        }
    }

    @Test
    public void nullArgument() throws Throwable {
        PartitionRouter router = new PartitionRouter(liteInstance, null, null, 0);
        Method method = PartitionedService.class.getMethod("normalArg", Object.class);
        try {
            router.getTarget(method, new Object[]{null});
            fail();
        } catch (NullPointerException expected) {

        }
    }

    @Test
    public void normalArgument() throws Throwable {
        PartitionRouter router = new PartitionRouter(liteInstance, null,null, 0);

        Method method = PartitionedService.class.getMethod("normalArg", Object.class);
        String arg = "foo";
        Target target = router.getTarget(method, new Object[]{arg});

        Partition partition = liteInstance.getPartitionService().getPartition(arg);
        Member expectedMember = partition.getOwner();
        assertEquals(expectedMember, target.getMember());
        assertEquals((long)partition.getPartitionId(), target.getPartitionId());
    }

    @Test
    public void partitionAwareArgument() throws Throwable {
        PartitionRouter router = new PartitionRouter(liteInstance, null,null, 0);

        Method method = PartitionedService.class.getMethod("normalArg", Object.class);
        PartitionAware arg = mock(PartitionAware.class);
        String partitionKey = "foo";
        when(arg.getPartitionKey()).thenReturn(partitionKey);
        Target target = router.getTarget(method, new Object[]{arg});

        Partition partition = liteInstance.getPartitionService().getPartition(partitionKey);
        Member expectedMember = partition.getOwner();
        assertEquals(expectedMember, target.getMember());
        assertEquals((long)partition.getPartitionId(), target.getPartitionId());
    }

    @Test
    public void argumentWithPartitionKeyMethod_whenArgumentIsNull() throws Throwable {
        Method propertyMethod = Person.class.getMethod("id");
        PartitionRouter router = new PartitionRouter(liteInstance, propertyMethod, null,0);

        Method method = PartitionedService.class.getMethod("argumentWithPropertyMethod", Person.class);
        try {
            router.getTarget(method, new Object[]{null});
            fail();
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void argumentWithPartitionKeyMethod_whenReturnsNull() throws Throwable {
        Method propertyMethod = Person.class.getMethod("id");
        PartitionRouter router = new PartitionRouter(liteInstance, propertyMethod,null, 0);

        Method method = PartitionedService.class.getMethod("argumentWithPropertyMethod", Person.class);
        Person person = mock(Person.class);
        when(person.id()).thenReturn(null);
        try {
            router.getTarget(method, new Object[]{person});
            fail();
        } catch (NullPointerException expected) {
        }
    }

    @Test
    public void argumentWithPartitionKeyMethod_whenSuccess() throws Throwable {
        Method propertyMethod = Person.class.getMethod("id");
        PartitionRouter router = new PartitionRouter(liteInstance, propertyMethod,null, 0);

        Method method = PartitionedService.class.getMethod("argumentWithPropertyMethod", Person.class);
        String partitionKey = "foo";
        Person person = mock(Person.class);
        when(person.id()).thenReturn(partitionKey);
        Target target = router.getTarget(method, new Object[]{person});

        Partition partition = liteInstance.getPartitionService().getPartition(partitionKey);
        Member expectedMember = partition.getOwner();
        assertEquals(expectedMember, target.getMember());
        assertEquals((long)partition.getPartitionId(), target.getPartitionId());
    }

    @Test
    public void argumentWithPartitionKeyMethod_whenExceptionIsThrown() throws Throwable {
        Method propertyMethod = Person.class.getMethod("id");
        PartitionRouter router = new PartitionRouter(liteInstance, propertyMethod,null, 0);

        Method method = PartitionedService.class.getMethod("argumentWithPropertyMethod", Person.class);
        Person person = mock(Person.class);
        when(person.id()).thenThrow(new MyException());

        try {
            router.getTarget(method, new Object[]{person});
            fail();
        } catch (MyException expected) {
        }
    }

    @DistributedService
    public interface PartitionedService {
        @Partitioned
        void normalArg(@PartitionKey Object arg);

        @Partitioned
        void argumentWithPropertyMethod(@PartitionKey(property = "id") Person arg);
    }

    private interface Person extends Serializable {
        public String id();
    }

    private static class MyException extends RuntimeException {
    }
}
