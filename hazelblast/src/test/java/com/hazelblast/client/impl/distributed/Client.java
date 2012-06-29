package com.hazelblast.client.impl.distributed;


import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Client {

    public static void main(String[] args){
        //HazelcastInstance hazelcastInstance = Hazelcast.getDefaultInstance();
        PojoSlice pojoSlice = new PojoSlice(new Pojo(null));
        SliceServer sliceServer = new SliceServer(pojoSlice).start();
        System.out.println("completed");
    }

    static public class Pojo implements HazelcastInstanceProvider {
        @Exposed
        public LoadBalancedService loadBalancedService = new LoadBalancedServiceImpl();

        private final HazelcastInstance hazelcastInstance = Hazelcast.getDefaultInstance();

        public Pojo(HazelcastInstance hazelcastInstance) {
            //this.hazelcastInstance = hazelcastInstance;
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }
    }

    public static class LoadBalancedServiceImpl implements LoadBalancedService{
        public void test() {
        }
    }
}
