package com.hazelblast.client.impl.distributed;

import com.hazelblast.TestUtils;
import com.hazelblast.client.impl.ProxyProviderImpl;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelblast.TestUtils.newLiteInstance;

public class LoadBalanced_DistributedClusterStressTest {
    private final AtomicLong idGenerator = new AtomicLong();
    private final AtomicBoolean run = new AtomicBoolean(true);

    String main = Client.class.getName();
    String jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    String jvmArgs = "-Djava.net.preferIPv4Stack=true";
    List<ClientProcess> clients = new LinkedList<ClientProcess>();

    @Before
    public void setUp() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws IOException, InterruptedException {
        HazelcastInstance clientInstance = newLiteInstance();
        ProxyProviderImpl proxyProvider = new ProxyProviderImpl(clientInstance);
        LoadBalancedService loadBalancedService = proxyProvider.getProxy(LoadBalancedService.class);

        for (int k = 0; k < 5; k++) {
            ClientProcess clientProcess = new ClientProcess();
            clients.add(clientProcess);
        }

        ControlThread controlThread = new ControlThread();
        controlThread.start();
        TestUtils.scheduleSetFalse(run, 300*1000);

        System.out.println("Starting");

        int k=0;
        while(run.get()){
            k++;
            loadBalancedService.test();
            if (k % 1000 == 0) {
                System.out.println("at " + k);
            }
        }

        System.out.println("FInished");

        for (ClientProcess process : clients) {
            process.clientProcess.destroy();
            process.clientProcess.waitFor();
        }
    }

    public class ControlThread extends Thread{
        private final Random random = new Random();

        public void run(){
            while(run.get()){
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException();
                }

                int r = random.nextInt();
                if(r % 5 ==0){
                    System.out.println("--------------------------------------------------------------");
                    System.out.println("Starting a new client");
                    System.out.println("--------------------------------------------------------------");

                    try {
                        startClientProcess();
                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }else if(r % 6 ==0){
                    System.out.println("--------------------------------------------------------------");
                    System.out.println("Killing a random client");
                    System.out.println("--------------------------------------------------------------");

                    ClientProcess removedClient = clients.remove(random.nextInt(clients.size()));
                    removedClient.clientProcess.destroy();
                }
            }
        }
    }


    public void startClientProcess() throws IOException {
        ClientProcess clientProcess = new ClientProcess();
        clients.add(clientProcess);
    }

    class ClientProcess extends Thread {
        private final Process clientProcess;

        ClientProcess() throws IOException {
            ProcessBuilder builder = new ProcessBuilder(jvm, "-cp", classpath, jvmArgs, main);
            builder.redirectErrorStream(true);
            clientProcess = builder.start();

            new StreamReader("client-" + idGenerator.incrementAndGet(), clientProcess.getInputStream()).start();
        }

        public void run() {

        }
    }

    class StreamReader extends Thread {
        private final BufferedReader in;
        private String clientId;

        StreamReader(String clientId, InputStream in) {
            this.in = new BufferedReader(new InputStreamReader(in));
            this.clientId = clientId;
        }

        public void run() {
            try {
                for (; ; ) {
                    String line = in.readLine();
                    if (line == null) {
                        return;
                    }
                    System.out.println(clientId + ": " + line);
                }
            } catch (IOException e) {
                //e.printStackTrace();
            }
        }
    }
}

