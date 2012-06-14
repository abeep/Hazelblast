Hazelblast
-------------------------
A layer on top of Hazelcast to provide transparent remote interfaces.

News
-------------------------
* 10 June 2012: Hazelblast 0.1 has been released

Example
-------------------------
This interface can be shared between the client (that calls this service) and the server (that implements
this service). The fire method is routed on the employeeId.

    @RemoteInterface
    class FireService{

        @Partitioned
        void fire(@PartitionKey String employeeId);
    }

And it can be called like this:

    ProxyProvider proxyProvider = new DefaultProxyProvider();
    FireService fireService = proxyProvider.getProxy(FireService.class);
    fireService.fire("123");

On the server, the following can be done:

    class DefaultFireService implements FireService{
        private final Map map = Hazelcast.getMap("employees");

        public void fire(String employeeId){
            Employee e = map.get(employeeId);
            e.setFired(true);
            map.put(employeeId, e);
        }
    }

There is functionality in Hazelblast to use POJO's or Spring to run this service on each Hazelcast node. It
is quite simple to add additional strategies for running services.

There currently are 2 different types of calls

* Partitioned: the call gets forwarded to the machine responsible for running that partition. Once you are
on that machine, data is local.
* LoadBalanced: the call gets forwarded to one of the machine; it doesn't matter which one.

In the 0.2 release also the ForkJoin will be added:
* ForkJoin: the call gets send to all machines, and the results are aggregated.

For a full example check out the Hazelblast-examples module

Maven
-------------------------
First a repo needs to be added:

     <distributionManagement>
        <!-- use this if you want to work against a final-->
        <repository>
            <id>hazelblast-repository</id>
            <url>http://pveentjer.github.com/Hazelblast/repository/</url>
        </repository>

        <!-- use this if you want to use a snapshot of multiverse -->
        <snapshotRepository>
            <id>hazelblast-snapshot-repository</id>
            <url>http://pveentjer.github.com/Hazelblast/snapshot-repository/</url>
        </snapshotRepository>
    </distributionManagement>


And add the following dependency:

       <dependencies>
            <dependency>
                <groupId>com.hazelblast</groupId>
                <artifactId>hazelblast</artifactId>
                <version>0.1</version>
            </dependency>
        </dependencies>

And you should be good to go.