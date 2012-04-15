Hazelblast

-------------------------

An layer on top of Hazelcast to provide transparent remote interfaces.

Example
-------------------------


    class FireService{
        @Partitioned
        void fire(@RoutingId String employeeId);
    }

    //this will run on each node in the cluster
    class DefaultFireService implements FireService{
        private final Map map = Hazelcast.getMap("employees");

        public void fire(String employeeId){
            Employee e = map.get(employeeId);
            e.setFired(true);
            map.put(employeeId, e);
        }
    }


And it can be called like this:

    ProxyProvider proxyProvider = new ProxyProvider();
    FireService fireService = proxyProvider.getProxy(FireService.class);
    fireService.fire("123");

For a full example check out the Hazelblast-examples module
