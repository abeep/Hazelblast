package com.hazelblast.server;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.logging.Level;

public class ServerMain {
    private final static ILogger logger = Logger.getLogger(ServerMain.class.getName());

    public static void main(String[] args) {
        ServerMain main = new ServerMain();
        main.run();
    }

    private final PuMonitor puMonitor;
    private final PuContainer puContainer;

    public ServerMain() {
        puContainer = PuContainer.INSTANCE;
        puMonitor = new PuMonitor(puContainer);
    }

    private void run() {
        logger.log(Level.INFO, "Starting");
        puMonitor.start();
        logger.log(Level.INFO, "Started");
    }
}
