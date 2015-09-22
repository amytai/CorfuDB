package org.corfudb.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ConfigMasterServer;
import org.corfudb.infrastructure.ICorfuDBServer;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Created by mwei on 8/26/15.
 */

@Slf4j
public class CorfuInfrastructureBuilder {

    List<ICorfuDBServer> serverList;
    List<ICorfuDBServer> runningServers;

    Map<String, Object> configMap;
    Map<String, Object> layoutMap;
    List<Map<String,Object>> segmentMap;

    int configMasterPort;

    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder()
    {
        serverList = new LinkedList<ICorfuDBServer>();
        runningServers = new LinkedList<ICorfuDBServer>();
        configMap = new HashMap<String, Object>();

        configMap.put("sequencers", new LinkedList<String>());
        configMap.put("configmasters", new LinkedList<String>());
        configMap.put("epoch", 0L);
        configMap.put("pagesize", 4096);

        segmentMap = new LinkedList<Map<String,Object>>();
        segmentMap.add(new HashMap<String, Object>());
        segmentMap.get(0).put("replication", "cdbcr");
        segmentMap.get(0).put("start", 0L);
        segmentMap.get(0).put("sealed", 0L);
        segmentMap.get(0).put("replicas", 0L);

        segmentMap.get(0).put("layers", new ArrayList<HashMap<String, Object>>());
        ((ArrayList<HashMap<String, Object>>)segmentMap.get(0).get("layers")).add(new HashMap<String, Object>());
        ((ArrayList<HashMap<String, Object>>)segmentMap.get(0).get("layers")).add(new HashMap<String, Object>());
        (((ArrayList<HashMap<String, Object>>)segmentMap.get(0).get("layers")).get(0)).put("nodes", new ArrayList<String>());
        (((ArrayList<HashMap<String, Object>>)segmentMap.get(0).get("layers")).get(1)).put("nodes", new ArrayList<String>());

        segmentMap.get(0).put("groups", new LinkedList<HashMap<String, Object>>());
        ((LinkedList<HashMap<String, Object>>)segmentMap.get(0).get("groups")).add(new HashMap<String, Object>());
        (((LinkedList<HashMap<String, Object>>)segmentMap.get(0).get("groups")).get(0)).put("nodes", new LinkedList<String>());

        layoutMap = new HashMap<String, Object>();
        layoutMap.put("segments", segmentMap);

        configMap.put("layout", layoutMap);

    }

    public CorfuInfrastructureBuilder setReplicationProtocol(String protocol) {
        segmentMap.get(0).put("replication", protocol);
        return this;
    }

    /**
     * Add a sequencer to this configuration at the specified port.
     * @param port      The port this sequencer will serve on.
     * @param sequencerType       The type of sequencer to instantiate.
     * @param clientProtocol    The type of protocol to advertise to clients.
     * @param baseParams        The parameters to initialize with, or null for none.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder addSequencer(int port, Class<? extends ICorfuDBServer> sequencerType, String clientProtocol, Map<String,Object> baseParams)
    {
        Constructor<? extends ICorfuDBServer> serverConstructor = sequencerType.getConstructor();
        ICorfuDBServer server = serverConstructor.newInstance();
        Map<String, Object> configuration = baseParams == null ? new HashMap<>() : baseParams;
        configuration.put("port", port);
        serverList.add(server.getInstance(configuration));
        ((LinkedList<String>)configMap.get("sequencers")).add(clientProtocol + "://localhost:" + port);
        return this;
    }

    /**
     * Add a logging unit to the specified chain
     * @param port      The port this logunit will server on.
     * @param chain     The chain that this logunit will be attached to.
     * @param loggingType       The type of logging unit to instantiate.
     * @param clientProtocol    The type of protocol to advertise to clients.
     * @param baseParams        The parameters to initialize with, or null for none.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder addLoggingUnit(int port, int chain, Class<? extends ICorfuDBServer> loggingType, String clientProtocol, Map<String,Object> baseParams)
    {
        Constructor<? extends ICorfuDBServer> serverConstructor = loggingType.getConstructor();
        ICorfuDBServer server = serverConstructor.newInstance();
        Map<String, Object> configuration = baseParams == null ? new HashMap<>() : baseParams;
        configuration.put("port", port);
        serverList.add(server.getInstance(configuration));
        for (int i = ((LinkedList<HashMap<String, Object>>)segmentMap.get(0).get("groups")).size(); i < chain; i++)
        {
            ((LinkedList<HashMap<String, Object>>)segmentMap.get(0).get("groups")).add(new HashMap<String, Object>());
            (((LinkedList<HashMap<String, Object>>)segmentMap.get(0).get("groups")).get(i)).put("nodes", new LinkedList<String>());
        }

        ((LinkedList<String>)(((LinkedList<HashMap<String, Object>>)segmentMap.get(0).get("groups")).get(chain)).get("nodes")).add(clientProtocol + "://localhost:" + port);
        return this;
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder addSALoggingUnit(int port, int layer, Class<? extends ICorfuDBServer> loggingType, String clientProtocol, Map<String,Object> baseParams)
    {
        Constructor<? extends ICorfuDBServer> serverConstructor = loggingType.getConstructor();
        ICorfuDBServer server = serverConstructor.newInstance();
        Map<String, Object> configuration = baseParams == null ? new HashMap<>() : baseParams;
        configuration.put("port", port);
        serverList.add(server.getInstance(configuration));
        ((ArrayList<String>)((ArrayList<HashMap<String, Object>>)(segmentMap.get(0).get("layers"))).get(layer).get("nodes")).add(clientProtocol + "://localhost:" + port);

        return this;
    }


    /**
     * Start the configuration by initializing the configmaster at the specified port and running each server.
     * @param configMasterPort     The port to run the configuration master on.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public CorfuInfrastructureBuilder start(int configMasterPort)
    {
        configMap.put("port", configMasterPort);
        ((LinkedList<String>)configMap.get("configmasters")).add("cdbcm://localhost:" + configMasterPort);
        log.info("Starting dynamically created infrastructure...");
        serverList.forEach(r -> {
            r.start();
            runningServers.add(r);
        });
        ConfigMasterServer cms = new ConfigMasterServer();
        ICorfuDBServer r = cms.getInstance(configMap);
        this.configMasterPort = configMasterPort;
        /* wait for all threads to start*/
        runningServers.forEach( th -> {
            if (!th.getThread().isAlive())
            {
                try {
                    Thread.sleep(1000); //don't want to hang, so just sleep 1s hope it'll come alive..
                } catch (InterruptedException ie) {}
                if (!th.getThread().isAlive())
                {
                    log.warn("Waited for 1s, but thread is still not alive!");
                }
            }
        });
        //again, wait for everything to settle...
        //TODO:: loop until everything is pingable...
        r.start();
        runningServers.add(r);
        Thread.sleep(1000);
        log.info("Dynamically created infrastruacture built and started...");
       return this;
    }

    /**
     * Factory class for getting an infrastructure builder.
     * @return  An infrastructure builder.
     */
    public static CorfuInfrastructureBuilder getBuilder()
    {
        return new CorfuInfrastructureBuilder();
    }

    /**
     * Get the configuration string for this dynamically generated instance
     * @return  A configuration string.
     */
    public String getConfigString()
    {
        return "http://localhost:" + configMasterPort + "/corfu";
    }
    /**
     * Shutdown servers and wait. 
     */
    public void shutdownAndWait()
    {
        log.info("Shutting down dynamically created infrastructure...");
        runningServers.forEach(ICorfuDBServer::close);
    }
}
