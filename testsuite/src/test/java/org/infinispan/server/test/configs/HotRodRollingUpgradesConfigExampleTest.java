package org.infinispan.server.test.configs;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Comments inline. Test for Rolling Upgrades functionality: https://community.jboss.org/wiki/DesignForRollingUpgradesInInfinispan
 * + https://community.jboss.org/wiki/RollingUpgradesForRemoteClientsUsingHotRod
 * <p/>
 * This is basic Rolling Upgrades test which tests only 1 node to 1 node, both nodes of the same version.
 * <p/>
 * target node = node1 (standalone-hotrod-rolling-upgrade.xml)
 * source node = node2 (standalone.xml)
 * <p/>
 * <p/>
 * TEST CASE:
 * Load source node by entries, redirect clients to target node (using cache2 on node2).
 * After redirecting clients (= after stop using cache1 and start using only cache2) issue well known key dump (on source
 * node, node2) and synchronization (on target node, node1). After synchronization disconnect source node (which became
 * RemoteCacheStore for target node) - now we should be able to operate with target node only and be able to access all keys/values.
 * Try to access source mode (RCS) which should be disabled and unreachable.
 *
 * @author <a href="mailto:tsykora@redhat.com">Tomas Sykora</a>
 */
@RunWith(Arquillian.class)
public class HotRodRollingUpgradesConfigExampleTest {

    // Target node
    final int managementPortServer1 = 9999;
    MBeanServerConnectionProvider provider1;
    private final String CONTAINER1 = "container1";

    // Source node
    final int managementPortServer2 = 10099;
    MBeanServerConnectionProvider provider2;
    private final String CONTAINER2 = "container2";

    private final String CACHE_MANAGER = "local";
    private final String DEFAULT_CACHE = "default";

    @InfinispanResource(CONTAINER1)
    RemoteInfinispanServer server1;

    @InfinispanResource(CONTAINER2)
    RemoteInfinispanServer server2;

    @ArquillianResource
    ContainerController controller;

    RemoteCacheManager rcm1;
    RemoteCacheManager rcm2;

    @Test
    public void testHotRodRollingUpgrades() throws Exception {

        controller.start(CONTAINER2);
        Configuration conf2 = createTestBuilder11322().build();
        rcm2 = new RemoteCacheManager(conf2);

        final RemoteCache<String, String> cache2 = rcm2.getCache(DEFAULT_CACHE);

        cache2.put("key1", "value1");
        cache2.put("key2", "value2");
        cache2.put("key3", "value3");
        cache2.put("key4", "value4");
        cache2.put("key5", "value5");
        assertEquals("value1", cache2.get("key1"));
        assertEquals("value2", cache2.get("key2"));
        assertEquals("value3", cache2.get("key3"));
        assertEquals("value4", cache2.get("key4"));
        assertEquals("value5", cache2.get("key5"));

        Thread loadThread = new Thread() {
            public void run() {
                int i = 0;
                while (i < 50) {
                    cache2.put("keyLoad" + i, "valueLoad" + i);
                    i++;
                }
            }
        };
        loadThread.start();

        controller.start(CONTAINER1);

        Configuration conf1 = createTestBuilder11222().build();
        rcm1 = new RemoteCacheManager(conf1);

        RemoteCache<String, String> cache1 = rcm1.getCache(DEFAULT_CACHE);

        assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value1", cache1.get("key1"));
        assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value3", cache1.get("key3"));
        assertEquals("Can't access etries stored in source node (target's RemoteCacheStore).", "value5", cache1.get("key5"));

        provider1 = new MBeanServerConnectionProvider(server1.getHotrodEndpoint().getInetAddress().getHostName(), managementPortServer1);
        provider2 = new MBeanServerConnectionProvider(server2.getHotrodEndpoint().getInetAddress().getHostName(), managementPortServer2);

        final ObjectName rollMan = new ObjectName("jboss.infinispan:type=Cache," +
                "name=\"default(local)\"," +
                "manager=\"" + CACHE_MANAGER + "\"," +
                "component=RollingUpgradeManager");

        invokeOperation(provider2, rollMan.toString(), "recordKnownGlobalKeyset", new Object[]{}, new String[]{});

        invokeOperation(provider1, rollMan.toString(), "synchronizeData",
                new Object[]{"hotrod"},
                new String[]{"java.lang.String"});

        invokeOperation(provider1, rollMan.toString(), "disconnectSource",
                new Object[]{"hotrod"},
                new String[]{"java.lang.String"});

        // is source (RemoteCacheStore) really disconnected?
        cache2.put("disconnected", "source");
        assertEquals("Can't obtain value from cache1 (source node).", "source", cache2.get("disconnected"));
        assertNull("Source node entries should NOT be accessible from target node (after RCS disconnection)",
                cache1.get("disconnected"));

        // all entries migrated?
        assertEquals("Entry was not successfully migrated.", "value1", cache1.get("key1"));
        assertEquals("Entry was not successfully migrated.", "value2", cache1.get("key2"));
        assertEquals("Entry was not successfully migrated.", "value3", cache1.get("key3"));
        assertEquals("Entry was not successfully migrated.", "value4", cache1.get("key4"));
        assertEquals("Entry was not successfully migrated.", "value5", cache1.get("key5"));
        int i = 0;
        while (i < 50) {
            assertEquals("Entry was not successfully migrated.", "valueLoad" + i, cache1.get("keyLoad" + i));
            i++;
        }

        controller.stop(CONTAINER1);
        controller.stop(CONTAINER2);
    }

    private Object invokeOperation(MBeanServerConnectionProvider provider, String mbean, String operationName, Object[] params,
                                   String[] signature) throws Exception {
        return provider.getConnection().invoke(new ObjectName(mbean), operationName, params, signature);
    }

    private ConfigurationBuilder createTestBuilder11222() {
        ConfigurationBuilder result = new ConfigurationBuilder();
        result.addServers("localhost:11222");
        return result;
    }

    private ConfigurationBuilder createTestBuilder11322() {
        ConfigurationBuilder result = new ConfigurationBuilder();
        result.addServers("localhost:11322");
        return result;
    }
}