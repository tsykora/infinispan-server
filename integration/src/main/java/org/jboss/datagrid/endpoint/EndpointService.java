/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.jboss.datagrid.endpoint;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.memcached.MemcachedServer;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.Main;
import org.infinispan.server.core.ProtocolServer;
import org.jboss.as.server.services.net.SocketBinding;
import org.jboss.datagrid.DataGridConstants;
import org.jboss.datagrid.SecurityActions;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;
import org.jboss.logging.Logger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

/**
 * The service that configures and starts the endpoints supported by data grid.
 *
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 */
class EndpointService implements Service<Map<String, ProtocolServer>> {

    private static final Logger log = Logger.getLogger(EndpointService.class);
    
    private static final String HOTROD = "hotrod";
    private static final String MEMCACHED = "memcached";
    
    private final InjectedValue<EmbeddedCacheManager> cacheManager = new InjectedValue<EmbeddedCacheManager>();
    
    private final ModelNode config;
    private final Map<String, InjectedValue<SocketBinding>> socketBindings = new HashMap<String, InjectedValue<SocketBinding>>();
    private final Map<String, Properties> connectorPropertiesMap = new LinkedHashMap<String, Properties>();
    private final Properties topologyStateTransferProperties = new Properties();
    
    private final Map<String, ProtocolServer> protocolServers = new LinkedHashMap<String, ProtocolServer>();
    
    EndpointService(ModelNode config) {
        this.config = config.clone();
    }
    
    @Override
    public synchronized void start(final StartContext context) throws StartException {
        assert connectorPropertiesMap.isEmpty();
        assert topologyStateTransferProperties.isEmpty();
        
        ClassLoader origTCCL = SecurityActions.getContextClassLoader();
        boolean done = false;
        try {
            loadConnectorProperties(config);
            loadTopologyStateTransferProperties(config);
            
            // There has to be at least one connector defined.
            if (connectorPropertiesMap.isEmpty()) {
                throw new StartException("no connector is defined in the endpoint subsystem");
            }
            
            // 'hotrod' and 'memcached' are the only supported protocols.
            Set<String> protocols = new LinkedHashSet<String>(connectorPropertiesMap.keySet());
            protocols.remove(HOTROD);
            protocols.remove(MEMCACHED);
            if (!protocols.isEmpty()) {
                throw new StartException("unknown connector protocol(s): " + protocols);
            }
            
            // Log translated properties for debugging purposes
            for (Map.Entry<String, Properties> entry: connectorPropertiesMap.entrySet()) {
                log.debugf("Connector properties for '%s': %s", entry.getKey(), entry.getValue());
            }
            log.debugf("Topology state transfer properties: %s", topologyStateTransferProperties);

            // Retrieve the current cache manager from the application server
            EmbeddedCacheManager cacheManager = getCacheManager().getValue();
            
            // Start Hot Rod server
            Properties hotrodProperties = connectorPropertiesMap.get(HOTROD);
            if (hotrodProperties != null) {
                hotrodProperties = new Properties(hotrodProperties);
                hotrodProperties.putAll(topologyStateTransferProperties);
                
                SecurityActions.setContextClassLoader(HotRodServer.class.getClassLoader());
                HotRodServer hotrod = new HotRodServer();
                log.infof("Starting connector: %s", HOTROD);
                hotrod.start(hotrodProperties, cacheManager);
                protocolServers.put(HOTROD, hotrod);
            }
            
            // Start memcached server
            Properties memcachedProperties = connectorPropertiesMap.get(MEMCACHED);
            if (memcachedProperties != null) {
                memcachedProperties = new Properties(memcachedProperties);
                
                SecurityActions.setContextClassLoader(MemcachedServer.class.getClassLoader());
                MemcachedServer memcached = new MemcachedServer();
                log.infof("Starting connector: %s", MEMCACHED);
                memcached.start(memcachedProperties, cacheManager);
                protocolServers.put(MEMCACHED, memcached);
            }
            done = true;
        } catch (Exception e) {
            throw new StartException("failed to start service", e);
        } finally {
            if (!done) {
                doStop();
            }

            SecurityActions.setContextClassLoader(origTCCL);
        }
    }

    @Override
    public synchronized void stop(final StopContext context) {
        doStop();
    }
    
    private void doStop() {
        protocolServers.clear();
        socketBindings.clear();
        connectorPropertiesMap.clear();
        topologyStateTransferProperties.clear();
    }

    @Override
    public synchronized Map<String, ProtocolServer> getValue() throws IllegalStateException {
        if (protocolServers.isEmpty()) {
            throw new IllegalStateException();
        }
        return Collections.unmodifiableMap(protocolServers);
    }
    
    InjectedValue<EmbeddedCacheManager> getCacheManager() {
        return cacheManager;
    }
    
    Set<String> getRequiredSocketBindingNames() {
        if (!config.hasDefined(DataGridConstants.CONNECTOR)) {
            return Collections.emptySet();
        }
        
        Set<String> socketBindings = new HashSet<String>();
        for (Property property: config.get(DataGridConstants.CONNECTOR).asPropertyList()) {
            ModelNode connector = property.getValue();
            if (connector.hasDefined(DataGridConstants.SOCKET_BINDING)) {
                socketBindings.add(connector.get(DataGridConstants.SOCKET_BINDING).asString());
            }
        }
        
        return socketBindings;
    }
    
    InjectedValue<SocketBinding> getSocketBinding(String socketBindingName) {
        InjectedValue<SocketBinding> socketBinding = socketBindings.get(socketBindingName);
        if (socketBinding == null) {
            socketBinding = new InjectedValue<SocketBinding>();
            socketBindings.put(socketBindingName, socketBinding);
        }
        return socketBinding;
    }

    private void loadConnectorProperties(ModelNode config) {
        if (!config.hasDefined(DataGridConstants.CONNECTOR)) {
            return;
        }
        
        for (Property property: config.get(DataGridConstants.CONNECTOR).asPropertyList()) {
            String protocol = property.getName();
            ModelNode connector = property.getValue();
            Properties connectorProperties = new Properties();
            connectorPropertiesMap.put(protocol, connectorProperties);
            
            if (connector.hasDefined(DataGridConstants.SOCKET_BINDING)) {
                SocketBinding socketBinding = getSocketBinding(
                        connector.get(DataGridConstants.SOCKET_BINDING).asString()).getValue();
                connectorProperties.setProperty(
                        Main.PROP_KEY_HOST(),
                        socketBinding.getAddress().getHostAddress());
                connectorProperties.setProperty(
                        Main.PROP_KEY_PORT(),
                        String.valueOf(socketBinding.getPort()));
            }
            if (connector.hasDefined(DataGridConstants.WORKER_THREADS)) {
                connectorProperties.setProperty(
                        Main.PROP_KEY_WORKER_THREADS(),
                        connector.get(DataGridConstants.WORKER_THREADS).asString());
            }
            if (connector.hasDefined(DataGridConstants.IDLE_TIMEOUT)) {
                connectorProperties.setProperty(
                        Main.PROP_KEY_IDLE_TIMEOUT(),
                        connector.get(DataGridConstants.IDLE_TIMEOUT).asString());
            }
            if (connector.hasDefined(DataGridConstants.TCP_NODELAY)) {
                connectorProperties.setProperty(
                        Main.PROP_KEY_TCP_NO_DELAY(),
                        connector.get(DataGridConstants.TCP_NODELAY).asString());
            }
            if (connector.hasDefined(DataGridConstants.SEND_BUFFER_SIZE)) {
                connectorProperties.setProperty(
                        Main.PROP_KEY_SEND_BUF_SIZE(),
                        connector.get(DataGridConstants.SEND_BUFFER_SIZE).asString());
            }
            if (connector.hasDefined(DataGridConstants.RECEIVE_BUFFER_SIZE)) {
                connectorProperties.setProperty(
                        Main.PROP_KEY_RECV_BUF_SIZE(),
                        connector.get(DataGridConstants.RECEIVE_BUFFER_SIZE).asString());
            }
        }
    }
    
    private void loadTopologyStateTransferProperties(ModelNode config) {
        if (!config.hasDefined(DataGridConstants.TOPOLOGY_STATE_TRANSFER)) {
            return;
        }
        
        config = config.get(DataGridConstants.TOPOLOGY_STATE_TRANSFER);
        if (config.hasDefined(DataGridConstants.LOCK_TIMEOUT)) {
            topologyStateTransferProperties.setProperty(
                    Main.PROP_KEY_TOPOLOGY_LOCK_TIMEOUT(),
                    config.get(DataGridConstants.LOCK_TIMEOUT).asString());
        }
        if (config.hasDefined(DataGridConstants.REPLICATION_TIMEOUT)) {
            topologyStateTransferProperties.setProperty(
                    Main.PROP_KEY_TOPOLOGY_REPL_TIMEOUT(),
                    config.get(DataGridConstants.REPLICATION_TIMEOUT).asString());
        }
        if (config.hasDefined(DataGridConstants.UPDATE_TIMEOUT)) {
            topologyStateTransferProperties.setProperty(
                    Main.PROP_KEY_TOPOLOGY_UPDATE_TIMEOUT(),
                    config.get(DataGridConstants.UPDATE_TIMEOUT).asString());
        }
        if (config.hasDefined(DataGridConstants.EXTERNAL_HOST)) {
            topologyStateTransferProperties.setProperty(
                    Main.PROP_KEY_PROXY_HOST(),
                    config.get(DataGridConstants.EXTERNAL_HOST).asString());
        }
        if (config.hasDefined(DataGridConstants.EXTERNAL_PORT)) {
            topologyStateTransferProperties.setProperty(
                    Main.PROP_KEY_PROXY_PORT(),
                    config.get(DataGridConstants.EXTERNAL_PORT).asString());
        }
        if (config.hasDefined(DataGridConstants.LAZY_RETRIEVAL)) {
            topologyStateTransferProperties.setProperty(
                    Main.PROP_KEY_TOPOLOGY_STATE_TRANSFER(),
                    Boolean.toString(!config.get(DataGridConstants.LAZY_RETRIEVAL).asBoolean(false)));
        }
    }
}
