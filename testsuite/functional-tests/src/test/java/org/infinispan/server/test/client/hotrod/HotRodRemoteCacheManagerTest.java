package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for the HotRod client RemoteCacheManager class
 * 
 * @author Martin Gencur
 * @author Vitalii Chepeliuk
 *
 */
@RunWith(Arquillian.class)
public class HotRodRemoteCacheManagerTest extends AbstractRemoteCacheManagerTest {

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @InfinispanResource("container2")
   RemoteInfinispanServer server2;    //when run in LOCAL mode - inject here the same container as container1

   @Override
   protected List<RemoteInfinispanServer> getServers() {
      List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
      servers.add(server1);
      if (!isLocalMode()) {
         servers.add(server2);
      }
      return Collections.unmodifiableList(servers);
   }
}
