/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Tests for the HotRod client RemoteCache class
 *
 * @author Martin Gencur
 *
 */
@RunWith(Arquillian.class)
public class HotRodRemoteCacheTest extends AbstractRemoteCacheTest {

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @InfinispanResource("container2")
   RemoteInfinispanServer server2;   //when run in LOCAL mode - inject here the same container as container1

   @Override
   protected List<RemoteInfinispanServer> getServers() {
      List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
      servers.add(server1);
      if (!AbstractRemoteCacheManagerTest.isLocalMode()) {
         servers.add(server2);
      }
      return Collections.unmodifiableList(servers);
   }
}