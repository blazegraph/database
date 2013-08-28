/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.lookup.ServiceItem;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.ha.HAGlue;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.jini.ha.HAClient.HAConnection;
import com.bigdata.quorum.zk.QuorumServiceState;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.util.config.NicUtil;
import com.bigdata.zookeeper.ZooHelper;
import com.bigdata.zookeeper.ZooKeeperAccessor;

public class SimpleDiscovery {

	final HAClient client;

	/**
	 * Just needs the configuration file for the discovery service and the local
	 * zookeeper port
	 */
	public SimpleDiscovery(final String[] configFiles)
			throws ConfigurationException, IOException, InterruptedException {

		// wait for zk services to register!
		Thread.sleep(1000);

		client = new HAClient(configFiles);
	}
	
	public void shutdown() {
		client.disconnect(true);
	}


	public List<HAGlue> services(final String serviceRoot) throws IOException,
			ExecutionException, KeeperException, InterruptedException {
		
		final HAConnection cnxn = client.connect();

		List<HAGlue> ret = new ArrayList<HAGlue>();
		
		final ZooKeeper zk = cnxn.getZookeeperAccessor().getZookeeper();

		final List<String> data = zk.getChildren(
				serviceRoot + "/quorum/joined", null);

		// Now access serviceIDs so that we can use discovery to gain HAGlue
		// interface
		for (final String d : data) {
			final byte[] bytes = zk.getData(
					serviceRoot + "/quorum/joined/" + d, false, null);

			final QuorumServiceState qs = (QuorumServiceState) SerializerUtil
					.deserialize(bytes);

			ret.add(cnxn.getHAGlueService(qs
					.serviceUUID()));
		}

		return ret;

	}

}
