/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
/*
 * Created on Oct 22, 2013
 */
package com.bigdata.quorum.zk;

import java.rmi.Remote;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.bigdata.quorum.QuorumClient;

public interface ZKQuorumClient<S extends Remote> extends QuorumClient<S> {

    /**
     * Return the {@link ZooKeeper} client connection.
     */
    ZooKeeper getZooKeeper();

    /**
     * Return the {@link ACL}s to be used.
     */
    List<ACL> getACL();

}
