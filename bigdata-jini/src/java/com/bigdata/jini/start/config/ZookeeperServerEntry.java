/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Jan 2, 2009
 */

package com.bigdata.jini.start.config;

/**
 * A description of a zookeeper <code>server</code> entry as found in a
 * configuration file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperServerEntry {

    /**
     * The zookeeper service instance number (the value written in its
     * <code>myid</code> file).
     */
    public final int id;

    /**
     * The hostname for the server instance.
     */
    public final String hostname;

    /**
     * The first port (used for peer communications).
     */
    public final int peerPort;

    /**
     * The second port (used for leader elections).
     */
    public final int leaderPort;

    /**
     * Parses the value associated with a server entry.
     * 
     * @param id
     *            The zookeeper service instance number.
     * @param val
     *            The value as found in a zookeeper configuration file.
     */
    public ZookeeperServerEntry(final int id, final String val) {

        if (id < 0)
            throw new IllegalArgumentException();

        if (val == null)
            throw new IllegalArgumentException();

        this.id = id;

        // delimiter after the hostname.
        final int c1 = val.indexOf(":");

        // delimiter before the leaderPort.
        final int c2 = val.lastIndexOf(":");

        if (c1 == -1 || c2 == -1)
            throw new IllegalArgumentException();

        // the hostname.
        this.hostname = val.substring(0, c1);

        final String peerPort = val.substring(c1 + 1, c2);

        this.peerPort = Integer.parseInt(peerPort);

        final String leaderPort = val.substring(c2 + 1);

        this.leaderPort = Integer.parseInt(leaderPort);

    }

    /**
     * Return a representation of the property name that would be used for
     * this server.
     */
    public String getName() {
        
        return "server." + id;
        
    }
    
    /**
     * Return a representation of the property value that would be used for this
     * server.
     */
    public String getValue() {
        
        return hostname + ":" + peerPort + ":" + leaderPort;
        
    }
    
    /**
     * Returns a representation of the server property as
     * <code>name=value</code>.
     */
    public String toString() {
        
        return getName()+ "=" + getValue();
        
    }
    
}
