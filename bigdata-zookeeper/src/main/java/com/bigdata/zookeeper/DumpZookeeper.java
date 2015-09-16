/*

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
 * Created on Jan 9, 2009
 */

package com.bigdata.zookeeper;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.zookeeper.start.config.ZookeeperClientConfig;

/**
 * Utility for dumping out the portion of a zookeeper ensemble state pertaining
 * to a bigdata federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DumpZookeeper {

    private static final Logger log = Logger.getLogger(DumpZookeeper.class);

    final ZooKeeper z;
    
    /**
     * @param z
     */
    public DumpZookeeper(final ZooKeeper z) {

        this.z = z;
        
    }

    /**
     * Dumps the zookeeper znodes for the bigdata federation.
     * 
     * @param args
     *            A {@link Configuration} and optional overrides.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     * @throws ConfigurationException 
     * 
     * TODO Add a listener mode (tail zk events).
     */
    public static void main(final String[] args) throws IOException,
            InterruptedException, KeeperException, ConfigurationException {

        final Configuration config = ConfigurationProvider.getInstance(args); 
        
        final ZookeeperClientConfig zooClientConfig = new ZookeeperClientConfig(config); 

        /*
         * Note: You may temporarily uncomment this to set the zookeeper
         * configuration via -D parameters.
         */
//        final ZookeeperClientConfig zooClientConfig = new ZookeeperClientConfig();
        
        System.out.println(zooClientConfig.toString());
        
//        System.err.println(ZooHelper.dump(InetAddress.getLocalHost(),
//                clientPort));

        final boolean showData = true;
        
        final ZooKeeper z = new ZooKeeper(zooClientConfig.servers,
                2000/* sessionTimeout */, new Watcher() {

                    public void process(WatchedEvent event) {

                        log.info(event);

                    }
                });

        /*
         * The sessionTimeout as negotiated (effective sessionTimeout).
         * 
         * Note: This is not available until we actually request something
         * from zookeeper. 
         */
        {

            try {
                z.getData(zooClientConfig.zroot, false/* watch */, null/* stat */);
            } catch (NoNodeException ex) {
                // Ignore.
            } catch (KeeperException ex) {
                // Oops.
                log.error(ex, ex);
            }

            System.out.println("Negotiated sessionTimeout="
                    + z.getSessionTimeout() + "ms");
        }

        final PrintWriter w = new PrintWriter(System.out);
        try {

            // recursive dump.
            new DumpZookeeper(z)
                    .dump(w, showData, zooClientConfig.zroot, 0/* depth */);

            w.println("----");

            w.flush();

        } finally {

            z.close();
            w.close();

        }
        
    }

    /**
     * Recursively dumps some or all of the zookeeper hierarchy.
     * 
     * @param w
     *            Where to write the dump.
     * @param showData
     *            <code>true</code> to show the deserialized data.
     * @param zpath
     *            The zpath at which the dump will begin.
     * @param depth
     *            The level of the dump (controls indenting and should be zero
     *            for the top-level invocation of this method).
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void dump(final PrintWriter w, final boolean showData,
            final String zpath, final int depth) throws KeeperException,
            InterruptedException {

        final Stat stat = new Stat();

        final byte[] data;
        try {

            data = z.getData(zpath, false, stat);
            
        } catch (NoNodeException ex) {
            
            w.println("Not found: [" + zpath + "]");
            
            return;
            
        }

        // the current znode (last path component).
        final String znode = zpath.substring(zpath.lastIndexOf('/') + 1);
        
        final List<String> children;
        try {

            // Get children as an Array.
            final String[] a = z
                    .getChildren(zpath, false/* watch */).toArray(
                            new String[0]);

            // sort the array.
            Arrays.sort(a);

            // wrap as list again.
            children = Arrays.asList(a);

        } catch (NoNodeException ex) {

            w.println("Not found: [" + zpath + "]");
            
            return;

        }

        w.print(i(depth)
                + znode
                + (children.isEmpty()?"":"("+children.size()+" children)")
                + (stat.getEphemeralOwner() != 0 ? " (Ephemeral"
                        + (showData ? "" + stat.getEphemeralOwner() : "") + ")"
                        : "") + " ");

        {
            String obj;
            if (data == null)
                obj = "(null)";
            else if (data.length == 0)
                obj = "(empty)";
            else {
                try {
                    final Object x = SerializerUtil.deserialize(data);
                    if (showData) {
                        if (x.getClass().getComponentType() != null) {
                            obj = Arrays.toString((Object[]) x);
                        } else {
                            obj = x.toString();
                        }
                    } else {
                        obj = "{"+x.getClass().getSimpleName()+"}";
                    }
                } catch (Throwable t2) {
                    if (showData) {
                        obj = Arrays.toString(data);
                    } else {
                        obj = "bytes[" + data.length + "]";
                    }
                }
                w.print(obj);

            }
        }
            
        w.println();

        for (String child : children) {

            dump(w,showData, zpath + "/" + child, depth + 1);

        }
        
    }

    private String i(int d) {
        
        return ws.substring(0, d * 2);
        
    }
    static String ws = "                                                                               ";
    
}
