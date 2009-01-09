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
 * Created on Jan 9, 2009
 */

package com.bigdata.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DumpZookeeper {

    protected static final Logger log = Logger.getLogger(DumpZookeeper.class);

    final ZooKeeper z;
    
    /**
     * @param z
     */
    public DumpZookeeper(ZooKeeper z) {

        this.z = z;
        
    }

    /**
     * 
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException, KeeperException {

        int clientPort = 2181;

        System.err.println(ZooHelper.dump(InetAddress.getLocalHost(),
                clientPort));

        // if (args.length != 1) {
        //
        // System.err.println("hosts");
        //
        // System.exit(1);
        //
        // }
        //
        String hosts = "localhost:" + clientPort;

        // relative znode to start dump.
        String znode = "test-fed";

        boolean showData = true;
        
        ZooKeeper z = new ZooKeeper(hosts, 2000/* sessionTimeout */,
                new Watcher() {

                    public void process(WatchedEvent event) {

                        log.info(event);

                    }
                });

        try {
            
            new DumpZookeeper(z).dump(showData,"", znode, 0);
            
        } finally {

            z.close();

        }
        
    }

    /**
     * @throws InterruptedException
     * @throws KeeperException
     * 
     */
    private void dump(final boolean showData, final String path,
            final String znode, final int depth) throws KeeperException,
            InterruptedException {

        final String zpath = path + "/" + znode;

        final Stat stat = new Stat();
        
        final byte[] data = z.getData(zpath, false, stat);

        System.out.print(i(depth) + znode
                + (stat.getEphemeralOwner() != 0 ? " (Ephemeral)" : ""));
        
        if(showData) {

            String obj;
            if (data == null)
                obj = "<null>";
            else if (data.length == 0)
                obj = "<empty>";
            else {
                try {
                    obj = SerializerUtil.deserialize(data).toString();
                } catch (Throwable t) {
                    obj = Arrays.toString(data);
                }
            }

            System.out.print(" " + obj);

        }
        
        System.out.println();

        List<String> children = z.getChildren(zpath, false);

        for (String child : children) {

            dump(showData, zpath, child, depth + 1);
            
        }
        
    }

    private String i(int d) {
        
        return ws.substring(0, d * 2);
        
    }
    static String ws = "                                                                               ";
    
}
