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
 * Created on Jan 4, 2009
 */

package com.bigdata.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.bigdata.jini.start.config.AbstractHostConstraint;

/**
 * Utility class for issuing the four letter commands to a zookeeper service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZooHelper {

    protected static final Logger log = Logger.getLogger(ZooHelper.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * Inquires whether a zookeeper instance is running in a non-error state and
     * returns iff the service reports "imok".
     * 
     * @param addr
     *            The address of the zookeeper instance.
     * @param clientPort
     *            The client port.
     *            
     * @throws IOException
     *             if there is any problem communicating with the service,
     *             including a timeout (the service is not responsive).
     */
    static public void ruok(final InetAddress addr, final int clientPort)
            throws IOException {

        if (INFO)
            log.info("Querying service: hostname=" + addr + ", port="
                    + clientPort);

        final Socket socket = new Socket(addr, clientPort);

        // The socket read timeout in milliseconds.
        final int timeout = 250;
        
        try {
    
            socket.setSoTimeout(timeout);
    
            final OutputStream os = socket.getOutputStream();
    
            os.write("ruok".getBytes("ASCII"));
    
            os.flush();
    
            final InputStream is = socket.getInputStream();
    
            final byte[] b = new byte[4];
    
            // read : will timeout if no response.
            is.read(b);
    
            if (INFO)
                log.info(new String(b, "ASCII"));
    
            return;
    
        } finally {
    
            socket.shutdownOutput();
    
            socket.shutdownInput();
    
        }
    
    }

    /**
     * Kills a local zookeeper instance responding at the specified client port.
     * 
     * @param clientPort
     *            The client port.
     * 
     * @throws IOException
     *             if there is any problem communicating with the service,
     *             including a timeout (the service is not responsive).
     */
    public static void kill(final int clientPort) throws UnknownHostException,
            IOException {

        if (INFO)
            log.info("Killing service: @ port=" + clientPort);

        final Socket socket = new Socket(InetAddress.getLocalHost(), clientPort);

        try {
    
            socket.setSoTimeout(100/* timeout(ms) */);
    
            OutputStream os = socket.getOutputStream();
    
            os.write("kill".getBytes("ASCII"));
    
            os.flush();
    
            if (INFO)
                log.info("Message sent");
    
            return;
    
        } finally {
    
            socket.shutdownOutput();
    
            socket.shutdownInput();
    
        }
    
    }

    /**
     * Sends a "stat" command and return the result.
     * 
     * @param addr
     *            The address of the zookeeper instance.
     * @param clientPort
     *            The client port for zookeeper.
     * 
     * @return The result.
     * 
     * @throws UnknownHostException
     *             if the hostname can not be resolved.
     * @throws IOException
     *             if there is any problem communicating with the service,
     *             including a timeout (the service is not responsive).
     */
    public static String stat(final InetAddress addr, final int clientPort)
            throws UnknownHostException, IOException {
    
        if (INFO)
            log.info("hostname=" + addr + ", port=" + clientPort);
    
        final Socket socket = new Socket(InetAddress.getLocalHost(), clientPort);
    
        try {
    
            socket.setSoTimeout(100/* timeout(ms) */);
    
            {
    
                final OutputStream os = socket.getOutputStream();
    
                os.write("stat".getBytes("ASCII"));
    
                os.flush();
    
                if (INFO)
                    log.info("Message sent");
    
            }
    
            {
    
                final InputStream is = socket.getInputStream();
    
                final StringBuilder sb = new StringBuilder();
    
                int ch;
    
                while ((ch = is.read()) != -1) {
    
                    sb.append((char) ch);
    
                }
    
                return sb.toString();
    
            }
    
        } finally {
    
            socket.shutdownOutput();
    
            socket.shutdownInput();
    
        }
    
    }

    /**
     * Sends a "dump" command and return the result.
     * 
     * @param addr
     *            The address of the zookeeper instance.
     * @param clientPort
     *            The client port for zookeeper.
     *            
     * @return The result.
     * 
     * @throws UnknownHostException
     *             if the hostname can not be resolved.
     * @throws IOException
     *             if there is any problem communicating with the service,
     *             including a timeout (the service is not responsive).
     */
    public static String dump(final InetAddress addr, final int clientPort)
            throws UnknownHostException, IOException {
    
        if (INFO)
            log.info("hostname=" + addr + ", port=" + clientPort);
    
        final Socket socket = new Socket(InetAddress.getLocalHost(), clientPort);
    
        try {
    
            socket.setSoTimeout(100/* timeout(ms) */);
    
            {
    
                final OutputStream os = socket.getOutputStream();
    
                os.write("dump".getBytes("ASCII"));
    
                os.flush();
    
                if (INFO)
                    log.info("Message sent");
    
            }
    
            {
    
                final InputStream is = socket.getInputStream();
    
                final StringBuilder sb = new StringBuilder();
    
                int ch;
    
                while ((ch = is.read()) != -1) {
    
                    sb.append((char) ch);
    
                }
    
                return sb.toString();
    
            }

        } finally {

            socket.shutdownOutput();

            socket.shutdownInput();

        }

    }

    /**
     * Return <code>true</code> if zookeeper is running on the specified host
     * at the client port.
     * 
     * @return <code>true</code> if zookeeper responds to an [ruok] request on
     *         that host and client port.
     */
    public static boolean isRunning(final InetAddress addr, final int clientPort) {

        try {

            ZooHelper.ruok(addr, clientPort);

            if (INFO)
                log.info("Zookeeper running: " + addr.getCanonicalHostName()
                        + ":" + clientPort);

            return true;

        } catch (IOException ex) {

            // ignore.
            if (INFO)
                log.info("Zookeeper not found: " + addr.getCanonicalHostName()
                        + ":" + clientPort);

            return false;

        }

    }
    
    /**
     * Destroys all znodes under the specified zpath and then the znode at the
     * specified zpath.
     * 
     * @param zookeeper
     * @param zpath
     *            The path to the root of the hierarchy to be destroyed.
     * @param depth
     *            The depth (initially zero).
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    static public void destroyZNodes(final ZooKeeper zookeeper,
            final String zpath, final int depth) throws KeeperException,
            InterruptedException {

        //        System.err.println("enter : " + zpath);

        final List<String> children;
        try {

            children = zookeeper.getChildren(zpath, false);
            
        } catch (NoNodeException ex) {
            
            // node is gone.
            return;
        }

        for (String child : children) {

            destroyZNodes(zookeeper, zpath + "/" + child, depth + 1);

        }

        if(INFO)
            log.info("delete: " + zpath);

        try {
            zookeeper.delete(zpath, -1/* version */);

        } catch(NoNodeException ex) {
        
            // node is gone.
            
        }
        
    }
    
    
    /**
     * Send four letter commands to zookeeper.
     * 
     * @param args
     *            host:clientPort command
     *            
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 2) {

            usage();
            
        }
        
        final String s = args[0];
        
        final int pos = s.indexOf(':');
        
        if(pos==-1) usage();
        
        final String host = s.substring(0, pos);
        
        final int clientPort = Integer.parseInt(s.substring(pos+1));
        
        final InetAddress addr = InetAddress.getByName(host);
        
        final String cmd = args[1];
        
        if (cmd.equals("ruok")) {
            ruok(addr, clientPort);
            // note: if not Ok then exception will be thrown.
            System.out.println("imok");
        } else if (cmd.equals("dump")) {
            System.out.println(dump(addr, clientPort));
        } else if (cmd.equals("stat")) {
            System.out.println(stat(addr, clientPort));
        } else if (cmd.equals("kill")) {
            if(!AbstractHostConstraint.isLocalHost(host)) {
                usage();
            }
            kill(clientPort);
        } else {
            usage();
        }

    }

    private static void usage() {

        System.err.println("usage: host:clientPort [ruok|dump|stat|kill]");

        System.err
                .println("       kill may only be used on the local host.");

        System.exit(1);

    }
    
}
