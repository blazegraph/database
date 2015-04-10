package com.bigdata.zookeeper;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.rawstore.Bytes;

/**
 * Double-barrier using zookeeper.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo this is actually a double-barrier, which we don't need right now. Do a
 *       simpler single count-down barrier.
 *       <p>
 *       The barrier breaks (by deleting the barrier node) when the #of
 *       ephemeral sequential children (representing processes) reaches the size
 *       specified when the barrier node was created. Each process that blocks
 *       break need only set a watch on the znode for the barrier itself. When
 *       that node is deleted, the blocked processes are released.
 *       <p>
 *       The process that satisifies the barrier has an opportunity to execute a
 *       {@link Runnable} target before it deletes the barrier node.
 * 
 * @todo The ctor for the ZooBarrier object needs to be separated from the
 *       creation of the barrier node since the object needs to be instantiated
 *       on distributed JVMs. It is less a "barrier" class than a barrier
 *       manager class which realizes a barrier pattern.
 */
public class ZooBarrier extends AbstractZooPrimitive {

    protected static final Logger log = Logger.getLogger(ZooBarrier.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    public final String zroot;
    
    public final int size;
    
    /**
     * Barrier constructor
     * 
     * @param zookeeper
     * @param zroot
     *            The znode for the barrier.
     * @param acl
     *            The ACL to be used for the barrier znode.
     * @param size
     *            The size of the barrier.
     */
    public ZooBarrier(final ZooKeeper zookeeper, final String zroot,
            final List<ACL> acl, final int size) throws KeeperException,
            InterruptedException {

        super(zookeeper);

        if (zroot == null)
            throw new IllegalArgumentException();
        
        if (size <= 0)
            throw new IllegalArgumentException();
        
        this.zroot = zroot;

        this.size = size;

        try {

            final ByteBuffer b = ByteBuffer.allocate(Bytes.SIZEOF_INT);

            b.putInt(0, size);

            b.flip();

            // Create barrier node
            zookeeper.create(zroot, b.array(), acl, CreateMode.PERSISTENT);

            if (INFO)
                log.info("New barrier: " + zroot);

        } catch (KeeperException.NodeExistsException ex) {

            if (INFO)
                log.info("Existing barrier: "+zroot);

            final ByteBuffer b = ByteBuffer.wrap(zookeeper.getData(zroot,
                    false/* watch */, new Stat()));

            final int actualSize = b.getInt(0);

            if (size != actualSize) {

                throw new IllegalArgumentException("Expected size=" + size
                        + ", actual=" + actualSize);

            }

        }
        
    }
    
    /**
     * Join barrier
     * 
     * @param id
     *            The process identifier.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void enter(final String id) throws KeeperException,
            InterruptedException {

        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("node=" + zroot + ", id=" + id);
        
        zookeeper.create(zroot + "/" + id, new byte[0],
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        while (true) {

            final List<String> list = zookeeper
                    .getChildren(zroot, this/* watcher */);

            if (list.size() < size) {

                synchronized (lock) {

                    lock.wait();

                }

                continue;

            }

            return;

        }

    }

    /**
     * Wait until all reach barrier
     * 
     * @param id
     *            The process identifier.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void leave(final String id) throws KeeperException,
            InterruptedException {

        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("node=" + zroot + ", id=" + id);
        
        zookeeper.delete(zroot + "/" + id, 0);

        while (true) {

            final List<String> list = zookeeper
                    .getChildren(zroot, this/* watcher */);

            if (list.size() == 0) {

                return;

            }

            synchronized (lock) {

                lock.wait();

            }

        }

    }

}
