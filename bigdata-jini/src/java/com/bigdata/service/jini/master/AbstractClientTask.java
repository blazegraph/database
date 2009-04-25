package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.DataService;
import com.bigdata.service.DataServiceCallable;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.TaskMaster.JobState;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZLockImpl;

/**
 * An abstract base class which may be used for client tasks run by the
 * master on one or more data services. This class contends for a
 * {@link ZLock} based on the assigned {@link #clientNum} and then invoked
 * {@link #runWithZLock()} if the {@link ZLock} if granted. If the lock is
 * lost, it will continue to contend for the lock and then run until
 * finished.
 * <p>
 * Note: This implementation presumes that {@link #runWithZLock()} has some
 * means of understanding when it is done and can restart its work from
 * where it left off. One way to handle that is to write the state of the
 * client into the client's znode and to update that from time to time as
 * the client makes progress on its task.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 *            The generic for the {@link JobState}.
 * @param <U>
 *            The generic for the {@link Callable}'s return type.
 * @param <V>
 *            The generic type of the client state (stored in zookeeper).
 */
abstract public class AbstractClientTask<S extends TaskMaster.JobState, U, V extends Serializable>
        extends DataServiceCallable<U> implements Callable<U> {

    final protected static Logger log = Logger
            .getLogger(AbstractClientTask.class);

    protected final S jobState;

    protected final int clientNum;

    /**
     * The zpath for this client (set once the client starts executing on
     * the target {@link DataService}). The data of this znode is the
     * client's state (if it saves its state in zookeeper).
     */
    protected transient String clientZPath;

    protected transient List<ACL> acl;

    /**
     * The zpath for the {@link ZLock} node. Only the instance of this task
     * holding the {@link ZLock} is allowed to run. This makes it safe to
     * run multiple instances of this task for the same {@link #clientNum}.
     * If one instance dies, the instance that gains the {@link ZLock} will
     * read the client's state from zookeeper and continue processing.
     * <p>
     * This is <code>null</code> until {@link #call()}.
     */
    protected transient ZLockImpl zlock;

    public void setDataService(final DataService dataService) {

        super.setDataService(dataService);

        this.clientZPath = jobState.getClientZPath(getFederation(), clientNum);

        this.acl = getFederation().getZooConfig().acl;

    }

    public JiniFederation getFederation() {

        return (JiniFederation)super.getFederation();

    }

    public String toString() {

        return getClass().getName() + "{clientNum=" + clientNum + "}";

    }

    protected AbstractClientTask(final S jobState, final int clientNum) {

        this.jobState = jobState;

        this.clientNum = clientNum;

    }

    /**
     * Runs the generator.
     */
    public U call() throws Exception {

        if (log.isInfoEnabled())
            log.info("Running: client#=" + clientNum + ", " + jobState);

        while (true) {

            zlock = ZLockImpl.getLock(getFederation().getZookeeper(), jobState
                    .getLockNodeZPath(getFederation(), clientNum), acl);

            zlock.lock();
            try {

                final U ret = runWithZLock();

                if (log.isInfoEnabled())
                    log.info("Finished: client#=" + clientNum + ", "
                            + jobState);

                return ret;

            } catch (SessionExpiredException ex) {

                /*
                 * Log warning and then try to re-obtain the zlock so we can
                 * finish the job.
                 */

                log.warn(this + " : will seek zlock again", ex);

                continue;

            } finally {

                zlock.unlock();

            }

        }

    }

    /**
     * Do work while holding the {@link ZLock}. The implementation SHOULD
     * verify from time to time that it in fact holds the {@link ZLock}
     * using {@link ZLock#isLockHeld()}.
     * 
     * @return The result.
     * 
     * @throws Exception
     * @throws KeeperException
     * @throws InterruptedException
     */
    abstract protected U runWithZLock() throws Exception, KeeperException,
            InterruptedException;

    /**
     * Method should be invoked from within {@link #runWithZLock()} if the
     * client wishes to store state in zookeeper. If there is no state in
     * zookeeper, then {@link #newClientState()} is invoked and the result
     * will be stored in zookeeper. You can update the client's state from
     * time to time using #writeClientState(). If the client looses the
     * {@link ZLock}, it can read the client state from zookeeper using
     * this method and pick up processing more or less where it left off
     * (depending on when you last updated the client state in zookeeper).
     * 
     * @return
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected V setupClientState() throws InterruptedException,
            KeeperException {

        if (!zlock.isLockHeld())
            throw new InterruptedException("Lost ZLock");

        final ZooKeeper zookeeper = zlock.getZooKeeper();

        final String clientZPath = jobState.getClientZPath(getFederation(), clientNum);

        V clientState;
        try {

            clientState = newClientState();

            zookeeper.create(clientZPath, SerializerUtil
                    .serialize(clientState), getFederation().getZooConfig().acl,
                    CreateMode.PERSISTENT);

            if (log.isInfoEnabled())
                log.info("Running: " + clientState);

        } catch (NodeExistsException ex) {

            clientState = (V) SerializerUtil.deserialize(zookeeper.getData(
                    clientZPath, false, new Stat()));

            if (log.isInfoEnabled())
                log.info("Client will restart: " + clientState);

        }

        return clientState;
        
    }

    /**
     * Method updates the client state in zookeeper. The caller MUST be
     * holding the {@link ZLock} (this is verified).
     * 
     * @param clientState
     *            The state to be written into the znode identified by
     *            {@link #clientZPath}.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected void writeClientState(final V clientState)
            throws KeeperException, InterruptedException {

        if (clientState == null)
            throw new IllegalArgumentException();
        
        if (!zlock.isLockHeld())
            throw new InterruptedException("Lost ZLock");

        final ZooKeeper zookeeper = zlock.getZooKeeper();

        try {
            /*
             * Update the client state.
             */
            zookeeper.setData(clientZPath, SerializerUtil
                    .serialize(clientState), -1/* version */);
        } catch (ConnectionLossException ex) {
            /*
             * Note: There are a variety of transient errors which can
             * occur. Next time we are connected and and this method is
             * invoked we will update the client state, and that should be
             * good enough.
             */
            log.warn(ex);
        }

    }
    
    /**
     * Return a new instance of the client's state.
     */
    abstract protected V newClientState();

}