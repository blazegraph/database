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
import com.bigdata.service.DataServiceCallable;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.TaskMaster.JobState;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZLockImpl;

/**
 * An abstract base class which may be used for client tasks run by the master
 * on one or more data services. This class contends for a {@link ZLock} based
 * on the assigned {@link #clientNum} and then invoked {@link #runWithZLock()}
 * if the {@link ZLock} if granted. If the lock is lost, it will continue to
 * contend for the lock and then run until finished.
 * <p>
 * Note: This implementation presumes that {@link #runWithZLock()} has some
 * means of understanding when it is done and can restart its work from where it
 * left off. One way to handle that is to write the state of the client into the
 * client's znode and to update that from time to time as the client makes
 * progress on its task. You can invoke {@link #setupClientState()} to do that.
 * <p>
 * Note: This class DOES NOT have to be submitted to an {@link IDataService} for
 * execution. Most client tasks will in fact run on {@link IClientService}s
 * rather than {@link IDataService}s. This class extends
 * {@link DataServiceCallable} for the convienence of subclasses which MAY
 * introduce a requirement to execute on an {@link IDataService}.
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
     * Return the jobstate.
     */
    public S getJobState() {
        
        return jobState;
        
    }
    
    /**
     * Return the index assigned to the client.
     * 
     * @return The client index.
     */
    public int getClientNum() {
        
        return clientNum;
        
    }

    /**
     * The zpath for this client (set once the client starts executing on the
     * target {@link IRemoteExecutor}). The data of this znode is the client's
     * state (if it saves its state in zookeeper).
     */
    private transient String clientZPath;

    /**
     * The ACL to use with zookeeper.
     */
    private transient List<ACL> acl;

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

    public void setFederation(final IBigdataFederation fed) {

        super.setFederation(fed);

        this.clientZPath = jobState.getClientZPath(getFederation(), clientNum);

        this.acl = getFederation().getZooConfig().acl;

    }

    public JiniFederation getFederation() {

        return (JiniFederation) super.getFederation();

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

        final V clientState = setupClientState();
        
        while (true) {

            zlock = ZLockImpl.getLock(getFederation().getZookeeper(), jobState
                    .getLockNodeZPath(getFederation(), clientNum), acl);

            zlock.lock();
            try {

                final U ret = runWithZLock(clientState);

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
     * Do work while holding the {@link ZLock}. The implementation SHOULD verify
     * from time to time that it in fact holds the {@link ZLock} using
     * {@link ZLock#isLockHeld()}.
     * 
     * @return The result.
     * 
     * @throws Exception
     * @throws KeeperException
     * @throws InterruptedException
     */
    abstract protected U runWithZLock(V clientState) throws Exception,
            KeeperException, InterruptedException;

    /**
     * The method invoked {@link #newClientState()} and attempts to create the
     * client's znode with the serialized state as its data and that state will
     * be returned to the caller. If there is an existing znode for the client,
     * then the data of the znode is de-serialized and returned by this method.
     * <p>
     * This method is invoked automatically from within {@link #call()} before
     * the client attempts to obtain the {@link ZLock} (the zlock is a child of
     * the client's znode).
     * <p>
     * You can update the client's state from time to time using
     * {@link #writeClientState(Serializable)}. If the client looses the
     * {@link ZLock}, it can read the client state from zookeeper using this
     * method and pick up processing more or less where it left off (depending
     * on when you last updated the client state in zookeeper).
     * 
     * @return The client's state.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * 
     * @see JobState#getClientZPath(JiniFederation, int)
     */
    @SuppressWarnings("unchecked")
    protected V setupClientState() throws InterruptedException, KeeperException {

        final ZooKeeper zookeeper = getFederation().getZookeeperAccessor()
                .getZookeeper();

        final String clientZPath = jobState.getClientZPath(getFederation(),
                clientNum);

        V clientState;
        try {

            clientState = newClientState();

            zookeeper.create(clientZPath,
                    SerializerUtil
                    .serialize(clientState), getFederation().getZooConfig().acl,
                    CreateMode.PERSISTENT);

            if (log.isInfoEnabled())
                log.info("Created: clientZPath=" + clientZPath + ", state="
                        + clientState);

        } catch (NodeExistsException ex) {

            clientState = (V) SerializerUtil.deserialize(zookeeper.getData(
                    clientZPath, false, new Stat()));

            if (log.isInfoEnabled())
                log.info("Existing: clientZPath=" + clientZPath + ", state="
                        + clientState);

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

//    /**
//     * This is a hack which resolves the {@link IRemoteExecutor} for the local
//     * service. It works by getting the {@link UUID} of the
//     * {@link IFederationDelegate} and then (attempting to) resolve the service
//     * for that {@link UUID}.
//     * 
//     * @throws RuntimeException
//     *             if something goes wrong.
//     * 
//     * @return FIXME {@link IFederationDelegate} should probably be able to
//     *         directly return the reference of the delegate, which is the
//     *         client or {@link IService}. It should probably have a generic
//     *         type for the type of the delegate, but that information is also
//     *         available via {@link IFederationDelegate#getServiceIface()}.
//     */
//    protected IRemoteExecutor getLocalRemoteExecutor() {
//
//        // UUID of the local service.
//        final UUID serviceUUID = getFederation().getServiceUUID();
//
//        ServiceItem serviceItem;
//
//        serviceItem = getFederation().getClientServiceClient().getServiceItem(
//                serviceUUID);
//
//        if (serviceItem == null) {
//
//            serviceItem = getFederation().getDataServicesClient()
//                    .getServiceItem(serviceUUID);
//
//        }
//
//        if (serviceItem == null) {
//
//            try {
//                serviceItem = getFederation().getServiceDiscoveryManager()
//                        .lookup(
//                                new ServiceTemplate(JiniUtil
//                                        .uuid2ServiceID(serviceUUID),
//                                        new Class[] { IRemoteExecutor.class },
//                                        null/* attribs */), null/* filter */,
//                                1000/* timeoutMillis */);
//                
//            } catch (RemoteException e) {
//
//                throw new RuntimeException(e);
//
//            } catch (InterruptedException e) {
//
//                throw new RuntimeException(e);
//                
//            }
//
//        }
//        
//        if (serviceItem == null) {
//
//            throw new RuntimeException("Could not resolve local "
//                    + IRemoteExecutor.class);
//            
//        }
//
//        return (IRemoteExecutor) serviceItem.service;
//        
//    }

}
