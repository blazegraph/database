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
 * Created on Jul 10, 2008
 */

package com.bigdata.journal;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.MDC;

import com.bigdata.concurrent.NamedReadWriteLock;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.AbstractService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IServiceShutdown;

/**
 * A low-latency {@link IResourceLockService} with deadlock detection,
 * escalation from shared (read/write) to exclusive, and resource hierarchy
 * locking. This is used primarily to support distributed operations on
 * relations, such as create/destroy vs use and lock of the relation owner for
 * db create / destroy. However, the resource hierarchy is conceptual and locks
 * may be obtained that bear no relationship to actual indices or relations.
 * This can be convenient for coordinating distributed processes.
 * 
 * FIXME test suite for Journal, LDS, EDS, and JDS.
 * 
 * FIXME The current implementation uses the namespace as a token, not a
 * hierarchy.
 * <p>
 * The concept of the resource hierarchy described here can be realized by
 * storing lock "tokens" in a B+Tree with Unicode keys. Lock tokens have two
 * kinds: exclusive and shared. The token must contain an identifier for the
 * lock instance, which could be a {@link UUID}. A exclusive lock token would
 * containing only a single {@link UUID} while a shared lock token would contain
 * one for each process having a shared lock on the resource. A granted time
 * should also be associated with the lock instance stored in the BTree. In
 * addition, for a distributed system, we would like to know who owns the lock
 * (the host). This could also be handled simply as {@link IResourceLock}
 * objects.
 * <p>
 * Rather than storing N objects in the value under a key, we could also format
 * the B+Tree like a sparse row store (or use a sparse row store). The primary
 * key would then be [(schema)|namespace|UUID] : [lockToken]. Since the
 * {@link UUID} is moved out of the value, we only store the granted time and
 * host info in the value.
 * <p>
 * An unisolated key range scan corresponding to the resource hierarchy would be
 * required to detect conflicting locks and grant any compatible locks.
 * <p>
 * Shared locks are compatible and could be processed together, but there needs
 * to be a queue and locks can only be granted for a resource hierarchy in a
 * fair ordering for that queue. (The definition here is a bit iffy since a lock
 * request for "ab" and for "abc" would presumably be on different queues or
 * perhaps a single queue is scanned for all locks.)
 * <p>
 * When a lock is released, we can scan the set of compatible locks waiting in
 * the queue and then grant them in the same unisolated operation.
 * <p>
 * Note: Full transaction support will be 2PL, but a specialized case that
 * should not require deadlock detection since all locks will be declared by the
 * time we do the commit.
 * <p>
 * 
 * @todo use a lock refresh policy for lock leases (5 minute lease default). The
 *       application acquires and releases locks. on the client side, a lock
 *       counter is maintained and the lease for the lock is automatically
 *       renewed before the lease would expire if the counter is non-zero.
 *       <p>
 *       Add a method to force the release of the lock and its lease and use
 *       that to release all locks held by the client when (a) they become
 *       finalizable; and (b) when the {@link IBigdataFederation} is closed.
 *       Make the locks canonical so that any shared lock for the same resource
 *       is the same lock object and any exclusive lock for the same resource is
 *       the same lock object. (In particular, the relation cache maintained by
 *       the {@link DefaultResourceLocator} should not keep other processes from
 *       acquiring an exclusive resource lock if the application is done with
 *       its shared lock.) Perhaps locks older than N seconds need to be closed,
 *       where N might be equal to the 1.5 x of the lease renew period without
 *       an acquire().
 *       <p>
 *       Add the ability to invalidate all shared locks in order to obtain an
 *       exclusive lock, e.g., for emergency ops. Clients must be notified that
 *       their lock is no good (throw exception) the next time they try to renew
 *       their shared lock (including when acquire is a cache hit on a lock
 *       lease).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ResourceLockService extends AbstractService implements
        IResourceLockService, IServiceShutdown {

    /**
     * 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
    };
    
    /**
     * Table of locks. Locks are weakly held. DGC is therefore required when
     * locks are exported for RMI.
     * 
     * @todo this is not a namespace hierarchy, but only locks for a specified
     *       token. See notes above on support for a hierarchy but also consider
     *       ZooKeeper, which also allows data to be stored in the nodes.
     */
    private final NamedReadWriteLock<String/* namespace */> locks = new NamedReadWriteLock<String>();

    public ResourceLockService(final Properties properties) {
        
    }
    
    public IResourceLock acquireExclusiveLock(final String namespace) {

        setupLoggingContext();
        
        try {
        
            final Lock lock = locks.acquireWriteLock(namespace);

            final ResourceLock resourceLock = newLock(namespace,
                    true/* exclusive */, lock);
            
            return getProxy( resourceLock );
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public IResourceLock acquireExclusiveLock(final String namespace,
            final long timeout) throws InterruptedException {
    
        setupLoggingContext();

        try {

            final Lock lock = locks.acquireWriteLock(namespace, timeout,
                    TimeUnit.MILLISECONDS);

            final ResourceLock resourceLock = newLock(namespace,
                    true/* exclusive */, lock);

            return getProxy(resourceLock);

        } finally {
            
            clearLoggingContext();
            
        }

    }

    public IResourceLock acquireSharedLock(String namespace) {

        setupLoggingContext();
        
        try {
        
            final Lock lock = locks.acquireReadLock(namespace);

            final ResourceLock resourceLock = newLock(namespace,
                    false /* exclusive */, lock);

            return getProxy(resourceLock);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public IResourceLock acquireSharedLock(String namespace, long timeout)
            throws InterruptedException {
        
        setupLoggingContext();
        
        try {
        
            final Lock lock = locks.acquireReadLock(namespace, timeout,
                    TimeUnit.MILLISECONDS);

            final ResourceLock resourceLock = newLock(namespace,
                    false /* exclusive */, lock);

            return getProxy(resourceLock);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }
    
    /**
     * {@link ResourceLock} factory.
     * 
     * @param namespace
     *            The namespace on which the lock was acquired.
     * @param exclusive
     *            <code>true</code> iff this is an exclusive lock.
     * @param lock
     *            The {@link Lock}.
     * 
     * @return A new lock.
     */
    protected ResourceLock newLock(final String namespace,
            final boolean exclusive, Lock lock) {
        
        return new ResourceLock(namespace, exclusive, lock);
        
    }

    /**
     * Return a proxy for a lock. The default implementation simply returns the
     * lock itself. Distributed (RMI) based services must override this and
     * return a suitable proxy object.
     * 
     * @param lock
     *            The lock.
     * 
     * @return The proxy.
     */
    protected IResourceLock getProxy(ResourceLock lock) {
       
        if (lock == null)
            throw new IllegalArgumentException();
        
        return lock;
        
    }
    
    /**
     * Inner class for lock tokens (NOT serializable).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ResourceLock implements IResourceLock {
        
        protected final String namespace;
        protected final boolean exclusive;
        protected final Lock lock;

        /**
         * 
         * @param namespace
         *            The namespace.
         * @param exclusive
         *            <code>true</code> iff this is an exclusive lock.
         * @param lock
         *            The lock (must have been acquired by the caller).
         */
        protected ResourceLock(String namespace, boolean exclusive, final Lock lock) {

            if (namespace == null)
                throw new IllegalArgumentException();

            if (lock == null)
                throw new IllegalArgumentException();

            this.namespace = namespace;
            
            this.exclusive = exclusive;

            this.lock = lock;
            
        }
        
        public void unlock() {

            lock.unlock();

        }

    }

    /**
     * Returns {@link ILoadBalancerService}.
     */
    public Class getServiceIface() {
        
        return IResourceLockService.class;
        
    }

    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * The implementation adds the "serviceUUID" parameter to the {@link MDC}
     * for <i>this</i> service. The serviceUUID is, in general, assigned
     * asynchronously by the service registrar. Once the serviceUUID becomes
     * available it will be added to the {@link MDC}. This datum can be
     * injected into log messages using %X{serviceUUID} in your log4j pattern
     * layout.
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceUUID", serviceUUID.toString());

        } catch(Throwable t) {
            /*
             * Ignore.
             */
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceUUID");
        
    }

    @Override
    public synchronized ResourceLockService start() {

        return this;
        
    }
   
    public boolean isOpen() {
        
        return open;
        
    }
    private boolean open = true;

    synchronized public void shutdown() {
        
        if(!isOpen()) return;
        
        super.shutdown();
        
    }
    
    synchronized public void shutdownNow() {
        
        if(!isOpen()) return;
        
        super.shutdownNow();
        
    }
    
}
