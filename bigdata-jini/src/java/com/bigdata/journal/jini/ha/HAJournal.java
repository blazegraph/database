/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import net.jini.config.Configuration;
import net.jini.export.Exporter;
import net.jini.jeri.BasicILFactory;
import net.jini.jeri.BasicJeriExporter;
import net.jini.jeri.InvocationLayerFactory;
import net.jini.jeri.tcp.TcpServerEndpoint;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.Quorum;
import com.bigdata.service.proxy.ClientFuture;
import com.bigdata.service.proxy.RemoteFuture;
import com.bigdata.service.proxy.RemoteFutureImpl;
import com.bigdata.service.proxy.ThickFuture;
import com.bigdata.zookeeper.ZooKeeperAccessor;

/**
 * A {@link Journal} that that participates in a write replication pipeline. The
 * {@link HAJournal} is configured an River {@link Configuration}. The
 * configuration includes properties to configured the underlying
 * {@link Journal} and information about the {@link HAJournal}s that will
 * participate in the replication pattern.
 * <p>
 * All instances declared in the {@link Configuration} must be up, running, and
 * able to connect for any write operation to succeed. All instances must vote
 * to commit for an operation to commit. If any units fail (or timeout) then the
 * operation will abort. All instances are 100% synchronized at all commit
 * points. Read-only operations can be load balanced across the instances and
 * uncommitted data will never be visible to readers. Writes must be directed to
 * the first instance in the write replication pipeline. A read error on an
 * instance will internally failover to another instance in an attempt to read
 * from good data.
 * <p>
 * The write replication pipeline is statically configured. If an instance is
 * lost, then the configuration file must be changed, the change propagated to
 * all nodes, and the services "bounced" before writes can resume. Bouncing a
 * service only requires that the Journal is closed and reopened. Services do
 * not have to be "bounced" at the same time, and (possibly new) leader must be
 * "bounced" last to ensure that writes do not propagate until the write
 * pipeline is in a globally consistent order that excludes the down node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAJournal extends Journal {

    private static final Logger log = Logger.getLogger(HAJournal.class);

    public interface Options extends Journal.Options {
        
        /**
         * The address at which this journal exposes its write pipeline
         * interface.
         */
        String WRITE_PIPELINE_ADDR = HAJournal.class.getName()
                + ".writePipelineAddr";

    }
    
    private final InetSocketAddress writePipelineAddr;
    
    public HAJournal(final Properties properties) {

        this(properties, null);
        
    }

    public HAJournal(final Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        super(properties, quorum);

        /*
         * Note: We need this so pass it through to the HAGlue class below.
         * Otherwise this service does not know where to setup its write
         * replication pipeline listener.
         */
        
        writePipelineAddr = (InetSocketAddress) properties
                .get(Options.WRITE_PIPELINE_ADDR);

        if (writePipelineAddr == null)
            throw new RuntimeException(Options.WRITE_PIPELINE_ADDR
                    + " : required property not found.");

    }

    @Override
    protected HAGlue newHAGlue(final UUID serviceId) {

        return new HAGlueService(serviceId, writePipelineAddr);

    }

    @Override
    protected final void setQuorumToken(final long newValue) {
    
        super.setQuorumToken(newValue);
        
    }

    /**
     * Extended implementation supports RMI.
     */
    protected class HAGlueService extends BasicHA {

        protected HAGlueService(final UUID serviceId,
                final InetSocketAddress writePipelineAddr) {

            super(serviceId, writePipelineAddr);

        }

        /**
         * {@inheritDoc}
         * 
         * TODO This should actually bounce the zk connection. We need to pass
         * the {@link ZooKeeperAccessor} into the constructor for that.
         */
        @Override
        public Future<Void> bounceZookeeperConnection() {
            return super.bounceZookeeperConnection();
        }
        
        /**
         * Note: The invocation layer factory is reused for each exported proxy (but
         * the exporter itself is paired 1:1 with the exported proxy).
         * 
         * TODO Configuration for this stuff.
         */
        final private InvocationLayerFactory invocationLayerFactory = new BasicILFactory();
        
        /**
         * Return an {@link Exporter} for a single object that implements one or
         * more {@link Remote} interfaces.
         * <p>
         * Note: This uses TCP Server sockets.
         * <p>
         * Note: This uses [port := 0], which means a random port is assigned.
         * <p>
         * Note: The VM WILL NOT be kept alive by the exported proxy (keepAlive is
         * <code>false</code>).
         * 
         * @param enableDGC
         *            if distributed garbage collection should be used for the
         *            object to be exported.
         * 
         * @return The {@link Exporter}.
         */
        protected Exporter getExporter(final boolean enableDGC) {
            
            return new BasicJeriExporter(TcpServerEndpoint
                    .getInstance(0/* port */), invocationLayerFactory, enableDGC,
                    false/* keepAlive */);
            
        }

        /**
         * Note that {@link Future}s generated by
         * <code>java.util.concurrent</code> are NOT {@link Serializable}.
         * Futher note the proxy as generated by an {@link Exporter} MUST be
         * encapsulated so that the object returned to the caller can implement
         * {@link Future} without having to declare that the methods throw
         * {@link IOException} (for RMI).
         * 
         * @param future
         *            The future.
         * 
         * @return A proxy for that {@link Future} that masquerades any RMI
         *         exceptions.
         * 
         *         TODO All of these methods that return a {@link Future} are
         *         going to cause a problem with DGC. They should just do the
         *         work whenever possible, especially for methods that have high
         *         overhead. One workaround is to return a {@link ThickFuture}.
         */
        @Override
        protected <E> Future<E> getProxy(final Future<E> future) {

            /*
             * This was borrowed from a fix for a DGC thread leak on the
             * clustered database.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/433
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/437
             */
            return new ThickFuture(future);

//            /*
//             * Setup the Exporter for the Future.
//             * 
//             * Note: Distributed garbage collection is enabled since the proxied
//             * future CAN become locally weakly reachable sooner than the client can
//             * get() the result. Distributed garbage collection handles this for us
//             * and automatically unexports the proxied iterator once it is no longer
//             * strongly referenced by the client.
//             */
//            final Exporter exporter = getExporter(true/* enableDGC */);
//            
//            // wrap the future in a proxyable object.
//            final RemoteFuture<E> impl = new RemoteFutureImpl<E>(future);
//
//            /*
//             * Export the proxy.
//             */
//            final RemoteFuture<E> proxy;
//            try {
//
//                // export proxy.
//                proxy = (RemoteFuture<E>) exporter.export(impl);
//
//                if (log.isInfoEnabled()) {
//
//                    log.info("Exported proxy: proxy=" + proxy + "("
//                            + proxy.getClass() + ")");
//
//                }
//
//            } catch (ExportException ex) {
//
//                throw new RuntimeException("Export error: " + ex, ex);
//
//            }
//
//            // return proxy to caller.
//            return new ClientFuture<E>(proxy);

        }

    }

}
