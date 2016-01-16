/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

/**
 * Base class for {@link QuorumClient}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorumClient<S extends Remote> implements
        QuorumClient<S> {

    static protected final transient Logger log = Logger
            .getLogger(AbstractQuorumClient.class);

    private final AtomicReference<Quorum<?, ?>> quorum = new AtomicReference<Quorum<?, ?>>();

    private final String logicalServiceZPath;

    @Override
    final public String getLogicalServiceZPath() {
        
        return logicalServiceZPath;
        
    }
    
    /**
     * 
     * @param logicalServiceZPath
     *            the fully qualified logical service identifier (for zookeeper,
     *            this is the logicalServiceZPath).
     */
    protected AbstractQuorumClient(final String logicalServiceZPath) {

        if(logicalServiceZPath == null)
            throw new IllegalArgumentException();
        
        this.logicalServiceZPath = logicalServiceZPath;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * 
     * @return The reference from an atomic variable that will be cleared if the
     *         quorum terminates.
     */
    @Override
    public Quorum<?,?> getQuorum() {

        final Quorum<?,?> tmp = quorum.get();

        if (tmp == null)
            throw new IllegalStateException();

        return tmp;

    }

    @Override
    public void start(final Quorum<?,?> quorum) {

        if (quorum == null)
            throw new IllegalArgumentException();

        if (!this.quorum.compareAndSet(null/* expect */, quorum/* update */)) {

            throw new IllegalStateException();

        }
    
    }

    @Override
    public void terminate() {
        
        this.quorum.set(null);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation is a NOP.
     */
    @Override
    public void disconnected() {
        
    }

    @Override
    public S getLeader(final long token) {
        final Quorum<?,?> q = getQuorum();
        q.assertQuorum(token);
        final UUID leaderId = q.getLeaderId();
        if (leaderId == null) {
            q.assertQuorum(token);
            throw new AssertionError();
        }
        return getService(leaderId);
    }

    @Override
    abstract public S getService(UUID serviceId);

    @Override
    public void notify(QuorumEvent e) {

    }

}
