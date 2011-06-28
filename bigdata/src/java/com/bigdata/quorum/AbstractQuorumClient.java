/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

    private final String logicalServiceId;

    public String getLogicalServiceId() {
        
        return logicalServiceId;
        
    }
    
    protected AbstractQuorumClient(final String logicalServiceId) {

        if(logicalServiceId == null)
            throw new IllegalArgumentException();
        
        this.logicalServiceId = logicalServiceId;
        
    }

    public Quorum<?,?> getQuorum() {

        final Quorum<?,?> tmp = quorum.get();

        if (tmp == null)
            throw new IllegalStateException();

        return tmp;

    }

    public void start(final Quorum<?,?> quorum) {

        if (quorum == null)
            throw new IllegalArgumentException();

        if (!this.quorum.compareAndSet(null/* expect */, quorum/* update */)) {

            throw new IllegalStateException();

        }
    
    }

    public void terminate() {
        
        this.quorum.set(null);
        
    }
    
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

    abstract public S getService(UUID serviceId);

    public void notify(QuorumEvent e) {
    }

}
