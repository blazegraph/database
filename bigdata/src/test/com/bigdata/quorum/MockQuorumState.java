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

/**
 * The shared state for a {@link MockQuorum}. This class provides a factory for
 * {@link MockQuorum} instances which track the shared quorum state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockQuorumState<S extends Remote, C extends QuorumClient<S>, Q extends AbstractQuorum<S, C>>
        extends AbstractQuorum<S, C> {

    /**
     * @param k
     */
    protected MockQuorumState(final int k) {
        super(k);
    }

    /**
     * Factory for a {@link Quorum} running for a client.
     * 
     * @param client
     *            The client.
     * 
     * @return
     */
    public Q newQuorum(final C client) {

        final Q quorum = (Q) new AbstractQuorum(replicationFactor()) {
        };

        quorum.start(client);

        return quorum;

    }

}
