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
 * Created on Oct 16, 2008
 */

package com.bigdata.relation.rule.eval;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;

import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.JoinMasterTask.JoinStats;
import com.bigdata.relation.rule.eval.JoinMasterTask.DistributedJoinTask;

/**
 * Interface exported by the {@link JoinMasterTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJoinMaster extends Remote {
    
    /**
     * Return a unique identifier for the {@link JoinMasterTask} instance. This
     * is used to concentrate all {@link DistributedJoinTask} that target the
     * same tail predicate and index partition onto the same
     * {@link DistributedJoinTask} sink.
     * 
     * @return The unique identifer.
     */
    UUID getUUID() throws IOException;

    /**
     * A proxy for the buffer on which the last {@link DistributedJoinTask} must
     * write its <em>query</em> solutions. Note that mutation operations DO
     * NOT use this buffer in order to avoid sending all data through the
     * master.
     * 
     * @throws UnsupportedOperationException
     *             if the operation is not a query.
     */
    IBuffer<ISolution[]> getSolutionBuffer() throws IOException;
    
    /**
     * Used to send join stats to the master.
     */
    void report(JoinStats joinStats) throws IOException;

}
