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
 * Created on Jun 13, 2010
 */
package com.bigdata.ha;

import cern.colt.Arrays;


/**
 * The 2-phase prepare outcome as coordinated by the leader.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PrepareResponse {

    private final int k;
    private final int nyes;
    private final boolean willCommit;
    private final boolean[] votes;

    /**
     * The replication factor for the quorum.
     */
    public int replicationFactor() {
        return k;
    }

    /**
     * Return the #of services that voted "YES".
     */
    public int getYesCount() {
        return nyes;
    }

    /**
     * Return <code>true</code> iff the transaction will commit based on the
     * responses to the prepare requests.
     */
    public boolean willCommit() {
        return willCommit;
    }

    public boolean getVote(final int index) {

        return votes[index];
        
    }
    
    /**
     * 
     * @param k
     *            The replication factor for the quorum.
     * @param nyes
     *            The #of YES votes.
     * @param willCommit
     *            <code>true</code> iff the transaction will commit based on the
     *            responses to the prepare requests.
     */
    public PrepareResponse(final int k, final int nyes,
            final boolean willCommit, final boolean[] votes) {

        if (k < 1)
            throw new IllegalArgumentException();

        if (nyes > k)
            throw new IllegalArgumentException();

        this.k = k;
        this.nyes = nyes;
        this.willCommit = willCommit;
        this.votes = votes;
        
    }

    @Override
    public String toString() {
     
        return super.toString() + "{k=" + k + ", nyes=" + nyes
                + ", willCommit=" + willCommit + ", votes="
                + Arrays.toString(votes) + "}";
        
    }

}
