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
 * Created on Nov 1, 2013
 */
package com.bigdata.ha;

import java.util.ArrayList;

import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Response for a 2-phase commit.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CommitResponse {

    /**
     * The COMMIT message.
     */
    private final CommitRequest req;

    /**
     * An array of the root cause exceptions for any errors encountered when
     * instructing the services to execute the COMMIT message. The indices into
     * this collection are correlated with the service join order and the
     * PREPARE vote order. The leader is always at index zero.
     */
    private final ArrayList<Throwable> causes;
    
    /**
     * The number of COMMIT messages that are known to have been processed
     * successfully.
     */
    private final int nok;
    /**
     * The number of COMMIT messages that were issued and which failed.
     */
    private final int nfail;

    public CommitResponse(final CommitRequest req,
            final ArrayList<Throwable> causes) {

        this.req = req;
        this.causes = causes;

        int nok = 0, nfail = 0;

        for (Throwable t : causes) {

            if (t == null)
                nok++; // request issued and was Ok.
            else
                nfail++; // request issued and failed.

        }

        this.nok = nok;
        this.nfail = nfail;

    }

    public boolean isLeaderOk() {

        return causes.get(0) == null;

    }

    /**
     * Number of COMMIT messages that were generated and succeeded.
     */
    public int getNOk() {

        return nok;

    }

    /**
     * Number of COMMIT messages that were generated and failed.
     */
    public int getNFail() {

        return nfail;

    }

    /**
     * Return the root cause for the ith service -or- <code>null</code> if the
     * COMMIT did not produce an exception for that service.
     */
    public Throwable getCause(final int i) {

        return causes.get(i);

    }

    /**
     * Throw out the exception(s).
     * <p>
     * Note: This method is guaranteed to not return normally!
     * 
     * @throws Exception
     *             if one or more services that voted YES failed the COMMIT.
     * 
     * @throws IllegalStateException
     *             if all services that voted YES succeeded.
     */
    public void throwCauses() throws Exception {

        if (causes.isEmpty()) {

            // There were no errors.
            throw new IllegalStateException();

        }

        // Throw exception back to the leader.
        if (causes.size() == 1)
            throw new Exception(causes.get(0));

        final int k = req.getPrepareResponse().replicationFactor();

        throw new Exception("replicationFactor=" + k + ", nok=" + nok
                + ", nfail=" + nfail, new ExecutionExceptions(causes));

    }

}
