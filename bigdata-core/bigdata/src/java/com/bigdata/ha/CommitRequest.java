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

/**
 * Commit request for a 2-phase commit as coodinated by the leader (local
 * object).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CommitRequest {

    private final PrepareRequest prepareRequest;
    private final PrepareResponse prepareResponse;

    /**
     * The request used for the PREPARE.
     */
    public PrepareRequest getPrepareRequest() {
        return prepareRequest;
    }

    /**
     * The response for the PREPARE. This indicates which services voted to
     * commit and which did not.
     */
    public PrepareResponse getPrepareResponse() {
        return prepareResponse;
    }

    /**
     * 
     * @param prepareRequest
     *            The request used for the PREPARE.
     * @param prepareResponse
     *            The response for the PREPARE. This indicates which services
     *            voted to commit and which did not.
     */
    public CommitRequest(final PrepareRequest prepareRequest,
            final PrepareResponse prepareResponse) {

        if (prepareRequest == null)
            throw new IllegalArgumentException();

        if (prepareResponse == null)
            throw new IllegalArgumentException();
        
        this.prepareRequest = prepareRequest;
        this.prepareResponse = prepareResponse;
    }

    @Override
    public String toString() {
    
        return super.toString() + "{req=" + prepareRequest + ", resp="
                + prepareResponse + "}";

    }
    
}
