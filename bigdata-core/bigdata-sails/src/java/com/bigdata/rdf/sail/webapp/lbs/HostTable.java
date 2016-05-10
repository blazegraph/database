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
package com.bigdata.rdf.sail.webapp.lbs;

import java.util.Arrays;

/**
 * Class bundles together the set of {@link HostScore}s for services that are
 * joined with the met quorum and the {@link HostScore} for this service (iff it
 * is joined with the met quorum).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HostTable {

    /**
     * The most recent score for this host -or- <code>null</code> iff there
     * is not score for this host.
     */
    final public HostScore thisHost;

    /**
     * The table of pre-scored hosts -or- <code>null</code> iff there are no
     * host scores. Only hosts that have services that are joined with the met
     * quorum will appear in this table. 
     */
    final public HostScore[] hostScores;

    public HostTable(final HostScore thisHost, final HostScore[] hostScores) {
        this.thisHost = thisHost;
        this.hostScores = hostScores;
    }

    @Override
    public String toString() {
        
        return "HostTable{this=" + thisHost + ",hostScores="
                + (hostScores == null ? "N/A" : Arrays.toString(hostScores))
                + "}";

    }
    
}
