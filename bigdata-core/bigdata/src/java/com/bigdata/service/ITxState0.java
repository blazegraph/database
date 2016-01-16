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
package com.bigdata.service;

/**
 * Immutable state for a transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ITxState0 {

    /**
     * The start time for the transaction as assigned by a centralized
     * transaction manager service. Transaction start times are unique and also
     * serve as transaction identifiers. Note that this is NOT the time at which
     * a transaction begins executing on a specific journal as the same
     * transaction may start at different moments on different journals and
     * typically will only start on some journals rather than all.
     * 
     * @return The transaction start time.
     * 
     * @see #getReadsOnCommitTime()
     * 
     *      TODO Rename since the sign indicates read-only vs read-write and
     *      hence it can not be directly interpreted as a commitTime? (e.g.,
     *      getTxId()).
     */
    long getStartTimestamp();

    /**
     * The timestamp of the commit point against which this transaction is
     * reading.
     * <p>
     * Note: This is not currently available on a cluster. In that context, we
     * wind up with the same timestamp for {@link #startTime} and
     * {@link #readsOnCommitTime} which causes cache pollution for things which
     * cache based on {@link #readsOnCommitTime}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/266">
     *      Refactor native long tx id to thin object</a>
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/546" > Add
     *      cache for access to historical index views on the Journal by name
     *      and commitTime. </a>
     */
    long getReadsOnCommitTime();
    
 }
