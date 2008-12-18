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
 * Created on May 5, 2008
 */

package com.bigdata.journal;

/**
 * Some static helper methods for timestamps.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TimestampUtility {

    /**
     * Formats the timestamp as a String.
     * 
     * @param timestamp
     * 
     * @return
     */
    static public String toString(long timestamp) {

        if (timestamp == ITx.UNISOLATED)
            return "unisolated";

        if (timestamp == ITx.READ_COMMITTED)
            return "read-committed";

        if (isReadWriteTx(timestamp))
            return "readWriteTX(" + timestamp + ")";

        return "readOnly(" + timestamp + ")";

    }
    
    /**
     * True iff the timestamp is a possible commit time (GT ZERO).
     * 
     * @param timestamp
     *            The timestamp.
     * @return
     */
    static public boolean isCommitTime(long timestamp) {
        
        return timestamp > 0;
        
    }

    static public boolean isReadOnly(long timestamp) {
        
//        return timestamp < ITx.READ_COMMITTED;
        return timestamp > 0 || timestamp == ITx.READ_COMMITTED;
        
    }

    static public boolean isReadWriteTx(long timestamp) {
        
//        return timestamp > 0;
        
        return timestamp < ITx.READ_COMMITTED;
        
    }
    
    static public boolean isReadCommittedOrUnisolated(long timestamp) {
        
        return timestamp == ITx.READ_COMMITTED || timestamp == ITx.UNISOLATED;
        
    }

    static public boolean isReadCommitted(long timestamp) {
        
        return timestamp == ITx.READ_COMMITTED;
        
    }

    static public boolean isUnisolated(long timestamp) {
        
        return timestamp == ITx.UNISOLATED;
        
    }

    /**
     * Accepts a commitTime and returns a timestamp that will be interpreted as
     * a historical read (this is a NOP).
     * 
     * @param commitTime
     *            The commit time from {@link IIndexStore#getLastCommitTime()},
     *            etc.
     *            
     * @return The corresponding timestamp that will be interpreted as a
     *         historical read against the state of the database having that
     *         commit time.
     *         
     * @throws IllegalArgumentException
     *             if <i>commitTime</i> is negative (zero is permitted for some
     *             edge cases).
     */
    static public long asHistoricalRead(final long commitTime) {
    
        if (commitTime < 0)
            throw new IllegalArgumentException("commitTime: " + commitTime);
        
//        return -commitTime;
        return commitTime;
        
    }
    
}
