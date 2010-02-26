/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

/**
 * A read-only view of an {@link ICommitRecord}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CommitRecord implements ICommitRecord {

    private final long timestamp;
    private final long commitCounter;
    private final long[] roots;

    /**
     * @todo this may not be the correct commit counter unless this method is
     *       synchronized with the writeService.
     * 
     * @todo are commit counters global or local?
     */
    public CommitRecord() {

        this(0L/* timestamp */, 0L/* commitCounter */,
                new long[ICommitRecord.MAX_ROOT_ADDRS]);
        
    }

    public CommitRecord(final long timestamp, final long commitCounter,
            final long[] roots) {

//        assert timestamp != 0L; // @todo what constraint?
        
        assert roots != null;
        
        assert roots.length == ICommitRecord.MAX_ROOT_ADDRS : "roots.length="
                + roots.length + ", but expecting: "
                + ICommitRecord.MAX_ROOT_ADDRS;
        
        this.timestamp = timestamp;
        
        this.commitCounter = commitCounter;
        
        this.roots = roots;
        
    }

    final public long getTimestamp() {
        
        return timestamp;
        
    }

    final public long getCommitCounter() {
        
        return commitCounter;
        
    }
    
    final public int getRootAddrCount() {
        
        return roots.length;
        
    }
    
    final public long getRootAddr(int index) {
        
        return roots[index];
        
    }

    public String toString() {
        
        StringBuffer sb = new StringBuffer();
        
        sb.append("CommitRecord");
        
        sb.append("{timestamp="+timestamp);
        
        sb.append(", commitCounter="+commitCounter);
        
        sb.append(", roots=[");
        
        for( int i=0; i< roots.length; i++) {
            
            if(i>0) sb.append(", ");
            
//            sb.append(Addr.toString(roots[i]));
            sb.append(roots[i]);
            
        }
        
        sb.append("]}");
        
        return sb.toString();
        
    }
    
}
