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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

/**
 * A read-only view of an {@link ICommitRecord}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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

    @Override
    final public long getTimestamp() {
        
        return timestamp;
        
    }

    @Override
    final public long getCommitCounter() {
        
        return commitCounter;
        
    }
    
    @Override
    final public int getRootAddrCount() {
        
        return roots.length;
        
    }
    
    @Override
    final public long getRootAddr(int index) {
        
        return roots[index];
        
    }

    @Override
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

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ICommitRecord))
            return false;
        final ICommitRecord t = (ICommitRecord) o;
        if (timestamp != t.getTimestamp())
            return false;
        if (commitCounter != t.getCommitCounter())
            return false;
        if (roots.length != t.getRootAddrCount())
            return false;
        for (int i = 0; i < roots.length; i++) {
            if (roots[i] != t.getRootAddr(i))
                return false;
        }
        return true;
    }
    
}
