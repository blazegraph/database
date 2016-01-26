/*

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
 * Created on Oct 1, 2012
 */
package com.bigdata.rdf.sparql.ast.service.history;

import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.spo.ISPO;

/**
 * Extended to include a revision time for each record.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HistoryChangeRecord extends ChangeRecord {

    private final long revisionTime;

    public HistoryChangeRecord(final ISPO stmt, final ChangeAction action,
            final long revisionTime) {

        super(stmt, action);

        this.revisionTime = revisionTime;

    }

    public HistoryChangeRecord(final IChangeRecord changeRecord,
            final long revisionTime) {

        this(changeRecord.getStatement(), changeRecord.getChangeAction(),
                revisionTime);

    }

    /**
     * The revision time is <code>lastCommitTime+1</code>.
     * 
     * TODO The revision time for the entries in the history index is currently
     * <code>lastCommitTime+1</code>, but this needs to be thought through some
     * more and various use cases considered.
     * <p>
     * The issue with revision time is that we have to record things in the
     * history index incrementally, so it can not be a commit time because we do
     * not have that yet. lastCommitTime+1 will always be strictly greater than
     * the previous commit point. When you scan the history index, you can then
     * use fromKey=firstCommitTime to visit (or null for the head of the index).
     * toKey=firstCommitTime to be excluded (or null for the tail of the index).
     * That all has semantics that are pretty much what people would expect for
     * the scan. However, the reported revision times are not going to
     * correspond directly to the commit points.
     * <p>
     * When reporting this data, we could resolve (and cache) the first commit
     * time greater than that commit point, but only if we still have access to
     * that commit point (the resolution would be against the commit record
     * index, and commit points are pruned from that index when they are
     * recycled).
     */
    public long getRevisionTime() {

        return revisionTime;

    }

    public String toString() {

        return "revisionTime=" + revisionTime + " : " + super.toString();

    }
    
    @Override
    public boolean equals(final Object o) {
        
        if (o == this)
            return true;
        
        if (o == null || o instanceof HistoryChangeRecord == false)
            return false;
        
        final HistoryChangeRecord rec = (HistoryChangeRecord) o;
        
        if(revisionTime != rec.getRevisionTime())
            return false;
        
        return super.equals(o);

    }

}
