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
package com.bigdata.rdf.changesets;

import java.util.Comparator;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOComparator;

public class ChangeRecord implements IChangeRecord {
    
    private final ISPO stmt;
    
    private final ChangeAction action;
    
    public ChangeRecord(final ISPO stmt, final ChangeAction action) {
        
        this.stmt = stmt;
        this.action = action;
        
    }
    
    public ChangeAction getChangeAction() {
        
        return action;
        
    }

    public ISPO getStatement() {
        
        return stmt;
        
    }
   
    @Override
    public int hashCode() {

        return stmt.hashCode();
        
    }
    
    @Override
    public boolean equals(final Object o) {
        
        if (o == this)
            return true;
        
        if (o == null || o instanceof IChangeRecord == false)
            return false;
        
        final IChangeRecord rec = (IChangeRecord) o;
        
        final ISPO stmt2 = rec.getStatement();
        
        // statements are equal
        if (stmt == stmt2 || 
                (stmt != null && stmt2 != null && stmt.equals(stmt2))) {
            
            // actions are equal
            return action == rec.getChangeAction();
            
        }
        
        return false;
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append(action).append(": ").append(stmt);
        
        return sb.toString();
        
    }

    /**
     * Comparator imposes an {@link ISPO} order.
     */
    public static final Comparator<IChangeRecord> COMPARATOR = 
        new Comparator<IChangeRecord>() {
        
        public int compare(final IChangeRecord r1, final IChangeRecord r2) {
            
            final ISPO spo1 = r1.getStatement();
            final ISPO spo2 = r2.getStatement();
            
            return SPOComparator.INSTANCE.compare(spo1, spo2);
            
        }
        
    };
    
}
