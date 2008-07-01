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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation.rdf;

import com.bigdata.relation.IRelationName;

/**
 * Describes a read-only view of two SPO relations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelationView implements IRelationName<SPO> {

    private static final long serialVersionUID = 5083166024993505026L;
    
    private final IRelationName<SPO> database;
    private final IRelationName<SPO> focusStore;
    
    public IRelationName<SPO> getDatabase() {
        
        return database;
        
    }

    public IRelationName<SPO> getFocusStore() {
        
        return focusStore;
        
    }
    
    public SPORelationView(IRelationName<SPO> database,
            IRelationName<SPO> focusStore) {

        if (database == null)
            throw new IllegalArgumentException();
        
        if (focusStore == null)
            throw new IllegalArgumentException();
        
        if (database == focusStore)
            throw new IllegalArgumentException();
        
        this.database = database;
        
        this.focusStore = focusStore;
        
    }

    public String toString() {
        
        return "{" + database + "," + focusStore + "}";
        
    }
    
}
