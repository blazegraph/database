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

package com.bigdata.join.rdf;

import com.bigdata.join.IRelation;
import com.bigdata.join.IRelationLocator;
import com.bigdata.join.IRelationName;
import com.bigdata.join.RelationFusedView;

/**
 * Knows how to locate a local triple store and an optional focuStore.
 * 
 * FIXME This is suitable for the LocalTripleStore and TempTripleStore impls
 * only. Write another one that uses the namespace for the triple store to
 * resolve the various indices. However, how we resolve the relation name needs
 * to depend on the database variant on which the triple store was deployed
 * (local Journal, LDS, JDS, etc.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORelationLocator implements IRelationLocator<ISPO> {

    private final SPORelation database;
    
    private final SPORelation focusStore;
    
    public SPORelationLocator(TestTripleStore database) {
        
        this(database, null);
        
    }
    
    public SPORelationLocator(TestTripleStore database,
            TestTripleStore focusStore) {
     
        if (database == null)
            throw new IllegalArgumentException();

        this.database = new SPORelation(database);
        
        this.focusStore = focusStore==null?null:new SPORelation(focusStore);
        
    }

    /**
     * @todo this ignores the actual names associated with the view. if it is a
     *       view then it returns a [database+focusStore] view. Otherwise it
     *       returns the [database].
     */
    public IRelation<ISPO> getRelation(IRelationName<ISPO> relationName) {

        if (relationName instanceof SPORelationView) {

            return new RelationFusedView<ISPO>(database,focusStore);
            
        } else {

            return database;

        }
        
    }

}
