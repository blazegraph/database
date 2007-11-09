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
 * Created on Nov 5, 2007
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A buffer for {@link SPO}s which causes the corresponding statements (and
 * their {@link Justification}s) be retracted from the database when it is
 * {@link #flush()}ed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPORetractionBuffer extends AbstractSPOBuffer {

    /**
     * @param store
     * @param capacity
     */
    public SPORetractionBuffer(AbstractTripleStore store, int capacity) {
        
        super(store, null/*filter*/, capacity);
        
    }

    public int flush() {

        if (isEmpty()) return 0;
        
        int nremoved = store.removeStatements(new SPOArrayIterator(stmts,numStmts));

        // reset the counter.
        numStmts = 0;

        return nremoved;
        
    }

}
