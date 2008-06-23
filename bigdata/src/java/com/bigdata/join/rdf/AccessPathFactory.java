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
 * Created on Jun 21, 2008
 */

package com.bigdata.join.rdf;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.join.AbstractAccessPath;
import com.bigdata.join.IAccessPath;
import com.bigdata.join.IAccessPathFactory;
import com.bigdata.join.IPredicate;

/**
 * Selects access paths for RDF triple patterns.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AccessPathFactory implements IAccessPathFactory<ISPO> {

    // @todo IRawTripleStore when re-factored back to the rdf module.
    private transient final long NULL = 0L;
    
    public IIndex getIndex(String name) {

        /*
         * @todo integrate with triple store impls to returns the appropriate
         * statement index. Add ctor accepting the IRawTripleStore and use
         * getStatementIndex(String name) to obtain the appropriate index. This
         * works if we assume that the triple store is fully indexed.
         */
        throw new UnsupportedOperationException();
        
    }
    
    public IAccessPath<ISPO> getAccessPath(KeyOrder keyOrder,IPredicate<ISPO> pred) {
        
        return new AbstractAccessPath<ISPO>(pred, keyOrder, getIndex(keyOrder
                .getName()), IRangeQuery.KEYS | IRangeQuery.VALS/*flags*/) {
            
        };
        
    }
    
    /**
     * Return the {@link KeyOrder} that will be used to read from the statement
     * index that is most efficient for the specified triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     * @return
     */
    public IAccessPath<ISPO> getAccessPath(IPredicate<ISPO> pred) {

        final long s = pred.get(0).isVar() ? NULL : (Long) pred.get(0).get();
        final long p = pred.get(1).isVar() ? NULL : (Long) pred.get(1).get();
        final long o = pred.get(2).isVar() ? NULL : (Long) pred.get(2).get();

        if (s != NULL && p != NULL && o != NULL) {

            return getAccessPath(KeyOrder.SPO, pred);

        } else if (s != NULL && p != NULL) {

            return getAccessPath(KeyOrder.SPO, pred);

        } else if (s != NULL && o != NULL) {

            return getAccessPath(KeyOrder.OSP, pred);

        } else if (p != NULL && o != NULL) {

            return getAccessPath(KeyOrder.POS, pred);

        } else if (s != NULL) {

            return getAccessPath(KeyOrder.SPO, pred);

        } else if (p != NULL) {

            return getAccessPath(KeyOrder.POS, pred);

        } else if (o != NULL) {

            return getAccessPath(KeyOrder.OSP, pred);

        } else {

            return getAccessPath(KeyOrder.SPO, pred);

        }

    }

}
