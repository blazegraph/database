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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.util;

import java.util.Comparator;

import com.bigdata.rdf.spo.OSPComparator;
import com.bigdata.rdf.spo.POSComparator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Represents the key order used by an index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum KeyOrder {

    SPO,
    POS,
    OSP;

    final static transient private long NULL = IRawTripleStore.NULL;    

    private KeyOrder() {
        
    }
    
    /**
     * Return the comparator that places {@link SPO}s into the natural order
     * for the associated index.
     */
    final public Comparator<SPO> getComparator() {
        
        switch (this) {
        case SPO:
            return SPOComparator.INSTANCE;
        case POS:
            return POSComparator.INSTANCE;
        case OSP:
            return OSPComparator.INSTANCE;
        default:
            throw new IllegalArgumentException("Unknown: " + this);
        }
        
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
    final public static KeyOrder get(long s, long p, long o) {
       
        if (s != NULL && p != NULL && o != NULL) {

            return KeyOrder.SPO;
            
        } else if (s != NULL && p != NULL) {

            return KeyOrder.SPO;
            
        } else if (s != NULL && o != NULL) {

            return KeyOrder.OSP;
            
        } else if (p != NULL && o != NULL) {

            return KeyOrder.POS;

        } else if (s != NULL) {

            return KeyOrder.SPO;

        } else if (p != NULL) {

            return KeyOrder.POS;

        } else if (o != NULL) {

            return KeyOrder.OSP;

        } else {

            return KeyOrder.SPO;

        }

    }
    
}
