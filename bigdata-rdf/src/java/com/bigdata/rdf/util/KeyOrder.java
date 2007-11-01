/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
