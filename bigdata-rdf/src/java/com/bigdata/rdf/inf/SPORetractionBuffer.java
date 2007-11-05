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
 * Created on Nov 5, 2007
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.SPO;
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

        log.info("numStmts=" + numStmts);

        final long begin = System.currentTimeMillis();

        int n = 0;

        /*
         * @todo It might be worth doing a more efficient method for bulk
         * statement removal. This will wind up doing M * N operations. The N
         * are parallelized, but the M are not.
         */

        for(int i=0; i<numStmts; i++) {

            SPO spo = stmts[i];
            
            n += store.getAccessPath(spo.s, spo.p, spo.o).removeAll();
        
        }
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Retracted "+n+" statements in "+elapsed+"ms");

        // reset the counter.
        numStmts = 0;

        return n;
        
    }

}
