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
 * Created on Nov 6, 2007
 */

package com.bigdata.rdf.rules;

import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.inf.Axioms;
import com.bigdata.rdf.inf.BaseAxioms;
import com.bigdata.rdf.inf.OwlAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for the {@link Axioms}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAxiomModel extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestAxiomModel() {
        super();
       
    }

    /**
     * @param name
     */
    public TestAxiomModel(String name) {
        super(name);
       
    }

    public void test_isAxiom() {
        
        AbstractTripleStore store = getStore();
        
        try {

            // store is empty.
            assertEquals(0,store.getStatementCount());
            
            BaseAxioms axioms = new OwlAxioms(store);

            // store is still empty (on demand assertion of axioms).
            assertEquals(0,store.getStatementCount());

            assertTrue(axioms.isAxiom(new StatementImpl(RDF.TYPE,
                    RDF.TYPE, RDF.PROPERTY)));
            
            // We have to do this explicitly so that the identifiers for the
            // axioms are defined.
            axioms.addAxioms();
            
            assertTrue(axioms.isAxiom(store.getTermId(RDF.TYPE), store
                    .getTermId(RDF.TYPE), store
                    .getTermId(RDF.PROPERTY)));
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}
