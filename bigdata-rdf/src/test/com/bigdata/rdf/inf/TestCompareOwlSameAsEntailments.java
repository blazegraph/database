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
 * Created on Nov 9, 2007
 */

package com.bigdata.rdf.inf;

import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * Test compares the compuation of owl:sameAs using the full forward closure and
 * the fast forward closure methods. The fast forward closure breaks down the
 * computation of the reflexive and transitive closure, which is highly bound,
 * into its own fixed point for {@link RuleOwlSameAs1} and
 * {@link RuleOwlSameAs1b} and then runs the rules {@link RuleOwlSameAs2} and
 * {@link RuleOwlSameAs3} in sequence without fixed point. This is significantly
 * faster.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCompareOwlSameAsEntailments extends
        AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestCompareOwlSameAsEntailments() {
    }

    /**
     * @param name
     */
    public TestCompareOwlSameAsEntailments(String name) {
        super(name);
    }

    public void test_compareEntailments() throws IOException {
        
        String[] resource = new String[]{"/com/bigdata/rdf/inf/testOwlSameAs.rdf"};
        String[] baseURL = new String[]{""};
        RDFFormat[] format = new RDFFormat[]{RDFFormat.RDFXML};

        doCompareEntailments(resource, baseURL, format);
        
    }
    
    /**
     * 
     * Note: This always uses {@link TempTripleStore} rather than whatever is
     * configured since we need to create two stores and we need control over
     * the options for each store.
     * 
     * @param resource
     * @param baseURL
     * @param format
     * @throws IOException
     */
    protected void doCompareEntailments(String resource[], String baseURL[], RDFFormat[] format) throws IOException {
        
        final AbstractTripleStore store1;
        final AbstractTripleStore store2;
        
        Properties properties = new Properties(getProperties());
        
        // close each set of resources after it has been loaded.
        properties.setProperty(DataLoader.Options.CLOSURE,DataLoader.ClosureEnum.Batch.toString());

        properties.setProperty(DataLoader.Options.CLOSURE,DataLoader.ClosureEnum.Batch.toString());

        { // use the "full" forward closure.
            
            Properties tmp = new Properties(properties);
            
            tmp.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,InferenceEngine.ForwardClosureEnum.Full.toString());
            
            store1 = new TempTripleStore(tmp);

        }
        
        { // use the "fast" forward closure.
            
            Properties tmp = new Properties(properties);

            tmp.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,InferenceEngine.ForwardClosureEnum.Fast.toString());

            store2 = new TempTripleStore(tmp);
        
        }
        
        try {

            store1.getDataLoader().loadData(resource, baseURL, format);
            store2.getDataLoader().loadData(resource, baseURL, format);
        
            assert modelsEqual(store1,store2);
            
        } finally {
            
            store1.closeAndDelete();
            store2.closeAndDelete();
            
        }
        
    }
    
    /**
     * Compares two RDF graphs for equality (same statements) - does NOT handle
     * bnodes, which much be treated as variables for RDF semantics.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     */
    public static boolean modelsEqual(AbstractTripleStore expected,
            AbstractTripleStore actual) {

        int actualSize = 0; 
        boolean sameStatements1 = true;
        {

            StatementIterator it = actual.getStatements(null, null, null);

            try {

                while(it.hasNext()) {

                    Statement stmt = it.next();

                    if (!expected.hasStatement(stmt.getSubject(), stmt
                            .getPredicate(), stmt.getObject())) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);

                    }

                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
            
            log("all the statements in actual in expected? " + sameStatements1);

        }

        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            StatementIterator it = expected.getStatements(null, null, null);

            try {

                while(it.hasNext()) {

                Statement stmt = it.next();

                if (!actual.hasStatement(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject())) {

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);

                }
                
                expectedSize++; // counts statements actually visited.

                }
                
            } finally {
                
                it.close();
                
            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        final boolean sameSize = expectedSize == actualSize;
        
        log("size of 'expected' repository: " + expectedSize);

        log("size of 'actual'   repository: " + actualSize);

        return sameSize && sameStatements1 && sameStatements2;

    }

    private static void log(String s) {

        System.err.println(s);

    }

}
