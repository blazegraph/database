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

package com.bigdata.rdf.rules;

import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
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
public class TestCompareOwlSameAsEntailments extends AbstractRuleTestCase {

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

    public void test_compareEntailments() throws IOException, SailException {
        
        String[] resource = new String[]{"/com/bigdata/rdf/rules/testOwlSameAs.rdf"};
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
    protected void doCompareEntailments(String resource[], String baseURL[],
            RDFFormat[] format) throws IOException, SailException {

        final AbstractTripleStore store1;
        final AbstractTripleStore store2;
        
        Properties properties = new Properties(getProperties());
        
        // close each set of resources after it has been loaded.
        properties.setProperty(DataLoader.Options.CLOSURE,
                DataLoader.ClosureEnum.Batch.toString());

        properties.setProperty(DataLoader.Options.CLOSURE,
                DataLoader.ClosureEnum.Batch.toString());

        { // use the "full" forward closure.

            Properties tmp = new Properties(properties);

            tmp.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,
                    InferenceEngine.ForwardClosureEnum.Full.toString());

            store1 = new TempTripleStore(tmp);

        }

        { // use the "fast" forward closure.

            Properties tmp = new Properties(properties);

            tmp.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,
                    InferenceEngine.ForwardClosureEnum.Fast.toString());

            store2 = new TempTripleStore(tmp);

        }
        
        try {

            {

                LoadStats loadStats = store1.getDataLoader().loadData(resource,
                        baseURL, format);
                
                System.err.println("Full forward closure: " + loadStats);
                
            }
            
            {

                LoadStats loadStats = store2.getDataLoader().loadData(resource,
                        baseURL, format);
                
                System.err.println("Fast forward closure: " + loadStats);
                
            }
        
            assertTrue(modelsEqual(store1, store2));
            
        } finally {
            
            store1.closeAndDelete();
            store2.closeAndDelete();
            
        }
        
    }
    
}
