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
 * Created on Oct 23, 2007
 */

package com.bigdata.rdf.rio;

import java.io.File;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Test loads an RDF/XML resource into a database and then verifies by re-parse
 * that all expected statements were made persistent in the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLoadAndVerify extends AbstractRIOTestCase {

    /**
     * 
     */
    public TestLoadAndVerify() {
    }

    /**
     * @param name
     */
    public TestLoadAndVerify(String name) {
        super(name);
    }

    /**
     * Test with the "small.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_small() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/small.rdf";

        doLoadAndVerifyTest(resource);

    }

    /**
     * Test with the "sample data.rdf" data set.
     * 
     * @throws Exception
     */
    public void test_loadAndVerify_sampleData() throws Exception {
        
        final String resource = "bigdata-rdf/src/test/com/bigdata/rdf/rio/sample data.rdf";

        doLoadAndVerifyTest( resource );
        
    }
   
    /**
     * @todo use some modest to largish RDF/XML that we can pack with the
     *       distribution.
     */
    public void test_loadAndVerify_modest() throws Exception {
        
//      final String file = "../rdf-data/nciOncology.owl";
        final String file = "../rdf-data/alibaba_v41.rdf";

        if (!new File(file).exists()) {

            fail("Resource not found: " + file + ", test skipped: " + getName());

            return;
            
        }

        doLoadAndVerifyTest( file );
        
    }

    /**
     * Note: This allows an override of the properties that effect the data
     * load, in particular whether or not the full text index and statement
     * identifiers are maintained. It can be useful to disable those features in
     * order to estimate the best load rate for a data set.
     */
    protected AbstractTripleStore getStore() {

        final Properties properties = new Properties(getProperties());

        // properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX,
        // "false");
        //
        // properties.setProperty(
        // AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");

        return getStore(properties);

    }
    
    /**
     * Test loads an RDF/XML resource into a database and then verifies by
     * re-parse that all expected statements were made persistent in the
     * database.
     * 
     * @param resource
     * 
     * @throws Exception
     */
    protected void doLoadAndVerifyTest(final String resource) throws Exception {

        AbstractTripleStore store = getStore();

        try {

            load(store, resource);

            store.commit();

            if (store.isStable()) {

                store = reopenStore(store);

            }

            verify(store, resource);

        } finally {

            store.closeAndDelete();

        }

    }

    /**
     * Load the file using the {@link DataLoader}.
     * <p>
     * Note: Normally we disable closure for this test, but that is not
     * critical. If you compute the closure of the data set then there will
     * simply be additional statements whose self-consistency among the
     * statement indices will be verified, but it will not verify the
     * correctness of the closure.
     */
    protected void load(final AbstractTripleStore store, final String resource)
            throws Exception {

        // avoid modification of the properties.
        final Properties properties = new Properties(getProperties());

        // turn off RDFS closure for this test.
        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None
                .toString());

        final DataLoader dataLoader = new DataLoader(properties, store);

        // load into the datbase.
        dataLoader.loadData(resource, "" /* baseURI */, RDFFormat.RDFXML);

        // // database-at-once closure (optional for this test).
        // store.getInferenceEngine().computeClosure(null/*focusStore*/);

    }

}
