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
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.Journal;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Variant of {@link TestTripleStoreLoadRateWithJiniFederation} that tests with
 * an embedded {@link Journal} but without the use of the concurrency API (it is
 * not thread-safe).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreLoadRateLocal extends ProxyTestCase {

    /**
     * 
     */
    public TestTripleStoreLoadRateLocal() {
        super();
    }

    /**
     * @param arg0
     */
    public TestTripleStoreLoadRateLocal(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {

        Properties properties = new Properties(super.getProperties());

        // turn off incremental truth maintenance.
        properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());
        
        // choice of fast vs full closure (iff enabled below).
//        properties.setProperty(Options.CLOSURE_CLASS,FastClosure.class.getName());
//        properties.setProperty(Options.CLOSURE_CLASS,FullClosure.class.getName());
        
        // turn off the full text index for literals.
        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");

        // turn off statement identifiers.
        properties.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, "false");
        
        return properties;

    }

    /**
     * Option to compute the database-at-once closure.
     */
    protected final boolean computeClosure = false;

    public void test_U10() throws IOException {

        final String file = "../rdf-data/lehigh/U10";

        final String baseURI = null;//file;
        
        doTest(new File(file), baseURI, RDFFormat.RDFXML, new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return !name.endsWith(".txt");
            }
        });

    }
    
    public void test_U5() throws IOException {

        String file = "../rdf-data/lehigh/U5";

        final String baseURI = null;//file;
        
        doTest(new File(file), baseURI, RDFFormat.RDFXML, new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return !name.endsWith(".txt");
            }
        });

    }
    
    public void test_U1() throws IOException {
        
        final String file = "../rdf-data/lehigh/U1";

        final String baseURI = null;//file;

        doTest(new File(file), baseURI, RDFFormat.RDFXML, new FilenameFilter() {

                public boolean accept(File dir, String name) {
                    return ! name.endsWith(".txt");
                }
        });
      
    }
    
    /**
     * Note: The order does not matter since we are using database-at-once
     * closure (assuming that closure is enabled).
     */
    public void test_loadWordnet() throws IOException {

        doTest(//
                new String[] { "../rdf-data/wordnet_nouns-20010201.rdf",
                        "../rdf-data/wordnet-20000620.rdfs"
                        },
                //
                new String[] { "", "" },
                //
                new RDFFormat[] { RDFFormat.RDFXML, RDFFormat.RDFXML });
        
    }

    public void test_loadThesaurus() throws IOException {

        doTest("../rdf-data/Thesaurus.owl","",RDFFormat.RDFXML);
        
    }

    public void test_alibaba() throws IOException {

        doTest("../rdf-data/alibaba_v41.rdf","",RDFFormat.RDFXML);

    }

    public void test_loadNCIOncology() throws IOException {

        doTest("../rdf-data/nciOncology.owl","",RDFFormat.RDFXML);
        
    }

    protected void doTest(String file, String baseURL, RDFFormat rdfFormat)
            throws IOException {

        doTest(new String[] { file }, new String[] { baseURL },
                new RDFFormat[] { rdfFormat });

    }

    protected void doTest(String[] file, String[] baseURL, RDFFormat[] rdfFormat)
            throws IOException {

        AbstractTripleStore store = getStore(getProperties());

        try {

            // load the data set.
            System.out.println(store.getDataLoader().loadData(file, baseURL,
                    rdfFormat).toString());

            if(computeClosure) {
                
                // compute the database at once closure.
                System.out.println(store.getInferenceEngine().computeClosure(
                        null/* focusStore */).toString());
                
            }

            store.commit();

        } finally {

            store.closeAndDelete();

        }

    }

    protected void doTest(File file, String baseURL, RDFFormat rdfFormat,
            FilenameFilter filter) throws IOException {

        AbstractTripleStore store = getStore(getProperties());

        try {

            // load the data set.
            System.out.println(store.getDataLoader().loadFiles(file, baseURL,
                    rdfFormat, filter).toString());

            if (computeClosure) {

                // compute the database at once closure.
                System.out.println(store.getInferenceEngine().computeClosure(
                        null/* focusStore */).toString());

            }

            store.commit();

        } finally {

            store.closeAndDelete();

        }

    }

}
