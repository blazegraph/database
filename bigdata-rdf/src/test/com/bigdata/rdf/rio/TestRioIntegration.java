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
package com.bigdata.rdf.rio;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import com.bigdata.journal.BufferMode;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.AbstractTripleStoreTestCase;
import com.bigdata.rdf.TripleStore;

/**
 * A test of the RIO integration.
 * 
 * @todo load a data file that we can include in CVS with some known
 *       characteristics and verify those characteristics after the load.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestRioIntegration extends AbstractTripleStoreTestCase {

    /**
     * 200M
     */
    protected long getInitialExtent() {
        
        return Bytes.megabyte*200;
//        return Options.DEFAULT_INITIAL_EXTENT;
        
    }
    
    protected BufferMode getBufferMode() {
        
//        return BufferMode.Disk;
        
//        return BufferMode.Transient;
        
        return BufferMode.Direct;
        
    }

    /**
     * 
     */
    public TestRioIntegration() {
    }

    /**
     * @param name
     */
    public TestRioIntegration(String name) {
        super(name);
    }

    /**
     * Test of RIO integration.
     * 
     * @param args
     *            list of test resources to be parsed and inserted into
     *            the triple store
     */
    public static void main(String[] args) throws Exception {

        if ( args.length == 0 ) {
            
            args = new String[] {
//                    "data/wordnet_nouns-20010201.rdf"
                    "data/nciOncology.owl"
//                    "data/alibaba_v41.rdf"
                    };
            
        }
        
        TestRioIntegration test = new TestRioIntegration("TestInsertRateStore");
        test.setUp();
        test.doTest( new PresortRioLoader(test.store), args );
//        test.doTest( new BulkRioLoader(test.store), args );
        test.tearDown();
            
    }

    /**
     * Primary driver for the RIO integration test.
     * <p>
     * RIO parse time for the test data sets:
     * <p>
     * alibaba_v5a: 42844 stmts added in 1.188 secs, rate= 36063
     * <p>
     * wordnet_nouns-20010201: 273644 stmts added in 3.078 secs, rate= 88903
     * <p>
     * wordnet-glossary: 115424 stmts added in 3.86 secs, rate= 29902
     * <p>
     * avrs_v1.rdf: 6408888 stmts added in 94.016 secs, rate= 68168
     * <p>
     * More details on the wordnet_nouns-20010201 data set:
     * <p>
     * Basic loading (no presort): 273644 stmts added in 37.422 secs, rate= 7312
     * 
     * @param resources
     *            list of test resources to be parsed and inserted into the
     *            triple store
     * 
     * @todo modify {@link TripleStore#loadData(java.io.File)} to chain input
     *       sources together for better bulk builds rather than using a
     *       separate loader for each input source.
     */
    public void doTest( IRioLoader loader, final String[] resources ) throws IOException {

        long total_stmts = 0;

//        long begin = System.currentTimeMillis();
        
        for ( int i = 0; i < resources.length; i++ ) {

            log.info( "ingesting resource: " + 
//                      getClass().getResource( resources[i] )
                    resources[i]
                      );
            
            Reader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(resources[i])));

            //            Reader reader = new InputStreamReader
//            ( getClass().getResourceAsStream( resources[i] )
//              );
            
            loader.addRioLoaderListener( new RioLoaderListener() {
                
                public void processingNotification( RioLoaderEvent e ) {
                    
                    log.info
                        ( e.getStatementsProcessed() + 
                          " stmts added in " + 
                          ((double)e.getTimeElapsed()) / 1000d +
                          " secs, rate= " + 
                          e.getInsertRate() 
                          );
                    
                }
                
            });
            
            try {
                
                // @todo no baseURI here.
                loader.loadRdfXml( reader, "" );
                
                long nstmts = loader.getStatementsAdded();
                
                total_stmts += nstmts;

                // commit the loaded data.
                final long beginCommit = System.currentTimeMillis();
                store.commit();
                final long elapsedCommit = System.currentTimeMillis()-beginCommit;
                
                log.info( nstmts + 
                          " stmts added in " + 
                          ((double)loader.getInsertTime()) / 1000d +
                          " secs, rate= " + 
                          loader.getInsertRate()+
                          ", commit="+elapsedCommit+"ms"
                          );
                
            } catch ( Exception ex ) {
                
                ex.printStackTrace();
                
            } finally {
                
                reader.close();
                
            }
            
        } // next source to load.

        store.closeAndDelete();
        
//        long elapsed = System.currentTimeMillis() - begin;
//        
//        log.info(total_stmts
//                        + " stmts added in "
//                        + ((double) elapsed)
//                        / 1000d
//                        + " secs, rate= "
//                        + ((long) (((double) total_stmts) / ((double) elapsed) * 1000d)));

    }

    public void test_loadFile_basicRioLoader() throws IOException {

        doTest(new BasicRioLoader(), testData);

        // Note: does not load any data.
        
    }
    
    public void test_loadFile_presortRioLoader() throws IOException {

        doTest(new PresortRioLoader(store,
                PresortRioLoader.DEFAULT_BUFFER_SIZE, true), testData);

        assertDataLoaded();
        
    }

    public void test_load_file_wikipedia() throws IOException {

        String[] testData = new String[]{"data/wikipedia/enwiki/20060306.rdf"};
        
        doTest(new PresortRioLoader(store,
                PresortRioLoader.DEFAULT_BUFFER_SIZE, true), testData);
        

    }
    
    public void test_loadFile_multiThreadedPresortRioLoader() throws IOException {

        doTest(new MultiThreadedPresortRioLoader( store ), testData);

        assertDataLoaded();
        
    }
    
    public void test_loadFile_bulkRioLoader() throws IOException {

        doTest(new BulkRioLoader( store ), testData);

        assertDataLoaded();
        
    }
    
    protected String[] testData = new String[] {
            "data/nciOncology.owl" // nterms := 289844
//            "data/wordnet_nouns-20010201.rdf"
//            "data/taxonomy.rdf"
            };
    
    protected void assertDataLoaded() {

        // wordnet
//        final int nterms = 223146;
//        final int nstatements = 273644;
        
        // nciOncology
        final int nterms = 289844;
        final int nstatements = 464841;
        
//        assertEquals("#terms",nterms,store.getTermIdIndex().getEntryCount());
//        
//        assertEquals("#ids",nterms,store.getIdTermIndex().getEntryCount());
//        
//        assertEquals("#spo",nstatements,store.getSPOIndex().getEntryCount());
//        
//        assertEquals("#pos",nstatements,store.getPOSIndex().getEntryCount());
//        
//        assertEquals("#ops",nstatements,store.getOSPIndex().getEntryCount());
        
    }
    
}
