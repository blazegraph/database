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
 * Created on Apr 3, 2008
 */

package com.bigdata.search;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Properties;

import com.bigdata.service.AbstractLocalDataServiceFederationTestCase;

/**
 * Test suite using examples based on <a
 * href="http://www.ec-securehost.com/SIAM/SE17.html"><i>Understanding Search
 * Engines</i></a> by Barry and Browne. I recommend the book as a good
 * overview of search engine basis and an excellent reader for latent semantic
 * indexing (Barry was one of the original people involved with LSI).
 * <p>
 * There is a worksheet <code>src/architecture/search.xls</code> that gives
 * the background for this test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSearch extends AbstractLocalDataServiceFederationTestCase {

    public TestSearch() {
        super();
    }

    public TestSearch(String name) {
        super(name);
    }

    FullTextIndex ndx;

    /**
     * Note: the examples have been modified to only expose the terms that were
     * accepted by the indexer used in the book. Since the authors were using
     * stemming, the examples are pre-stemmed. This just reduces the complexity
     * of the system under test.
     */
    String[] docs = new String[]{
            /* Infant & Toddler First Aid */
            "Infant Toddler",//
            /* Babies Children's Room (For Your Home) */
            "Bab Child Home",//
            /* Child Safety at Home */
            "Child Safety Home",// 
            /* Your Baby's Health and Safety: From Infant to Toddler */
            "Bab Health Safety Infant Toddler",//
            /* Baby Proofing Basics */
            "Bab Proofing",
            /* Your Guide To Easy Rust Proofing */
            "Guide Proofing",
            /* Beanie Babies Collector's Guide */
            "Bab Guide"
    };
    
    /**
     * Overrides some properties to setup the {@link FullTextIndex} configuration.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        /*
         * FIXME specify the desired local, global, and document normalization
         * methods.
         */
        
        return properties;
        
    }

    /** all documents are in English. */
    final String languageCode = "EN";

    public void setUp() throws Exception {

        super.setUp();
        
        ndx = new FullTextIndex(client, "test");

        /*
         * Index the documents.
         */
        long docId = 1;
        final int fieldId = 0;
        final TokenBuffer buffer = new TokenBuffer(docs.length,ndx);
        for(String s : docs) {
        
            ndx.index(buffer, docId++, fieldId, languageCode, new StringReader( s ));
            
        }
        
        // flush index writes to the database.
        buffer.flush();
        
    }

    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }
    
    public void test_ChildProofing() throws InterruptedException {
        
        String query = "child proofing";
        
        Hiterator itr = ndx.search(query, languageCode,0d/*minCosine*/,Integer.MAX_VALUE/*maxRank*/);

        assertSameHits(new IHit[]{
                new HT(5L,.5),//
                new HT(6L,.5),//
                new HT(2L,.408248290463863d),//
                new HT(3L,.408248290463863d),//
        }, itr);
        
    }
    
    /**
     * Compares the hit list to the expected hit list.
     * <p>
     * Note: Ties on cosine are broken by sorting the ties into increasing order
     * by docId.
     * 
     * @param hits
     *            The expected hits.
     * @param itr
     *            The iterator visiting the actual hits.
     */
    protected void assertSameHits(IHit[] hits,Iterator<? extends IHit> itr) {
        
        final int nhits = hits.length;
        
        for(int i=0; i<nhits; i++) {
            
            assertTrue("Iterator exhausted after "+(i)+" hits out of "+nhits, itr.hasNext()); 
            
            final IHit expected = hits[i];
            
            final IHit actual = itr.next();
            
            log.info("rank="+(i+1)+", expected="+expected+", actual: "+actual);
            
            // first check the document.
            assertEquals("wrong document: rank="+(i+1),expected.getDocId(),actual.getDocId());

            // then verify the cosine.
            assertEquals("wrong cosine: rank="+(i+1),expected.getCosine(),actual.getCosine());
            
        }
        
        assertFalse("Iterator will visit too many hits - only " + nhits
                + " are expected", itr.hasNext());
        
    }
    
    protected static class HT implements IHit {

        final private long docId;
        final private double cosine;
        
        public HT(long docId,double cosine) {
            
            this.docId = docId;
            
            this.cosine = cosine;
            
        }
        
        public double getCosine() {
            
            return cosine;
            
        }

        public long getDocId() {

            return docId;
            
        }
        
        public String toString() {
            
            return "{docId="+docId+",cosine="+cosine+"}";
            
        }
        
    }
    
}
