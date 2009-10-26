package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * This is a test class that fails on centos 64bit. On working machines, it runs
 * and always prints out 100, but on centos it will randomly print out 0. Note
 * that the test as written is specific to a quad store (it makes the quad store
 * assumption about the context position). (This issue has been linked to a bug
 * in {@link BlockingBuffer#add(Object)}, where it was failing to test the
 * return code from
 * {@link BlockingBuffer#add(Object, long, java.util.concurrent.TimeUnit)} and
 * the latter had a bug in its timeout logic.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestCentos extends AbstractTripleStoreTestCase {

    public Properties getProperties() {
        
        final Properties properties = new Properties(super.getProperties());
        
        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        properties.setProperty(AbstractTripleStore.Options.STORE_BLANK_NODES, "true");
        properties.setProperty(AbstractTripleStore.Options.JUSTIFY, "false");
//        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");
        properties.setProperty(AbstractTripleStore.Options.BLOOM_FILTER, "false");
        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        properties.setProperty(AbstractTripleStore.Options.NESTED_SUBQUERY, "false");

        return properties;
        
    }
    
    public void test_stress() {

        final AbstractTripleStore store = getStore(getProperties());
        
        try {

            if(!store.isQuads()) {
                
                // The test as written is specific to a quad store.
                return;
                
            }
            
            /*
             * Populate N graphs of M statements each.
             */
            
            final int N = 1000, M = 100;

            for (int k = 0; k < N; k++) {
                
                StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
                        store, 20000);
                
                BigdataURI g = store.getValueFactory().createURI(
                        "http://test/g" + k);

                for (int l = 0; l < M; l++) {

                    BigdataURI s = store.getValueFactory().createURI(
                            "http://test/s" + (l % 99));
                    
                    BigdataURI p = store.getValueFactory().createURI(
                            "http://test/p" + (l % 37));
                    
                    BigdataURI o = store.getValueFactory().createURI(
                            "http://test/o" + (l % 399));
                    
                    sb.add(store.getValueFactory().createStatement(s, p, o, g,
                            StatementEnum.Explicit));
                
                }
                
                sb.flush();
                
                store.commit();

                AbstractTripleStore readStore = (AbstractTripleStore) store
                        .getIndexManager()
                        .getResourceLocator()
                        .locate(
                                store.getNamespace(),
                                TimestampUtility.asHistoricalRead(store
                                        .getIndexManager().getLastCommitTime()));

                /*
                 * Verify the #of statements in the most recently written graph.
                 */
                int size = 0;
                
                BigdataStatementIterator iter = readStore.getStatements(null,
                        null, null, g);
                
                while (iter.hasNext()) {
                
                    iter.next();
                    
                    size++;
                    
                }

                System.err.println("Size=" + size);
                
                assertEquals("size", M, size);
                
            }

        } finally {

            store.destroy();

        }
        
    }

}
