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
package com.bigdata.rdf.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase2;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.load.IStatementBufferFactory;
import com.bigdata.rdf.load.LoadStatementBufferFactory;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.decls.FOAFVocabularyDecl;

public class AbstractGraphTestCase extends TestCase2 {

    public AbstractGraphTestCase() {
    }

    public AbstractGraphTestCase(String name) {
        super(name);
    }

    protected BigdataSail sail = null;

    @Override
    public Properties getProperties() {
        
        final Properties p = new Properties();

        p.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.MemStore.toString());
        
        /*
         * TODO Test both triples and quads.
         * 
         * Note: We need to use different data files for quads (trig). If we use
         * trig for a triples mode kb then we get errors (context bound, but not
         * quads mode).
         */
        p.setProperty(BigdataSail.Options.TRIPLES_MODE, "true");
//        p.setProperty(BigdataSail.Options.QUADS_MODE, "true");
        p.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        return p;
        
    }
    
    @Override
    protected void setUp() throws Exception {
        
        super.setUp();

        sail = new BigdataSail(getProperties());
        
    }
    
    @Override
    protected void tearDown() throws Exception {

        if(sail != null) {
            
            sail.__tearDownUnitTest();

            sail = null;
            
        }
        
        super.tearDown();
        
    }

    /**
     * 
     * @param resource
     *            A filename that is relative to the base of the project.
     * 
     */
    protected void loadGraph(final String resource) {

        try {

            _loadGraph(resource, sail.getDatabase());
            
//            final RDFFormat rdfFormat = RDFFormat.forFileName(resource);
//
//            // Load data.
//            final LoadStats loadStats = sail.getDatabase().getDataLoader()
//                    .loadData(resource, "file:" + resource, rdfFormat);
//
//            if (log.isInfoEnabled())
//                log.info(loadStats.toString());

        } catch (IOException ex) {

            fail(resource + " : " + ex, ex);

        } catch (URISyntaxException ex) {

            fail(resource + " : " + ex, ex);

        }

        // Commit.
        sail.getDatabase().commit();

    }

    /**
     * Load a resource from the classpath or the file system.
     * 
     * @param resource
     *            A resource on the class path, a file, or a directory.
     * 
     * @param factory
     * 
     * @throws IOException
     * @throws URISyntaxException
     */
    private void _loadGraph(final String resource, final AbstractTripleStore kb)
            throws IOException, URISyntaxException {

        if (log.isInfoEnabled())
            log.info("Loading: " + resource );

        String baseURI = null;
        
        InputStream rdfStream = null;
        try {

            // try the classpath
            rdfStream = getClass().getResourceAsStream(resource);

            if (rdfStream != null) {
                
                // set for resource on classpath.
                baseURI = getClass().getResource(resource).toURI().toString();
                
            } else {

                // try file system.
                final File file = new File(resource);

//                if (file.isHidden() || !file.canRead()
//                        || file.isDirectory()) {
//
//                    log.warn("Ignoring file: " + file);
//
//                    // Done.
//                    return;
//                    
//                }
                
                if (file.exists()) {

                    rdfStream = new FileInputStream(file);

                    // set for file as URI.
                    baseURI = file.toURI().toString();
                    
                } else {
                    
                    fail("Could not locate resource: " + resource);
                
                }
                
            }

            /*
             * Obtain a buffered reader on the input stream.
             */
            final Reader reader = new BufferedReader(new InputStreamReader(
                    rdfStream));

            try {

                // guess at the RDF Format, assume RDF/XML.
                final RDFFormat rdfFormat = RDFFormat.forFileName(resource,
                        RDFFormat.RDFXML);
                
                final RDFParserOptions options = new RDFParserOptions();
                
                // verify RDF/XML syntax.
                options.setVerifyData(true);


                final IStatementBufferFactory<? extends BigdataStatement> factory = new LoadStatementBufferFactory<BigdataStatement>(
                        kb, 10000/* bufferCapacity */);

                // Setup the loader.
                final PresortRioLoader loader = new PresortRioLoader(factory
                        .newStatementBuffer());

                // add listener to log progress.
                loader.addRioLoaderListener(new RioLoaderListener() {

                    public void processingNotification(RioLoaderEvent e) {

                        if (log.isInfoEnabled())
                            log.info(e.getStatementsProcessed() + " stmts added in "
                                    + (e.getTimeElapsed() / 1000d) + " secs, rate= "
                                    + e.getInsertRate());

                    }

                });

                loader.loadRdf((Reader) reader, baseURI, rdfFormat, baseURI, options);

                if (log.isInfoEnabled())
                    log.info("Done: " + resource);
//                + " : tps="
//                            + loader.getInsertRate() + ", elapsed="
//                            + loader.getInsertTime() + ", statementsAdded="
//                            + loader.getStatementsAdded());

            } catch (Exception ex) {

                throw new RuntimeException("While loading: " + resource, ex);

            } finally {

                try {
                    reader.close();
                } catch (Throwable t) {
                    log.error(t);
                }

            }

        } finally {

            if (rdfStream != null) {

                try {
                    rdfStream.close();
                } catch (Throwable t) {
                    log.error(t);
                }

            }

        }

    }

    
    /**
     * Make a set
     * 
     * @param a
     *            The objects for the set.
     *            
     * @return The set.
     */
    static protected <T> Set<T> set(final T...a) {
        final Set<T> tmp = new LinkedHashSet<T>();
        for(T x : a) {
            tmp.add(x);
        }
        return tmp;
    }

    /**
     * Assert that two sets are the same.
     */
    protected <T> void assertSameEdges(final Set<T> expected,
            final Set<T> actual) {

        final Set<T> tmp = new LinkedHashSet<T>();

        tmp.addAll(expected);

        for (T t : actual) {

            if (!tmp.remove(t)) {

                fail("Not expected: " + t);

            }

        }

        if (!tmp.isEmpty()) {

            fail("Expected but not found: " + tmp.toString());

        }

    }

    /**
     * A small foaf data set relating some of the project contributors (triples
     * mode data).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class SmallGraphProblem {

        /**
         * The data file.
         */
        static final String smallGraph = "bigdata-rdf/src/test/com/bigdata/rdf/graph/data/smallGraph.ttl";
        
        public final BigdataURI rdfType, foafKnows, foafPerson, mike, bryan,
                martyn;

        public SmallGraphProblem() {

            loadGraph(smallGraph);
            
            rdfType = (BigdataURI) sail.getValueFactory().createURI(
                    RDF.TYPE.stringValue());

            foafKnows = (BigdataURI) sail.getValueFactory().createURI(
                    FOAFVocabularyDecl.knows.stringValue());

            foafPerson = (BigdataURI) sail.getValueFactory().createURI(
                    FOAFVocabularyDecl.Person.stringValue());

            mike = (BigdataURI) sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Mike");

            bryan = (BigdataURI) sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Bryan");

            martyn = (BigdataURI) sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Martyn");

            final BigdataValue[] terms = new BigdataValue[] { rdfType,
                    foafKnows, foafPerson, mike, bryan, martyn };

            // batch resolve existing IVs.
            sail.getDatabase().getLexiconRelation()
                    .addTerms(terms, terms.length, true/* readOnly */);

            for (BigdataValue v : terms) {
                if (v.getIV() == null)
                    fail("Did not resolve: " + v);
            }

        }

    }

    /**
     * Load and setup the {@link SmallGraphProblem}.
     */
    protected SmallGraphProblem setupSmallGraphProblem() {

        return new SmallGraphProblem();

    }

}
