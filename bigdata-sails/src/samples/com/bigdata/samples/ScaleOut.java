package com.bigdata.samples;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * This class demonstrates concurrent reading and writing with the U10 data set
 * on the scale-out architecture.  One thread writes the U10 data files, doing
 * a commit after every file (this is not the fastest way to perform load, as
 * it simulates incremental updates vs bulk load).  Another thread asks for
 * the number of "FullProfessors" every three seconds.  You can watch the
 * number of results rise as more data is loaded.
 * 
 * @author mikep
 *
 */
public class ScaleOut {
    
    protected final static Logger log = Logger.getLogger(ScaleOut.class);
    
    /**
     * The name of the triple store instance inside the scale-out database.
     */
    private static final String namespace = "kb";
    
    /**
     * A query asking for the full professors instances.
     */
    private static final String query = 
        "select ?x where { ?x <"+RDF.TYPE+"> <"+LUBM.FULL_PROFESSOR+"> . }";
    
    /**
     * Manage the control flow of the program.  Open a proxy to the federation,
     * kick off the writer, kick off the reader, wait for the writer to 
     * complete, kill the reader, wait for the reader to complete, shutdown the 
     * federation.
     * 
     * @param args specify the location of the cluster's JINI config file
     */
    public static final void main(String[] args) {
        
        if (args.length == 0) {
            System.err.println("usage: filename");
            System.exit(1);
        }
        
        final String config = args[0];
        log.info("config: " + config);

        JiniFederation fed = null;

        try {

            fed = new JiniClient(args).connect();

            // force the triple store to be created if it doesn't already exist
            createTripleStore(fed);

            // create the writer and reader
            BigdataWriter writer = new BigdataWriter(fed);
            BigdataReader reader = new BigdataReader(fed);
            
            // launch the threads and get their futures
            // bigdata has an executor service but any executor service will do
            Future writerFuture = fed.getExecutorService().submit(writer);
            Future readerFuture = fed.getExecutorService().submit(reader);
            
            // wait for writer to complete
            writerFuture.get();
            
            // kill the reader
            reader.kill();
            
            // wait for reader to complete
            readerFuture.get();

        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        } finally {

            if (fed != null) fed.shutdown();
                
        }

    }
    
    /**
     * Create our triple store instance if it doesn't exist.
     * 
     * @param fed the jini federation
     * @return the triple store instance
     * @throws Exception
     */
    private static AbstractTripleStore createTripleStore(JiniFederation fed) 
        throws Exception {

        AbstractTripleStore tripleStore = null;
        
        // locate the resource declaration (aka "open").
        tripleStore = (AbstractTripleStore) fed.getResourceLocator().locate(
            namespace, ITx.UNISOLATED);
        
        if (tripleStore == null) {
            
            /*
             * Pick up properties configured for the client as defaults.
             */
            final Properties properties = fed.getClient().getProperties(
                    ScaleOut.class.getName());
            
            tripleStore = new ScaleOutTripleStore(
                fed, namespace, ITx.UNISOLATED, properties);

            // create the triple store.
            tripleStore.create();
            
        }
        
        return tripleStore;
        
    }

    /**
     * Lookup the triple store instance using the specified timestamp.  Pass in
     * ITx.UNISOLATED for the writable instance (not safe for concurrent 
     * readers), otherwise use a transaction id or timestamp for a historical 
     * view.  This is demonstrated below.  
     * 
     * @param fed the jini federation
     * @param timestamp the timestamp or transaction id
     * @return the triple store instance
     * @throws Exception
     */
    private static AbstractTripleStore openTripleStore(
        JiniFederation fed, long timestamp) throws Exception {
        
        AbstractTripleStore tripleStore = null;
        
        // locate the resource declaration (aka "open").
        tripleStore = (AbstractTripleStore) fed.getResourceLocator().locate(
            namespace, timestamp);
        
        if (tripleStore == null) {

            throw new RuntimeException("triple store does not exist!");
            
        }
        
        return tripleStore;
        
    }
/*    
    private static void testQuery(JiniFederation fed) throws Exception {
        
        long transactionId =  
            //ITx.UNISOLATED;
            fed.getTransactionService().newTx(ITx.READ_COMMITTED);
        
        log.info("transaction id = " + 
            (transactionId == ITx.UNISOLATED ? "UNISOLATED" : transactionId));
        
        try {
            
            // get the unisolated triple store for writing
            final AbstractTripleStore tripleStore = 
                openTripleStore(fed, transactionId);
            
            final BigdataSail sail = new BigdataSail(tripleStore);
            final Repository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            RepositoryConnection cxn = repo.getConnection();
            try {

                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true);
                TupleQueryResult result = tupleQuery.evaluate();
                // do something with the results
                int resultCount = 0;
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    log.info(bindingSet);
                    resultCount++;
                }
                log.info(resultCount + " results");
                
            } finally {
                // close the repository connection
                cxn.close();
            }
            
            repo.shutDown();
            
        } finally {
            
            if (transactionId != ITx.UNISOLATED) {
                
                fed.getTransactionService().abort(transactionId);
                
            }
            
        }
        
    }
*/
    
    /**
     * A writer task to load the U10 data set.
     */
    private static class BigdataWriter implements Runnable {
        
        /**
         * The jini federation.
         */
        private JiniFederation fed;
        
        /**
         * Construct the writer task.
         * 
         * @param fed the jini federation
         */
        public BigdataWriter(JiniFederation fed) {
            
            this.fed = fed;
            
        }
        
        /**
         * Opens the triple store and writes the LUBM ontology and U10 data
         * files.  Does a commit after every file, which is not the most
         * efficient way to bulk load, but simulates incremental updates. 
         */
        public void run() {

            try {
                
                // get the unisolated triple store for writing
                final AbstractTripleStore tripleStore = 
                    openTripleStore(fed, ITx.UNISOLATED);

                // wrap the triple store in a Sesame SAIL
                final BigdataSail sail = new BigdataSail(tripleStore);
                final Repository repo = new BigdataSailRepository(sail);
                repo.initialize();
                
                // load the data
                loadU10(repo);
                
                // shut it down
                repo.shutDown();
                
            } catch (Exception ex) {
                
                ex.printStackTrace();
                
            }
            
        }
        
        /**
         * Load the LUBM ontology and U10 data into a Sesame Repository.
         * 
         * @param repo the sesame repository
         * @throws Exception
         */
        private void loadU10(Repository repo) throws Exception {
            
            // always, always autocommit = false
            RepositoryConnection cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            
            try {
                long stmtsBefore = cxn.size();
                log.info("statements before: " + stmtsBefore);
                long start = System.currentTimeMillis();
                
                { // first add the LUBM ontology
                    InputStream is = 
                        ScaleOut.class.getResourceAsStream("univ-bench.owl");
                    Reader reader = 
                        new InputStreamReader(new BufferedInputStream(is));
                    cxn.add(reader, LUBM.NS, RDFFormat.RDFXML);
                    cxn.commit();
                }
                
                { // then process the LUBM sample data files one at a time
                    InputStream is = 
                        ScaleOut.class.getResourceAsStream("U10.zip");
                    ZipInputStream zis = 
                        new ZipInputStream(new BufferedInputStream(is));
                    ZipEntry ze = null;
                    while ((ze = zis.getNextEntry()) != null) {
                        if (ze.isDirectory()) {
                            continue;
                        }
                        String name = ze.getName();
                        log.info(name);
                        ByteArrayOutputStream baos = 
                            new ByteArrayOutputStream();
                        byte[] bytes = new byte[4096];
                        int count;
                        while ((count = zis.read(bytes, 0, 4096)) != -1) {
                            baos.write(bytes, 0, count);
                        }
                        baos.close();
                        Reader reader = new InputStreamReader(
                            new ByteArrayInputStream(baos.toByteArray())
                            );
                        cxn.add(reader, LUBM.NS, RDFFormat.RDFXML);
                        cxn.commit();
                    }
                    zis.close();
                }
                
                // gather statistics
                long elapsed = System.currentTimeMillis() - start;
                long stmtsAfter = cxn.size();
                long stmtsAdded = stmtsAfter - stmtsBefore;
                int throughput =
                        (int) ((double) stmtsAdded / (double) elapsed * 1000d);
                log.info("statements after: " + stmtsAfter);
                log.info("loaded: " + stmtsAdded + " in " + elapsed
                        + " millis: " + throughput + " stmts/sec");
            
            } catch (Exception ex) {
                cxn.rollback();
                throw ex;
            } finally {
                // close the repository connection
                cxn.close();
            }
            
        }
        
    }
    
    /**
     * A reader task to issue concurrent queries.  Asks for the # of full
     * professors every three seconds.
     */
    private static class BigdataReader implements Runnable {
        
        /**
         * The jini federation.
         */
        private JiniFederation fed;
        
        /**
         * Allows the reader to be stopped gracefully.
         */
        private volatile boolean kill = false;
        
        /**
         * Create the reader.
         * 
         * @param fed the jini federation
         */
        public BigdataReader(JiniFederation fed) {
            
            this.fed = fed;
            
        }
        
        /**
         * Kills the reader gracefully.
         */
        public void kill() {
            
            this.kill = true;
            
        }
        
        /**
         * Opens a read-committed view of the triple store using the last
         * commit point and issues a query for a list of all LUBM full 
         * professors.  Does this every three seconds until killed.
         */
        public void run() {
            
            try {

                while (!kill) {
                    
                    doQuery();
                    
                    Thread.sleep(3000);
                    
                }
                
            } catch (Exception ex) {
                
                ex.printStackTrace();
                
            }
            
        }
        
        /**
         * Issue the query.
         * 
         * @throws Exception
         */
        private void doQuery() throws Exception {
           
            // this is how you get a read-only transaction.  MUST be
            // committed or aborted later, see below.
            long transactionId = 
                fed.getTransactionService().newTx(ITx.READ_COMMITTED);
            
            try {

                // open the read-only triple store
                final AbstractTripleStore tripleStore = 
                    openTripleStore(fed, transactionId);
                
                // wrap it in a Sesame SAIL
                final BigdataSail sail = new BigdataSail(tripleStore);
                final Repository repo = new BigdataSailRepository(sail);
                repo.initialize();
                
                RepositoryConnection cxn = repo.getConnection();
                try {

                    final TupleQuery tupleQuery = 
                        cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                    tupleQuery.setIncludeInferred(true /* includeInferred */);
                    TupleQueryResult result = tupleQuery.evaluate();
                    // do something with the results
                    int resultCount = 0;
                    while (result.hasNext()) {
                        BindingSet bindingSet = result.next();
                        // log.info(bindingSet);
                        resultCount++;
                    }
                    log.info(resultCount + " results");
                    
                } finally {
                    // close the repository connection
                    cxn.close();
                }
                
                repo.shutDown();
                
            } finally {
                
                // MUST close the transaction, abort is sufficient
                fed.getTransactionService().abort(transactionId);
                
            }
            
        }
        
    }

    
}
