package com.bigdata.samples;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
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

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

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
public class Concurrency {
    
    protected final static Logger log = Logger.getLogger(Concurrency.class);
    
    /**
     * The # of reader threads to create.
     */
    private static final int numReaders = 3;
    
    /**
     * A query asking for the full professors instances.
     */
    private static final String query = 
        "select ?x where { ?x <"+RDF.TYPE+"> <"+LUBM.FULL_PROFESSOR+"> . }";
    
    /**
     * Manage the control flow of the program.  Open a bigdata repository,
     * kick off the writer, kick off the readers, wait for the writer to 
     * complete, kill the readers, wait for the readers to complete, shutdown 
     * the repository.
     * 
     * @param args 
     */
    public static final void main(String[] args) {
        
        try {

            Properties properties = new Properties();
            
            // create a backing file
            File journal = File.createTempFile("bigdata", ".jnl");
            log.info(journal.getAbsolutePath());
            journal.deleteOnExit();
            properties.setProperty(
                BigdataSail.Options.FILE, 
                journal.getAbsolutePath()
                );

            // create the sail and the repository
            BigdataSail sail = new BigdataSail(properties);
            BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();

            // create the writer and readers
            BigdataWriter writer = new BigdataWriter(repo);
            Collection<BigdataReader> readers = new LinkedList<BigdataReader>();
            for (int i = 0; i < numReaders; i++) {
                readers.add(new BigdataReader(repo));
            }
            
            // launch the threads and get their futures
            // bigdata has an executor service but any executor service will do
            ExecutorService executor = Executors.newCachedThreadPool();
            Future writerFuture = executor.submit(writer);
            Collection<Future> readerFutures = new LinkedList<Future>();
            for (BigdataReader reader : readers) {
                readerFutures.add(executor.submit(reader));
            }
            
            // wait for writer to complete
            writerFuture.get();
            
            // kill the readers
            for (BigdataReader reader : readers) {
                reader.kill();
            }
            
            // wait for readers to complete
            for (Future readerFuture : readerFutures) {
                readerFuture.get();
            }
            
            repo.shutDown();

        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        }
        
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
         * The bigdata repository
         */
        private BigdataSailRepository repo;
        
        /**
         * Construct the writer task.
         * 
         * @param fed the bigdata repository
         */
        public BigdataWriter(BigdataSailRepository repo) {
            
            this.repo = repo;
            
        }
        
        /**
         * Opens the triple store and writes the LUBM ontology and U10 data
         * files.  Does a commit after every file, which is not the most
         * efficient way to bulk load, but simulates incremental updates. 
         */
        public void run() {

            try {
                
                // load the data
                loadU10(repo);
                
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
                // fast range count!
                long stmtsBefore = ((BigdataSailRepository)repo).getDatabase().getStatementCount();
//                // full index scan!
//                long stmtsBefore = cxn.size();
                log.info("statements before: " + stmtsBefore);
                long start = System.currentTimeMillis();
                
                { // first add the LUBM ontology
                    InputStream is = 
                        Concurrency.class.getResourceAsStream("univ-bench.owl");
                    Reader reader = 
                        new InputStreamReader(new BufferedInputStream(is));
                    cxn.add(reader, LUBM.NS, RDFFormat.RDFXML);
                    cxn.commit();
                }
                
                { // then process the LUBM sample data files one at a time
                    InputStream is = 
                        Concurrency.class.getResourceAsStream("U10.zip");
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
                // fast range count!
                long stmtsAfter = ((BigdataSailRepository)repo).getDatabase().getStatementCount();
//                long stmtsAfter = cxn.size();
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
         * The bigdata repository
         */
        private BigdataSailRepository repo;
        
        /**
         * Allows the reader to be stopped gracefully.
         */
        private volatile boolean kill = false;
        
        /**
         * Create the reader.
         * 
         * @param fed the bigdata repository
         */
        public BigdataReader(BigdataSailRepository repo) {
            
            this.repo = repo;
            
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
         * professors.  Does this every few seconds until killed.
         */
        public void run() {
            
            try {

                while (!kill) {
                    
                    doQuery();
                    
                    // sleep somewhere between 0 and 3 seconds
                    Thread.sleep((int) (Math.random() * 3000d));
                    
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
           
            RepositoryConnection cxn = repo.getQueryConnection();
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
                
        }
        
    }

    
}
