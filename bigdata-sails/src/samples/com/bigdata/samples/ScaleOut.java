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

public class ScaleOut {
    
    protected final static Logger log = Logger.getLogger(ScaleOut.class);
    
    private static final String namespace = "kb";
    
    private static final String query = 
        "select ?x where { ?x <"+RDF.TYPE+"> <"+LUBM.PROFESSOR+"> . }";
    
    public static final void main(String[] args) {
        
        if (args.length == 0) {

            System.err.println("usage: filename");

            System.exit(1);
            
        }
        
        final String config = args[0];
        
        log.info("config: " + config);

        JiniFederation fed = null;

        try {

            fed = new JiniClient(new String[] { config }).connect();

            // force the triple store to be created if it doesn't already exist
            createTripleStore(fed);
            
            BigdataWriter writer = new BigdataWriter(fed);
            
            BigdataReader reader = new BigdataReader(fed);
            
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

            if (fed != null) fed.shutdownNow();
                
        }

    }
    
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
    
    private static class BigdataWriter implements Runnable {
        
        private JiniFederation fed;
        
        private boolean kill = false;
        
        public BigdataWriter(JiniFederation fed) {
            
            this.fed = fed;
            
        }
        
        public void run() {

            try {
                
                // get the unisolated triple store for writing
                final AbstractTripleStore tripleStore = 
                    openTripleStore(fed, ITx.UNISOLATED);
                
                final BigdataSail sail = new BigdataSail(tripleStore);
                final Repository repo = new BigdataSailRepository(sail);
                repo.initialize();
                
                loadU10(repo);
                
                repo.shutDown();
                
            } catch (Exception ex) {
                
                ex.printStackTrace();
                
            }
            
        }
        
        private void loadU10(Repository repo) throws Exception {
            
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
    
    private static class BigdataReader implements Runnable {
        
        private JiniFederation fed;
        
        private boolean kill = false;
        
        public BigdataReader(JiniFederation fed) {
            
            this.fed = fed;
            
        }
        
        public void kill() {
            
            this.kill = true;
            
        }
        
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
        
        private void doQuery() throws Exception {
            
            long transactionId = 
                fed.getTransactionService().newTx(ITx.READ_COMMITTED);
            
            try {

                final AbstractTripleStore tripleStore = 
                    openTripleStore(fed, transactionId);
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
                
                fed.getTransactionService().abort(transactionId);
                
            }
            
        }
        
    }

    
}
