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
    
    public static final void main(String[] args) {
        
        if (args.length == 0) {

            System.err.println("usage: filename");

            System.exit(1);
            
        }
        
        final String filename = args[0];
        
        System.out.println("filename: " + filename);

        String query = 
            "select ?x where { ?x <"+RDF.TYPE+"> <"+LUBM.PROFESSOR+"> . }";
        ConcurrentReader reader = new ConcurrentReader(filename, query);
        Thread t = new Thread(reader);
        t.start();
        
        JiniFederation fed = null;

        try {

            fed = new JiniClient(new String[] { filename }).connect();

            final Properties properties = fed.getClient().getProperties(
                    ScaleOut.class.getName());
            
            for (Entry<Object,Object> e : properties.entrySet()) {
                
                log.info("property: " + e.getKey() + " = " + e.getValue());
                
            }
            
            final AbstractTripleStore tripleStore = new ScaleOutTripleStore(
                    fed, namespace, ITx.UNISOLATED, properties);
            final BigdataSail sail = new BigdataSail(tripleStore);
            final Repository repo = new BigdataSailRepository(sail);
            repo.initialize();

            loadU10(repo);
            
            repo.shutDown();
            
        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        } finally {

            fed.shutdownNow();
                
        }

        reader.kill();
        
        try {
            Thread.sleep(10000);
        } catch (Exception ex) { }
        
    }
    
    private static void loadU10(Repository repo) throws Exception {
        
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
                InputStream is = ScaleOut.class.getResourceAsStream("U10.zip");
                ZipInputStream zis = 
                    new ZipInputStream(new BufferedInputStream(is));
                ZipEntry ze = null;
                while ((ze = zis.getNextEntry()) != null) {
                    if (ze.isDirectory()) {
                        continue;
                    }
                    String name = ze.getName();
                    log.info(name);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
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
    
    private class Writer implements Runnable {
        
        private File config;
        
        public Writer(File config) {
            
            this.config = config;
            
        }
        
        public void run() {
            
        }
        
    }
    
    private static class ConcurrentReader implements Runnable {
        
        private String config;
        
        private boolean kill = false;
        
        private String query;
        
        public ConcurrentReader(String config, String query) {
            
            this.config = config;
            
            this.query = query;
            
        }
        
        public void kill() {
            
            this.kill = true;
            
        }
        
        public void run() {
            
            JiniFederation fed = null;
            
            try {

                fed = new JiniClient(new String[] { config }).connect();

                final Properties properties = fed.getClient().getProperties(
                        ScaleOut.class.getName());
                
                while (!kill) {
                    
                    doQuery(fed, properties);
                    
                    Thread.sleep(3000);
                    
                }
                
            } catch (Exception ex) {
                
                ex.printStackTrace();
                
            } finally {
                
                fed.shutdownNow();
                
            }
            
        }
        
        private void doQuery(JiniFederation fed, Properties properties) 
            throws Exception {
            
            long transactionId = 
                fed.getTransactionService().newTx(ITx.READ_COMMITTED);
            
            try {

                final AbstractTripleStore tripleStore = new ScaleOutTripleStore(
                        fed, namespace, transactionId, properties);
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
