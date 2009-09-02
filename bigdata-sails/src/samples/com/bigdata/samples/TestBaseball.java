package com.bigdata.samples;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;

public class TestBaseball extends SampleCode {
    
    public static final void main(String[] args) {
        try {
            SampleCode sampleCode = new TestBaseball();
            
            // use one of our pre-configured option-sets or "modes"
            Properties properties = 
                sampleCode.loadProperties("fullfeature.properties");
            
            // when loading a large data file, it's sometimes better to do
            // database-at-once closure rather than incremental closure.  this
            // is how you do it.
            properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

            // we won't be doing any retraction, so no justifications either
            properties.setProperty(BigdataSail.Options.JUSTIFY, "false");

            // no free text search
            properties.setProperty(BigdataSail.Options.TEXT_INDEX, "false");

            // no statement identifiers
            properties.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");

            // create a backing file
            File journal = File.createTempFile("baseball", ".jnl");
            log.info(journal.getAbsolutePath());
            // journal.deleteOnExit();
            properties.setProperty(
                BigdataSail.Options.FILE, 
                journal.getAbsolutePath()
                );
            
            // instantiate a sail
            BigdataSail sail = new BigdataSail(properties);
            Repository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            // demonstrate some basic functionality
            String resource = "baseball.stats.out.rdf";
            String baseURL = "http://www.clarkparsia.com/#";
            
            long start = System.currentTimeMillis();
            
            sampleCode.loadSomeDataFromADocument(repo, resource, baseURL);
            
            long duration = System.currentTimeMillis() - start;
            
            RepositoryConnection cxn = repo.getConnection();
            long size = cxn.size();
            cxn.close();
            
            log.info("loaded " + size + " stmts in " + duration + " millis.");
            
            long rate = (long) (((double) size) / ((double) duration) * 1000d);
            
            log.info("rate: " + rate + " stmts/sec");
            
            // here is what I'm seeing:
            // Wed Sep 02 14:14:22 MDT 2009
            // INFO : 285047 main         com.bigdata.samples.TestBaseball.main(TestBaseball.java:73): loaded 3294798 stmts in 282282 millis.
            // INFO : 285047 main         com.bigdata.samples.TestBaseball.main(TestBaseball.java:77): rate: 11672 stmts/sec
            
            repo.shutDown();
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * I overrided this method so that I could explicitly do database-at-once
     * closure after the data has been loaded.  Incremental truth maintenance
     * is not appropriate in this case.
     */
    @Override
    public void loadSomeDataFromADocument(Repository repo, String resource, 
        String baseURL) throws Exception {
        
        BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) 
            repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            InputStream is = getClass().getResourceAsStream(resource);
            Reader reader = new InputStreamReader(new BufferedInputStream(is));
            cxn.add(reader, baseURL, RDFFormat.RDFXML);
            cxn.computeClosure();
            cxn.commit();
        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }
    
}
