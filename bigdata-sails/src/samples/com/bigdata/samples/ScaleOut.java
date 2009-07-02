package com.bigdata.samples;

import java.io.File;
import java.util.Properties;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailHelper;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.EmbeddedClient;
import com.bigdata.service.LocalDataServiceClient;
import com.bigdata.service.jini.JiniClient;

public class ScaleOut {
    
    public static final void main(String[] args) {
        
        if (args.length == 0) {

            System.err.println("usage: filename");

            System.exit(1);
            
        }
        
        final String filename = args[0];
        
        final File file = new File(filename);
        
        final String namespace = "kb";

        final long timestamp = ITx.UNISOLATED;

        final BigdataSailHelper helper = new BigdataSailHelper();

        System.out.println("filename: " + filename);

        AbstractFederation fed = null;

        BigdataSail sail = null;

        try {

            fed = new JiniClient(new String[] { args[0] }).connect();

            sail = helper.getSail(fed, namespace, timestamp);

            Repository repo = new BigdataSailRepository(sail);
            
            repo.initialize();
            
            SampleCode sampleCode = new SampleCode();
            
            // demonstrate some basic functionality
            URI MIKE = new URIImpl("http://www.bigdata.com/rdf#Mike");
            sampleCode.loadSomeData(repo);
            sampleCode.readSomeData(repo, MIKE);
            sampleCode.executeSelectQuery(repo, "select ?p ?o where { <"+MIKE.toString()+"> ?p ?o . }", QueryLanguage.SPARQL);
            sampleCode.executeConstructQuery(repo, "construct { <"+MIKE.toString()+"> ?p ?o . } where { <"+MIKE.toString()+"> ?p ?o . }", QueryLanguage.SPARQL);
            
            repo.shutDown();

        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        } finally {

            try {
            
                sail.shutDown();

            } catch (Exception ex) {

                ex.printStackTrace();
                
            }
            
            fed.shutdownNow();
                
        }

    }
}
