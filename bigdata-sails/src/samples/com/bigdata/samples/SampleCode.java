package com.bigdata.samples;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.store.BNS;

/**
 * Demonstrate how to use bigdata.
 * 
 * @author mikep
 */
public class SampleCode {
    
    /**
     * Do you have log4j set up correctly?  Bigdata turns its logging level way
     * down by default (WARN).  You should not be seeing INFO or DEBUG log 
     * statements from bigdata - if you are, then this will severely impact
     * performance.
     */
    protected static final Logger log = Logger.getLogger(SampleCode.class);

    /**
     * Load a Properties object from a file.
     * 
     * @param resource
     * @return
     * @throws Exception
     */
    public Properties loadProperties(String resource) throws Exception {
        Properties p = new Properties();
        InputStream is = getClass().getResourceAsStream(resource);
        p.load(new InputStreamReader(new BufferedInputStream(is)));
        return p;
    }
    
    /**
     * Add a statement to a repository.
     * 
     * @param repo
     * @throws Exception
     */
    public void loadSomeData(Repository repo) throws Exception {
        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            Resource s = new URIImpl("http://www.bigdata.com/rdf#Mike");
            URI p = new URIImpl("http://www.bigdata.com/rdf#loves");
            Value o = new URIImpl("http://www.bigdata.com/rdf#RDF");
            Statement stmt = new StatementImpl(s, p, o);
            cxn.add(stmt);
            cxn.commit();
        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }
    }

    /**
     * Load a document into a repository.
     * 
     * @param repo
     * @param resource
     * @param baseURL
     * @throws Exception
     */
    public void loadSomeDataFromADocument(Repository repo, String resource, 
        String baseURL) throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            InputStream is = getClass().getResourceAsStream(resource);
            Reader reader = new InputStreamReader(new BufferedInputStream(is));
            cxn.add(reader, baseURL, RDFFormat.RDFXML);
            cxn.commit();
        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }
    
    /**
     * Read some statements from a repository.
     * 
     * @param repo
     * @param uri
     * @throws Exception
     */
    public void readSomeData(Repository repo, URI uri) throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        try {
            
            RepositoryResult<Statement> stmts = 
                cxn.getStatements(uri, null, null, true /* include inferred */);
            while (stmts.hasNext()) {
                Statement stmt = stmts.next();
                Resource s = stmt.getSubject();
                URI p = stmt.getPredicate();
                Value o = stmt.getObject();
                // do something with the statement
                log.info(stmt);
                
                // cast to BigdataStatement to get at additional information
                BigdataStatement bdStmt = (BigdataStatement) stmt;
                if (bdStmt.isExplicit()) {
                    // do one thing
                } else if (bdStmt.isInferred()) {
                    // do another thing
                } else { // bdStmt.isAxiom()
                    // do something else
                }
                log.info(bdStmt.getStatementType());
            }
            
        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }
    
    /**
     * Execute a "select" query.
     * 
     * @param repo
     * @param query
     * @param ql
     * @throws Exception
     */
    public void executeSelectQuery(Repository repo, String query, 
        QueryLanguage ql) throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        try {

            final TupleQuery tupleQuery = cxn.prepareTupleQuery(ql, query);
            tupleQuery.setIncludeInferred(true /* includeInferred */);
            TupleQueryResult result = tupleQuery.evaluate();
            // do something with the results
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                log.info(bindingSet);
            }
            
        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }
        
    /**
     * Execute a "construct" query.
     * 
     * @param repo
     * @param query
     * @param ql
     * @throws Exception
     */
    public void executeConstructQuery(Repository repo, String query, 
        QueryLanguage ql) throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        try {

            // silly construct queries, can't guarantee distinct results
            final Set<Statement> results = new LinkedHashSet<Statement>();
            final GraphQuery graphQuery = cxn.prepareGraphQuery(ql, query);
            graphQuery.setIncludeInferred(true /* includeInferred */);
            graphQuery.evaluate(new StatementCollector(results));
            // do something with the results
            for (Statement stmt : results) {
                log.info(stmt);
            }

        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }

    /**
     * Demonstrate execution of a free-text query.
     * 
     * @param repo
     * @throws Exception
     */
    public void executeFreeTextQuery(Repository repo) throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            cxn.add(new URIImpl("http://www.bigdata.com/A"), RDFS.LABEL,
                    new LiteralImpl("Yellow Rose"));
            cxn.add(new URIImpl("http://www.bigdata.com/B"), RDFS.LABEL,
                    new LiteralImpl("Red Rose"));
            cxn.add(new URIImpl("http://www.bigdata.com/C"), RDFS.LABEL,
                    new LiteralImpl("Old Yellow House"));
            cxn.add(new URIImpl("http://www.bigdata.com/D"), RDFS.LABEL,
                    new LiteralImpl("Loud Yell"));
            cxn.commit();
        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }
        
        String query = "select ?x where { ?x <"+BNS.SEARCH+"> \"Yell\" . }";
        executeSelectQuery(repo, query, QueryLanguage.SPARQL);
        // will match A, C, and D
        
    }

    /**
     * Demonstrate execution of statement level provenance.
     * 
     * @param repo
     * @throws Exception
     */
    public void executeProvenanceQuery(Repository repo) throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            cxn.remove((Resource)null, (URI)null, (Value)null);
            cxn.commit();
            
            cxn.add(getReader(getClass(), "provenance.rdf"), 
                "", RDFFormat.RDFXML);
            cxn.commit();
            
            RepositoryResult<Statement> results = 
                cxn.getStatements(null, null, null, false);
            while(results.hasNext()) {
                log.info(results.next());
            }
            
        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }

        String NS = "http://www.bigdata.com/rdf#";
        String MIKE = NS + "Mike";
        String LOVES = NS + "loves";
        String RDF = NS + "RDF";
        String query = 
            "construct { ?sid ?p ?o } " +
            "where { " +
            "  ?sid ?p ?o ." +
            "  graph ?sid { <"+MIKE+"> <"+LOVES+"> <"+RDF+"> } " +
            "}";
        executeConstructQuery(repo, query, QueryLanguage.SPARQL);
        // should see the provenance information for { Mike loves RDF }
        
    }

    /**
     * Run a simple LUBM load and query benchmark.
     * 
     * @param lubmResource the ZIP file containng the LUBM data files
     * @param filter helps filter out non-data files in the ZIP file
     * @throws Exception
     */
    public void doLUBMTest(final String lubmResource, final String filter) 
        throws Exception {
        
        /*
         We are going to use the "fast load" mode for this LUBM test.  In fast 
         mode, we lose certain features, like the full text index and statement 
         identifiers.  The database also does not do inference automatically, so
         we have to tell the inference engine explicitly when to compute 
         closure.  Also, there is no recording of justification chains for 
         inferences, so this mode is extremely bad for retraction.  If we were 
         to retract a statement, we would have to tell the inference engine to 
         remove all inferences and completely re-compute the closure for the 
         entire database!
         */
        Properties properties = loadProperties("fastload.properties");
        
        // create a backing file
        File journal = File.createTempFile("bigdata", ".jnl");
        journal.deleteOnExit();
        properties.setProperty(
            BigdataSail.Options.FILE, 
            journal.getAbsolutePath()
            );
        
        // instantiate a sail
        BigdataSail sail = new BigdataSail(properties);
        Repository repo = new BigdataSailRepository(sail);
        repo.initialize();
        
        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        try {
            long stmtsBefore = cxn.size();
            log.info("statements before: " + stmtsBefore);
            long start = System.currentTimeMillis();
            
            // first add the LUBM ontology
            cxn.add(getReader(getClass(), "univ-bench.owl"), LUBM.NS,
                    RDFFormat.RDFXML);
            
            // then process the LUBM sample data files one at a time
            InputStream is = getClass().getResourceAsStream(lubmResource);
            ZipInputStream zis = 
                new ZipInputStream(new BufferedInputStream(is));
            ZipEntry ze = null;
            while ((ze = zis.getNextEntry()) != null) {
                if (ze.isDirectory()) {
                    continue;
                }
                String name = ze.getName();
                log.info(name);
                if (!name.startsWith(filter)) {
                    continue;
                }
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
            }
            zis.close();
            
            // autocommit is false, we need to commit our SAIL "transaction"
            cxn.commit();
            
            // when we are in "fast load" mode there is no automatic inference
            // as statements are loaded.  we therefore must invoke the inference
            // engine ourselves when we are done loading data.
            BigdataSailConnection sailCxn = (BigdataSailConnection)
                ((BigdataSailRepositoryConnection) cxn).getSailConnection();
            sailCxn.computeClosure();
            sailCxn.getTripleStore().commit();

            // gather statistics
            long elapsed = System.currentTimeMillis() - start;
            long stmtsAfter = cxn.size();
            long stmtsAdded = stmtsAfter - stmtsBefore;
            int throughput =
                    (int) ((double) stmtsAdded / (double) elapsed * 1000d);
            log.info("statements after: " + stmtsAfter);
            log.info("loaded: " + stmtsAdded + " in " + elapsed
                    + " millis: " + throughput + " stmts/sec");
            
            // ok, now let's do one of the LUBM queries
/*
[query4]
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>
SELECT ?x ?y1 ?y2 ?y3
WHERE{
    ?x a ub:Professor;
        ub:worksFor <http://www.Department0.University0.edu>;
        ub:name ?y1;
        ub:emailAddress ?y2;
        ub:telephone ?y3.
}
*/
            // build LUBM query 4 using the handy SparqlBuilder utility
            // note: SparqlBuilder is for "construct" queries only
            // but you could modify it easily to do "select" instead
            SparqlBuilder sb = new SparqlBuilder();
            sb.addTriplePattern("?x", RDF.TYPE, LUBM.PROFESSOR);
            sb.addTriplePattern("?x", LUBM.WORKS_FOR, 
                    new URIImpl("http://www.Department0.University0.edu"));
            sb.addTriplePattern("?x", LUBM.NAME, "?y1");
            sb.addTriplePattern("?x", LUBM.EMAIL_ADDRESS, "?y2");
            sb.addTriplePattern("?x", LUBM.TELEPHONE, "?y3");

            log.info("evaluating LUBM query 4...");
            start = System.currentTimeMillis();
            
            final GraphQuery graphQuery = 
                cxn.prepareGraphQuery(QueryLanguage.SPARQL, sb.toString());
            final StringWriter sw = new StringWriter();
            graphQuery.evaluate(new RDFXMLWriter(sw));
            
            elapsed = System.currentTimeMillis() - start;
            log.info("done. evaluted in " + elapsed + " millis");
            
            // if you want to see the output, here it is:
            log.info(sw.toString());
            
        } catch (Exception ex) {
            cxn.rollback();
            throw ex;
        } finally {
            // close the repository connection
            cxn.close();
        }
        
    }
    
    public void doU10() throws Exception {
        doLUBMTest("U10.zip", "U10/University");
    }

    public void doU1() throws Exception {
        doLUBMTest("U1.zip", "U1/University");
    }

    public Reader getReader(Class c, String resource) {
        InputStream is = c.getResourceAsStream(resource);
        return new InputStreamReader(new BufferedInputStream(is));
    }
    
    /**
     * Are you running with the -server JVM option?  You should be.  Also, try
     * increasing heap size with -Xmx512m or even -Xmx1024m if you have enough
     * RAM.
     * 
     * @param args
     */
    public static final void main(String[] args) {
        try {
            SampleCode sampleCode = new SampleCode();
            
            // use one of our pre-configured option-sets or "modes"
            Properties properties = 
                sampleCode.loadProperties("fullfeature.properties");
                //sampleCode.loadProperties("rdfonly.properties");
            //sampleCode.loadProperties("fastload.properties");
            
            // create a backing file
            File journal = File.createTempFile("bigdata", ".jnl");
            journal.deleteOnExit();
            properties.setProperty(
                BigdataSail.Options.FILE, 
                journal.getAbsolutePath()
                );
            
            // instantiate a sail
            BigdataSail sail = new BigdataSail(properties);
            Repository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            // demonstrate some basic functionality
            URI MIKE = new URIImpl("http://www.bigdata.com/rdf#Mike");
            sampleCode.loadSomeData(repo);
            sampleCode.readSomeData(repo, MIKE);
            sampleCode.executeSelectQuery(repo, "select ?p ?o where { <"+MIKE.toString()+"> ?p ?o . }", QueryLanguage.SPARQL);
            sampleCode.executeConstructQuery(repo, "construct { <"+MIKE.toString()+"> ?p ?o . } where { <"+MIKE.toString()+"> ?p ?o . }", QueryLanguage.SPARQL);
            sampleCode.executeFreeTextQuery(repo);
            sampleCode.executeProvenanceQuery(repo);
            
            repo.shutDown();
            
            // run one of the LUBM tests
            //sampleCode.doU10(); // I see loaded: 1752215 in 116563 millis: 15032 stmts/sec, what do you see?
            //sampleCode.doU1();
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
}
