package benchmark.bigdata;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Properties;

import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import benchmark.testdriver.TestDriver;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataQueryDriver {
    /**
     * @param args
     *            USAGE: [bsbm args...] propertyFile
     *            
     * @see TestDriver for other options.
     */
    public static void main(final String[] args) {
        Journal jnl = null;
        /*
         * @todo this is the default kb namespace. it should be a parameter to
         * the test harness since you can have multiple instances in a single
         * journal.  It could the the "SPARQL-Endpoint" parameter.
         */
        final String namespace = "kb";
        try {
            if (args.length < 1) {
                System.err.println("USAGE: [bsbm args...] propertyFile");
                System.exit(1);
            }
            final String propertyFile = args[args.length - 1];
            final File file = new File(propertyFile);
            if (!file.exists()) {
                throw new RuntimeException("Could not find propertyFile: "
                        + file);
            }
            System.out.println("propertyFile: " + file);

            final String[] bsbmArgs = new String[args.length - 1];
            System.arraycopy(args, 0, bsbmArgs, 0, bsbmArgs.length);

            /*
             * Note: we only need to specify the FILE when re-opening a journal
             * containing a pre-existing KB.
             */
            final BigdataSail sail;
            {
//                final File propertyFile = new File(args[0]);
                
                final Properties properties = new Properties();
                {
                    // Read the properties from the file.
                    final InputStream is = new BufferedInputStream(
                            new FileInputStream(propertyFile));
                    try {
                        properties.load(is);
                    } finally {
                        is.close();
                    }
                    if (System.getProperty(BigdataSail.Options.FILE) != null) {
                        // Override/set from the environment.
                        properties.setProperty(BigdataSail.Options.FILE, System
                                .getProperty(BigdataSail.Options.FILE));
                    }
                }
                
//                final Properties properties = new Properties();
//            properties.setProperty(
//                    BigdataSail.Options.QUADS, "false");
//            properties.setProperty(
//                    BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
//            properties.setProperty(
//                    BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
//            properties.setProperty(
//                    BigdataSail.Options.TRUTH_MAINTENANCE, "false");
//                properties.setProperty(BigdataSail.Options.FILE, file
//                        .getAbsolutePath());
                
                jnl = new Journal(properties);

                // resolve the kb instance of interest.
                final AbstractTripleStore tripleStore = (AbstractTripleStore) jnl
                        .getResourceLocator().locate(namespace, ITx.UNISOLATED);

                if (tripleStore == null) {

                    throw new RuntimeException("No such kb: "+namespace);
                    
                }

                // since the kb exists, wrap it as a sail.
                sail = new BigdataSail(tripleStore);
                
            }
//            final BigdataSail sail = new BigdataSail(properties);
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            // take the repository for a test spin
            // testRepo(repo);
            
            // run TestDriver with bsbmArgs, repo
            TestDriver.main(bsbmArgs, repo);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(jnl!=null) {
                jnl.close();
            }
        }
    }
    
    private static final void testRepo(BigdataSailRepository repo) 
            throws Exception {
        
        RepositoryConnection cxn = repo.getConnection();
        try {
/*            
            RepositoryResult<Statement> stmts = 
                cxn.getStatements(null, null, null, true);
            while (stmts.hasNext()) {
                Statement stmt = stmts.next();
                System.err.println(stmt);
            }
*/            
            String queryString = 
/*
"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
"PREFIX rev: <http://purl.org/stuff/rev#> " +
"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
"PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/> " +
"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
"CONSTRUCT {  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:product ?productURI . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:productlabel ?productlabel . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:vendor ?vendorname . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:vendorhomepage ?vendorhomepage . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:offerURL ?offerURL . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:price ?price . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:deliveryDays ?deliveryDays . " +
"             <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm-export:validuntil ?validTo } " +
// "SELECT * " +
"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm:product ?productURI . " +
"        ?productURI rdfs:label ?productlabel .  " +
"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm:vendor ?vendorURI . " +
"        ?vendorURI rdfs:label ?vendorname . " +
"        ?vendorURI foaf:homepage ?vendorhomepage . " +
"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm:offerWebpage ?offerURL . " +
"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm:price ?price . " +
"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm:deliveryDays ?deliveryDays . " +
"        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer9> bsbm:validTo ?validTo " +
"}";
*/
"PREFIX rev: <http://purl.org/stuff/rev#> " +
"DESCRIBE ?x " +
"WHERE { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review96> rev:reviewer ?x }";
                       
            StringWriter writer = new StringWriter();
/*            
            TupleQuery query = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(writer)));
*/
            BigdataSailGraphQuery query = (BigdataSailGraphQuery)
                cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
            query.setUseNativeConstruct(false);
            query.evaluate(new RDFXMLWriter(writer));

            System.out.println(queryString);
            System.out.println(writer.toString());
            
        } finally {
            cxn.close();
        }
        
    }
    
}
