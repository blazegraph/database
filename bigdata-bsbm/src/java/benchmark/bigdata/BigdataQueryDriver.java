package benchmark.bigdata;

import java.io.File;
import java.io.StringWriter;
import java.util.Properties;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import benchmark.testdriver.TestDriver;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class BigdataQueryDriver {
    /**
     * @param args
     *          USAGE: -journal <journal file> -data <data file>
     */
    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.err.println("USAGE: <bsbm args...> -journal <journal file>");
            }
            String journal = args[args.length-1];
            File file = new File(journal);
            if (!file.exists()) {
                throw new RuntimeException("could not find the journal");
            }
            
            String[] bsbmArgs = new String[args.length-2];
            System.arraycopy(args, 0, bsbmArgs, 0, bsbmArgs.length);
            
            Properties properties = new Properties();
            properties.setProperty(
                    BigdataSail.Options.QUADS, "false");
            properties.setProperty(
                    BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
            properties.setProperty(
                    BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
            properties.setProperty(
                    BigdataSail.Options.TRUTH_MAINTENANCE, "false");
            properties.setProperty(
                    BigdataSail.Options.FILE, file.getAbsolutePath());

            BigdataSail sail = new BigdataSail(properties);
            BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            // take the repository for a test spin
            // testRepo(repo);
            
            // run TestDriver with bsbmArgs, repo
            TestDriver.main(bsbmArgs, repo);
            
        } catch (Exception ex) {
            ex.printStackTrace();
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
