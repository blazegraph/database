package benchmark.bigdata;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTreeBuilder;
import com.bigdata.rdf.store.AbstractTripleStore;

public class TestBSBM {
    
	private static final Logger log = Logger.getLogger(TestBSBM.class);
	
	private static final String PROPS =
		"/Users/mikepersonick/Documents/workspace/bigdata-query-branch/bigdata-perf/bsbm/RWStore.properties";

	private static final String JNL =
    	"/Users/mikepersonick/Documents/systap/bsbm/bsbm-qual/bigdata-bsbm.RW.jnl";
    
	private static final String NS = "qual";
    
    private static final String QUERY =
        
    	"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
	     " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
	     " PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
	     " SELECT DISTINCT ?product ?productLabel " +
	     " WHERE { " + 
	     " 	?product rdfs:label ?productLabel . " +
	     "     FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer36/Product1625> != ?product) " +
	     " 	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer36/Product1625> bsbm:productFeature ?prodFeature . " +
	     " 	?product bsbm:productFeature ?prodFeature . " +
	     " 	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer36/Product1625> bsbm:productPropertyNumeric1 ?origProperty1 . " +
	     " 	?product bsbm:productPropertyNumeric1 ?simProperty1 . " +
	     " 	FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120)) " +
	     " 	<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer36/Product1625> bsbm:productPropertyNumeric2 ?origProperty2 . " +
	     " 	?product bsbm:productPropertyNumeric2 ?simProperty2 . " +
	     " 	FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170)) " +
	     " } " +
	     " ORDER BY ?productLabel " +
	     " LIMIT 5";
    
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        
        try {

//            args = new String[] {
//                "-query",
//                queryStr,
//                serviceURL
//            };
//            
//            NanoSparqlClient.main(args);
            
        	
        	final Properties props = new Properties();
        	final FileInputStream fis = new FileInputStream(PROPS);
        	props.load(fis);
        	fis.close();        	
        	props.setProperty(AbstractTripleStore.Options.FILE, JNL);
        	props.setProperty(BigdataSail.Options.NAMESPACE, NS);
        	
        	System.err.println(props);
        	
        	final BigdataSail sail = new BigdataSail(props);
        	sail.initialize();
        	final BigdataSailRepository repo = new BigdataSailRepository(sail);
        	final BigdataSailRepositoryConnection cxn = 
        		repo.getReadOnlyConnection();
        	
        	try {
        		
//        		final RepositoryResult<Statement> stmts = cxn.getStatements(
//        			null, RDF.TYPE, 
//        			new URIImpl("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138"), false);
//        		if (stmts.hasNext()) {
//	        		while (stmts.hasNext()) {
//	        			System.err.println(stmts.next());
//	        		}
//        		} else {
//        			System.err.println("no stmts");
//        		}
        		
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, QUERY);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	           
	            if (log.isInfoEnabled()) {
	            	
		            final BigdataSailTupleQuery bdTupleQuery =
		            	(BigdataSailTupleQuery) tupleQuery;
		            final QueryRoot root = (QueryRoot) bdTupleQuery.getTupleExpr();
		            log.info(root);
		            TupleExpr te = root;
		            while (te instanceof UnaryTupleOperator) {
		            	te = ((UnaryTupleOperator) te).getArg();
		            }
		            
		            final SOpTreeBuilder stb = new SOpTreeBuilder();
		            final SOpTree tree = stb.collectSOps(te);
	           
	                log.info(tree);
	            	log.info(QUERY);
	
	            	final TupleQueryResult result = tupleQuery.evaluate();
	            	if (result.hasNext()) {
		                while (result.hasNext()) {
		                    log.info(result.next());
		                }
	            	} else {
	            		log.info("no results for query");
	            	}
	            	
	            }
        		
        	} finally {
        		cxn.close();
        		repo.shutDown();
        	}
        	
        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        }
        
    }
}
