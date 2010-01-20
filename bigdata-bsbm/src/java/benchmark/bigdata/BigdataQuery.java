package benchmark.bigdata;

import info.aduna.xml.XMLWriter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import benchmark.testdriver.Query;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Executes a query against a supplied Bigdata Sesame Repository.  For the
 * purposes of this benchmark, this class will simulate remote access to the
 * repository, by serializing results into XML and return an InputStream
 * against that XML document (String).
 * 
 * @author mike
 */
public class BigdataQuery {

    /**
     * Start time.
     */
	private Long start;
    
    /**
     * End time.
     */
	private Long end;
    
    /**
     * Bigdata Sesame Repository to query.
     */
    private BigdataSailRepository repo;
    
    /**
     * The SPARQL query string.
     */
    private String query;
    
    /**
     * See {@link Query}.
     */
    private byte queryType;
    
	protected BigdataQuery(BigdataSailRepository repo, String query, 
            byte queryType) {

        this.repo = repo;
        this.query = query;
        this.queryType = queryType;
        
        if (queryType != Query.DESCRIBE_TYPE &&
            queryType != Query.SELECT_TYPE &&
            queryType != Query.CONSTRUCT_TYPE) {
            throw new IllegalArgumentException("unrecognized query type");
        }
        
	}
	
    /**
     * Execute the supplied select, describe, or construct query.
     * 
     * @return an InputStream to the XML results
     */
	protected InputStream exec() {
        start = System.nanoTime();
        
        try {
            
            if (queryType == Query.SELECT_TYPE) {
                return execTupleQuery();
            } else if (queryType == Query.CONSTRUCT_TYPE) {
                return execGraphQuery();
            }
            
        } catch (Exception ex) {
            System.err.println("Query execution error:");
            System.err.println(query);
            ex.printStackTrace(System.err);
            System.exit(-1);
        }

        return null;
        
	}
    
    protected InputStream execTupleQuery() throws Exception {
        SailRepositoryConnection cxn = repo.getQueryConnection();
        try {
            StringWriter writer = new StringWriter();
            TupleQuery query = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, this.query);
            query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(writer)));
            String results = writer.toString();
            return new ByteArrayInputStream(results.getBytes());
        } finally {
            cxn.close();
        }
    }
    
    protected InputStream execGraphQuery() throws Exception {
        SailRepositoryConnection cxn = repo.getQueryConnection();
        try {
            StringWriter writer = new StringWriter();
            BigdataSailGraphQuery query = (BigdataSailGraphQuery) 
                cxn.prepareGraphQuery(QueryLanguage.SPARQL, this.query);
            /*
             * FIXME BSBM constructs statements with values not actually in the
             * database, which breaks the native construct iterator.
             */ 
            query.setUseNativeConstruct(false);
            query.evaluate(new RDFXMLWriter(writer));
            String results = writer.toString();
            return new ByteArrayInputStream(results.getBytes());
        } finally {
            cxn.close();
        }
    }
	
	protected double getExecutionTimeInSeconds() {
		end = System.nanoTime();
		Long interval = end-start;
		Thread.yield();
		return interval.doubleValue()/1000000000;
	}
	
	protected void close() {
	}
}
