package benchmark.bigdata;

import info.aduna.xml.XMLWriter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import benchmark.testdriver.Query;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class BigdataQuery {

	HttpURLConnection conn;
	Long start;
	Long end;
	
    private BigdataSailRepository repo;
    private String query;
    private byte queryType;
    private String defaultGraph;
    private int timeout;
    
	protected BigdataQuery(BigdataSailRepository repo, String query, byte queryType, String defaultGraph, int timeout) {
        this.repo = repo;
        this.query = query;
        this.queryType = queryType;
        this.defaultGraph = defaultGraph;
        this.timeout = timeout;
        
        if (queryType != Query.DESCRIBE_TYPE &&
            queryType != Query.SELECT_TYPE &&
            queryType != Query.CONSTRUCT_TYPE) {
            throw new IllegalArgumentException("unrecognized query type");
        }
        
/*
        try {
			String urlString = serviceURL + "?query=" + URLEncoder.encode(query, "UTF-8");
//			System.out.println(urlString);
			
			if(defaultGraph!=null)
				urlString +=  "&default-graph-uri=" + defaultGraph;

			URL url = new URL(urlString);
			conn = (HttpURLConnection)url.openConnection();

			conn.setRequestMethod("GET");
			conn.setDefaultUseCaches(false);
			conn.setDoOutput(true);
			conn.setUseCaches(false);
			conn.setReadTimeout(timeout);
			if(queryType==Query.DESCRIBE_TYPE || queryType==Query.CONSTRUCT_TYPE)
				conn.setRequestProperty("Accept", "application/rdf+xml");
			else
				conn.setRequestProperty("Accept", "application/sparql-results+xml");
		} catch(UnsupportedEncodingException e) {
			System.err.println(e.toString());
			System.exit(-1);
		} catch(MalformedURLException e) {
			System.err.println(e.toString());
			System.exit(-1);
		} catch(IOException e) {
			System.err.println(e.toString());
			System.exit(-1);
		}
*/        
	}
	
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
            ex.printStackTrace();
            System.exit(-1);
            return null;
        }
/*	
        try {
			conn.connect();

		} catch(IOException e) {
			System.err.println("Could not connect to SPARQL Service.");
			e.printStackTrace();
			System.exit(-1);
		}
		try {
			start = System.nanoTime();
			int rc = conn.getResponseCode();
			if(rc < 200 || rc >= 300) {
				System.err.println("Query execution: Received error code " + rc + " from server for Query:\n");
				System.err.println(URLDecoder.decode(conn.getURL().getQuery(),"UTF-8"));
			}
			return conn.getInputStream();
		} catch(SocketTimeoutException e) {
			return null;
		} catch(IOException e) {
			System.err.println("Query execution error:");
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
*/
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
            GraphQuery query = 
                cxn.prepareGraphQuery(QueryLanguage.SPARQL, this.query);
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
