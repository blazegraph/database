package com.bigdata.gom;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.jetty.client.HttpClient;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import com.bigdata.BigdataStatics;
import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.NanoSparqlObjectManager;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.util.httpd.Config;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Simple graph program constructs a local graph using a SPARQL query that
 * extracts edges of interest from the SPARQL server.
 * <p>
 * To get started, start the NanoSparqlServer. Then LOAD the data set into the
 * server using SPARQL UPDATE (you will have to use the path the file on your
 * local machine).
 * 
 * <pre>
 * LOAD  <file:///Users/bryan/Documents/workspace/BIGDATA_RELEASE_1_2_0/bigdata-gom/src/samples/com/bigdata/gom/samples/example2.trig>
 * </pre>
 * 
 * You can then run this example, and it will construct the graph. You can load
 * a different data set if you want to test this out on your own data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Example1 implements Callable<Void> {

    private final IObjectManager om;
    
    public Example1(final IObjectManager om) {
        
        this.om = om;
        
    }

    @Override
    public Void call() throws Exception {

        final ICloseableIterator<Statement> itr = om
                .evaluateGraph(
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n" + //
                        "CONSTRUCT { \n"+//
                        " ?u a foaf:Person . \n" + //
                        " ?u foaf:knows ?v . \n"+
                        " ?u rdfs:label ?name . \n"+
                        "} \n"+//
                        "WHERE {\n"+//
                        " ?u a foaf:Person . \n" + //
                        " ?u foaf:knows ?v . \n"+
                        " OPTIONAL { ?u rdfs:label ?name } . \n"+
                        "} LIMIT 100"//
                        );
        
        final Map<Resource, IGPO> vertices = om.initGPOs(itr);

        System.out.println("Found " + vertices.size() + " instances.");

        for(IGPO gpo : vertices.values()) {
            
            System.out.println(gpo.pp());
            
        }
        
        return null;
        
    }
    
    static public void main(final String[] args) throws Exception {

        /**
         * The top-level SPARQL end point for a NanoSparqlServer instance.
         */
        final String serviceURL = "http://localhost:" + Config.BLAZEGRAPH_HTTP_PORT + "/"
                + BigdataStatics.getContextPath();

        /**
         * The namespace of the KB instance that you want to connect to on that
         * server. The default namespace is "kb".
         */
        final String namespace = "kb";
        
        ExecutorService executor = null;
        
        RemoteRepositoryManager repo = null;
        
        HttpClient client = null;

        try {

            executor = Executors.newCachedThreadPool();
            
           	client = HttpClientConfigurator.getInstance().newInstance();

            repo = new RemoteRepositoryManager(
            		serviceURL, client, executor);

            final IObjectManager om = new NanoSparqlObjectManager(repo.getRepositoryForDefaultNamespace(),
                    namespace);

            new Example1(om).call();

        } finally {

            if (repo != null) {
            	repo.close();
            }
            
            if (client != null) {
            	client.stop();
            }

            if (executor != null) {
                executor.shutdownNow();
            }

        }

    }

}
