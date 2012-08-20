package com.bigdata.gom.samples;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.NanoSparqlObjectManager;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Simple graph program.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Example2 implements Callable<Void> {

    private final IObjectManager om;
    
    public Example2(final IObjectManager om) {
        
        this.om = om;
        
    }

    public Void call() throws Exception {

        /*
         * URI used to recommend possible connections.
         */
        final BigdataURI connectTo = om.getValueFactory().createURI(
                "http://example.org/connectTo");

        /*
         * URI used for the connection count (link weight).
         */
        final BigdataURI weightProperty = om.getValueFactory().createURI(
                "http://example.org/connectionCount");

        final ICloseableIterator<BindingSet> itr = om
                .evaluate("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n" + //
                        "SELECT ?x ?z (count(?y) as ?connectionCount) \n" + //
                        "WHERE {\n" + //
                        "  ?x foaf:knows ?y . \n" + //
                        "  ?y foaf:knows ?z . \n" + //
                        "  FILTER NOT EXISTS { ?x foaf:knows ?z } . \n" + //
                        "  FILTER ( ?x != ?z ) . \n"+//
                        "} \n" + //
                        "GROUP BY ?x ?z"
                );

        /*
         * Convert solutions into a graph, collecting the vertices of interest
         * for that graph.
         */
        final Set<IGPO> roots = new LinkedHashSet<IGPO>();
        try {

            while (itr.hasNext()) {

                final BindingSet bset = itr.next();

                final IGPO x = om.getGPO((Resource) bset.getValue("x"));

                final IGPO z = om.getGPO((Resource) bset.getValue("z"));

                final Literal connectionCount = (Literal) bset
                        .getValue("connectionCount");

                // x.getLinksOut(connectTo).add(z);

                x.addValue(connectTo, z.getId());

                final IGPO link = om.getGPO(new StatementImpl(x.getId(),
                        connectTo, z.getId()));

                link.addValue(weightProperty, connectionCount);

                roots.add(x);

            }
            
        } finally {
            
            itr.close();
            
        }

        System.out.println("Found " + roots.size()
                + " friends having unconnected friends of friends");

        for(IGPO gpo : roots) {
            
            System.out.println(gpo.pp());
            
        }
        
        return null;
        
    }
    
    static public void main(final String[] args) throws Exception {

        /**
         * The top-level SPARQL end point for a NanoSparqlServer instance.
         */
        final String sparqlEndpointURL = "http://localhost:8080/sparql/";

        /**
         * The namespace of the KB instance that you want to connect to on that
         * server. The default namespace is "kb".
         */
        final String namespace = "kb";
        
        ClientConnectionManager ccm = null;

        ExecutorService executor = null;

        try {

            executor = Executors.newCachedThreadPool();

            ccm = DefaultClientConnectionManagerFactory.getInstance()
                    .newInstance();

            final HttpClient httpClient = new DefaultHttpClient(ccm);

            final RemoteRepository repo = new RemoteRepository(
                    sparqlEndpointURL, httpClient, executor);

            final IObjectManager om = new NanoSparqlObjectManager(repo,
                    namespace);

            new Example2(om).call();

        } finally {

            if (ccm != null) {

                ccm.shutdown();

            }

            if (executor != null) {

                executor.shutdownNow();

            }

        }

    }

}
