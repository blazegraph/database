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
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.NanoSparqlObjectManager;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Simple graph program identifies friends of friends that are not directly
 * connected using a SPARQL query and then constructs an in-memory graph
 * consisting of those vertices and using the connection count as the link
 * weights connecting the vertices. The graph is symmetric.
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
 * <p>
 * A real application could use the skin pattern to provide application specific
 * interfaces that simplify access to the vertex and link attributes of
 * interest.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Example2 implements Callable<Void> {

    private final IObjectManager om;
    
    public Example2(final IObjectManager om) {
        
        this.om = om;
        
    }

    /**
     * Return the string value of the property iff it exists and otherwise
     * <code>null</code>.
     * 
     * @param gpo
     *            The object.
     * @param property
     *            The property.
     *            
     * @return The string value -or- <code>null</code> if the property is not
     *         bound to a {@link Literal}.
     */
    static private String getStr(final IGPO gpo, final URI property) {

        final Value val = gpo.getValue(RDFS.LABEL);

        if (val instanceof Literal) {

            return ((Literal) val).getLabel();

        }

        return null;

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
                        "SELECT ?x ?z (count(?y) as ?connectionCount) \n"+
                        "       (sample(?xname2) as ?xname) \n" + //
                        "       (sample(?zname2) as ?zname) \n" + //
                        "WHERE {\n" + //
                        "  ?x foaf:knows ?y . \n" + //
                        "  ?y foaf:knows ?z . \n" + //
                        "  FILTER NOT EXISTS { ?x foaf:knows ?z } . \n" + //
                        "  FILTER ( ?x != ?z ) . \n"+//
                        "  OPTIONAL { ?x rdfs:label ?xname2 } .\n"+//
                        "  OPTIONAL { ?z rdfs:label ?zname2 } .\n"+//
                        "} \n" + //
                        "GROUP BY ?x ?z "
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

                x.addValue(connectTo, z.getId());

                final IGPO link = x.getLink(connectTo, z);

                link.addValue(weightProperty, connectionCount);

                final Literal xname = (Literal) bset.getValue("xname");

                final Literal zname = (Literal) bset.getValue("zname");

                if (xname != null) {

                    x.setValue(RDFS.LABEL, xname);

                }

                if (zname != null) {

                    z.setValue(RDFS.LABEL, zname);

                }

                roots.add(x);

            }
            
        } finally {
            
            itr.close();
            
        }

        System.out.println("Found " + roots.size()
                + " friends having unconnected friends of friends");

        for (IGPO gpo : roots) {

            System.out.println("---------");

            final ILinkSet friends = gpo.getLinksOut(connectTo);

            if (!friends.isEmpty()) {

                System.out.println("person=" + gpo.getId() + " (name="
                        + getStr(gpo, RDFS.LABEL) + ") has " + friends.size()
                        + " unconnected friends of friends.");

                for (IGPO friendOfAFriend : friends) {

                    final int connectionCount = ((Literal) gpo.getLink(
                            connectTo, friendOfAFriend)
                            .getValue(weightProperty)).intValue();

                    System.out.println("friendOfAFriend: "
                            + friendOfAFriend.getId() + " (name="
                            + getStr(friendOfAFriend, RDFS.LABEL)
                            + "), connectionCount=" + connectionCount);

                }

            }

//            System.out.println(gpo.pp());

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
