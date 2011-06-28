package com.bigdata.samples.remoting;

import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;
import com.bigdata.rdf.store.BD;
import com.bigdata.samples.SparqlBuilder;

public class DemoSesameServer {
    
    private static final String sesameURL = "http://localhost:8080/openrdf-sesame";
    
    private static final String repoID = "bigdata";
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        
        try {
            _main(args);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
    }
    
    public static void _main(String[] args) throws Exception {

        String sesameURL, repoID;
        
        if (args != null && args.length == 2) {
            sesameURL = args[0];
            repoID = args[1];
        } else {
            sesameURL = DemoSesameServer.sesameURL;
            repoID = DemoSesameServer.repoID;
        }
        
        Repository repo = new HTTPRepository(sesameURL, repoID);
        repo.initialize();
        
        RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try { // load some statements built up programmatically
            
            URI mike = new URIImpl(BD.NAMESPACE + "Mike");
            URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
            URI loves = new URIImpl(BD.NAMESPACE + "loves");
            URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
            Graph graph = new GraphImpl();
            graph.add(mike, loves, rdf);
            graph.add(bryan, loves, rdf);
            
            cxn.add(graph);
            cxn.commit();
            
        } finally {
            
            cxn.close();
            
        }
        
        { // show the entire contents of the repository

            SparqlBuilder sparql = new SparqlBuilder();
            sparql.addTriplePattern("?s", "?p", "?o");
            
            GraphQuery query = cxn.prepareGraphQuery(
                    QueryLanguage.SPARQL, sparql.toString());
            GraphQueryResult result = query.evaluate();
            while (result.hasNext()) {
                Statement stmt = result.next();
                System.err.println(stmt);
            }
            
        }
        
        
    }
}
