package com.bigdata.samples.remoting;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryLanguage;
import com.bigdata.rdf.sail.remoting.GraphRepository;
import com.bigdata.rdf.sail.remoting.GraphRepositoryClient;
import com.bigdata.rdf.sail.remoting.IOUtils;
import com.bigdata.rdf.store.BD;
import com.bigdata.samples.SparqlBuilder;

public class DemoRestRepository {
    
    private static final String servletURL = "http://localhost:8080/bigdata";
    
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
        
        GraphRepository repo = new GraphRepositoryClient(servletURL);
        
        { // load some statements built up programmatically
            
            URI mike = new URIImpl(BD.NAMESPACE + "Mike");
            URI bryan = new URIImpl(BD.NAMESPACE + "Bryan");
            URI loves = new URIImpl(BD.NAMESPACE + "loves");
            URI rdf = new URIImpl(BD.NAMESPACE + "RDF");
            Graph graph = new GraphImpl();
            graph.add(mike, loves, rdf);
            graph.add(bryan, loves, rdf);
            
            repo.create(graph);
            
        }
        
        { // load some statements from a file
            
            InputStream is = DemoRestRepository.class.getResourceAsStream("sample.rdf");
            if (is == null)
                throw new Exception("Could not locate resource: sample.rdf");
            Reader reader = new InputStreamReader(new BufferedInputStream(is));
            repo.create(IOUtils.readString(reader));
            
        }
        
        { // show the entire contents of the repository

            SparqlBuilder sparql = new SparqlBuilder();
            sparql.addTriplePattern("?s", "?p", "?o");
            Collection<Statement> graph = 
                repo.executeConstructQuery(sparql.toString(), QueryLanguage.SPARQL, true);
            for (Statement s : graph) {
                System.err.println(s);
            }
            
        }
        
        
    }
}
