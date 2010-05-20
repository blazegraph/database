package benchmark.bigdata;

import info.aduna.xml.XMLWriter;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Properties;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryConnection;
import benchmark.testdriver.TestDriver;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * A variation on the {@link TestDriver} which may be used to connect to either
 * a standalone {@link Journal} or a {@link JiniFederation}. If you want to
 * submit queries directly to a SPARQL endpoint, then use {@link TestDriver}
 * instead.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataQueryDriver {
    /**
     * 
     * @param args
     *            USAGE:
     *            <code>[bsbm args...] <i>namespace</i> (propertyFile|configFile)</code>
     *            , where
     *            <dl>
     *            <dt>namespace</dt>
     *            <dd>The namespace of the target KB instance ("kb" is the
     *            default namespace).</dd>
     *            <dt>propertyFile</dt>
     *            <dd>A java properties file for a standalone {@link Journal}</dd>
     *            <dt>configFile</dt>
     *            <dd>A jini configuration file for a bigdata federation</dd>
     *            </dl>
     * 
     * @see TestDriver for other options.
     * 
     * @todo introduce "-jnl" or "-fed" options to specify the standalone
     *       journal or a jini federation?
     */
    public static void main(final String[] args) {
        Journal jnl = null;
        JiniClient<?> jiniClient = null;
        try {
            if (args.length < 2) {
                System.err.println("USAGE: [bsbm args...] namespace (propertyFile|configFile)");
                System.exit(1);
            }
            final String namespace = args[args.length - 2];
            final String propertyFile = args[args.length - 1];
            final File file = new File(propertyFile);
            if (!file.exists()) {
                throw new RuntimeException("Could not find file: " + file);
            }
            boolean isJini = false;
            if (propertyFile.endsWith(".config")) {
                isJini = true;
            } else if (propertyFile.endsWith(".properties")) {
                isJini = false;
            } else {

                /*
                 * Note: This is a hack, but we are recognizing the jini
                 * configuration file with a .config extension and the journal
                 * properties file with a .properties extension.
                 */
                System.err
                        .println("File should have '.config' or '.properties' extension: "
                                + file);
                System.exit(1);
            }
            System.out.println("namespace: " + namespace);
            System.out.println("file: " + file);

            final String[] bsbmArgs = new String[args.length - 1];
            System.arraycopy(args, 0, bsbmArgs, 0, bsbmArgs.length);

            /*
             * Note: we only need to specify the FILE when re-opening a journal
             * containing a pre-existing KB.
             */
            final BigdataSail sail;
            {

                final IIndexManager indexManager;
                
                if (isJini) {

                    jiniClient = new JiniClient(new String[]{propertyFile});
                    
                    indexManager = jiniClient.connect();
                    
                } else {

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
                            properties
                                    .setProperty(
                                            BigdataSail.Options.FILE,
                                            System
                                                    .getProperty(BigdataSail.Options.FILE));
                        }
                    }

                    indexManager = jnl = new Journal(properties);
                    
                }

                // resolve the kb instance of interest.
                final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                        .getResourceLocator().locate(namespace, ITx.UNISOLATED);

                if (tripleStore == null) {

                    throw new RuntimeException("No such kb: "+namespace);
                    
                }

                // since the kb exists, wrap it as a sail.
                sail = new BigdataSail(tripleStore);
                
            }
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            // run TestDriver with bsbmArgs, repo
            TestDriver.main(bsbmArgs, repo);
//            testRepo(repo);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if(jnl!=null) {
                jnl.close();
            }
            if(jiniClient!=null) {
                jiniClient.disconnect(true/*immediateShutdown*/);
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
"PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> " +
"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +

"SELECT DISTINCT ?product ?label ?propertyTextual WHERE { " +
"    { " +
"       ?product rdfs:label ?label . " +
"       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType75> . " +
"       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature379> . " +
"       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature2248> . " +
"       ?product bsbm:productPropertyTextual1 ?propertyTextual . " +
"       ?product bsbm:productPropertyNumeric1 ?p1 . " +
"    } UNION { " +
"       ?product rdfs:label ?label . " +
"       ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType75> . " +
"       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature379> . " +
"       ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature2251> . " +
"       ?product bsbm:productPropertyTextual1 ?propertyTextual . " +
"       ?product bsbm:productPropertyNumeric2 ?p2 . " +
"    } " +
"       FILTER ( ?p1 > 160 || ?p2> 461 ) " +
"} " +
"ORDER BY ?label " +
"OFFSET 5 " +
"LIMIT 10";
                       
            StringWriter writer = new StringWriter();
            
            TupleQuery query = 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            query.evaluate(new SPARQLResultsXMLWriter(new XMLWriter(writer)));
/*
            BigdataSailGraphQuery query = (BigdataSailGraphQuery)
                cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
            query.setUseNativeConstruct(false);
            query.evaluate(new RDFXMLWriter(writer));
*/
            System.out.println(queryString);
            System.out.println(writer.toString());
            
        } finally {
            cxn.close();
        }
        
    }
    
}
