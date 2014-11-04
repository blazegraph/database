package com.bigdata.rdf.sail.webapp;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.HttpException;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.vocab.decls.DCTermsVocabularyDecl;
import com.bigdata.rdf.vocab.decls.VoidVocabularyDecl;
import com.bigdata.relation.RelationSchema;

/**
 * Proxied test suite for the Multi-Tenancy API.
 * 
 * TODO Verify that the URLs in the VoID and ServiceDescription are correct for
 * both the default sparql end point and for each namespace specific sparql end
 * point. We have to correctly configured things in order to get this right and
 * the code must be correct as well (in terms of using the configured serviceURL
 * correctly).
 */
public class TestMultiTenancyAPI<S extends IIndexManager> extends
        AbstractTestNanoSparqlJettyClient<S> {

    public TestMultiTenancyAPI() {

    }

    public TestMultiTenancyAPI(final String name) {

        super(name);

    }

    /**
     * A model of a VoID summary of a data set.
     * <p>
     * Note: This contains just the information which is provided for the NSS
     * method which describes the known data sets, rather than the more detailed
     * report which is provided with the Service Description for a specific end
     * point.
     */
    static class VoidSummary {
        /**
         * The {@link Graph} from which the summary was extracted. The summary
         * is specific to the data set identified to the constructor. The
         * {@link Graph} MAY contain descriptions of many data sets.
         */
        final Graph g;
        /**
         * The {@link Resource} used to model the data set in the {@link Graph}.
         */
        final Resource dataset;
        /** The dc:title(s) for the data set. */
        final List<Literal> title = new LinkedList<Literal>();
        /** The bigdata namespace for the data set. */
        final Literal namespace;
        /** The sparql end point URL(s) for the data set. */
        final List<URI> sparqlEndpoint = new LinkedList<URI>();

        /**
         * 
         * @param dataset
         *            The {@link Resource} used to model the data set in the
         *            {@link Graph}.
         * @param g
         *            The {@link Graph} containing the description of that data
         *            set.
         */
        public VoidSummary(final Resource dataset, final Graph g) {

            this.dataset = dataset;

            this.g = g;
            
            /* The namespace of the data set should be described using dc:title. */
            for (Statement x : getMatches(g, dataset,
                    DCTermsVocabularyDecl.title, null/* namespace */)) {

                title.add((Literal) x.getObject());
                
            }

            // Extract the single unambiguous value for the namespace.
            Literal namespace = null;
            for (Statement x : getMatches(g, dataset,
                    DCTermsVocabularyDecl.title, null/* namespace */)) {

                if(namespace != null)
                    throw new RuntimeException(
                            "Multiple declarations of the namespace?");

                namespace = (Literal) x.getObject();

            }
            this.namespace = namespace;

            /*
             * Extract the SPARQL end point for new data set.
             */
            for(Statement x : getMatches(g, dataset,
                    VoidVocabularyDecl.sparqlEndpoint, null/* endpointURL */)) {
                
                sparqlEndpoint.add((URI) x.getObject());

            }

        }

    }

    /**
     * Return an index over the {@link VoidSummary} collection using the
     * namespace of the data set as the key for the index.
     * 
     * @param c
     *            The {@link Collection}.
     * @return The index.
     */
    protected static Map<String, VoidSummary> indexOnNamespace(
            final Collection<VoidSummary> c) {

        final Map<String, VoidSummary> map = new TreeMap<String, VoidSummary>();

        for(VoidSummary x : c) {
            
            map.put(x.namespace.stringValue(), x);
            
        }
        
        return map;
        
    }
    
    /**
     * Extract the VoID summary of the known data sets.
     * 
     * @return The VoID summary of the known data sets.
     * 
     * @throws Exception
     */
    protected Map<Resource, VoidSummary> getRepositoryDescriptions()
            throws Exception {

        // The discovered data sets.
        final Map<Resource, VoidSummary> summaries = new LinkedHashMap<Resource, VoidSummary>();

        // Do the discovery.
        final Graph g = RemoteRepository.asGraph(m_repo
                .getRepositoryDescriptions());

        final Statement[] a = getMatches(g, null/* dataset */, RDF.TYPE,
                VoidVocabularyDecl.Dataset);

        for (Statement x : a) {

            // The RDF Value used to model a data set.
            final Resource dataset = x.getSubject();

            // Extract a summary of that data set from the graph.
            summaries.put(dataset, new VoidSummary(dataset, g));

        }

        return summaries;

    }

    /**
     * Verify that the default data set (aka KB) is described.
     */
    public void test_describeDataSets01() throws Exception {

        // Obtain the summary for all known data sets.
        final Map<Resource, VoidSummary> summaries = getRepositoryDescriptions();

		/*
		 * There should be at least one data set (the default KB). There can be
		 * more if the end point is restart safe across the test suite, e.g., a
		 * federation.
		 */
		if (summaries.isEmpty()) {

			fail("No repository descriptions");

		}

        // Get the summary for each data set.
		final Iterator<Map.Entry<Resource, VoidSummary>> itr = summaries.entrySet().iterator();

		while(itr.hasNext()) {
			
			final Map.Entry<Resource,VoidSummary> e = itr.next();

			final Resource namespaceName = e.getKey();
			
			final VoidSummary summary = e.getValue();
			
			final String namespaceStr = summary.namespace.stringValue();
			
	        // Verify the expected namespace.
	        assertEquals(new LiteralImpl(namespaceStr), summary.namespace);

	        // Verify at least SPARQL end point was described for that data set.
	        assertFalse(summary.sparqlEndpoint.isEmpty());

		}

        /*
         * TODO This does not verify that the SPARQL end points are correct. We
         * should at least execute a simple SPARQL query against each reported
         * end point, and ideally we should test Query, Update, and the NSS REST
         * API methods which target the other servlets (InsertServlet,
         * DeleteServlet) to make sure that all http requests are being routed
         * correctly.
         */

    }

    /**
     * Verify the ability to obtain the effective configuration properties for
     * the default data set.
     * 
     * FIXME Should /properties also work or just namespace/kb/properties?
     */
    public void test_getRepositoryProperties01() throws Exception {

        final Properties p = m_repo.getRepositoryProperties(namespace);

//        log.error(p.toString());
        
        /*
         * ERROR: 1403 main com.bigdata.rdf.sail.webapp.TestMultiTenancyAPI.
         * test_getRepositoryProperties01(TestMultiTenancyAPI.java:222):
         * {com.bigdata
         * .relation.namespace=test_getRepositoryProperties01d16045fb
         * -0343-4c40-b49a-1865253e26cc,
         * com.bigdata.search.FullTextIndex.fieldsEnabled=false,
         * com.bigdata.relation.class=com.bigdata.rdf.store.LocalTripleStore,
         * com
         * .bigdata.rdf.store.AbstractTripleStore.vocabularyClass=com.bigdata.
         * rdf.vocab.NoVocabulary,
         * com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers=false,
         * com.bigdata.rdf.sail.truthMaintenance=false,
         * com.bigdata.rdf.store.AbstractTripleStore
         * .axiomsClass=com.bigdata.rdf.axioms.NoAxioms}
         */

        assertEquals(namespace, p.getProperty(RelationSchema.NAMESPACE));

    }
    
    /**
     * Unit test creates one (or more) namespaces, verifies that we can list the
     * namespaces, verifies that we can obtain the effective properties for each
     * namespace, verifies that we can obtain the ServiceDescription for the
     * namespaces, verifies that we can Query/Update each namespace, and
     * verifies that we can delete each namespace.
     * 
     * @throws Exception
     */
    public void test_create01() throws Exception {

        /*
         * Create a new data set. The namespace incorporates a UUID in case we
         * are running against a server rather than an embedded per-test target.
         * The properties are mostly inherited from the default configuration,
         * but the namespace of the new data set is explicitly set for the
         * CREATE operation.
         */
        final String namespace2 = "kb2-" + UUID.randomUUID();

        doTestCreate(namespace2);
        
    }
    
    /**
     * Test for correct URL encoding of the namespace in the URL requests.
     * 
     * @throws Exception
     */
    public void test_create02() throws Exception {

        /*
         * Create a new data set. The namespace incorporates a UUID in case we
         * are running against a server rather than an embedded per-test target.
         * The properties are mostly inherited from the default configuration,
         * but the namespace of the new data set is explicitly set for the
         * CREATE operation.
         * 
         * Note: The '/' character is reserved by zookeeper for a path separator.
         * It can not appear in a bigdata namespace in scale-out.
         */
//        final String namespace2 = "kb2-" + UUID.randomUUID() + "-&/<>-foo";
        final String namespace2 = "kb2-" + UUID.randomUUID() + "-&<>-foo";

        doTestCreate(namespace2);
        
    }
    
    private void doTestCreate(final String namespace2) throws Exception {
        
        final Properties properties = new Properties();

        properties.setProperty(BigdataSail.Options.NAMESPACE, namespace2);

        { // verify does not exist.
            try {
                m_repo.getRepositoryProperties(namespace2);
                fail("Should not exist: " + namespace2);
            } catch (HttpException ex) {
                // Expected status code.
                assertEquals(404,ex.getStatusCode());
            }
        }
        
        m_repo.createRepository(namespace2, properties);

        { // verify exists.
            final Properties p = m_repo.getRepositoryProperties(namespace2);
            assertNotNull(p);
        }

        /*
         * Verify error if attempting to create a KB for a namespace which
         * already exists.
         */
        try {

            m_repo.createRepository(namespace2, properties);
            
            fail("Expecting: " + HttpServletResponse.SC_CONFLICT);
            
        } catch (HttpException ex) {
            
            assertEquals(HttpServletResponse.SC_CONFLICT, ex.getStatusCode());
            
        }

        // Get the summaries, indexed by the data set namespace.
        final Map<String/* namespace */, VoidSummary> summaries = indexOnNamespace(getRepositoryDescriptions()
                .values());

		// Should be (at least) two such summaries (more if end point is durable
		// across test suite runs, e.g., a federation).
		if (summaries.size() < 2)
			fail("Expecting at least 2 summaries, but only have "
					+ summaries.size());

        final VoidSummary defaultKb = summaries.get(namespace);
        assertNotNull(defaultKb);
        assertFalse(defaultKb.sparqlEndpoint.isEmpty());

        final VoidSummary otherKb = summaries.get(namespace2);
        assertNotNull(otherKb);
        assertFalse(otherKb.sparqlEndpoint.isEmpty());

        /*
         * Remove any other KBs from the map so we do not have side-effects.
         */
		{
			final Iterator<Map.Entry<String, VoidSummary>> itr = summaries.entrySet().iterator();

			while(itr.hasNext()) {
				
				final Map.Entry<String,VoidSummary> e = itr.next();
				
				if(e.getKey().equals(namespace)||e.getKey().equals(namespace2)) 
					continue;
				
				itr.remove();
				
			}

			// Only two are left.
			assertEquals(2,summaries.size());
			
        }
        
        /*
         * Exercise the known data sets.
         */
        
        // Decremented as we delete data sets.
        int ndatasets = summaries.size(); 
        
        for(VoidSummary summary : summaries.values()) {

            // The namespace for that data set.
            final String ns = summary.namespace.stringValue();
            
            // GET the properties for that data set.
            {
                final Properties p = m_repo.getRepositoryProperties(ns);

                assertEquals(ns, p.getProperty(RelationSchema.NAMESPACE));
            }

            final JettyRemoteRepository tmp = m_repo.getRepositoryForNamespace(ns);

            {

                // GET the Service Description for the data set.
                {

                    RemoteRepository.asGraph(tmp.getServiceDescription());

                }

                // Test a SPARQL 1.1. Query against the data set.
                {

                    final TupleQueryResult result = tmp.prepareTupleQuery(
                            "SELECT (COUNT(*) as ?count) {?s ?p ?o}")
                            .evaluate();

                    final long nresults = countResults(result);

                    if(log.isInfoEnabled())
                    	log.info("namespace=" + ns + ", triples=" + nresults);

                }

                // Test a SPARQL 1.1 Update against the data set.
                {

                    tmp.prepareUpdate(
                            "PREFIX : <http://www.bigdata.com> \n"
                                    + "INSERT DATA {:a :b :c}").evaluate();

                }

                /*
                 * TODO Test the other REST API methods
                 * (InsertServlet,DeleteServlet). This will verify that the
                 * servlet routing is correct for all of those parts of the API
                 * (this could also be done by running the base NSS test suite
                 * twice, once against the default sparql end point and once
                 * against the /namespace/NAMESPACE/sparql end point).
                 */
            }

            /*
             * Delete the data set and verify that its description is gone and
             * that it no longer responds to various requests (properties,
             * service description, sparql query & update, etc).
             */
            {
                
                m_repo.deleteRepository(ns);
                
                // one fewer data sets known to the server.
                ndatasets--;
                
            }

            // Describe data sets now reports one fewer data sets.
            {
                
                // Get the summaries, indexed by the data set namespace.
                final Map<String/* namespace */, VoidSummary> summaries2 = indexOnNamespace(getRepositoryDescriptions()
                        .values());

//                // Verify expected #of known data sets.
//                assertEquals(ndatasets, summaries2.size());                
             
                // The deleted namespace is no longer reported.
                assertNull(summaries2.get(ns));
                
            }
            
            // Properties now fails.
            {
             
                try {
                
                    m_repo.getRepositoryProperties(ns);
                    
                    fail("Expecting " + BigdataServlet.HTTP_NOTFOUND);

                } catch (HttpException ex) {
                
                    assertEquals(BigdataServlet.HTTP_NOTFOUND,
                            ex.getStatusCode());
                    
                }
                
            }

            // Service Description now fails.
            {

                try {
                
                    tmp.getServiceDescription();
                    
                    fail("Expecting " + BigdataServlet.HTTP_NOTFOUND);

                } catch (HttpException ex) {
                
                    assertEquals(BigdataServlet.HTTP_NOTFOUND,
                            ex.getStatusCode());
                    
                }

            }

            // SPARQL 1.1. Query against the data set now fails.
            {

                TupleQueryResult result = null;
                try {
                    
                    result = tmp.prepareTupleQuery(
                            "SELECT (COUNT(*) as ?count) {?s ?p ?o}")
                            .evaluate();
                    
                    fail("Expecting " + BigdataServlet.HTTP_NOTFOUND);

                } catch (HttpException ex) {

                    assertEquals(BigdataServlet.HTTP_NOTFOUND,
                            ex.getStatusCode());
                } finally {

                    if (result != null)
                        result.close();
                
                }

            }

            // SPARQL 1.1 Update against the data set now fails.
            {

                try {
                    
                    tmp.prepareUpdate(
                            "PREFIX : <http://www.bigdata.com> \n"
                                    + "INSERT DATA {:a :b :c}").evaluate();

                    fail("Expecting " + BigdataServlet.HTTP_NOTFOUND);

                } catch (HttpException ex) {

                    assertEquals(BigdataServlet.HTTP_NOTFOUND,
                            ex.getStatusCode());

                }

            }

        }

        // TODO Verify that top-level status and counters still work.
        
    }

}
