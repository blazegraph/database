package com.bigdata.rdf.sail.webapp;

import junit.framework.Test;

import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepository.AddOp;

public class TestNanoSparqlJettyClient2 <S extends IIndexManager> extends
AbstractTestNanoSparqlJettyClient<S> {

    public TestNanoSparqlJettyClient2() {

    }

	public TestNanoSparqlJettyClient2(final String name) {

		super(name);

	}
	
	static public Test suite() {
		return ProxySuiteHelper.suiteWhenStandalone(TestNanoSparqlJettyClient2.class,"test.*", TestMode.quads,TestMode.sids,TestMode.triples);
	}

    /**
     * Test bulk load and database at once closure.
     */
    public void test_INFERENCE() throws Exception {

    	final URI person = new URIImpl("bd:/person");
    	final URI company = new URIImpl("bd:/company");
    	final URI entity = new URIImpl("bd:/entity");
    	final URI mike = new URIImpl("bd:/mike");
    	final URI bryan = new URIImpl("bd:/bryan");
    	final URI systap = new URIImpl("bd:/sytap");
    	// 
    	
    	log.warn(m_serviceURL);
    	
    	final Graph ontology = new GraphImpl();
    	ontology.add(person, RDFS.SUBCLASSOF, entity);
    	ontology.add(company, RDFS.SUBCLASSOF, entity);
    	
    	assertEquals(m_repo.add(new AddOp(ontology)), 2);
    	
    	{ // batch 1
    		
//        	m_repo.setTruthMaintenance(false);
        	
        	final Graph data = new GraphImpl();
        	data.add(mike, RDF.TYPE, person);
        	data.add(bryan, RDF.TYPE, person);
        	
    		m_repo.add(new AddOp(data));
    	
//    		m_repo.doClosure();
//    		
//    		m_repo.setTruthMaintenance(true);
    		
    	}
    	
    	{ // batch 2
    		
//        	m_repo.setTruthMaintenance(false);
        	
        	final Graph data = new GraphImpl();
        	data.add(systap, RDF.TYPE, company);
        	
    		m_repo.add(new AddOp(data));
    	
//    		m_repo.doClosure();
//    		
//    		m_repo.setTruthMaintenance(true);
    		
    	}
    	
    }
    
}
