package com.bigdata.rdf.sail.webapp;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Proxied test suite.
 *
 * @param <S>
 */
public class TestNanoSparqlClient2<S extends IIndexManager> extends
        AbstractTestNanoSparqlClient<S> {

    public TestNanoSparqlClient2() {

    }

	public TestNanoSparqlClient2(final String name) {

		super(name);

	}

    /**
     * Delete everything matching an access path description.
     */
    public void test_IMPLEMENT_ME() throws Exception {

    	final RemoteRepositoryManager rrm = super.m_repo;
    	
    	// do something here
        
    }
    
}
