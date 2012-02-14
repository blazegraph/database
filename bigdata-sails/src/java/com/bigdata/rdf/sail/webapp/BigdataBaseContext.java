package com.bigdata.rdf.sail.webapp;

import com.bigdata.journal.IIndexManager;
import com.bigdata.service.IBigdataFederation;

/**
 * Context object provides access to the {@link IIndexManager}.
 * 
 * @author Martyn Cutcher
 */
public class BigdataBaseContext {

//	static private final Logger log = Logger.getLogger(BigdataBaseContext.class); 

	private final IIndexManager m_indexManager;
	
    public BigdataBaseContext(final IIndexManager indexManager) {

		if (indexManager == null)
			throw new IllegalArgumentException();

		m_indexManager = indexManager;

	}

	public IIndexManager getIndexManager() {

	    return m_indexManager;
	    
	}

    /**
     * Return <code>true</code> iff the {@link IIndexManager} is an
     * {@link IBigdataFederation} and {@link IBigdataFederation#isScaleOut()}
     * reports <code>true</code>.
     */
    public boolean isScaleOut() {

        return m_indexManager instanceof IBigdataFederation
                && ((IBigdataFederation<?>) m_indexManager).isScaleOut();
	    
	}

}
