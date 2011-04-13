package com.bigdata.rdf.sail.webapp;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;

/**
 * Context object provides access to the {@link IIndexManager}.
 * 
 * @author Martyn Cutcher
 */
public class BigdataBaseContext {

	static private final Logger log = Logger.getLogger(BigdataBaseContext.class); 

	private final IIndexManager m_indexManager;
	
    public BigdataBaseContext(final IIndexManager indexManager) {

		if (indexManager == null)
			throw new IllegalArgumentException();

		m_indexManager = indexManager;

	}

	public IIndexManager getIndexManager() {

	    return m_indexManager;
	    
	}

}
