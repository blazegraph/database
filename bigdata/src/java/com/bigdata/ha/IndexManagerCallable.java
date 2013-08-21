package com.bigdata.ha;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue.IIndexManagerCallable;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.jini.ha.HAJournal;

@SuppressWarnings("serial")
public abstract class IndexManagerCallable<T> implements IIndexManagerCallable<T> {
    protected static final Logger log = Logger.getLogger(HAJournal.class);

    private transient IIndexManager indexManager;
	
	public IndexManagerCallable() {
		
	}
	
    public void setIndexManager(IIndexManager indexManager) {
    	this.indexManager = indexManager;
    }
    
    /**
     * Return the {@link IIndexManager}.
     * 
     * @return The data service and never <code>null</code>.
     * 
     * @throws IllegalStateException
     *             if {@link #setIndexManager(IIndexManager)} has not been invoked.
     */
    public IIndexManager getIndexManager() {
    	if (indexManager == null)
    		throw new IllegalStateException();
    	
    	return indexManager;
    }
}

