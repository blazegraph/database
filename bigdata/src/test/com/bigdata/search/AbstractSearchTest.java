package com.bigdata.search;

import java.util.Properties;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;

public abstract class AbstractSearchTest  extends ProxyTestCase<IIndexManager>  {
    private String namespace;  
    private IIndexManager indexManager;
    private FullTextIndex<Long> ndx;
    private IndexMetadata indexMetadata;
    private Properties properties;
    
    public AbstractSearchTest() {
	}
    
    public AbstractSearchTest(String arg0) {
    	super(arg0);
	}

	void init(String ...propertyValuePairs) {
        namespace = getName(); 
        properties = getProperties();
        for (int i=0; i<propertyValuePairs.length; ) {
        	properties.setProperty(propertyValuePairs[i++], propertyValuePairs[i++]);
        }
        indexManager = getStore();
        ndx = new FullTextIndex<Long>(indexManager, namespace, ITx.UNISOLATED, properties);
        ndx.create();
        indexMetadata = ndx.getIndex().getIndexMetadata();
    }
	
	public void tearDown() throws Exception {
        indexManager.destroy();
		super.tearDown();
	}

	String getNamespace() {
		return namespace;
	}

	IIndexManager getIndexManager() {
		return indexManager;
	}

	void setIndexManager(IIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	FullTextIndex<Long> getNdx() {
		return ndx;
	}

	IndexMetadata getIndexMetadata() {
		return indexMetadata;
	}


	Properties getSearchProperties() {
		return properties;
	}



}
