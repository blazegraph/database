package com.bigdata.search;

import java.util.Properties;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;

public abstract class AbstractSearchTest  extends ProxyTestCase<IIndexManager>  {
    String NAMESPACE;  
    IIndexManager indexManager;
    FullTextIndex<Long> ndx;
    IndexMetadata indexMetadata;
    Properties properties;
    
    public AbstractSearchTest() {
	}
    
    public AbstractSearchTest(String arg0) {
    	super(arg0);
	}

	void init(String ...propertyValuePairs) {
        NAMESPACE = getName(); 
        properties = getProperties();
        for (int i=0; i<propertyValuePairs.length; ) {
        	properties.setProperty(propertyValuePairs[i++], propertyValuePairs[i++]);
        }
        indexManager = getStore();
        ndx = new FullTextIndex<Long>(indexManager, NAMESPACE, ITx.UNISOLATED, properties);
        ndx.create();
        indexMetadata = ndx.getIndex().getIndexMetadata();
    }
	
	public void tearDown() throws Exception {
        indexManager.destroy();
		super.tearDown();
	}


}
