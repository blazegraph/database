/**

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on May 7, 2014
 */
package com.bigdata.search;

import java.util.Properties;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;

public abstract class AbstractSearchTest  extends ProxyTestCase<IIndexManager>  {
    private String namespace;  
    private IIndexManager indexManager;
    private FullTextIndex<Long> ndx;
    private Properties properties;
    
    public AbstractSearchTest() {
	}
    
    public AbstractSearchTest(String arg0) {
    	super(arg0);
	}

	void init(String ...propertyValuePairs) {
        namespace = getClass().getName()+"#"+getName(); 
        indexManager = getStore();
        properties = (Properties) getProperties().clone();
        ndx = createFullTextIndex(namespace, properties, propertyValuePairs);
    }
	
	private FullTextIndex<Long> createFullTextIndex(String namespace, Properties properties, String ...propertyValuePairs) {
        for (int i=0; i<propertyValuePairs.length; ) {
        	properties.setProperty(propertyValuePairs[i++], propertyValuePairs[i++]);
        }
        FullTextIndex<Long> ndx = new FullTextIndex<Long>(indexManager, namespace, ITx.UNISOLATED, properties);
        ndx.create();
        return ndx;
	}

	FullTextIndex<Long> createFullTextIndex(String namespace, String ...propertyValuePairs) {
        return createFullTextIndex(namespace, getProperties(), propertyValuePairs);
	}
	
	public void tearDown() throws Exception {
		if (indexManager != null) {
           indexManager.destroy();
		}
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

	Properties getSearchProperties() {
		return properties;
	}

}
