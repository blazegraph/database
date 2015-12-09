/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Dec 19, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.locator.ILocatableResource;

/**
 *
 */
public class TestRebuildTextIndex extends AbstractTripleStoreTestCase {

	private static final transient Logger log = Logger.getLogger(TestRebuildTextIndex.class);
	
    /**
     * 
     */
    public TestRebuildTextIndex() {
    }

    /**
     * @param name
     */
    public TestRebuildTextIndex(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {

    	final Properties properties = new Properties(super.getProperties());
        
        // enable the full text index.
        properties.setProperty(AbstractTripleStore.Options.TEXT_INDEX,"true");
               
        return properties;

    }

    private String droppedIndex;
	private String registeredIndex;

    public void test_rebuildTextIndex() throws InterruptedException {

	    Properties properties = getProperties();

	    // create/re-open journal.
	    final Journal journal = new Journal(properties ) {
	    	
			@Override
	    	public void dropIndex(String name) {
	    		super.dropIndex(name);
	    		droppedIndex = name;
	    	}
			@Override
			public ICheckpointProtocol register(String name,
					IndexMetadata metadata) {
				registeredIndex = name;
				return super.register(name, metadata);
			}
			
	    };
	    journal.getResourceLocator().locate(namespace, timestamp)
	    

       // journal.getProperties().setProperty(AbstractTripleStore.Options.);

        AbstractTripleStore store = new TempTripleStore(getProperties());

        try {

            assertNotNull(store.getLexiconRelation().getSearchEngine());

            final BigdataValueFactory f = store.getValueFactory();
            
            store.getLexiconRelation().rebuildTextIndex();
            
            
          
                       
        } finally {

            store.__tearDownUnitTest();

        }

    }
    
}

