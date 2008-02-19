/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.BytesUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.repo.BigdataRepository.Options;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using in-process services rather
 * than service discovery (which means that there is no network IO).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractEmbeddedBigdataFederationTestCase extends AbstractBTreeTestCase {

    /**
     * 
     */
    public AbstractEmbeddedBigdataFederationTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractEmbeddedBigdataFederationTestCase(String arg0) {
        super(arg0);
    }

    protected IBigdataClient client;
    protected IBigdataFederation fed;
    protected IDataService dataService0;
    protected IDataService dataService1;

    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        // Note: uses transient mode for tests.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                .toString());
        
        // when the data are persistent use the test to name the data directory.
        properties.setProperty(EmbeddedBigdataFederation.Options.DATA_DIR,
                getName());
        
        return properties;
        
    }
    
    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp() throws Exception {
      
        File dataDir = new File( getName() );
        
        if(dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete( dataDir );
            
        }

        client = new EmbeddedBigdataClient(getProperties());
        
        fed = client.connect();

        dataService0 = ((EmbeddedBigdataFederation)fed).getDataService(0);

        dataService1 = ((EmbeddedBigdataFederation)fed).getDataService(1);
        
    }
    
    public void tearDown() throws Exception {
        
        client.terminate();
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(File f) {
        
        if(f.isDirectory()) {
            
            File[] children = f.listFiles();
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        System.err.println("Removing: "+f);
        
        if (!f.delete())
            throw new RuntimeException("Could not remove: " + f);

    }
    
    /**
     * Verifies that two splits have the same data.
     * 
     * @param expected
     * @param actual
     */
    final public static void assertEquals(Split expected, Split actual) {
       
        assertEquals("partition",expected.pmd,actual.pmd);
        assertEquals("fromIndex",expected.fromIndex,actual.fromIndex);
        assertEquals("toIndex",expected.toIndex,actual.toIndex);
        assertEquals("ntuples",expected.ntuples,actual.ntuples);
        
    }
    
    /**
     * Verify that a named index is registered on a specific {@link DataService}
     * with the specified indexUUID.
     * 
     * @param dataService
     *            The data service.
     * @param name
     *            The index name.
     * @param indexUUID
     *            The unique identifier assigned to all instances of that index.
     */
    final protected void assertIndexRegistered(IDataService dataService, String name,
            UUID indexUUID) throws IOException {

        IndexMetadata metadata = dataService.getIndexMetadata(name);
        
        assertNotNull("metadata",metadata);
        
        assertEquals("indexUUID", indexUUID, metadata.getIndexUUID());
        
    }
    
    /**
     * Compares two byte[][]s for equality.
     * 
     * @param expected
     * @param actual
     */
    final public void assertEquals(byte[][] expected, byte[][] actual ) {
        
        assertEquals(null, expected, actual);
        
    }
    
    /**
     * Compares two byte[][]s for equality.
     * 
     * @param expected
     * @param actual
     */
    final public void assertEquals(String msg,byte[][] expected, byte[][] actual ) {
        
        if(msg==null) msg=""; else msg = msg+":";
        
        assertEquals(msg+"length", expected.length, actual.length);
        
        for( int i=0; i<expected.length; i++ ) {

            if (expected[i] == null) {

                if (actual[i] != null) {
                    
                    fail("expected " + i + "th entry to be null.");
                    
                }
                
            } else {

                if (BytesUtil.compareBytes(expected[i], actual[i]) != 0) {

                    fail("expected=" + BytesUtil.toString(expected[i])
                            + ", actual=" + BytesUtil.toString(actual[i]));

                }

            }
            
        }
        
    }

}
