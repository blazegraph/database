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
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.repo.BigdataRepository.Options;

import junit.framework.TestCase2;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using in-process services rather
 * than service discovery (which means that there is no network IO).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractEmbeddedBigdataFederationTestCase extends TestCase2 {

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

    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        // @todo use transient mode for tests if possible.
        properties.setProperty(Options.BUFFER_MODE,""+BufferMode.Transient);
        
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
        
    }
    
    public void tearDown() throws Exception {
        
        client.terminate();
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f A file or directory.
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
    
}
