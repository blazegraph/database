/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 22, 2008
 */

package com.bigdata.resources;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.resources.ResourceManager.Options;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractResourceManagerBootstrapTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractResourceManagerBootstrapTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractResourceManagerBootstrapTestCase(String arg0) {
        super(arg0);
    }

    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());

        log.info("Setting " + Options.DATA_DIR + "=" + dataDir);
        
        properties.setProperty(
                com.bigdata.resources.ResourceManager.Options.DATA_DIR, dataDir
                        .toString());

        // disable the write cache to avoid memory leak in the test suite.
        properties.setProperty(Options.WRITE_CACHE_ENABLED, "false");

        return properties;
        
    }
    
    /** The data directory. */
    File dataDir;
    /** The subdirectory containing the journal resources. */
    File journalsDir;
    /** The subdirectory spanning the index segment resources. */
    File segmentsDir;
    /** The temp directory. */
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));

    /**
     * Sets up the per-test data directory.
     */
    public void setUp() throws Exception {

        super.setUp();
        
        /*
         * Create a normal temporary file whose path is the path of the data
         * directory and then delete the temporary file.
         */

        dataDir = File.createTempFile(getName(), "", tmpDir).getCanonicalFile();
        
        assertTrue(dataDir.delete()); 

        assertFalse(dataDir.exists());

        journalsDir = new File(dataDir,"journals");

        segmentsDir = new File(dataDir,"segments");
        
    }

}
