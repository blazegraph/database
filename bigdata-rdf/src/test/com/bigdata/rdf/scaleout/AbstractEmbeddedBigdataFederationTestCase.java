/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jul 25, 2007
 */

package com.bigdata.rdf.scaleout;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.EmbeddedBigdataFederation;
import com.bigdata.service.IBigdataClient;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using in-process services rather
 * than service discovery (which means that there is no network IO).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractEmbeddedBigdataFederationTestCase extends TestCase {

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

    IBigdataClient client;

    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp() throws Exception {
      
        File dataDir = new File( getName() );
        
        if(dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete( dataDir );
            
        }
        
        Properties properties = new Properties(System.getProperties());
              
        properties.setProperty(EmbeddedBigdataFederation.Options.DATA_DIR,
                getName());
        
        client = new EmbeddedBigdataClient(properties);
        
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
