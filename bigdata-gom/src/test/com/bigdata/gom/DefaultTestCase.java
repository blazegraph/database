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

package com.bigdata.gom;

import java.io.File;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import org.CognitiveWeb.generic.GenericAbstractTestCase;
import org.CognitiveWeb.generic.ObjectManagerFactory;
import org.CognitiveWeb.generic.core.TestAll;
import org.CognitiveWeb.generic.core.om.ObjectManager;
import org.CognitiveWeb.generic.core.om.RuntimeOptions;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;

/**
 * <p>
 * Test the GOM for bigdata integration.
 * </p>
 * <p>
 * This runs some bootstrap tests, the test suite defined in the generic-test
 * CVS module, and the test suite defined for the GOM implementation.
 * </p>
 */
public class DefaultTestCase extends GenericAbstractTestCase {

    public DefaultTestCase() {
        
        super();
        
    }

    public DefaultTestCase(String name) {
    
        super(name);
        
    }
    
    public static Test suite() {

        final DefaultTestCase delegate = new DefaultTestCase(); // !!!! THIS CLASS !!!!

		/*
		 * Use a proxy test suite and specify the delegate.
		 */

		ProxyTestSuite suite = new ProxyTestSuite(delegate,"bigdata GOM");

        /*
         * Bootstrap unit tests. 
         */
        suite.addTestSuite(DefaultTestCase.class);
        
		/*
		 * Pickup GOM implementation test suite.
		 */
		suite.addTest(TestAll.suite());

		/*
		 * Pickup the generic-test suite. This is a proxied test suite, so we
		 * pass in the suite which has the reference to the delegate.
		 */
		addGenericSuite(suite);

		return suite;

	}

	/**
     * <p>
     * Sets the properties for the object manager implementation and the
     * persistence store integration layer for bigdata.
     * </p>
     * 
     * @see ObjectManagerFactory#OBJECT_MANAGER_CLASSNAME
     * @see RuntimeOptions#PERSISTENT_STORE
     * @see com.bigdata.Options
     */
	public Properties getProperties(){
        
		Properties properties = super.getProperties();
        
        properties.setProperty(ObjectManagerFactory.OBJECT_MANAGER_CLASSNAME,
                ObjectManager.class.getName());
        
        properties.setProperty(RuntimeOptions.PERSISTENT_STORE,
                RuntimeOptions.PERSISTENT_STORE_BIGDATA);

        if(properties.getProperty(Options.BUFFER_MODE)==null) {

            // use the disk-based journal by default.
            
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
            
        }

        if (properties.getProperty(Options.FILE) == null) {
        
            // Use a temporary file by default.
//            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.FILE, defaultTestDatabase);
            
            properties.setProperty(Options.DELETE_ON_EXIT,"true");
            
            properties.setProperty(dropAfterTest,"true");

        }
        
		return properties;
        
	}
	
	
	/**
	 * The name of the default database used by this test suite.
	 */
	protected static final String defaultTestDatabase = "GOMTest"+Options.JNL;

	/**
     * Deletes the persistent store files with the given basename.
     * The store files are generally named for the test.  Each store
     * has two files, one with a ".db" extension and one with a ".lg"
     * extension.
     */
    protected void dropStore() {

		String filename = getProperties().getProperty(
                com.bigdata.journal.Options.FILE);

		if (filename == null) {

            // Not a persistent store.
            
			return;

		}

        File file = new File(filename);
        
		if (file.exists()) {

			log.info("deleting pre-existing file: " + file);

			if( ! file.delete() ) {
				
				log.warn("Could not delete file: " + file );
				
			}

		}

	}

    /*
     * bootstrap tests.
     */
    
    /**
     * This is just a placeholder for any bootstrap tests.
     */
    public void test_bootstraps() {
        
    }
    
}
