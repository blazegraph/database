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
 * Created on Oct 7, 2009
 */

package com.bigdata.service.jini.master;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import junit.framework.TestCase2;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.KeeperException;

import com.bigdata.rdf.load.MappedRDFDataLoadMaster;
import com.bigdata.service.jini.util.JiniServicesHelper;

/**
 * Unit tests for the {@link MappedTaskMaster}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMappedRDFDataLoadMaster extends TestCase2 {

    /**
     * 
     */
    public TestMappedRDFDataLoadMaster() {
    }

    /**
     * @param name
     */
    public TestMappedRDFDataLoadMaster(String name) {
        super(name);
    }
    
    private JiniServicesHelper helper;
    
//    protected void setUp() throws Exception {
//        
//        super.setUp();
//
//        helper = new JiniServicesHelper();
//
//    }
//
//    protected void tearDown() throws Exception {
//
//        if (helper != null) {
//
//            helper.destroy();
//            
//        }
//        
//        super.tearDown();
//        
    // }

    /**
     * This runs a U1 data load.  
     * 
     * FIXME Go further and test the behavior of the pending set.
     * 
     * @throws KeeperException
     */
    public void test() throws ConfigurationException, InterruptedException,
            IOException, ExecutionException, KeeperException {

        JiniServicesHelper helper = null;

        final File tempConfigFile = JiniServicesHelper
                .append(new File(
                        "bigdata-jini/src/test/com/bigdata/service/jini/master/TestMappedRDFDataLoadMaster.config"));

        final String[] args = new String[] { //
        tempConfigFile.getPath(),
        // optional overrides.
        /*
         * Note: This does not work because the JiniServicesHelper is already
         * overriding 'properties' for the DataServer.
         */
//        DataServer.class.getName()+".properties = new NV[]{" +
//        		"new NV(DataServer.Options.OVERFLOW_ENABLED,false)"+//
//        		"}",
        };

        try {

            helper = new JiniServicesHelper(args);

            helper.start();

            new MappedRDFDataLoadMaster(helper.getFederation()).execute();

        } finally {

            // delete the temp file containing the federation configuration.
            tempConfigFile.delete();

            if (helper != null)
                helper.destroy();

        }

    }

}
