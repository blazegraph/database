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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ForceEnum;
import com.bigdata.scaleup.PartitionedJournal.Options;
import com.bigdata.rawstore.Bytes;

/**
 * Base class for test suites for inference engine and the magic sets
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractTripleStoreTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractTripleStoreTestCase() {
    }

    /**
     * @param name
     */
    public AbstractTripleStoreTestCase(String name) {
        super(name);
    }

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
            properties.setProperty(Options.BUFFER_MODE, getBufferMode().toString());
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
            
            properties.setProperty(Options.FORCE_WRITES,ForceEnum.No.toString());
//            properties.setProperty(Options.FORCE_WRITES,ForceEnum.Force.toString());
//            properties.setProperty(Options.FORCE_WRITES,ForceEnum.ForceMetadata.toString());
            
            properties.setProperty(Options.SEGMENT, "0");
            if(properties.getProperty(Options.FILE)==null) {
                properties.setProperty(Options.FILE, getName()+".jnl");
            }
            if(properties.getProperty(Options.BASENAME)==null) {
                properties.setProperty(Options.BASENAME, getName());
            }
//            properties.setProperty(Options.MAXIMUM_EXTENT,""+Bytes.megabyte32*20);
            properties.setProperty(Options.INITIAL_EXTENT,""+getInitialExtent());
//            properties.setProperty(Options.DELETE_ON_CLOSE,"true");

        }

        return properties;

    }

    protected Properties properties;

    /**
     * Invoked the first time {@link #getProperties()} is called for each test
     * to set the initial extent of the journal.
     */
//  * 
//  * @return The initial extent for the journal (default is 10M). Some tests
//  *         need significantly larger journals.
    protected long getInitialExtent() {
        
//        return Bytes.megabyte*10;
        return Options.DEFAULT_INITIAL_EXTENT;
        
    }

    /**
     * Invoked the first time {@link #getProperties()} is called for each test
     * to set mode in which the {@link Journal} will be opened.
     * 
     * @return {@link BufferMode#Transient}
     * 
     * @see BufferMode#Transient
     * @see BufferMode#Direct
     */
    protected BufferMode getBufferMode() {
        
        return BufferMode.Transient;
        
    }
    
    public void setUp() throws Exception {
        
        Properties properties = getProperties();

        String filename = properties.getProperty(Options.FILE);
        
        if( filename != null ) {
            
            File file = new File(filename);
            
            if(file.exists() && ! file.delete() ) {
                
                throw new RuntimeException("Could not delete file: "+file.getAbsoluteFile());
                
            }
            
        }

        store = new TripleStore(properties);
        
    }
    
    public void tearDown() {

        if(store.isOpen()) store.closeAndDelete();
        
    }
    
    protected TripleStore store;
    
}
