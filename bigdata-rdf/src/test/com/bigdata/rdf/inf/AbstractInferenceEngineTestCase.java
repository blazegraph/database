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

package com.bigdata.rdf.inf;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.scaleup.MasterJournal.Options;

/**
 * Base class for test suites for inference engine and the magic sets
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractInferenceEngineTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractInferenceEngineTestCase() {
    }

    /**
     * @param name
     */
    public AbstractInferenceEngineTestCase(String name) {
        super(name);
    }

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
//                    .toString());
            properties.setProperty(Options.BUFFER_MODE, getBufferMode().toString());
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
//                    .toString());
//            properties.setProperty(Options.SLOT_SIZE, ""+Bytes.kilobyte32);
            if(properties.getProperty(Options.FILE)==null) {
                properties.setProperty(Options.FILE, getName()+".jnl");
            }
            if(properties.getProperty(Options.BASENAME)==null) {
                properties.setProperty(Options.BASENAME, getName());
            }
//            properties.setProperty(Options.INITIAL_EXTENT,""+getInitialExtent());

        }

        return properties;

    }

    private Properties properties;

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
        
        return BufferMode.Direct;
        
    }
    
    public void setUp() throws Exception {
        
        Properties properties = getProperties();

        String filename = properties.getProperty(Options.FILE);
        
        if( filename != null ) {
            
            File file = new File(filename);
            
            System.err.println( "store file: " + file.getAbsolutePath() );
        
            if(file.exists() && ! file.delete() ) {
                
                throw new RuntimeException("Could not delete file: "+file.getAbsoluteFile());
                
            }
            
        }

        store = new InferenceEngine(properties);
        
    }
    
    public void tearDown() {

        if (store.isOpen())
            store.closeAndDelete();
        
    }
    
    protected InferenceEngine store;
    
}
