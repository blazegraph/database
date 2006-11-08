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
 * Created on Oct 30, 2006
 */

package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Bytes;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Test basics for the {@link ObjectIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Refactor to test each of the buffer modes and also a variety of other
 *       configuration factors that impact the object index, notably the
 *       branching factor and perhaps the slot size and initial extent.
 */
public class TestObjectIndexBasics extends AbstractObjectIndexTestCase {

    /**
     * 
     */
    public TestObjectIndexBasics() {
    }

    /**
     * @param arg0
     */
    public TestObjectIndexBasics(String arg0) {
        super(arg0);
    }

    public void test_ctor() throws IOException {
        
        Properties properties = new Properties();
        /* FIXME We can not test with "transient" until there is a root block
         * defined for that journal mode.
         */ 
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Direct.toString());
        properties.setProperty(Options.OBJECT_INDEX_SIZE,"16");
        properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte32);
        properties.setProperty(Options.SEGMENT,"0");
        // FIXME Use the existing AbstractTestCase to setup the Journal - it will force file release.
        File file = File.createTempFile("foo",".jnl");
        file.delete();
        properties.setProperty(Options.FILE,file.toString());
        Journal journal = new Journal(properties);
        ObjectIndex ndx = new ObjectIndex(journal);
 
        /*
         * @todo Create the root node and test its characteristics.
         * 
         * @todo Refactor a test setup that can be used for testing the
         * {@link NodeSerializer} in some depth.
         * 
         * @todo Inserts into the object index need to be performed with the
         * ISlotAllocation of the current version of the object and then need to
         * reconcile based on whether or not there is a hit in the index.
         * 
         * @todo There probably needs to be a lookup variant that returns the
         * IObjectIndexEntry.
         */
        
        // object id.
        final int id1 = 1;
        
        // write onto the journal, obtaining the slots on which the data was written.
        ISlotAllocation id1v0 = journal.write(ByteBuffer.wrap(new byte[]{1,2,3,4}));
        
        // create a root node having a single entry for id1.
        Node root = new Node(ndx,id1,id1v0);
        
    }

}
