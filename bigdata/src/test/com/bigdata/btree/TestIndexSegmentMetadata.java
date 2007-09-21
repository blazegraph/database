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
 * Created on Sep 7, 2007
 */

package com.bigdata.btree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;

import junit.framework.TestCase;

import com.bigdata.rawstore.WormAddressManager;

/**
 * Test suite for {@link IndexSegmentMetadata}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentMetadata extends TestCase {

    /**
     * 
     */
    public TestIndexSegmentMetadata() {
        super();
    }

    /**
     * @param arg0
     */
    public TestIndexSegmentMetadata(String arg0) {
        super(arg0);
    }

    /**
     * Test the ability to create an {@link IndexSegmentMetadata} record, write
     * it on a file, and read back the same data from the file. The data for
     * this test are somewhat faked since there are assertions on legal metadata
     * and we can not write arbitrary values into the various fields.
     * 
     * @throws IOException
     */
    public void test_write_read01() throws IOException {

//        IRawStore store = new SimpleMemoryRawStore();

        int branchingFactor = 3;

        UUID indexUUID = UUID.randomUUID();

//        IValueSerializer valSer = ByteArrayValueSerializer.INSTANCE;
//        
//        BTree btree = new BTree(store,branchingFactor,indexUUID,valSer);
//        
//        btree.insert(new byte[]{}, new byte[]{1,2,3});
//
//        btree.write();

        int offsetBits = 48;

        /*
         * @todo this probably needs to be a custom address manager for the
         * IndexSegmentFileStore.
         */
        WormAddressManager am = new WormAddressManager(offsetBits);
        
        int height = 3; // btree.height;
        boolean useChecksum = false;
        int nleaves = 1; //btree.nleaves;
        int nnodes = 0; // btree.nnodes;
        int nentries = 1; //btree.nentries;
        int maxNodeOrLeafLength = 12; // arbitrary non-zero value.u
        long addrLeaves = am.toAddr(maxNodeOrLeafLength, IndexSegmentMetadata.SIZE);
        long addrNodes = 0L;
        long addrRoot = addrLeaves;
        long addrExtensionMetadata = 0L;
        long addrBloom = 0L;
        double errorRate = 0d;
        long length = IndexSegmentMetadata.SIZE + maxNodeOrLeafLength;
        UUID segmentUUID = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();
        
        IndexSegmentMetadata expected = new IndexSegmentMetadata(
                offsetBits, branchingFactor, height, useChecksum, nleaves, nnodes,
                nentries, maxNodeOrLeafLength, addrLeaves, addrNodes, addrRoot,
                addrExtensionMetadata, addrBloom, errorRate, length, indexUUID,
                segmentUUID, timestamp);
        
        System.err.println("Expected: "+expected);
        
        File tmp = File.createTempFile("test", "ndx");
        
        tmp.deleteOnExit();
        
        RandomAccessFile raf = new RandomAccessFile(tmp,"rw");
        
        try {

            // the metadata record (starting at 0L in the file).
            expected.write(raf);
        
            // additional bytes for the nodes/leaves.
            raf.write(new byte[maxNodeOrLeafLength]);
            
//            // force to disk.
//            raf.getChannel().force(true);
//            
//            raf.close();
//            
//            raf = new RandomAccessFile(tmp,"r");
            
            // rewind.
            raf.seek(0L);
            
            // read back from the file.
            IndexSegmentMetadata actual = new IndexSegmentMetadata(raf);

            System.err.println("Actual: "+actual);

            assertEquals("offsetBits",offsetBits,actual.offsetBits);
            
            assertEquals("branchingFactor",branchingFactor,actual.branchingFactor);
            
            assertEquals("height",height,actual.height);
            
            assertEquals("useChecksum",useChecksum,actual.useChecksum);
            
            assertEquals("nleaves",nleaves,actual.nleaves);
            
            assertEquals("nnodes",nnodes,actual.nnodes);
            
            assertEquals("nentries",nentries,actual.nentries);

            assertEquals("maxNodeOrLeafLength",maxNodeOrLeafLength,actual.maxNodeOrLeafLength);
            
            assertEquals("addrLeaves",addrLeaves,actual.addrLeaves);

            assertEquals("addrNodes",addrNodes,actual.addrNodes);

            assertEquals("addrRoot",addrRoot,actual.addrRoot);

            assertEquals("addrExtensionMetadata",addrExtensionMetadata,actual.addrExtensionMetadata);

            assertEquals("addrBloom",addrBloom,actual.addrBloom);

            assertEquals("errorRate",errorRate,actual.errorRate);

            assertEquals("length",length,actual.length);

            assertEquals("indexUUID",indexUUID,actual.indexUUID);

            assertEquals("segmentUUID",segmentUUID,actual.segmentUUID);

            assertEquals("timestamp",timestamp,actual.timestamp);

        } finally {
            
            try {raf.close();} catch(Throwable t) {/*ignore*/}
            
            tmp.delete();
            
        }
        
    }
    
}
