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
