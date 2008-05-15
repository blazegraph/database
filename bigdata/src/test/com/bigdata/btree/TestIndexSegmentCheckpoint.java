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

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Test suite for {@link IndexSegmentCheckpoint}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentCheckpoint extends TestCase {

    /**
     * 
     */
    public TestIndexSegmentCheckpoint() {
        super();
    }

    /**
     * @param arg0
     */
    public TestIndexSegmentCheckpoint(String arg0) {
        super(arg0);
    }

    /**
     * Test the ability to create an {@link IndexSegmentCheckpoint} record, write
     * it on a file, and read back the same data from the file. The data for
     * this test are somewhat faked since there are assertions on legal metadata
     * and we can not write arbitrary values into the various fields.
     * 
     * @throws IOException
     */
    public void test_write_read01() throws IOException {

        /*
         * Note: allows records up to 64M in length.
         */
        final int offsetBits = WormAddressManager.SCALE_OUT_OFFSET_BITS;

        /*
         * Fake a checkpoint record. The only parts of this that we need are the
         * addresses of the nodes and blobs. Those addresses MUST be formed as
         * relative to the BASE region of the file. The offsets encoded within
         * those addresses will be used to decode addresses in the non-BASE
         * regions of the file.
         * 
         * Note: the checkpoint ctor has a variety of assertions so that
         * constrains how we can generate this fake checkpoint record.
         */
        final IndexSegmentCheckpoint checkpoint;
        final long extentLeaves = 216;
        final long offsetLeaves = IndexSegmentCheckpoint.SIZE;
        final long offsetNodes;
        final long offsetBlobs;
        final long extentNodes;
        final long extentBlobs;
        final int height = 1;
        final int nleaves = 5;
        final int nnodes = 1;
        final int nentries = 29;
        final int maxNodeOrLeafLength = 128; // arbitrary non-zero value.
//        final long addrLeaves;
//        final long addrNodes;
        final long addrRoot;
//        final long addrBlobs;
        final long addrBloom;
        final long addrMetadata;
        final long length;
        final UUID segmentUUID = UUID.randomUUID();
        final long commitTime = System.currentTimeMillis();
        {

            // Used to encode the addresses.
            WormAddressManager am = new WormAddressManager(offsetBits);

//            addrLeaves = am.toAddr(extentLeaves, IndexSegmentRegion.BASE
//                    .encodeOffset(offsetLeaves));

            extentNodes = 123;
            
            offsetNodes = offsetLeaves + extentLeaves;
            
//            addrNodes = am.toAddr(extentNodes, IndexSegmentRegion.BASE
//                    .encodeOffset(offsetNodes));

            // Note: only one node and it is the root, so addrRoot==addrNodes
            addrRoot = am.toAddr((int)extentNodes, IndexSegmentRegion.BASE
                    .encodeOffset(offsetNodes));
            
            extentBlobs = Bytes.megabyte32 * 20;
            
            offsetBlobs = offsetNodes + extentNodes;
            
//            addrBlobs = am.toAddr(extentBlobs, IndexSegmentRegion.BASE
//                    .encodeOffset(offsetBlobs));

            addrBloom = 0L;
            
            final int sizeMetadata = 712;
            
            final long offsetMetadata = offsetBlobs + extentBlobs;

            addrMetadata = am.toAddr(sizeMetadata, IndexSegmentRegion.BASE
                    .encodeOffset(offsetMetadata));

            length = offsetMetadata + sizeMetadata; 
            
            checkpoint = new IndexSegmentCheckpoint(
                offsetBits,//
                height,//
                nleaves,//
                nnodes,//
                nentries,//
                maxNodeOrLeafLength,//
                offsetLeaves,extentLeaves,//
                offsetNodes,extentNodes,//
                offsetBlobs,extentBlobs,//
                addrRoot,//
                addrMetadata,//
                addrBloom, //
                length,//
                segmentUUID,//
                commitTime//
                );
        
            System.err.println("Checkpoint: "+checkpoint);
            
        }
        
        final IndexSegmentCheckpoint expected = new IndexSegmentCheckpoint(
                offsetBits, height, nleaves, nnodes, nentries,
                maxNodeOrLeafLength, offsetLeaves, extentLeaves, offsetNodes,
                extentNodes, offsetBlobs, extentBlobs, addrRoot, addrMetadata,
                addrBloom, length, segmentUUID,
                commitTime);
        
        System.err.println("Expected: "+expected);
        
        File tmp = File.createTempFile("test", "ndx");
        
        tmp.deleteOnExit();
        
        RandomAccessFile raf = new RandomAccessFile(tmp,"rw");
        
        try {

            // the checkpoint record (starting at 0L in the file).
            expected.write(raf);
        
            // extend to full size.
            raf.getChannel().truncate(length);
            
            // seek near the end.
            raf.seek(length-128);
            
            // write up to the end of the file.
            raf.write(new byte[128]);
            
//            // additional bytes for the nodes/leaves.
//            raf.write(new byte[maxNodeOrLeafLength]);
//            
//            // force to disk.
//            raf.getChannel().force(true);
//            
//            raf.close();
//            
//            raf = new RandomAccessFile(tmp,"r");
            
            // read back from the file.
            final IndexSegmentCheckpoint actual = new IndexSegmentCheckpoint(raf);

            System.err.println("Actual: "+actual);

            assertEquals("offsetBits",offsetBits,actual.offsetBits);
            
            assertEquals("height",height,actual.height);
            
            assertEquals("nleaves",nleaves,actual.nleaves);
            
            assertEquals("nnodes",nnodes,actual.nnodes);
            
            assertEquals("nentries",nentries,actual.nentries);

            assertEquals("maxNodeOrLeafLength",maxNodeOrLeafLength,actual.maxNodeOrLeafLength);
            
            assertEquals("offsetLeaves",offsetLeaves,actual.offsetLeaves);
            assertEquals("extentLeaves",extentLeaves,actual.extentLeaves);

            assertEquals("offsetNodes",offsetNodes,actual.offsetNodes);
            assertEquals("extentNodes",extentNodes,actual.extentNodes);

            assertEquals("offsetBlobs",offsetBlobs,actual.offsetBlobs);
            assertEquals("extentBlobs",extentBlobs,actual.extentBlobs);

            assertEquals("addrRoot",addrRoot,actual.addrRoot);

            assertEquals("addrMetadata",addrMetadata,actual.addrMetadata);

            assertEquals("addrBloom",addrBloom,actual.addrBloom);

            assertEquals("length",length,actual.length);

            assertEquals("segmentUUID",segmentUUID,actual.segmentUUID);

            assertEquals("commitTime",commitTime,actual.commitTime);

        } finally {
            
            try {raf.close();} catch(Throwable t) {/*ignore*/}
            
            tmp.delete();
            
        }
        
    }
    
}
