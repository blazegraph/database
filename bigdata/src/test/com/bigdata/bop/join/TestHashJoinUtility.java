/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Oct 14, 2011
 */

package com.bigdata.bop.join;

import java.util.List;
import java.util.UUID;

import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Test suite for the {@link HashJoinUtility}.
 * 
 * TODO Unit test to verify vectoring of left solutions having the same hash
 * code (and look at whether we can vector optional solutions
 * as well).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHashJoinUtility extends AbstractHashJoinUtilityTestCase {

    /**
     * 
     */
    public TestHashJoinUtility() {
    }

    /**
     * @param name
     */
    public TestHashJoinUtility(String name) {
        super(name);
    }
    
    private MemoryManager mmgr;

    private HTree rightSolutions, joinSet;

    @Override
    protected void tearDown() throws Exception {

        if (rightSolutions != null) {
            rightSolutions.close();
            rightSolutions = null;
        }

        if (joinSet != null) {
            joinSet.close();
            joinSet = null;
        }

        if (mmgr != null) {
            mmgr.clear();
            mmgr = null;
        }

        super.tearDown();

    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();
    
        /*
         * This wraps an efficient raw store interface around a
         * child memory manager created from the IMemoryManager
         * which is backing the query.
         */
        
        mmgr = new MemoryManager(DirectBufferPool.INSTANCE);

        final IRawStore store = new MemStore(mmgr.createAllocationContext());

        /*
         * Create the map(s).
         */

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setAddressBits(HTreeAnnotations.DEFAULT_ADDRESS_BITS);

        metadata.setRawRecords(HTreeAnnotations.DEFAULT_RAW_RECORDS);

        metadata.setMaxRecLen(HTreeAnnotations.DEFAULT_MAX_RECLEN);

        metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code keys.

        /*
         * TODO This sets up a tuple serializer for a presumed case of 4 byte
         * keys (the buffer will be resized if necessary) and explicitly chooses
         * the SimpleRabaCoder as a workaround since the keys IRaba for the
         * HTree does not report true for isKeys(). Once we work through an
         * optimized bucket page design we can revisit this as the
         * FrontCodedRabaCoder should be a good choice, but it currently
         * requires isKeys() to return true.
         */
        final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                new FrontCodedRabaCoder(),// keys : TODO Optimize for int32!
                new SimpleRabaCoder() // vals
        );

        metadata.setTupleSerializer(tupleSer);

        // Will support incremental eviction and persistence.
        rightSolutions = HTree.create(store, metadata);

        // Used to handle optionals (should be ignored otherwise).
        joinSet = HTree.create(store, metadata.clone());

    }

    /**
     * Test helper.
     * 
     * @param optional
     * @param joinVars
     * @param selectVars
     * @param left
     * @param right
     * @param expected
     */
    protected void doHashJoinTest(//
            final boolean optional,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final List<IBindingSet> left, //
            final List<IBindingSet> right,//
            final IBindingSet[] expected//
            ) {

        // Load the right solutions into the HTree.
        {
        
            final BOpStats stats = new BOpStats();
            
            HashJoinUtility.acceptSolutions(
                    new Chunkerator<IBindingSet>(right.iterator()), joinVars, stats,
                    rightSolutions, optional);

            assertEquals(right.size(), rightSolutions.getEntryCount());
            
            assertEquals(right.size(),stats.unitsIn.get());
            
        }

        /*
         * Run the hash join.
         */

        final ICloseableIterator<IBindingSet> leftItr = new CloseableIteratorWrapper<IBindingSet>(
                left.iterator());

        // Buffer used to collect the solutions.
        final TestBuffer<IBindingSet> outputBuffer = new TestBuffer<IBindingSet>();
        
        // Compute the "required" solutions.
        HashJoinUtility
                .hashJoin(leftItr, outputBuffer, joinVars, selectVars,
                        constraints, rightSolutions, joinSet, optional, true/* leftIsPipeline */);

        if(optional) {
            
            // Output the optional solutions.
            HashJoinUtility.outputOptionals(outputBuffer, rightSolutions, joinSet);
            
        }

        // Verify the expected solutions.
        assertSameSolutionsAnyOrder(expected, outputBuffer.iterator());
        
    }
    
}
