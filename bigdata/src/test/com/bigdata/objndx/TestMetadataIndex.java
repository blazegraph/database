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
 * Created on Dec 7, 2006
 */

package com.bigdata.objndx;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Vector;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;
import com.bigdata.objndx.ndx.IntegerComparator;

/**
 * A test suite that will be evolved into a metadata index designed to locate
 * the host/process responsible for a given key range of an index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMetadataIndex extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestMetadataIndex() {
    }

    /**
     * @param name
     */
    public TestMetadataIndex(String name) {
        super(name);
    }

    /**
     * A metadata index for the index segments of a distributed index. There is
     * one metadata index for each distributed index. The keys of the metadata
     * index are the first key that would be directed into the corresponding
     * index segment (this is just the standard btree semantics). Keys in the
     * metadata index are updated as index segments are split (or joined).
     * Splits of index segments are performed when the fully compacted segment
     * exceeds ~100M. Joins of index segments are performed when sibling
     * segments in the key order could be merged to produce an index segment of
     * ~100M.  Splitting and joining index segments is atomic and respects 
     * transaction isolation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MetadataIndex extends BTree {

        /**
         * Create a new tree.
         * 
         * @todo change the key type to a general purpose one.
         * 
         * @param store
         * @param branchingFactor
         */
        public MetadataIndex(IRawStore store, int branchingFactor) {
            super(store,
                    ArrayType.INT,
                    branchingFactor,
                    new HardReferenceQueue<PO>(
                    new DefaultEvictionListener(),
                    BTree.DEFAULT_LEAF_QUEUE_CAPACITY,
                    BTree.DEFAULT_LEAF_QUEUE_SCAN),
                    Integer.valueOf(0),
                    null, // no comparator for primitive key type.
                    Int32OIdKeySerializer.INSTANCE,
                    new IndexSegmentMetadata.Serializer());
        }

        /**
         * Load existing tree.
         * 
         * @param store
         * @param metadataId
         */
        public MetadataIndex(IRawStore store, long metadataId) {
            super(store, metadataId, new HardReferenceQueue<PO>(
                    new DefaultEvictionListener(),
                    BTree.DEFAULT_LEAF_QUEUE_CAPACITY,
                    BTree.DEFAULT_LEAF_QUEUE_SCAN),
                    Integer.valueOf(0),
                    null, // no comparator for primitive key type.
                    Int32OIdKeySerializer.INSTANCE,
                    new IndexSegmentMetadata.Serializer());
        }

        /**
         * Update the metadata index to reflect the split of one index segment
         * into two index segments.
         * 
         * @param separatorKey
         *            Requests greater than or equal to the separatorKey (and
         *            less than the next largest separatorKey in the metadata
         *            index) are directed into seg2. Requests less than the
         *            separatorKey (and greated than any proceeding separatorKey
         *            in the metadata index) are directed into seg1.
         * @param seg1
         *            The metadata for the index segment that was split.
         * @param seg2
         *            The metadata for the right sibling of the split index
         *            segment in terms of the key range of the distributed
         *            index.
         */
        public void split(Object separatorKey, IndexSegmentMetadata md1, IndexSegmentMetadata md2) {
            
        }

        /**
         * @todo join of index segment with left or right sibling. unlike the
         *       nodes of a btree we merge nodes whenever a segment goes under
         *       capacity rather than trying to redistribute part of the key
         *       range from one index segment to another.
         */
        public void join() {
            
        }
        
    }
    
    /**
     * The location of the process that will handle requests for some key range
     * together with some metadata about that {process + key range}. Note that
     * many key ranges, potentially for different indices, are multiplexed onto
     * the same process. Each process is backed by a journal providing
     * transactional isolation. Periodicall the journal is frozen, a new journal
     * is created, and index segment files are created from the journal. Those
     * files are incorporated into the view of the index iff they contain
     * committed state. (Uncommitted state from a transaction that bridges a
     * journal boundary if permitted must be handled once the transaction
     * commits.)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class IndexSegmentMetadata {

        /**
         * The metadata about an index segment life cycle as served by a
         * specific service instance on some host.
         * 
         * @todo we need to track load information for the service and the host.
         *       however that information probably does not need to be restart
         *       safe so it is easily maintained within a rather small hashmap
         *       indexed by the service address.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class IndexSegmentServiceMetadata {

            /**
             * The service that is handling this index segment. This service
             * typically handles many index segments and multiplexes them on a
             * single journal.
             * 
             * @todo When a client looks up an index segment in the metadata index,
             *       what we send them is the set of key-addr entries from the leaf
             *       in which the index segment was found. If a request by the
             *       client to that service discovers that the service no longer
             *       handles a key range, that the service is dead, etc., then the
             *       client will have to invalidate its cache entry and lookup the
             *       current location of the index segment in the metadata index.
             */
            public InetSocketAddress addr;
            
        }

        /**
         * An array of the services that are registered as handling this index
         * segment. One of these services is the master and accepts writes from
         * the client. The other services mirror the segment and provide
         * redundency for failover and load balancing. The order in which the
         * segments are listed in this array could reflect the master (at
         * position zero) and the write pipeline from the master to the
         * secondaries could be simply the order of the entries in the array.
         */
        public IndexSegmentServiceMetadata services[];
        
        /**
         * The time that the index segment was started on that service.
         */
        public long startTime;

        /**
         * A log of events for the index segment. This could just be a linked
         * list of strings that get serialized as a single string. Each event is
         * then a semi-structured string, typically generated by a purpose
         * specific logging appender.
         */
        public Vector<Event> eventLog;
        
        public IndexSegmentMetadata(InetSocketAddress addr) {
        }

        /**
         * An event for an index segment.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class Event {
        
//            public long timestamp;
            
            public String msg;
            
            /**
             * Serialization for an event.
             * 
             * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
             * @version $Id$
             */
            public static class Serializer /*implements ...*/{
                
            }
            
        }
        
        /**
         * Serialization for an index segment metadata entry.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class Serializer implements IValueSerializer {

            public int getSize(int n) {
                // TODO Auto-generated method stub
                return 0;
            }

            public void getValues(ByteBuffer buf, Object[] values, int n) {
                // TODO Auto-generated method stub
                
            }

            public void putValues(ByteBuffer buf, Object[] values, int n) {
                // TODO Auto-generated method stub
                
            }
            
        }
    }
        
}
