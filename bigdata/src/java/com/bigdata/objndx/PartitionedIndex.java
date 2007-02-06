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
 * Created on Dec 22, 2006
 */

package com.bigdata.objndx;

import com.bigdata.journal.Journal;
import com.bigdata.rawstore.IRawStore;

/**
 * An index that is dynamically decomposed into key-range partitions. Each
 * partition is defined by the first key that may be inserted on, or read from,
 * that parition. A total ordering over paritions and their locations is
 * maintained in a metadata index. An insert or read is directed to the first
 * parition having a minimum key greater than or equal to the probe key.
 * 
 * An index partition is comprised of a {@link Journal} having a {@link BTree}
 * that buffers writes for that partition and zero or more {@link IndexSegment}s.
 * A {@link Journal} will typically multiplex writes for multiple paritions, but
 * each parition is associated with a distinct {@link BTree}. When a
 * {@link Journal} nears is capacity its buffer and backing store are frozen and
 * a new buffer and backing store are deployed to absorb further writes. A
 * background thread then builds an {@link IndexSegment} from each {@link BTree}
 * on the {@link Journal} and then deletes the backing file and releases the
 * associated buffer. Each {@link IndexSegment} built in this manner contains a
 * historical snapshot of data written on {@link Journal} for a given partition.
 * Periodically {@link IndexSegment}s are combined using a merge sort,
 * producing a new {@link IndexSegment}. The inputs to this process are then
 * deleted.
 * 
 * A partition overflows and is split when the total data size of an index
 * parition (estimated as the sum of the {@link IndexSegment}s and the data on
 * the {@link Journal} for that parition) exceeds a threshold. Likewise,
 * partitions may underflow and be joined. These operations require updates to
 * the metadata index so that futher requests are directed based on the new
 * parition boundaries.
 * 
 * Likewise, the location of a partition, including the {@link Journal} service
 * and the {@link IndexSegment}s may change over time either due to load
 * balancing or to fail over.
 * 
 * Writes, including deletes, are absorbed by a {@link BTree} on a
 * {@link Journal}.
 * 
 * Reads are performed against a fused view of the partition, which is created
 * from the {@link BTree} as well as any live {@link IndexSegment}s. Each of
 * these sources are queried, potentially in parallel, and a merge sort is
 * performed that filters out "deleted" entries and streams to the caller only
 * those entries that satisify not only the key range query but additional
 * metadata constraints, e.g., on attribute names (really part of the key), or
 * on timestamps.
 * 
 * @todo Unisolated writes may be accumulated into a "commit group" for greater
 *       efficiency. A "native transaction" mechanism can be used to batch a set
 *       of unisolated writes, e.g., all attributes for some "row". This does
 *       not provide real transactional isolation, but it can be very effective
 *       and offer high throughput for a variety of applications.
 * 
 * @todo Transactional isolation can be provided in a variety of ways. The
 *       easiest is to buffer the changes in a memory-only data structure so
 *       that they may span journal overflow events transparently. The
 *       disadvantage of this approach is that both a large number of concurrent
 *       transactions and VLR Tx will cause heap churn and memory exhaustion on
 *       the server. The alternative is to use a persistence capable B+Tree to
 *       isolate transactions. In either case, we want to be able to efficiently
 *       continue to read against the historical ground state from which a
 *       transaction emerged.
 * 
 * To use a segmented index I will need to implement an abstraction that handles
 * a read against the btree on the journal and the various segments of the index
 * that have already been evicted from the journal, basically providing a merged
 * view. I also need to handle compacting merges of index segments, which gets
 * into the isolation policy a bit (you have to write delete markers and
 * timestamps for deleted entries and timestamps for updated entries). Hopefully
 * those will go pretty smoothly.
 * 
 * When you open an index segment the nodes are fully buffered, so that will
 * absorb an increasing amount of RAM as we get more segments on a server. The
 * reason to buffer the index segment nodes is that you have to do only one IO
 * per segment to read any data that may be in the segment relevant to a query.
 * In the end both RAM consumption and random seeks for index segment leaves
 * will probably prove to be the limited factors on the capacity of a server. We
 * could evict, e.g., close, segments that are not being queried to help stave
 * off RAM starvation and we could fully buffer the leaves for very hot index
 * segments to help stave off IO congestion. I don’t have the data today to
 * estimate the proportion of an index segment that is the nodes (vs the
 * leaves). It will depend on the size of the values stored in the leaves, but
 * generally the leaves are much larger than the nodes of the tree.
 * 
 * 
 * 
 * Beyond that we can either get into the distributed side (scale out) or
 * implement some bloom filters so that we can do fast correct rejections of
 * index point queries that will not be found if we did go out to the various
 * segments (bloom filters provide a fast “no” test).
 * 
 * 
 * Note: This does not extend {@link AbstractBTree} so that it does not have to
 * implement some operations that are far more expensive on a distributed index
 * than on a {@link BTree} or {@link IndexSegment}, e.g., reporting on the
 * height, #of nodes or #of leaves, or the #of entries in the index.
 * 
 * @todo define parallelized key scans?
 * 
 * @todo define the rangeIterator to use a leafScan and then filter the leaf.
 * 
 * @todo use bloom filters to perform fast "correct rejection" point tests for
 *       an index parition.
 * 
 * @todo There will need to be a restart safe means to identify the distributed
 *       index. Its state is divided between the metadata index and the index
 *       partitions. Each partition in turn is comprised of a {@link BTree} on a
 *       {@link Journal} and zero or more {@link IndexSegment}s. If the
 *       metadata index is turned into a service then multiple distributed
 *       indices could be registered with that service and the partition entries
 *       would be the index name and the separator key for the partition.
 * 
 * @todo this will eventually need to be refactored into a client API, protocol,
 *       a robust service for index metadata management, and a distributed
 *       protocol for access to the various index partitions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PartitionedIndex implements IBTree {

    /**
     * The metadata index.
     */
    final MetadataIndex ndx_md;

    /**
     * Create a new distributed index.
     * 
     * @todo add an index name and distributed index lookup service?
     * 
     * @todo persist all the index configuration metadata. we need it to
     *       initialize each new {@link BTree}, open each {@link IndexSegment},
     *       etc. This information just needs to get into the metadata record
     *       for the individual btrees.
     * 
     * @todo manage the edge condition when there is just one partition
     *       effectively. This is a bit awkward as there needs to be some
     *       separatorKey to insert things into the metadata index. perhaps the
     *       btree needs to be modified to permit NEGINF as a key, at least
     *       optionally? Alternatively, when you think of a node there is always
     *       one more child than there are keys -- this is the same problem. Or
     *       consider a root node that may become deficient unto holding only a
     *       single entry. The easy solutions are to either define the minimum
     *       #of partitions as two (2) or to handle one partition as a special
     *       case by explicitly storing its reference.
     */
    public PartitionedIndex(IRawStore store, int branchingFactor) {
        
        ndx_md = new MetadataIndex(store, branchingFactor);

//        ndx_md.insert(NEGINF, entry) = new MetadataIndex(store, branchingFactor, NEGINF, comparator,
//                keySerializer);

    }

    public Object insert(Object key, Object entry) {
        return null;
    }

    public Object lookup(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean contains(byte[] key) {
        // TODO Auto-generated method stub
        return false;
    }

    public Object remove(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    public int rangeCount(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return 0;
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return null;
    }

}
