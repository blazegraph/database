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
 * Created on Nov 15, 2006
 */
package com.bigdata.objndx;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.ContiguousSlotAllocation;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.Journal;
import com.bigdata.journal.SlotMath;

/**
 * <p>
 * This class implements a variant of a B+Tree in which all values are stored in
 * leaves, but the leaves are not connected with prior-next links. This
 * constraint arises from the requirement to support a copy-on-write policy.
 * </p>
 * <p>
 * Note: No mechanism is exposed for recovering a node or leaf of the tree other
 * than the root by its key. This is because the parent reference on the node
 * (or leaf) can only be set when it is read from the store in context by its
 * parent node.
 * </p>
 * <p>
 * Note: the leaves can not be stitched together with prior and next references
 * without forming cycles that make it impossible to write out the leaves of the
 * btree. This restriction arises because each time we write out a node or leaf
 * it is assigned a persistent identifier as an unavoidable artifact of
 * providing isolation for the object index.
 * </p>
 * <p>
 * Note: This implementation is NOT thread-safe. The index is intended for use
 * within a single-threaded context.
 * </p>
 * <p>
 * Note: This iterators exposed by this implementation do NOT support concurrent
 * structural modification. Concurrent inserts or removals of keys MAY produce
 * incoherent traversal whether or not they result in addition or removal of
 * nodes in the tree.
 * </p>
 * 
 * @todo track the #of nodes and leaves on the hard reference queue in touch and
 *       {@link DefaultEvictionListener}. Also track the number that are clean
 *       vs dirty.
 * 
 * @todo Modify to support "stealing" of immutable nodes by wrapping them with a
 *       thin class encapsulating the {parent, btree} references and refactor
 *       the design until it permits an isolated btree to reuse the in memory
 *       nodes and leaves of the base btree in order to minimize the resource
 *       and IO costs of having multiple concurrent transactions running on the
 *       same journal.
 * 
 * @todo support the concept of a "stride" for fixed length arrays of primitive
 *       data keys, e.g., long[4]. This would treat each run of N values in
 *       {@link AbstractNode#keys} as a single key. The advantage is to minimize
 *       object creation for some kinds of keys, e.g., a triple or quad store or
 *       a fixed length byte[] or char[] such as char[64]. Note that long fixed
 *       length arrays are better off doing reference copying as they will be
 *       moving less data (how large is a Java reference anyway, 4 bytes?).
 * 
 * @todo drop the jdbm-based btree implementation in favor of this one and test
 *       out a GOM integration. this will also require an extser service /
 *       index.
 * 
 * @todo Implement an "extser" index that does not use extser itself, but which
 *       could provide the basis for a database that does use extser. The index
 *       needs to map class names to entries. Those entries are a classId and
 *       set of {version : Serializer} entries.
 * 
 * @todo test object index semantics (clustered index of int32 or int64
 *       identifiers associate with inline objects and time/version stamps).
 *       There is a global index, and then one persistence capable index per
 *       transaction. The time/version stamps are used during validation to
 *       determine if there is a possible write-write conflict. The same kind of
 *       isolation should be provided for object and non-object indices. The
 *       only real difference is that the object index is using an int32 or
 *       int64 key.
 * 
 * @todo we could defer splits by redistributing keys to left/right siblings
 *       that are under capacity - this makes the tree a b*-tree. however, this
 *       is not critical since the journal is designed to be fully buffered and
 *       the index segments are read-only but it would reduce memory by reducing
 *       the #of nodes -- and we can expect that the siblings will be either
 *       resident or in the direct buffer for the journal
 * 
 * @todo should this btree should intrinsically support isolation of inline
 *       objects by carrying the necessary metadata for each index entry? maybe
 *       what we need is an "isolation" constructor? note that the original
 *       isolation scheme effectively carried references to the objects on the
 *       journal (slot allocations) rather than having them inline. we probably
 *       do not need to know where the historical versions is since we only need
 *       to release it on the journal if we are trying to keep a single file and
 *       reuse deallocated space. if we just discard entire journal extents once
 *       their data has been migrated into perfect segments then we never need
 *       to do that deallocation. this leaves us with a version counter (similar
 *       to a timestamp, and perhaps replaceable by a timestamp) and the object
 *       itself. if we make the timestamp part of the key, then all that we are
 *       left with is the object itself.
 * 
 * @todo consider using extser an option for serialization so that we can
 *       continually evolve the node and leaf formats. we will also need a node
 *       serializer that does NOT use extser in order to store the persistent
 *       extser mappings themselves, and perhaps for other things such as an
 *       index of the index ranges that are multiplexed on a given journal.
 *       finally, we will need to use extser to simplify reopening an index so
 *       that we can recover its key serializer, value serializer, and key
 *       comparator as well as various configuration values from its metadata
 *       record. that encapsulation will have to be layered over the basic btree
 *       class so that we can use a more parameterized btree instance to support
 *       extser itself. there will also need to be metadata maintained about the
 *       perfect index range segments so that we know how to decompress blocks,
 *       deserialize keys and values, and compare keys.
 * 
 * @todo model out the metadata index design to locate the components of an
 *       index key range. this will include the journal on which writes for the
 *       key range are multiplexed with other key ranges on either the same or
 *       other indices, any frozen snapshot of a journal that is being processed
 *       into index segment files, and those index segment files themselves. if
 *       a key range is always mapped (multiplexed) to a process on a host, then
 *       the historical journal snapshots and index key range files can be
 *       managed by the host rather than showing up in the metadata index
 *       directly.
 * 
 * @todo support column store style indices (key, column, timestamp), locality
 *       groups that partition the key space so that we can fully buffer parts
 *       of the index that matter, automated version history policies that
 *       expire old values based on either an external timestamp or write time
 *       on the server.
 * 
 * @todo support key range iterators that allow concurrent structural
 *       modification. structural mutations in a b+tree are relatively limited.
 *       When prior-next references are available, an iterator should be easily
 *       able to adjust for insertion and removal of keys.
 * 
 * @todo maintain prior-next references among leaves (and nodes?) in memory even
 *       if we are not able to write them onto the disk. when reading in a leaf,
 *       always set the prior/next reference iff the corresponding leaf is in
 *       memory - this is easily handled by checking the weak references on the
 *       parent node.
 * 
 * @todo support efficient insert of sorted data (batch or bulk insert).
 * 
 * @todo support efficient conditional inserts, e.g., if this key does not exist
 *       then insert this value.
 * 
 * @todo support key compression (prefix and suffix compression and split
 *       interval trickery).
 * 
 * @todo evict subranges by touching the node on the way up so that a node that
 *       is evicted from the hard reference cache will span a subrange that can
 *       be evicted together. this will help to preserve locality on disk. with
 *       this approach leaves might be evicted independently, but also as part
 *       of a node subrange eviction.
 * 
 * @todo since forward scans are much more common, change the post-order
 *       iterator for commit processing to use a reverse traversal so that we
 *       can write the next leaf field whenever we evict a sequence of leaves as
 *       part of a sub-range commit (probably not much of an issue since the
 *       journal is normally fully buffered and the perfect index segments will
 *       not have this problem).
 * 
 * @todo Actually, I could save both prior and next references using a
 *       hand-over-hand chaining in which I pre-serialize the leaf and separate
 *       the allocation step from the write on the store. With just a small
 *       change to the leaf serialization format so that I can write in the
 *       prior and next fields at a known location (which could even be the end
 *       of the buffer), I would then be able to persistent the prior/next
 *       references. <br>
 *       The first step is to start maintaining those references. Also, consider
 *       that it may be useful to maintain them at the node as well as the leaf
 *       level.
 * 
 * @todo pre-fetch leaves for range scans? this really does require asynchronous
 *       IO, which is not available for many platforms (it is starting to show
 *       up in linux 2.6 kernals).
 * 
 * @todo refactor object index implementation into two variants providing
 *       isolation. In one the objects are referenced but exist outside of the
 *       index. This is the current design. In the other, then objects are
 *       stored as part of the values in the index. This is a clustered object
 *       index design and is identicaly to the design where non-int64 keys are
 *       used for the index. The latter design requires that we replace the
 *       currentSlots reference with the current version data. The advantage of
 *       this design is two fold. First, the index may be split into multiple
 *       segments and therefore scales out. Second, objects with key locality
 *       have retrieval locality as well. The former design has an advantage iff
 *       the objects are migrated to a read-optimized segment in which the int64
 *       identifier directly addresses the object in a slot on a page. Making
 *       clustering work effectively with the int64 object index by reference
 *       design probably requires mechanisms for assigning persistent
 *       identifiers based on expected locality of reference, e.g., late
 *       assignment of identifiers to persistence capable transient objects or
 *       an explicit notion of clustering rules or "local" objects.
 * 
 * @todo Support periodic rebuilding (for locality rather than packing purposes
 *       since we have packed index nodes in any case). A goal would be to have
 *       both contiguous leaves with prior/next references intack and to have
 *       the index nodes in a known block on disk for efficient loading. Writes
 *       on a rebuilt index would naturally cause locality to degrade until the
 *       index was rebuilt.
 * 
 * @todo Note that efficient support for large branching factors requires a more
 *       sophisticated approach to maintaining the key order within a node or
 *       leaf. E.g., using a red-black tree or adaptive packed memory array.
 *       However, we can use smaller branching factors for btrees in the journal
 *       and use a separate implementation for bulk generating and reading
 *       "perfect" read-only key range segments.
 * 
 * @todo Support bloom filters. Figure out whether they go in front of an index
 *       or just an index range segment.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTree extends AbstractBTree implements IBTree {
    
    /**
     * The default branching factor.
     */
    static public final int DEFAULT_BRANCHING_FACTOR = 256;
    
    /**
     * The minimum hard reference queue capacity is two(2) in order to avoid
     * cache evictions of the leaves participating in a split.
     */
    static public final int MINIMUM_LEAF_QUEUE_CAPACITY = 2;
    
    /**
     * The size of the hard reference queue used to defer leaf eviction.
     * 
     * @todo if the journal is fully buffered, then the only IO that we are
     *       talking about is serialization of the leaves onto the buffer with
     *       incremental writes through to disk and NO random reads (since the
     *       entire store is buffered in RAM, even though it writes through to
     *       disk).
     * 
     * @todo The leaf cache capacity is effectively multiplied by the branching
     *       factor so it makes sense that we would use a smaller leaf cache
     *       when the branching factor was larger. This is a good reason for
     *       moving the default for this parameter inside of the btree
     *       implementation.
     * 
     * @todo testing with a large leaf cache and a large branching factor means
     *       that you nearly never evict leaves
     */
    static public final int DEFAULT_HARD_REF_QUEUE_CAPACITY = 500;

    /**
     * The #of entries on the hard reference queue that will be scanned for a
     * match before a new reference is appended to the queue. This trades off
     * the cost of scanning entries on the queue, which is handled by the queue
     * itself, against the cost of queue churn. Note that queue eviction drives
     * IOs required to write the leaves on the store, but incremental writes
     * occurr iff the {@link AbstractNode#referenceCount} is zero and the leaf
     * is dirty.
     */
    static public final int DEFAULT_HARD_REF_QUEUE_SCAN = 10;
    
    public int getBranchingFactor() {
        
        return branchingFactor;
        
    }

    public ArrayType getKeyType() {
        
        return keyType;
        
    }
    
    public int getHeight() {
        
        return height;
        
    }

    public int getNodeCount() {
        
        return nnodes;
        
    }

    public int getLeafCount() {
        
        return nleaves;
        
    }

    public int size() {
        
        return nentries;
        
    }

    public NodeSerializer getNodeSerializer() {

        return nodeSer;

    }

    /**
     * The metadata record used to load the last state of the index that was
     * written by {@link #write()}. When an index is loaded this is set to the
     * metadata specified to the constructor. When a new index is created, this
     * is initially <code>null</code>.
     */
    protected BTreeMetadata metadata = null;

    /**
     * The root of the btree. This is initially a leaf until the leaf is split,
     * at which point it is replaced by a node. The root is also replaced each
     * time copy-on-write triggers a cascade of updates.
     */
    protected AbstractNode root;

    /**
     * The height of the btree. The height is the #of leaves minus one. A btree
     * with only a root leaf is said to have <code>height := 0</code>. Note
     * that the height only changes when we split the root node.
     */
    protected int height;

    /**
     * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
     */
    protected int nnodes;

    /**
     * The #of leaf nodes in the btree. This is one (1) for a new btree.
     */
    protected int nleaves;

    /**
     * The #of entries in the btree. This is zero (0) for a new btree.
     */
    protected int nentries;

    public IAbstractNode getRoot() {

        return root;

    }

    /**
     * Constructor for a new btree.
     * 
     * @param store
     *            The persistence store.
     * @param branchingFactor
     *            The branching factor.
     * @param headReferenceQueue
     *            The hard reference queue. The minimum capacity is 2 to avoid
     *            cache evictions of the leaves participating in a split. A
     *            reasonable capacity is specified by
     *            {@link #DEFAULT_HARD_REF_QUEUE_CAPACITY}.
     * @param keyType
     *            The btree can store keys in an array of the specified
     *            primitive data type or in an {@link Object} array. The latter
     *            is required for general purpose keys, but int, long, float, or
     *            double keys may use a primitive data type for better memory
     *            efficiency and less heap churn.
     * @param NEGINF
     *            When keyType is {@link ArrayType#OBJECT} then this MUST be
     *            <code>null</code>. Otherwise this MUST be an instance of
     *            the Class corresponding to the primitive data type for the
     *            key, e.g., {@link Integer} for <code>int</code> keys, and
     *            the value of that instance is generally choosen to be zero(0).
     * @param comparator
     *            When keyType is {@link ArrayType#OBJECT} this is the
     *            comparator used to place the keys into a total ordering. It
     *            must be null for otherwise.
     * @param keySer
     *            Object that knows how to (de-)serialize the keys in a
     *            {@link Node} or a {@link Leaf} of the tree.
     * @param valueSer
     *            Object that knows how to (de-)serialize the values in a
     *            {@link Leaf}.
     */
    public BTree(
            IRawStore store,
            ArrayType keyType,
            int branchingFactor,
            HardReferenceQueue<PO> hardReferenceQueue,
            Object NEGINF,
            Comparator comparator,
            IKeySerializer keySer,
            IValueSerializer valueSer)
    {

        super(getTransitionalRawStore(store), keyType, branchingFactor,
                hardReferenceQueue, NEGINF, comparator,
                PackedAddressSerializer.INSTANCE, keySer, valueSer,
                NodeFactory.INSTANCE);

        /*
         * Note: the mutable BTree has a limit here so that split() will always
         * succeed. That limit does not apply for an immutable btree.
         */
        assert hardReferenceQueue.capacity() >= MINIMUM_LEAF_QUEUE_CAPACITY;
        
         /*
          * FIXME There is no fixed upper limit for URIs (or strings in
          * general), therefore the btree may have to occasionally resize its
          * buffer to accomodate very long variable length keys.
          */
        int maxNodeOrLeafSize = Math.max(
                // max size for a leaf.
                nodeSer.getSize(true, branchingFactor),
                // max size for a node.
                nodeSer.getSize(false, branchingFactor - 1));
        
        log.info("maxNodeOrLeafSize="+maxNodeOrLeafSize);
        
        this.buf = ByteBuffer.allocate(maxNodeOrLeafSize);

        this.height = 0;

        this.nnodes = nleaves = nentries = 0;
        
        this.root = new Leaf(this);
        
    }

    /**
     * Constructor for an existing btree.
     * 
     * @param store
     *            The persistence store.
     * @param metadataId
     *            The persistent identifier of btree metadata.
     * @param hardReferenceQueue
     *            The hard reference queue for {@link Leaf}s.
     * @param valueSer
     *            Object that knows how to (de-)serialize the values in a
     *            {@link Leaf}.
     * 
     * @todo deserialize the keySer, valueSer, comparator, and NEGINF from the
     *       metadata record?  Use default java serialization? extSer?
     */
    public BTree(
            IRawStore store,
            BTreeMetadata metadata,
            HardReferenceQueue<PO> hardReferenceQueue,
            Object NEGINF,
            Comparator comparator,
            IKeySerializer keySer,
            IValueSerializer valueSer
            )
    {

        super(getTransitionalRawStore(store), metadata.keyType,
                metadata.branchingFactor, hardReferenceQueue, NEGINF,
                comparator, PackedAddressSerializer.INSTANCE, keySer, valueSer,
                NodeFactory.INSTANCE);
        
        // save a reference to the immutable metadata record.
        this.metadata = metadata;
        
        // initialize mutable fields from the immutable metadata record.
        this.height = metadata.height;
        this.nnodes = metadata.nnodes;
        this.nleaves = metadata.nleaves;
        this.nentries = metadata.nentries;
        
        /*
         * FIXME There is no fixed upper limit for URIs (or strings in
         * general), therefore the btree may have to occasionally resize its
         * buffer to accomodate very long variable length keys.
         */
        int maxNodeOrLeafSize = Math.max(
                // max size for a leaf.
                nodeSer.getSize(true, branchingFactor),
                // max size for a node.
                nodeSer.getSize(false, branchingFactor - 1));

        log.info("maxNodeOrLeafSize="+maxNodeOrLeafSize);
        
        this.buf = ByteBuffer.allocate(maxNodeOrLeafSize);

        /*
         * Read the root node of the btree.
         */
        this.root = readNodeOrLeaf( metadata.addrRoot );

    }

    /**
     * Writes dirty nodes using a post-order traversal that first writes any
     * dirty leaves and then (recursively) their parent nodes. The parent nodes
     * are guarenteed to be dirty if there is a dirty child so the commit never
     * triggers copy-on-write. This is basically a checkpoint -- it is NOT an
     * atomic commit. The commit protocol is at the store level and involves
     * validating and merging down onto the corresponding global index.
     * 
     * @return The persistent identity of the metadata record for the btree. The
     *         btree can be reloaded from this metadata record. When used as
     *         part of an atomic commit protocol, the metadata record address is
     *         written into a slot on the root block or a named root object.
     * 
     * @todo consider returning a new {@link IBTree} view with the metadata
     *       field set as a means to support isolation rather than just updating
     *       this field.
     */
    public long write() {

        if (root.isDirty()) {

            writeNodeRecursive( root );
            
        }

        BTreeMetadata metadata = new BTreeMetadata(this);
        
        metadata.write(store);
        
        this.metadata = metadata;
        
        return metadata.addrMetadata;

    }
    
    /**
     * @todo Define the semantics for deleting the btree. If the delete occurs
     *       during a transaction the isolation means that we have to delete all
     *       of the keys, causing "delete" entries to spring into existance for
     *       each key in the tree. When the transaction commits, those delete
     *       markers will have to validate against the global state of the tree.
     *       If the transaction validates, then the merge down onto the global
     *       state will cause the corresponding entries to be removed from the
     *       global tree.
     * 
     * Note that if there are persistent nodes in the tree, then copy-on-write
     * is triggered during traversal. In order for us to write an iterator-based
     * delete of the existing keys (causing them to become "delete" markers) we
     * need the iterator to handle concurrent modification, at least to the
     * extent that it can follow the change from the persistent reference for a
     * node to the new mutable reference for that node.
     * 
     * Note that there is probably processing order that is more efficient for
     * delete, e.g., left-to-right vs right-to-left.
     * 
     * @todo There should also be an unisolated DROP INDEX that simply removes
     *       the index and its resources, including secondary index segment
     *       files, in the metadata index, and in the various journals that were
     *       absorbing writes for that index. That operation would require a
     *       lock on the index, which could be achieved by taking the index
     *       offline in the metadata index and invalidating all of the clients.
     *       A similar "ADD INDEX" method would create a distributed index and
     *       make it available to clients.
     * 
     * @todo GOM uses per-object indices. Once they span a single journal they
     *       will have to be registered with the metadata index. However it
     *       would be nice to avoid that overhead when the index is small and
     *       can be kept "near" the generic object owing the index.
     */
    public void delete() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Factory for mutable nodes and leaves used by the {@link NodeSerializer}.
     */
    protected static class NodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new NodeFactory();
        
        private NodeFactory() {}
        
        public ILeafData allocLeaf(IBTree btree, long id, int branchingFactor,
                ArrayType keyType, int nkeys, Object keys, Object[] values) {

            return new Leaf((BTree) btree, id, branchingFactor, nkeys, keys,
                    values);
            
        }

        public INodeData allocNode(IBTree btree, long id, int branchingFactor,
                ArrayType keyType, int nkeys, Object keys, long[] childAddr) {
            
            return new Node((BTree) btree, id, branchingFactor, nkeys, keys,
                    childAddr);
            
        }

    }
    
//    /*
//     * Interface for transactional isolation. 
//     */
//    
//    /**
//     * Returns a fully isolated btree suitable for use within a transaction.
//     * Writes will be applied to the isolated btree. Reads will read first on
//     * the isolated btree and then read through to the source btree iff no entry
//     * is found for a key in the outer btree. In order to commit the changes on
//     * the source btree, the changes must first be {@link #validate() validated},
//     * {@link #mergeDown() merged down} onto the source btree, and dirty nodes
//     * and leaves in the source btree must be written onto the store. Finally,
//     * the store must record the new metadata record for the source btree in a
//     * root block and commit. This protocol can be generalized to allow multiple
//     * btrees to be isolated and atomically committed within the same
//     * transaction. On abort, it is possible that nodes and leaves were written
//     * on the store for the isolated btree. Those data are unreachable and MAY
//     * be recovered depending on the nature of the store and its abort protocol.
//     * 
//     * @param src
//     *            An unisolated btree that will serve as the ground state for
//     *            the transactional view.
//     * 
//     * @todo There are a few problems with this approach <br>
//     *       First, using a btree with MVCC requires that we record a timestamp
//     *       or version counter with each object. This can be implemented using
//     *       either a wrapper class (VersionedBTree) that delegates to BTree and
//     *       encapsulates values as {value,timestamp} entries. Alternatively, we
//     *       could maintain version counters either full time or optionally in
//     *       all leaves as another int[] or long[], one per value. <br>
//     *       Second, we need a means to have active transactions span instances
//     *       of a journal (as each journal fills up it is eventually frozen, a
//     *       new journal is opened, and the indices from the old journal are
//     *       rebuilt in perfect read-only index segments on disk; those segments
//     *       are periodically compacted and segments that grow too large are
//     *       split). when we use a transient map to isolate writes then a
//     *       journal contains only committed state.<br>
//     *       Third, the isolated btree needs to start from the committed stable
//     *       state of another btree (a possible exception is the first
//     *       transaction to create a given btree). In order to verify that the
//     *       source btree meets those requirements we need to know that it was
//     *       loaded from a historical metadata record, e.g., as found in a root
//     *       block or a read-only root names index found in a root block. Merely
//     *       having a persistent root is NOT enough since just writing the tree
//     *       onto the store does not make it restart safe.<br>
//     *       Fourth, it would be very nice if we could reuse immutable nodes
//     *       from the last committed state of a given btree. However, we can not
//     *       simply use the BTree instance from the global state since
//     *       intervening writes will show up inside of its node set and the view
//     *       MUST be of a historical ground state.
//     */
//    public BTree(BTree src) {
//        
//        throw new UnsupportedOperationException();
//        
//    }
//    
//    /**
//     * <p>
//     * Validate changes made to the index within a transaction against the last
//     * committed state of the index in the global scope. In general there are
//     * two kinds of conflicts: read-write conflicts and write-write conflicts.
//     * Read-write conflicts are handled by NEVER overwriting an existing version
//     * (an MVCC style strategy). Write-write conflicts are detected by backward
//     * validation against the last committed state of the journal. A write-write
//     * conflict exists IFF the version counter on the transaction index entry
//     * differs from the version counter in the global index scope. Once
//     * detected, the resolution of a write-write conflict is delegated to a
//     * {@link IConflictResolver conflict resolver}. If a write-write conflict
//     * can not be validated, then validation will fail and the transaction must
//     * abort. The version counters are incremented during commit as part of the
//     * {@link #mergeDown()} of the transaction scope index onto the global scope
//     * index.
//     * </p>
//     * <p>
//     * Validation occurs as part of the prepare/commit protocol. Concurrent
//     * transactions MAY continue to run without limitation. A concurrent commit
//     * (if permitted) would force re-validation since the transaction MUST now
//     * be validated against the new baseline. (It is possible that this
//     * validation could be optimized.)
//     * </p>
//     * 
//     * @return True iff validation succeeds.
//     * 
//     * FIXME As a trivial case, if no intervening commits have occurred on the
//     * journal then this transaction MUST be valid regardless of its write (or
//     * delete) set. This test probably needs to examine the current root block
//     * and the transaction to determine if there has been an intervening commit.
//     * 
//     * FIXME Make validation efficient by a streaming pass over the write set of
//     * this transaction that detects when the transaction identifier for the
//     * global object index has been modified since the transaction identifier
//     * that serves as the basis for this transaction (the committed state whose
//     * object index this transaction uses as its inner read-only context).
//     */
//    public boolean validate() {
//        
//        if(true) throw new UnsupportedOperationException();
//
//        /*
//         * This MUST be the journal's object index. The journals' object index
//         * is NOT always the same as the inner object index map used normally by
//         * the transaction since other transactions MAY have committed on the
//         * journal since the transaction started. If you use the inner object
//         * index for the transaction by mistake then interleaved transactions
//         * will NOT be visible and write-write conflicts will NOT be detected.
//         */
//        final ObjectIndex globalScope = (ObjectIndex)journal.objectIndex;
//        
//        /*
//         * Note: Write-write conflicts can be validated iff a conflict resolver
//         * was declared when the Journal object was instantiated.
//         */
//        final IConflictResolver conflictResolver = journal.getConflictResolver();
//        
//        // Verify that this is a transaction scope object index.
//        assert baseObjectIndex != null;
//        
//        /*
//         * A read-only transaction whose ground state is the current committed
//         * state of the journal. This will be exposed to the conflict resolver
//         * so that it can read the current state of objects committed on the
//         * journal.
//         * 
//         * @todo Extract ITx and refactor Tx to write this class. What is the
//         * timestamp concept for this transaction or does it simply fail to
//         * register itself with the journal?
//         */
//        IStore readOnlyTx = null; // new ReadOnlyTx(journal);
//        
//        final KeyValueIterator itr = objectIndex.getRoot().entryIterator();
//        
//        while( itr.hasNext() ) {
//            
//            // The value for that persistent identifier in the transaction.
//            final SimpleEntry txEntry = (SimpleEntry) itr.next();
//            
//            // The persistent identifier.
//            final Integer id = (Integer) itr.getKey();
//            
//            // Lookup the entry in the global scope.
//            IObjectIndexEntry baseEntry = (IObjectIndexEntry)globalScope.objectIndex.lookup(id);
//            
//            /*
//             * If there is an entry in the global scope, then we MUST compare the
//             * version counters.
//             */
//            if( baseEntry != null ) {
//
//                /*
//                 * If the version counters do not agree then we need to perform
//                 * write-write conflict resolution.
//                 */
//                if( baseEntry.getVersionCounter() != txEntry.getVersionCounter() ) {
//
//                    if( conflictResolver == null ) {
//                        
//                        System.err.println("Could not validate write-write conflict: id="+id);
//                        
//                        // validation failed.
//                        
//                        return false;
//                        
//                    } else {
//                        
//                        try {
//                            
//                            conflictResolver.resolveConflict(id,readOnlyTx,tx);
//                            
//                        } catch( Throwable t ) {
//                            
//                            System.err.println("Could not resolve write-write conflict: id="+id+" : "+t);
//                            
//                            return false;
//                            
//                        }
//
//                        /*
//                         * FIXME We need to write the resolved version on the
//                         * journal. However, we have to take care since this can
//                         * result in a concurrent modification of the
//                         * transaction's object index, which we are currently
//                         * traversing.
//                         * 
//                         * The simple way to handle this is to accumulate
//                         * updates from conflict resolution during validation
//                         * and write them afterwards when we are no longer
//                         * traversing the transaction's object index.
//                         * 
//                         * A better way would operate at a lower level and avoid
//                         * the memory allocation and heap overhead for those
//                         * temporary structures - this works well if we know
//                         * that only the current entry will be updated by
//                         * conflict resolution.
//                         * 
//                         * Finally, if more than one entry can be updated when
//                         * we MUST use an object index data structure for the
//                         * transaction that is safe for concurrent modification
//                         * and we MUST track whether each entry has been
//                         * resolved and scan until all entries resolve or a
//                         * conflict is reported. Ideally cycles will be small
//                         * and terminate quickly (ideally validation itself will
//                         * terminate quickly), in which case we could use a
//                         * transient data structure to buffer concurrent
//                         * modifications to the object index. In that case, we
//                         * only need to buffer records that are actually
//                         * overwritten during validation - but that change would
//                         * need to be manifest throughout the object index
//                         * support since it is essentially stateful (or by
//                         * further wrapping of the transaction's object index
//                         * with a buffer!).
//                         */
//                        
//                    }
//                    
//                }
//                
//                if( baseEntry.getVersionCounter() == Long.MAX_VALUE ) {
//                    
//                    /*
//                     * @todo There may be ways to handle this, but that is
//                     * really a LOT of overwrites. For example, we could just
//                     * transparently promote the field to a BigInteger, which
//                     * would require storing it as a Number rather than a
//                     * [long]. Another approach is to only rely on "same or
//                     * different". With that approach we could use a [short] for
//                     * the version counter, wrap to zero on overflow, and there
//                     * would not be a problem unless there were 32k new versions
//                     * of this entry written while the transaction was running
//                     * (pretty unlikely, and you can always use a packed int or
//                     * long if you are worried :-) We could also just use a
//                     * random number and accept rollback if the random values
//                     * happened to collide.
//                     */
//                    
//                    throw new RuntimeException("Too many overwrites: id="+id);
//                    
//                }
//
//                /*
//                 * Increment the version counter. We add one to the current
//                 * version counter in the _global_ scope since that was the
//                 * current version at the time that the write-write conflict was
//                 * detected.
//                 * 
//                 * Note: We MUST bump the version counter even if the "WRITE"
//                 * was a "DELETE" otherwise we will fail to notice a write-write
//                 * conflict where an intervening transaction deletes the version
//                 * and commits before an overwrite of the version by a concurrent
//                 * transaction.
//                 */
//                txEntry.versionCounter = (short) (baseEntry.getVersionCounter() + 1);
//                                
//            }
//            
//        }
//
//        // validation suceeded.
//        
//        return true;
//
//    }
//
//    /**
//     * <p>
//     * Merge the transaction scope index onto its global scope index.
//     * </p>
//     * <p>
//     * Note: This method is invoked by a transaction during commit processing to
//     * merge the write set of an index into the global scope. This operation
//     * does NOT check for conflicts. The pre-condition is that the transaction
//     * has already been validated (hence, there will be no conflicts). This
//     * method is also responsible for incrementing the version counters that are
//     * used to detect write-write conflicts during validation.
//     * </p>
//     * 
//     * @todo For a persistence capable implementation of the object index we
//     *       could clear currentVersionSlots during this operation since there
//     *       should be no further access to that field. The only time that we
//     *       will go re-visit the committed object index for the transaction is
//     *       when we GC the pre-existing historical versions overwritten during
//     *       that transaction. Given that, we do not even need to store the
//     *       object index root for a committed transaction (unless we want to
//     *       provide a feature for reading historical states, which is NOT part
//     *       of the journal design). So another option is to just write a chain
//     *       of {@link ISlotAllocation} objects. (Note, per the item below GC
//     *       also needs to remove entries from the global object index so this
//     *       optimization may not be practical). This could be a single long
//     *       run-encoded slot allocation spit out onto a series of slots during
//     *       PREPARE. When we GC the transaction, we just read the chain,
//     *       deallocate the slots found on that chain, and then release the
//     *       chain itself (it could have its own slots added to the end so that
//     *       it is self-consuming). Just pay attention to ACID deallocation so
//     *       that a partial operation does not have side-effects (at least, side
//     *       effects that we do not want). This might require a 3-bit slot
//     *       allocation index so that we can encode the conditional transition
//     *       from (allocated + committed) to (deallocated + uncommitted) and
//     *       know that on restart the state should be reset to (allocated +
//     *       committed).
//     * 
//     * @todo GC should remove the 'deleted' entries from the global object index
//     *       so that the index size does not grow without limit simply due to
//     *       deleted versions. This makes it theoretically possible to reuse a
//     *       persistent identifier once it has been deleted, is no longer
//     *       visible to any active transaction, and has had the slots
//     *       deallocated for its last valid version. However, in practice this
//     *       would require that the logic minting new persistent identifiers
//     *       received notice as old identifiers were expired and available for
//     *       reuse. (Note that applications SHOULD use names to recover root
//     *       objects from the store rather than their persistent identifiers.)
//     * 
//     * FIXME Validation of the object index MUST specifically treat the case
//     * when no version for a persistent identifier exists in the ground state
//     * for a tx, another tx begins and commits having written a version for that
//     * identifier, and then this tx attempts to commit having written (or
//     * written and deleted) a version for that identifier. Failure to treat this
//     * case will cause problems during the merge since there will be an entry in
//     * the global scope that was NOT visible to this transaction (which executed
//     * against a distinct historical global scope). My take is the persistent
//     * identifier assignment does not tend to have semantics (they are not
//     * primary keys, but opaque identifiers) therefore we MUST NOT consider them
//     * to be the same "object" and an unreconcilable write-write conflict MUST
//     * be reported during validation. (Essentially, two transactions were handed
//     * the same identifier for new objects.)
//     * 
//     * FIXME Think up sneaky test cases for this method and verify its operation
//     * in some detail.
//     */
//    public void mergeDown() {
//
//        if(true) throw new UnsupportedOperationException();
//        
//        // Verify that this is a transaction scope object index.
//        assert baseObjectIndex != null;
//        
//        final KeyValueIterator itr = objectIndex.getRoot().entryIterator();
//        
//        while( itr.hasNext() ) {
//            
//            // The value for that persistent identifier.
//            IObjectIndexEntry entry = (IObjectIndexEntry) itr.next();
//            
//            // The persistent identifier.
//            final Integer id = (Integer) itr.getKey();
//            
//            if( entry.isDeleted() ) {
//
//                /*
//                 * IFF there was a pre-existing version in the global scope then
//                 * we clear the 'currentVersionSlots' in the entry in the global
//                 * scope and mark the index entry as dirty. The global scope
//                 * will now recognized the persistent identifier as 'deleted'.
//                 */
//                
//                if( entry.isPreExistingVersionOverwritten() ) {
//
//                    /*
//                     * Update the entry in the global object index.
//                     * 
//                     * Note: the same post-conditions could be satisified by
//                     * getting the entry in the global scope, clearing its
//                     * [currentVersionSlots] field, settting its
//                     * [preExistingVersionSlots] field and marking the entry as
//                     * dirty -- that may be more effective with a persistence
//                     * capable implementation. It is also more "obvious" and
//                     * safer since there is no reference sharing.
//                     */
//                    ((ObjectIndex)journal.objectIndex).objectIndex.insert(id,entry);
//                    
//                } else {
//                    
//                    /*
//                     * The deleted version never existed in the global scope.
//                     */
//                    
//                }
//
//            } else {
//
//                /*
//                 * Copy the entry down onto the global scope.
//                 */
//                ((ObjectIndex)journal.objectIndex).objectIndex.insert(id, entry);
//
//                /*
//                 * Mark the slots for the current version as committed.
//                 * 
//                 * @todo This MUST be atomic. (It probably will be once it is
//                 * modified for a persistence capable index since we do not
//                 * record the new root of the object index on the journal until
//                 * the moment of the commit, so while dirty index nodes may be
//                 * evicted onto the journal, they are not accessible in case of
//                 * a transaction restart. This does suggest a recursive twist
//                 * with whether or not the slots for the index nodes themsevles
//                 * are marked as committed on the journal -- all stuff that
//                 * needs tests!)
//                 */
//                journal.allocationIndex.setCommitted(entry.getCurrentVersionSlots());
//                
//            }
//            
//            /*
//             * The slots allocated to the pre-existing version are retained in
//             * the index entry for this transaction until the garbage collection
//             * is run for the transaction. This is true regardless of whether
//             * new version(s) were written in this transaction, if the
//             * pre-existing version was simply deleted, or if the most recent
//             * versions written by this transaction was finally deleted. If the
//             * entry is holding the slots for a pre-existing version that was
//             * overwritten then we MUST NOT remove it from the transaction's
//             * object index. That information is required later to GC the
//             * pre-existing versions.
//             */
//            
//            if( ! entry.isPreExistingVersionOverwritten() ) {
//
//                // Remove the index entry in the transaction scope.
//                
//                itr.remove();
//
//            }
//
//        }
//
//    }

//    /**
//     * Optional operation deletes @todo document and reconcile with a clustered object index. also, if we
//     *       go with a scale out design an journal snapshots where index ranges
//     *       are evicted to index segments then we never need to both with GC
//     *       inside of the journal and we can use a WORM style allocator
//     *       (perfect fit allocation vs slots).
//     * 
//     * After a commit, the only entries that we expect to find in the
//     * transaction's object index are those where a pre-existing version was
//     * overwritten by the transaction. We just deallocate the slots for those
//     * pre-existing versions.
//    
//    * This implementation simply scans the object index. After a commit, the
//    * only entries that we expect to find in the transaction's object index are
//    * those where a pre-existing version was overwritten by the transaction. We
//    * just deallocate the slots for those pre-existing versions.
//    * 
//    * @param allocationIndex
//    *            The index on which slot allocations are maintained.
//    * 
//    * FIXME The transaction's object index SHOULD be deallocated on the journal
//    * after garbage collection since it no longer holds any usable information.
//    * 
//    * FIXME Garbage collection probably MUST be atomic (it is Ok if it is both
//    * incremental and atomic, but it needs a distinct commit point, it must be
//    * restart safe, etc.).
//     */
//    public void gc() {
//        throw new UnsupportedOperationException();        
    // Verify that this is a transaction scope object index.
//    assert baseObjectIndex != null;
//    
//    final Iterator itr = objectIndex.getRoot().entryIterator();
//    
//    while( itr.hasNext() ) {
//        
//        // The value for that persistent identifier.
//        final IObjectIndexEntry entry = (IObjectIndexEntry)itr.next();
//        
//        // The slots on which the pre-existing version was written.
//        ISlotAllocation preExistingVersionSlots = entry
//                .getPreExistingVersionSlots();
//
//        // Deallocate those slots.
//        allocationIndex.clear(preExistingVersionSlots);
//        
//        /*
//         * Note: This removes the entry to avoid possible problems with
//         * double-gc. However, this issue really needs to be resolved by an
//         * ACID GC operation.
//         */
//        itr.remove();
//            
//    }
//}

    static protected IRawStore2 getTransitionalRawStore(IRawStore store) {
     
        if( store instanceof IRawStore2 ) {
            
            return (IRawStore2) store;
            
        } else {
            
            return new TransitionalRawStore( store );
            
        }
        
    }
    
    /**
     * Transition class allows the {@link BTree} to be used with the
     * {@link Journal} while I defer refactoring of the {@link Journal} to use
     * {@link Addr} vs {@link ISlotAllocation}.
     */
    protected static class TransitionalRawStore implements IRawStore2 {
        
        protected final IRawStore delegate;
        
        public TransitionalRawStore(IRawStore delegate) {
            
            assert delegate != null;
            
            this.delegate = delegate;
            
        }
        
        /**
         * Convert the persistent identifier into an {@link ISlotAllocation}.
         * 
         * @param id
         *            The persistent identifier.
         * 
         * @return The {@link ISlotAllocation}
         */
        protected ISlotAllocation asSlots(long id) {
            
            final int firstSlot = SlotMath.getFirstSlot(id);
            
            final int byteCount = SlotMath.getByteCount(id);

            final int slotCount = delegate.getSlotMath().getSlotCount(byteCount);
            
            return new ContiguousSlotAllocation(byteCount, slotCount, firstSlot);

        }

        /**
         * Note: This is the target method for reading data on the new
         * interface. The method signatures with {@link ISlotAllocation} are all
         * deprecated and will be refactor out soon.
         */
        public ByteBuffer read(long addr, ByteBuffer dst) {

            ISlotAllocation slots = asSlots(addr);

            return delegate.read(slots,dst);
        }

        public void delete(long addr) {
            
            delegate.delete(asSlots(addr));
            
        }

        public long write(ByteBuffer data) {
            
            return delegate.write(data).toLong();
            
        }
        
    }
    
}
