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
 * Created on Dec 19, 2006
 */

package com.bigdata.objndx;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;
import com.bigdata.objndx.IndexSegment.FileStore;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Base class for mutable and immutable B+Tree implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBTree implements IBTree {

    /**
     * Log for btree opeations.
     * 
     * @todo consider renaming the logger.
     */
    protected static final Logger log = Logger.getLogger(BTree.class);
    
    /**
     * Log for {@link BTree#dump(PrintStream)} and friends.
     */
    protected static final Logger dumpLog = Logger.getLogger(BTree.class
            .getName()
            + "#dump");

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Counters tracking various aspects of the btree.
     */
    protected final Counters counters = new Counters(this);

    /**
     * The persistence store.
     */
    final protected IRawStore2 store;

    /**
     * The type for keys for this btree. The key type may be a primitive data
     * type or {@link Object}.
     * 
     * @see ArrayType
     */
    final protected ArrayType keyType;

    /**
     * The branching factor for the btree.
     */
    final protected int branchingFactor;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     */
    final protected NodeSerializer nodeSer;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     * 
     * This should be pre-allocated to the maximum size of any node or leaf. If
     * the buffer overflows it will be re-allocated and the operation will be
     * retried.
     * 
     * @todo handle realloc. modify RecordCompressor to NOT use 2x for extending
     *       its buffer. try += 4k each time instead.
     */
    protected ByteBuffer buf;
    
    /**
     * The comparator used iff the key type is not a primitive data type. When
     * the key type is a primitive data type then comparison is performed using
     * the operations for EQ, GT, LT, etc. rather than a {@link Comparator}.
     * 
     * @todo extend to permit comparison of the value as well as the key so that
     *       a total order can be established that permits key duplicates with
     *       distinct value attributes -or- support duplicates by put(k,v) vs
     *       add(k,v) and set(k,v) semantics and provision a given tree either
     *       to permit duplicates or not.
     */
    final protected Comparator comparator;

    /**
     * An invalid key. This is used primarily as the value of keys that are not
     * defined within a node or a leaf of the tree. The value choosen for a
     * primitive data type is arbitrary, but common choices are zero (0), -1,
     * and the largest negative value in the value space for the data type. When
     * the btree is serving as an object identifier index, then the value
     * choosen is always zero(0) since it also carries the semantics of a null
     * reference. When the keys are Objects then the value choosen MUST be
     * <code>null</code>. The need to have an illegal value for keys of a
     * primitive data necessarily imposes a restriction of a value that may not
     * appear as a legal key.
     * 
     * @todo rename as "NULL" since it means a null reference and hence an
     *       invalid key more than it means anything else.
     * 
     * @todo do we actually need an illegal value for primitive keys or is this
     *       just paranoia that insists on unused keys being NEGINF vs whatever
     *       the last key value was (essentially garbage). As long as the code
     *       never looks at an unused key I think that things should be ok. As
     *       far as I can tell there is not any requirement for a key less than
     *       any valid key in the btree implementation. We do use NEGINF in
     *       several assertions and test suites in both the sense of the value
     *       that an unused key must have an in the sense of a value less than
     *       any legal key. There used to be a concept of POSINF but it proved
     *       useless except as part of range checking the keys and key range
     *       restrictions are best imposed, by subclassing or application
     *       constraints, or be a more declarative interface.
     */
    protected final Object NEGINF;

    /**
     * Leaves are added to a hard reference queue when they are created or read
     * from the store. On eviction from the queue the leaf is serialized by a
     * listener against the {@link IRawStore}. Once the leaf is no longer
     * strongly reachable its weak references may be cleared by the VM. Note
     * that leaves are evicted as new leaves are added to the hard reference
     * queue. This occurs in two situations: (1) when a new leaf is created
     * during a split of an existing leaf; and (2) when a leaf is read in from
     * the store. The minimum capacity for the hard reference queue is two (2)
     * so that a split may occur without forcing eviction of either leaf in the
     * split. Incremental writes basically make it impossible for the commit IO
     * to get "too large" where too large is defined by the size of the hard
     * reference cache.
     * 
     * Note: The code in {@link Node#postOrderIterator(boolean)} and
     * {@link DirtyChildIterator} MUST NOT touch the hard reference queue since
     * those iterators are used when persisting a node using a post-order
     * traversal. If a hard reference queue eviction drives the serialization of
     * a node and we touch the hard reference queue during the post-order
     * traversal then we break down the semantics of
     * {@link HardReferenceQueue#append(Object)} as the eviction does not
     * necessarily cause the queue to reduce in length.
     * 
     * @todo This is all a bit fragile. Another way to handle this is to have
     *       {@link HardReferenceQueue#append(Object)} begin to evict objects
     *       before is is actually at capacity, but that is also a bit fragile.
     * 
     * @todo This queue is now used for both nodes and leaves. Update the
     *       javadoc here, in the constants that provide minimums and defaults
     *       for the queue, and in the other places where the queue is used or
     *       configured. Also rename the field to nodeQueue or refQueue.
     * 
     * @todo Consider breaking this into one queue for nodes and another for
     *       leaves. Would this make it possible to create a policy that targets
     *       a fixed memory burden for the index? As it stands the #of nodes and
     *       the #of leaves in memory can vary and leaves require much more
     *       memory than nodes (for most trees).
     */
    final HardReferenceQueue<PO> leafQueue;

    /**
     * The minimum allowed branching factor (3).
     */
    static public final int MIN_BRANCHING_FACTOR = 3;
    
    /**
     * @param store
     *            The persistence store.
     * @param headReferenceQueue
     *            The hard reference queue.
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
    protected AbstractBTree(IRawStore2 store,
            ArrayType keyType,
            int branchingFactor,
            HardReferenceQueue<PO> hardReferenceQueue, Object NEGINF,
            Comparator comparator, IKeySerializer keySer,
            IValueSerializer valueSer, INodeFactory nodeFactory ) {
        
        assert store != null;
        
        assert keyType != null;

        assert branchingFactor >= MIN_BRANCHING_FACTOR;

        assert hardReferenceQueue != null;
        
        assert keySer != null;
        
        assert valueSer != null;

        assert nodeFactory != null;
        
        if( keyType == ArrayType.OBJECT ) {
            
            if (NEGINF != null) {

                throw new IllegalArgumentException(
                        "NEGINF must be null when not using a primitive key type.");

            }

            if( comparator == null ) {
                
                throw new IllegalArgumentException(
                        "A comparator must be specified unless using a primitive key type.");
                
            }
            
        } else {
            
            if( NEGINF == null ) {
                
                throw new IllegalArgumentException(
                        "NEGINF must be non-null when using a primtive key type.");
                
            }
            
            if( comparator != null ) {
                
                throw new IllegalArgumentException("The comparator must be null when using a primitive key type");
                
            }
            
        }
        
        this.store = store;

        this.keyType = keyType;

        this.branchingFactor = branchingFactor;

        this.leafQueue = hardReferenceQueue;

        this.comparator = comparator;
        
        this.NEGINF = NEGINF;

        this.nodeSer = new NodeSerializer(nodeFactory, keySer, valueSer);

    }
    
    /**
     * The persistence store.
     */
    public IRawStore2 getStore() {
        
        return store;
        
    }
    
    /**
     * The branching factor for the btree.
     */
    public int getBranchingFactor() {
        
        return branchingFactor;
        
    }
    
    /**
     * The key type for the btree.
     */
    public ArrayType getKeyType() {
        
        return keyType;
        
    }

    /**
     * The height of the btree. The height is the #of levels minus one. A btree
     * with only a root leaf has <code>height := 0</code>. A btree with a
     * root node and one level of leaves under it has <code>height := 1</code>.
     * Note that all leaves of a btree are at the same height (this is what is
     * means for the btree to be "balanced"). Also note that the height only
     * changes when we split or join the root node (a btree maintains balance by
     * growing and shrinking in levels from the top rather than the leaves).
     */
    abstract public int getHeight();
    
    /**
     * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
     */
    abstract public int getNodeCount();

    /**
     * The #of leaf nodes in the btree.  This is one (1) for a new btree.
     */
    abstract public int getLeafCount();

    /**
     * The #of entries (aka values) in the btree. This is zero (0) for a new
     * btree.  The returned value reflects only the #of entries in the btree
     * and does not report the #of entries in a segmented index.
     */
    abstract public int size();
    
    /**
     * The object responsible for (de-)serializing the nodes and leaves of the
     * {@link IBTree}.
     */
    public NodeSerializer getNodeSerializer() {
        
        return nodeSer;
        
    }
    
    /**
     * The root of the btree. This is initially a leaf until the leaf is
     * split, at which point it is replaced by a node. The root is also
     * replaced each time copy-on-write triggers a cascade of updates.
     */
    abstract public IAbstractNode getRoot();
    
    public Object insert(Object key, Object entry) {

        if( key == null ) throw new IllegalArgumentException();
        if( entry == null ) throw new IllegalArgumentException();
        
        counters.ninserts++;
        
        assert entry != null;

        if(INFO) {
            log.info("key="+key+", entry="+entry);
        }

        return getRoot().insert(key, entry);

    }

    public Object lookup(Object key) {

        if( key == null ) throw new IllegalArgumentException();

        counters.nfinds++;
        
        return getRoot().lookup(key);

    }

    public Object remove(Object key) {

        if( key == null ) throw new IllegalArgumentException();

        counters.nremoves++;

        if(INFO) {
            log.info("key="+key);
        }

        return getRoot().remove(key);

    }

    public IRangeIterator rangeIterator(Object fromKey, Object toKey) {

        return new RangeIterator(this,fromKey,toKey);
        
    }

    public KeyValueIterator entryIterator() {
    
        return getRoot().entryIterator();
        
    }
    
    /**
     * Iterator visits the leaves of the tree.
     * 
     * @return Iterator visiting the {@link Leaf leaves} of the tree.
     * 
     * @todo optimize this when prior-next leaf references are present, e.g.,
     *       for an {@link IndexSegment}.
     */
    protected Iterator leafIterator() {
        
        return new Striterator(getRoot().postOrderIterator())
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    protected boolean isValid(Object arg0) {

                        return arg0 instanceof Leaf;

                    }
                });
        
    }
        
    /**
     * Computes and returns the utilization of the tree. The utilization figures
     * do not factor in the space requirements of nodes and leaves.
     * 
     * @return An array whose elements are:
     *         <ul>
     *         <li>0 - the leaf utilization percentage [0:100]. The leaf
     *         utilization is computed as the #of values stored in the tree
     *         divided by the #of values that could be stored in the #of
     *         allocated leaves.</li>
     *         <li>1 - the node utilization percentage [0:100]. The node
     *         utilization is computed as the #of non-root nodes divided by the
     *         #of non-root nodes that could be addressed by the tree.</li>
     *         <li>2 - the total utilization percentage [0:100]. This is the
     *         average of the leaf utilization and the node utilization.</li>
     *         </ul>
     */
    public int[] getUtilization() {
        
        final int nnodes = getNodeCount();

        final int nleaves = getLeafCount();

        final int nentries = size();

        final int numNonRootNodes = nnodes + nleaves - 1;

        final int branchingFactor = getBranchingFactor();

        final int nodeUtilization = nnodes == 0 ? 100 : (100 * numNonRootNodes)
                / (nnodes * branchingFactor);

        final int leafUtilization = (100 * nentries)
                / (nleaves * branchingFactor);

        final int utilization = (nodeUtilization + leafUtilization) / 2;

        return new int[] { nodeUtilization, leafUtilization, utilization };
        
    }

    /**
     * Recursive dump of the tree.
     * 
     * @param out
     *            The dump is written on this stream.
     * 
     * @return true unless an inconsistency is detected.
     * 
     * @todo modify to write on log vs PrintStream.
     */
    public boolean dump(PrintStream out) {

        return dump(BTree.dumpLog.getEffectiveLevel(), out );

    }
        
    public boolean dump(Level level, PrintStream out) {
            
        // True iff we will write out the node structure.
        final boolean info = level.toInt() <= Level.INFO.toInt();

        int[] utils = getUtilization();
        
        if (info) {
            
            final int height = getHeight();
            
            final int nnodes = getNodeCount();

            final int nleaves = getLeafCount();

            final int nentries = size();

            final int branchingFactor = getBranchingFactor();
            
            log.info("height=" + height + ", branchingFactor="
                    + branchingFactor + ", #nodes=" + nnodes + ", #leaves="
                    + nleaves + ", #entries=" + nentries + ", nodeUtil="
                    + utils[0] + "%, leafUtil=" + utils[1] + "%, utilization="
                    + utils[2] + "%");
        }

        boolean ok = ((AbstractNode)getRoot()).dump(level, out, 0, true);

        return ok;

    }

    /**
     * <p>
     * Touch the node or leaf on the {@link #leafQueue}. If the node is not
     * found on a scan of the tail of the queue, then it is appended to the
     * queue and its {@link AbstractNode#referenceCount} is incremented. If the
     * a node is being appended to the queue and the queue is at capacity, then
     * this will cause a reference to be evicted from the queue. If the
     * reference counter for the evicted node or leaf is zero, then the node or
     * leaf will be written onto the store and made immutable. A subsequent
     * attempt to modify the node or leaf will force copy-on-write for that node
     * or leaf.
     * </p>
     * <p>
     * This method guarentees that the specified node will NOT be synchronously
     * persisted as a side effect and thereby made immutable. (Of course, the
     * node may be already immutable.)
     * </p>
     * <p>
     * In conjunction with {@link DefaultEvictionListener}, this method
     * guarentees that the reference counter for the node will reflect the #of
     * times that the node is actually present on the {@link #leafQueue}.
     * </p>
     * 
     * @param node
     *            The node or leaf.
     */
    protected void touch(AbstractNode node) {

        assert node != null;

        /*
         * We need to guarentee that touching this node does not cause it to be
         * made persistent. The condition of interest would arise if the queue
         * is full and the referenceCount on the node is zero before this method
         * was called. Under those circumstances, simply appending the node to
         * the queue would cause it to be evicted and made persistent.
         * 
         * We avoid this by incrementing the reference counter before we touch
         * the queue. Since the reference counter will therefore be positive if
         * the node is selected for eviction, eviction will not cause the node
         * to be made persistent.
         */
        node.referenceCount++;

        if( ! leafQueue.append(node) ) {
            
            /*
             * A false return indicates that the node was found on a scan of the
             * tail of the queue. In this case we do NOT want the reference
             * counter to be incremented since we have not actually added
             * another reference to this node onto the queue.  Therefore we 
             * decrement the counter (since we incremented it above) for a net
             * change of zero(0) across this method.
             */
            
            node.referenceCount--;
            
        }

    }
    
    /**
     * Read a node or leaf from the store.
     * 
     * @param addr
     *            The address in the store.
     *            
     * @return The node or leaf.
     */
    protected AbstractNode readNodeOrLeaf( long addr ) {
        
        buf.clear();
        
        ByteBuffer tmp = store.read(addr,buf);
        
        final int bytesRead = tmp.position();
        
        counters.bytesRead += bytesRead;
        
        AbstractNode node = (AbstractNode) nodeSer.getNodeOrLeaf(this, addr, tmp);
        
        node.setDirty(false);
        
        if (node instanceof Leaf) {
            
            counters.leavesRead++;

        } else {
            
            counters.nodesRead++;
            
        }
                
        touch(node);

        return node;
        
    }
    
    /**
     * Write a dirty node and its children using a post-order traversal that
     * first writes any dirty leaves and then (recursively) their parent nodes.
     * The parent nodes are guarenteed to be dirty if there is a dirty child so
     * this never triggers copy-on-write. This is used as part of the commit
     * protocol where it is invoked with the root of the tree, but it may also
     * be used to incrementally flush dirty non-root {@link Node}s.
     * 
     * Note: This will throw an exception if the backing store is read-only.
     * 
     * @param node
     *            The root of the hierarchy of nodes to be written. The node
     *            MUST be dirty. The node this does NOT have to be the root of
     *            the tree and it does NOT have to be a {@link Node}.
     */
    protected void writeNodeRecursive( AbstractNode node ) {

        assert node != null;
        
        assert node.isDirty();
        
        assert ! node.isDeleted();

        assert ! node.isPersistent();
        
        /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway.  If we were to evict everything from the
         * hard reference queue before a commit then the counters would be zero
         * but the queue would no longer be holding our nodes and leaves and
         * they would be GC'd soon as since they would no longer be strongly
         * reachable.
         */
        assert node.referenceCount >= 0;
        
        // #of dirty nodes written (nodes or leaves)
        int ndirty = 0;

        // #of dirty leaves written.
        int nleaves = 0;

        /*
         * Post-order traversal of children and this node itself.  Dirty
         * nodes get written onto the store.
         * 
         * Note: This iterator only visits dirty nodes.
         */
        Iterator itr = node.postOrderIterator(true);

        while (itr.hasNext()) {

            AbstractNode t = (AbstractNode) itr.next();

            assert t.isDirty();

            if (t != getRoot()) {

                /*
                 * The parent MUST be defined unless this is the root node.
                 */

                assert t.parent != null;
                assert t.parent.get() != null;

            }

            // write the dirty node on the store.
            writeNodeOrLeaf(t);

            ndirty++;

            if (t instanceof Leaf)
                nleaves++;

        }

        log.info("write: " + ndirty + " dirty nodes (" + nleaves
                + " leaves), addrRoot=" + node.getIdentity());
        
    }
    
    /**
     * Writes the node on the store (non-recursive). The node MUST be dirty. If
     * the node has a parent, then the parent is notified of the persistent
     * identity assigned to the node by the store. This method is NOT recursive
     * and dirty children of a node will NOT be visited.
     * 
     * Note: This will throw an exception if the backing store is read-only.
     * 
     * @return The persistent identity assigned by the store.
     * 
     * FIXME Support optional application of a {@link RecordCompressor} here or
     * handle that in the {@link IRawStore} implementation? The use of
     * compression only makes sense with large-ish branching factors, but it
     * could be useful with the journal for some indices so that we can absorb
     * more writes triggering an overflow. Compression could be added as a
     * wrapper around the base {@link IRawStore}. The {@link IndexSegment}
     * current builds compression into its {@link FileStore}.
     */
    protected long writeNodeOrLeaf( AbstractNode node ) {

        assert node != null;
        assert node.btree == this;
        assert node.isDirty();
        assert !node.isDeleted();
        assert !node.isPersistent();

        /*
         * Note we have to permit the reference counter to be positive and not
         * just zero here since during a commit there will typically still be
         * references on the hard reference queue but we need to write out the
         * nodes and leaves anyway.  If we were to evict everything from the
         * hard reference queue before a commit then the counters would be zero
         * but the queue would no longer be holding our nodes and leaves and
         * they would be GC'd soon as since they would no longer be strongly
         * reachable.
         */
        assert node.referenceCount >= 0;

        /*
         * Note: The parent should be defined unless this is the root node.
         * 
         * Note: A parent CAN NOT be serialized before all of its children have
         * persistent identity since it needs to write the identity of each
         * child in its serialization record.
         */
        Node parent = node.getParent();

        if (parent == null) {
            
            assert node == getRoot();

        } else {

            // parent must be dirty if child is dirty.
            assert parent.isDirty();

            // parent must not be persistent if it is dirty.
            assert !parent.isPersistent();
            
        }
        
        /*
         * Serialize the node or leaf onto a shared buffer.
         */
        
        node.assertInvariants();
        
        buf.clear();
        
        if( node.isLeaf() ) {
        
            nodeSer.putLeaf(buf, (Leaf)node);

            counters.leavesWritten++;
            
        } else {

            nodeSer.putNode(buf, (Node) node);

            counters.nodesWritten++;

        }
        
        /*
         * Flip the buffer and write the serialized node or leaf onto the store.
         */
        
        buf.flip();
        
        final long addr = store.write(buf);
        
        counters.bytesWritten += Addr.getByteCount(addr);
        
        /*
         * The node or leaf now has a persistent identity and is marked as
         * clean. At this point is MUST be treated as being immutable. Any
         * changes directed to this node or leaf MUST trigger copy-on-write.
         */

        node.setIdentity(addr);
        
        node.setDirty(false);

        if( parent != null ) {
            
            // Set the persistent identity of the child on the parent.
            parent.setChildKey(node);

//            // Remove from the dirty list on the parent.
//            parent.dirtyChildren.remove(node);

        }

        return addr;

    }

}
