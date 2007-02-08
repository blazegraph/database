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
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;
import com.ibm.icu.text.RuleBasedCollator;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * Base class for mutable and immutable B+-Tree implementations.
 * </p>
 * <p>
 * The B+-Tree implementation supports variable length unsigned byte[] keys and
 * provides a {@link KeyBuilder} utility class designed to make it possible to
 * generate keys from any combination of primitive data types and Unicode
 * strings. The total ordering imposed by the index is that of a bit-wise
 * comparison of the variable length unsigned byte[] keys. Encoding Unicode keys
 * is support by an integration with ICU4J and applications may choose the
 * locale, strength, and other properties that govern the sort order of sort
 * keys generated from Unicode strings. Sort keys produces by different
 * {@link RuleBaseCollator}s are NOT compable and applications that use Unicode
 * data in their keys MUST make sure that they use a {@link RuleBasedCollator}
 * that imposes the same sort order each time they provision a
 * {@link KeyBuilder}. ICU4J provides a version number that is changed each
 * time a software revision would result in a change in the generated sort
 * order.
 * </p>
 * <p>
 * The use of variable length unsigned byte[] keys makes it possible for the
 * B+-Tree to perform very fast comparison of a search key with keys in the
 * nodes and leaves of the tree. To support fast search, the leading prefix is
 * factored out each time a node or leaf is made immutable, e.g., directly
 * proceeding serialization. Further, the separator keys are choosen to be the
 * shortest separator key in order to further shorten the keys in the nodes of
 * the tree.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see KeyBuilder
 * @see RuleBasedCollator
 * @see http://icu.sourceforge.net
 * @see http://icu.sourceforge.net/userguide/Collate_ServiceArchitecture.html#Versioning
 */
abstract public class AbstractBTree implements IIndex, IBatchBTree {

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
     * flag turns on some more expensive assertions.
     */
    final protected boolean debug = false; 
    
    /*
     * Error messages.
     * 
     * @todo add error codes; localize.
     */
    private static final transient String ERR_NTUPLES_NON_POSITIVE = "ntuples is non-positive";
    private static final transient String ERR_KEYS_NULL = "keys is null";
    private static final transient String ERR_VALS_NULL = "values is null";
    private static final transient String ERR_NOT_ENOUGH_KEYS= "not enough keys";
    private static final transient String ERR_NOT_ENOUGH_VALS= "not enough values";

    /**
     * Counters tracking various aspects of the btree.
     */
    protected final Counters counters = new Counters(this);

    /**
     * The persistence store.
     */
    final protected IRawStore store;

    /**
     * The branching factor for the btree.
     */
    final protected int branchingFactor;

    /**
     * Used to serialize and de-serialize the nodes and leaves of the tree.
     */
    final protected NodeSerializer nodeSer;

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
    final protected HardReferenceQueue<PO> leafQueue;
    
    /**
     * The #of distinct nodes and leaves on the {@link #leafQueue}.
     */
    protected int ndistinctOnQueue;
    
    /**
     * The #of distinct nodes and leaves on the {@link HardReferenceQueue}.
     */
    final public int getNumDistinctOnQueue() {
        
        return ndistinctOnQueue;
        
    }
    
    /**
     * The capacity of the {@link HardReferenceQueue}.
     */
    final public int getHardReferenceQueueCapacity() {
        
        return leafQueue.capacity();
        
    }

    /**
     * The minimum allowed branching factor (3).  The branching factor may be
     * odd or even.
     */
    static public final int MIN_BRANCHING_FACTOR = 3;
    
    /**
     * @param store
     *            The persistence store.
     * @param branchingFactor
     *            The branching factor is the #of children in a node or values
     *            in a leaf and must be an integer greater than or equal to
     *            three (3). Larger branching factors result in trees with fewer
     *            levels. However there is a point of diminishing returns at
     *            which the amount of copying performed to move the data around
     *            in the nodes and leaves exceeds the performance gain from
     *            having fewer levels.
     * @param initialBufferCapacity
     *            When non-zero, this is the initial buffer capacity used by the
     *            {@link NodeSerializer}. When zero the initial buffer capacity
     *            will be estimated based on the branching factor, the key
     *            serializer, and the value serializer. The initial estimate is
     *            not critical and the buffer will be resized by the
     *            {@link NodeSerializer} if necessary.
     * @param headReferenceQueue
     *            The hard reference queue.
     * @param addrSer
     *            Object that knows how to (de-)serialize the child addresses in
     *            an {@link INodeData}.
     * @param valueSer
     *            Object that knows how to (de-)serialize the values in an
     *            {@link ILeafData}.
     * @param nodeFactory
     *            Object that provides a factory for node and leaf objects.
     * @param recordCompressor
     *            Object that knows how to (de-)compress serialized nodes and
     *            leaves (optional).
     * @param useChecksum
     *            When true, computes and verifies checksum of serialized nodes
     *            and leaves. This option is not recommended for use with a
     *            fully buffered store, such as a {@link Journal}, since all
     *            reads are against memory which is presumably already parity
     *            checked.
     */
    protected AbstractBTree(IRawStore store,
            int branchingFactor,
            int initialBufferCapacity,
            HardReferenceQueue<PO> hardReferenceQueue,
            IAddressSerializer addrSer, IValueSerializer valueSer,
            INodeFactory nodeFactory, RecordCompressor recordCompressor,
            boolean useChecksum) {
        
        assert store != null;
        
        assert branchingFactor >= MIN_BRANCHING_FACTOR;

        assert hardReferenceQueue != null;
        
        assert addrSer != null;
        
        assert valueSer != null;

        assert nodeFactory != null;
        
        this.store = store;

        this.branchingFactor = branchingFactor;

        this.leafQueue = hardReferenceQueue;
        
        this.nodeSer = new NodeSerializer(nodeFactory, branchingFactor,
                initialBufferCapacity, addrSer,
                KeyBufferSerializer.INSTANCE, valueSer,
                recordCompressor, useChecksum);
        
    }
    
    /**
     * The persistence store.
     */
    public IRawStore getStore() {
        
        return store;
        
    }
    
    /**
     * The branching factor for the btree.
     */
    public int getBranchingFactor() {
        
        return branchingFactor;
        
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
     * The #of non-leaf nodes in the {@link AbstractBTree}. This is zero (0)
     * for a new btree.
     */
    abstract public int getNodeCount();

    /**
     * The #of leaf nodes in the {@link AbstractBTree}. This is one (1) for a
     * new btree.
     */
    abstract public int getLeafCount();

    /**
     * The #of entries (aka values) in the {@link AbstractBTree}. This is zero
     * (0) for a new btree.
     */
    abstract public int getEntryCount();
    
    /**
     * The object responsible for (de-)serializing the nodes and leaves of the
     * {@link IIndex}.
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
    
    /**
     * @todo handle isolation using a TimestampValue(long,Object). This properly
     *       encapsulates the timestamp with the value so that the timestamp is
     *       returned with the value by the api. It also allows the isolated
     *       btree to set the timestamp to the timestamp associated with the tx
     *       or to treat it as an update counter. using a null value in a
     *       TimestampValue instead of remove when using the btree to support
     *       isolation. this will result in an entry with an updated timestamp
     *       and a null value and would leave the semantics of remove() removing
     *       the entry from the tree.
     * 
     * @todo disallow null value? it was allowed for isolation to indicate a
     *       removed value, but we can do that with a null value inside of a
     *       TimestampEntry object.
     * 
     * @todo factor out error messages into constants.
     */    
    public void insert(int ntuples, byte[][] keys, Object[] values) {

        if (ntuples <= 0)
            throw new IllegalArgumentException(ERR_NTUPLES_NON_POSITIVE);
            
        if (keys == null)
            throw new IllegalArgumentException(ERR_KEYS_NULL);

        if( keys.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_KEYS);

        if (values == null)
            throw new IllegalArgumentException(ERR_VALS_NULL);

        if( values.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_VALS);

        for( int tupleIndex=0; tupleIndex<ntuples; ) {

            /*
             * Each call MAY process more than one tuple.
             */
            int nused = ((AbstractNode) getRoot()).insert(ntuples, tupleIndex,
                    keys, values);
            
            assert nused > 0;

            /*
             * Note: it is legal to reuse a key iff the data in the key is
             * unchanged.  Unfortunately it is tricky to do a fast test for
             * this condition.
             */
//            {
//                /*
//                 * detect if the caller reuses the same byte[] key from one
//                 * insert to the next. This is an error since the key needs to
//                 * be donated to the btree. This problem only exists for
//                 * insert().
//                 */
//                    
//                byte[] key = keys[tupleIndex];
//
//                if (key == lastKey) {
//
//                    throw new IllegalArgumentException(
//                            "keys must not be reused.");
//
//                } else {
//
//                    lastKey = key;
//
//                }
//
//            }
            
            counters.ninserts += nused;
            
            tupleIndex += nused;

        }

    }

//    /**
//     * Used to detect when the caller reuses the same byte[] key from one insert
//     * to the next. This is an error since the key needs to be donated to the
//     * btree.  This problem only exists for insert().
//     */
//    private byte[] lastKey = null;


    public void lookup(int ntuples, byte[][] keys, Object[] values) {
     
        if (ntuples <= 0)
            throw new IllegalArgumentException(ERR_NTUPLES_NON_POSITIVE);
            
        if (keys == null)
            throw new IllegalArgumentException(ERR_KEYS_NULL);

        if( keys.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_KEYS);

        if (values == null)
            throw new IllegalArgumentException(ERR_VALS_NULL);

        if( values.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_VALS);

        for( int tupleIndex=0; tupleIndex<ntuples; ) {

            /*
             * Each call MAY process more than one tuple.
             */
            int nused = ((AbstractNode) getRoot()).lookup(ntuples, tupleIndex,
                    keys, values);
            
            assert nused > 0;
            
            counters.nfinds += nused;
            
            tupleIndex += nused;

        }
        
    }

    /**
     * @todo write tests for this - the code was cloned from lookup so it should
     *       work but there it no test suite for contains() at this time.
     */
    public void contains(int ntuples, byte[][] keys, boolean[] contains) {
     
        if (ntuples <= 0)
            throw new IllegalArgumentException(ERR_NTUPLES_NON_POSITIVE);
            
        if (keys == null)
            throw new IllegalArgumentException(ERR_KEYS_NULL);

        if( keys.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_KEYS);

        if (contains == null)
            throw new IllegalArgumentException(ERR_VALS_NULL);
        
        if( contains.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_VALS);
        
        for( int tupleIndex=0; tupleIndex<ntuples; ) {

            // skip tuples already marked as true.
            if (contains[tupleIndex]) {
                
                tupleIndex++;
                
                continue;
                
            }
                
            /*
             * Each call MAY process more than one tuple.
             */
            int nused = ((AbstractNode) getRoot()).contains(ntuples, tupleIndex,
                    keys, contains);
            
            assert nused > 0;
            
            counters.nfinds += nused;
                
            tupleIndex += nused;
                
        }
        
    }

    public void remove(int ntuples, byte[][] keys, Object[] values ) {

        if (ntuples <= 0)
            throw new IllegalArgumentException(ERR_NTUPLES_NON_POSITIVE);
            
        if (keys == null)
            throw new IllegalArgumentException(ERR_KEYS_NULL);

        if( keys.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_KEYS);

        if (values == null)
            throw new IllegalArgumentException(ERR_VALS_NULL);

        if( values.length < ntuples )
            throw new IllegalArgumentException(ERR_NOT_ENOUGH_VALS);

        for( int tupleIndex=0; tupleIndex<ntuples; ) {

            /*
             * Each call MAY process more than one tuple.
             */
            int nused = ((AbstractNode) getRoot()).remove(ntuples, tupleIndex,
                    keys, values);
            
            assert nused > 0;
            
            counters.ninserts += nused;
            
            tupleIndex += nused;

        }

    }

    /*
     * buffers used to convert non-batch operations into batch api
     * calls.
     * 
     * @todo we could also avoid autoboxing by exposing a type safe api
     * with appropriate key types, but that would multiple the #of methods
     * on the api several times.  derived classes could provide typesafe
     * direct use of the batch api easily if we exposed these objects as
     * protected arrays.
     */
    /** @deprecated */
    private final byte[][] _keys = new byte[1][];
    /** @deprecated */
    private final Object[] _values = new Object[1];
    /** @deprecated */
    private final boolean[] _contains = new boolean[1];
    /** @deprecated */
    private final KeyBuilder keyBuilder = new KeyBuilder();
    
    /**
     * Used to unbox an application key into a shared instance buffer
     * {@link #_keys}. This is NOT safe for concurrent operations, but the
     * mutable b+tree is only safe (and designed for) a single-threaded context.
     * 
     * @deprecated This is preserved solely to provide backward compatibility
     *             for int keys passed into the non-batch api. It will disappear
     *             as soon as I update the test suites.
     */
    final private byte[][] unbox(Object key) {
        if(key instanceof Integer) {
            return new byte[][]{
                keyBuilder.reset().append(((Integer)key).intValue()).getKey()
            };
        } else {
            return new byte[][] { (byte[]) key };
        }
    }
    
    public Object insert(Object key, Object value) {

        if (key == null)
            throw new IllegalArgumentException();

        _values[0] = value;
        
        insert(1,unbox(key),_values);
        
        return _values[0];
        
//
//        counters.ninserts++;
//        
//        if(INFO) {
//            log.info("key="+key+", entry="+entry);
//        }
//
//        return getRoot().insert(key, entry);

    }

    public Object lookup(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        lookup(1,unbox(key),_values);
        
        return _values[0];
        
//        counters.nfinds++;
//        
//        return getRoot().lookup(key);

    }

    public boolean contains(byte[] key) {

        if( key == null )
            throw new IllegalArgumentException();

        _contains[0] = false; // otherwise the request is ignored!
        
        contains(1,unbox(key),_contains);
        
        return _contains[0];
        
    }
    
    public Object remove(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        remove(1,unbox(key),_values);
        
        return _values[0];
        
//        counters.nremoves++;
//
//        if(INFO) {
//            log.info("key="+key);
//        }
//
//        return getRoot().remove(key);

    }

    /**
     * Lookup the index position of the key.
     * 
     * @param key
     *            The key.
     * 
     * @return The index of the search key, if found; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be found it it were
     *         inserted into the btree without intervening mutations. Note that
     *         this guarantees that the return value will be >= 0 if and only if
     *         the key is found. When found the index will be in [0:nentries).
     *         Adding or removing entries in the tree may invalidate the index.
     * 
     * @todo promote to {@link IIndex}?
     * 
     * @see #keyAt(int)
     * @see #valueAt(int)
     */
    public int indexOf(byte[] key) {
        
        if (key == null)
            throw new IllegalArgumentException();

        counters.nindexOf++;
        
        int index = ((AbstractNode)getRoot()).indexOf(key);
        
        return index;
        
    }

    /**
     * Return the key for the identified entry. This performs an efficient
     * search whose cost is essentially the same as {@link #lookup(Object)}.
     * 
     * @param index
     *            The index position of the entry (origin zero).
     * 
     * @return The key at that index position (not a copy).
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     * 
     * @see #indexOf(Object)
     * @see #getValue(int)
     */
    public byte[] keyAt(int index) {

        if (index < 0)
            throw new IndexOutOfBoundsException("less than zero");

        if (index >= getEntryCount())
            throw new IndexOutOfBoundsException("too large");

        counters.ngetKey++;
        
        return ((AbstractNode)getRoot()).keyAt(index);

    }
    
    /**
     * Return the value for the identified entry. This performs an efficient 
     * search whose cost is essentially the same as {@link #lookup(Object)}.
     * 
     * @param index
     *            The index position of the entry (origin zero).
     * 
     * @return The value at that index position.
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     *                
     * @see #indexOf(Object)
     * @see #keyAt(int)
     */
    public Object valueAt(int index) {

        if (index < 0)
            throw new IndexOutOfBoundsException("less than zero");

        if (index >= getEntryCount())
            throw new IndexOutOfBoundsException("too large");

        counters.ngetKey++;
        
        return ((AbstractNode)getRoot()).valueAt(index);

    }
    
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        /*
         * Note: the code will check for fromKey > toKey no later than when
         * next() is called for the first time. If eager rejection of bad
         * parameters is desired then invoke compareBytes on the keys (if both
         * are non-null) before calling rangeIterator on the root node.
         */
        return ((AbstractNode)getRoot()).rangeIterator(fromKey,toKey);
        
    }

    /**
     * @todo perhaps place the count of entries or entries remaining on the
     *       {@link IRangeIterator}?
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {

        final AbstractNode root = (AbstractNode)getRoot();
        
        int fromIndex = (fromKey == null ? 0 : root.indexOf(fromKey));

        int toIndex = (toKey == null ? getEntryCount() : root.indexOf(toKey));
        
        // Handle case when fromKey is not found.
        if( fromIndex < 0 ) fromIndex = -fromIndex - 1;
        
        // Handle case when toKey is not found.
        if( toIndex < 0 ) toIndex = -toIndex - 1;
        
        if( toIndex <= fromIndex ) {
            
            return 0;
            
        }
        
        int ret = toIndex - fromIndex;
        
        return ret;
        
    }
    
    /**
     * Visits all entries in key order.
     * 
     * @return An iterator that will visit all entries in key order.
     */
    public IEntryIterator entryIterator() {
    
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

        final int nentries = getEntryCount();

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

            final int nentries = getEntryCount();

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
    final protected void touch(AbstractNode node) {

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

        assert ndistinctOnQueue >= 0;

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
            
        } else {
            
            /*
             * Since we just added a node or leaf to the hard reference queue we
             * now update the #of distinct nodes and leaves on the hard
             * reference queue.
             * 
             * Also see {@link DefaultEvictionListener}.
             */
            if(node.referenceCount==1) {
                
                ndistinctOnQueue++;
                
            }
            
        }

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
        assert node.dirty;
        assert ! node.deleted;
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
     */
    protected long writeNodeOrLeaf( AbstractNode node ) {

        assert node != null;
        assert node.btree == this;
        assert node.dirty;
        assert !node.deleted;
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
        
//        /*
//         * Convert the keys buffer to an immutable keys buffer.  The immutable
//         * keys buffer is potentially much more compact.
//         */
//        if( node.keys instanceof MutableKeyBuffer ) {
//
//            node.keys = new ImmutableKeyBuffer((MutableKeyBuffer)node.keys);
//                
//        }
        
        /*
         * Serialize the node or leaf onto a shared buffer.
         */
        
        if(debug) node.assertInvariants();
        
        final ByteBuffer buf;
        
        if( node.isLeaf() ) {
        
            buf = nodeSer.putLeaf((Leaf)node);

            counters.leavesWritten++;
            
        } else {

            buf = nodeSer.putNode((Node) node);

            counters.nodesWritten++;

        }
        
        // write the serialized node or leaf onto the store.
        
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

    /**
     * Read a node or leaf from the store.
     * 
     * @param addr
     *            The address in the store.
     * 
     * @return The node or leaf.
     */
    protected AbstractNode readNodeOrLeaf( long addr ) {

        /*
         * offer the node serializer's buffer to the IRawStore.  it will be used
         * iff it is large enough and the store does not prefer to return a
         * read-only slice.
         */
//        nodeSer.buf.clear();
        
        ByteBuffer tmp = store.read(addr,nodeSer._buf);
        assert tmp.position() == 0;
        assert tmp.limit() == Addr.getByteCount(addr);
        
        final int bytesRead = tmp.limit();
                
        counters.bytesRead += bytesRead;

        // extract the node from the buffer.
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
    
}
