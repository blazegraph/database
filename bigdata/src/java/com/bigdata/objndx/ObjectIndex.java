/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package com.bigdata.objndx;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RootBlockView;

/**
 * <p>
 * B+Tree persistent indexing data structure. B+Trees are optimized for
 * block-based, random I/O storage because they store multiple keys on one tree
 * node (called <code>Node</code>). In addition, the leaf nodes directly
 * contain (inline) the values associated with the keys, allowing a single (or
 * sequential) disk read of all the values on the page.
 * </p>
 * <p>
 * B+Trees are n-airy, yeilding log(N) search cost. They are self-balancing,
 * preventing search performance degradation when the size of the tree grows.
 * </p>
 * <p>
 * Keys and associated values must be <code>Serializable</code> objects. The
 * user is responsible to supply a serializable <code>Comparator</code> object
 * to be used for the ordering of entries, which are also called
 * <code>Tuple</code>. The B+Tree allows traversing the keys in forward and
 * reverse order using a TupleBrowser obtained from the browse() methods.
 * </p>
 * <p>
 * This implementation does not directly support duplicate keys, but it is
 * possible to handle duplicates by inlining or referencing an object collection
 * as a value.
 * </p>
 * <p>
 * There is no limit on key size or value size, but it is recommended to keep
 * both as small as possible to reduce disk I/O. This is especially true for the
 * key size, which impacts all non-leaf <code>Node</code> objects.
 * </p>
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id$
 * 
 * FIXME The object index has to be intrinsically aware of isolation. Like versions,
 * the object index MUST NOT overwrite pre-existing data or the store will be
 * inconsistent after a crash. For a node that has not been written on in the
 * current isolation context (aka "clean"), changes to the object index need to
 * use copy on write with conditional deallocation of the old slots. Copy on
 * write requires that we clone a node on which we have a read-only view,
 * including all nodes up to the root. Copy on write results in a single tree in
 * which modifications are visible with read through to untouched nodes in the
 * base index. <br>
 * Once a node is modified within a transactional scope it may be freely
 * re-written until that isolation context commits. During the commit, the base
 * object index is written on - again using copy on write and conditional
 * deallocation. This ensures that a failure during commit does not leave the
 * object index in an incoherent state due to partial update. The new root of
 * the object index is written into the commit block, together with the new head
 * of the slots chain. Thereafter a restart will utilize the new object index
 * and the new slot chain. Conditional deallocation is achieved since the slots
 * are marked as deleted on the slot chain using copy on write (this needs to be
 * proved out).<br>
 * The object index needs to use a fixed size node serialization so that it can
 * "update" a node without concern over whether the same allocation can hold the
 * new state. In this way we can reconcile the notions of update for the object
 * index nodes and update for nodes in a general purpose btree. <br>
 * The base object index can be a btree with fixed size node serialization as
 * describe above. The precise design for the object index with a transaction is
 * less clear. The existing code uses a transient hash table. This design could
 * work well using a version of a hash table that supports concurrent
 * modification (or otherwise concurrent modifications during traversal during
 * validation). Commit processing then merges down the transient isolated object
 * index using copy on write onto the base object index. The drawbacks of this
 * design are that it may incur a lot of object allocation to support the object
 * index growth during the transaction and that it does not use a persistence
 * capable data structure for transactions, so the memory footprint of a
 * transaction will grow as a function of the #of objects written.<br>
 * However, making the transaction isolation object index also persistence
 * capable also has its challenges. If we copy the base index in response to
 * writes on objects stored in uncloned nodes of the base index, then the
 * isolated index size could (a) grow very quickly depending on the locality of
 * the objects written on; and (b) store all object mappings in copied nodes
 * rather than just those on which the transaction has actually written - this
 * could also make it more expensive to validate since we need to scan more
 * entries in order to find those that were written on by the tx. The plausible
 * alternative is to use a persistence capable object index supporting
 * concurrent modification (or buffering concurrent modifications arising from
 * validation) and only store objects written on by the tx in that index.
 * 
 * FIXME Support concurrent modification of the index structure (e.g.,
 * splitting, joining, and rotation of nodes) during traversal. Since copy on
 * write gives us isolation level, support for concurrent modification is only
 * concerned with structural changes within a given isolation level. This is
 * necessary for the object index during validation.  However, that case can be
 * handled by buffering with transient data structures.
 * 
 * @todo Convert API and data structures from Integer to int. This is an
 *       optimization and does not effect the basic logic.
 * 
 * @todo Add index node caching. This is essential, but also a bit tricky. The
 *       {@link Journal}, by design, only performs sequential writes (depending
 *       on the buffer mode, reads may be either fully buffered or read through
 *       a page cache to disk). The index node cache is responsible for
 *       deferring the write of index nodes until they are evicted from the
 *       cache, thereby preserving the sequential write nature of the
 *       {@link Journal}. This has several implications. One is that we need a
 *       hard reference to a node until it has been written onto the journal, at
 *       which point we obtain its {@link ISlotAllocation} and we can release
 *       the hard reference. Another is that a node, once written, becomes
 *       immutable. Since we never perform random writes we can not update the
 *       data in the node's slot allocation. Instead, we have to clone the node
 *       into a mutable transient object. When we finally write the node on the
 *       end of the journal, it again gets a {@link ISlotAllocation}. There are
 *       three cases for persistent nodes: 1) a newly created node for which
 *       there is no prior version on the journal; 2) a clone of a node that was
 *       written during the current isolation context, in which case prior
 *       version is immediately deallocated on the journal; and 3) a clone of a
 *       node that is part of the base object index - the prior version of the
 *       node can not be deleted until the current isolation context commits
 *       since it is part of the last consistent object index state and will be
 *       in use if the isolation context is rolled back. <br>
 *       I don't know if we need a weak reference cache. If so, then consider a
 *       "mru" that just holds the last N touched hard references ala MartynC w/
 *       the special test for nop if the reference was touched very recently.<br>
 *       There will need to be a dirty list that gets committed in any case.<br>
 *       The #of non-leaf nodes (and the space required to store them) is quite
 *       small for the bigdata use cases. We should differentiate between cache
 *       for non-leaf and leaf nodes. The former should always be cached -- the
 *       latter should only be cached during a key scan. Provide an option to
 *       read the non-leaf index into memory during restart, which will require
 *       random IOs iff we are not fully buffered. When a leaf node is used for
 *       a key scan, then it makes sense to cache it. Otherwise, we should avoid
 *       the de-serialization costs and index directly into the buffer using an
 *       alternative implementation of the node API (requires defining a node
 *       API).<br>
 *       We do not need a full MRU for a weak reference cache. Instead, the hard
 *       reference cache can be a strongly typed array of references to the
 *       nodes. The start/end index of the array are tracked, making it into a
 *       ring buffer. The last N enties are tested before appending to the ring
 *       buffer to avoid appends of recently written references. Objects are
 *       marked as dirty when they are added to the hard reference cache. When
 *       they are evicted, the dirty flag is tested and they are serialized iff
 *       they are still dirty. In this manner, repeated evictions of the same
 *       object do not cause repeated serializations. (This should work well with
 *       the copy on write semantics.)
 */
public class ObjectIndex /* FIXME implements IObjectIndex */
{

    private static final boolean DEBUG = false;

    /**
     * Default page size (number of entries per node)
     */
    public static final int DEFAULT_SIZE = 16;

    /**
     * Used to manage persistence of the index nodes.
     */
    final protected Journal _journal;

    /**
     * The branching factor (#of keys in an index node).
     */
    final protected int _pageSize;

    /**
     * Used to (de-)serialize nodes and leaves of the index.
     */
    final protected NodeSerializer _nodeSer;
    
    /**
     * @deprecated The {@link ObjectIndex} instance is not, itself, a persistent
     *             object.
     */
    private long _recid;
    
    /**
     * The {@link ISlotAllocation} for the root node of the object index,
     * encoded as a long integer. This value is stored in the
     * {@link RootBlockView}, but the {@link ObjectIndex} itself is not a
     * persistent object.
     */
    private long _root;

    /**
     * Height of the B+Tree. This is the number of BPages you have to traverse
     * to get to a leaf Node, starting from the root.
     * 
     * @todo If this is to survive restart then it must be written into the
     *       {@link RootBlockView}.
     */
    private int _height;

    /**
     * Total number of entries in the ObjectIndex
     * 
     * @todo If this is to survive restart then it must be written into the
     *       {@link RootBlockView}.
     */
    protected long _entries;

    /**
     * Used to indicate a "null" key value.
     */
    static final int NULL = 0;

    /**
     * Disallowed.
     */
    private ObjectIndex() {
        throw new UnsupportedOperationException();
    }

    /**
     * Create an object index for a new {@link Journal}. The {@link Journal} is
     * responsible for storing the location of the root node, the tree height,
     * and the #of entries in the tree in the {@link IRootBlockView} as part of
     * its commit protocol.
     * 
     * @param journal
     *            The journal.
     * 
     * @todo All we need to do is create an empty root node, save it on the
     *       journal, and set the height and #of entries fields in the root
     *       block to zeros. We can then use the other form of the constructor
     *       and an abort will revert to the empty root node.
     */
    public ObjectIndex(Journal journal) {

        if (journal == null) {
            throw new IllegalArgumentException("Argument 'journal' is null");
        }

        int pageSize = journal.getRootBlockView().getObjectIndexSize();

        // make sure there's an even number of entries per Node
        if ((pageSize & 1) != 0) {
            throw new IllegalArgumentException(
                    "Argument 'pageSize' must be even");
        }
        
        this._journal = journal;
        
        this._pageSize = pageSize;

        this._nodeSer = new NodeSerializer(journal.slotMath,pageSize);

//      // @todo We need to create the root node and record it.
//        _recid = _insert(this); // insert into store.
    }

    /**
     * Opens an existing object index using the provided root block. The
     * existing nodes in the index will be read-only. Any change in the index
     * results in cloning of the modified nodes and their ancestors up to the
     * root.
     * 
     * @param journal
     *            The journal.
     * @param rootBlock
     *            The root block.
     */
    public ObjectIndex(Journal journal, IRootBlockView rootBlock) {
        
        final int pageSize = journal.getRootBlockView().getObjectIndexSize();

        // make sure there's an even number of entries per Node
        assert (pageSize & 1) != 0;
        
        this._journal = journal;

        this._pageSize = pageSize;

        this._nodeSer = new NodeSerializer(journal.slotMath,pageSize);

        this._root = rootBlock.getObjectIndexRoot(); // @todo just store the firstSlot on the root block.

        assert _root != 0L;
        
        /*
         * @todo Eagerly load the root node or the entire index from the
         * journal depending on configuration parameters?
         */
        
    }
    
    /**
     * <p>
     * Insert an entry in the ObjectIndex.
     * </p>
     * <p>
     * The ObjectIndex cannot store duplicate entries. An existing entry can be
     * replaced using the <code>replace</code> flag. If an entry with the same
     * key already exists in the ObjectIndex, its value is returned.
     * </p>
     * 
     * @param key
     *            Insert key
     * @param value
     *            Insert value
     * @param replace
     *            Set to true to replace an existing key-value pair.
     *            
     * @return Existing value, if any.
     */
    public Object insert(Integer key, ISlotAllocation value, boolean replace)
    {

        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        if ( value == null ) {
            throw new IllegalArgumentException( "Argument 'value' is null" );
        }

        Node rootPage = getRoot();

        if ( rootPage == null ) {
            // ObjectIndex is currently empty, create a new root Node
            if (DEBUG) {
                System.out.println( "ObjectIndex.insert() new root Node" );
            }
            rootPage = new Node( this, key, value );
            _root = rootPage._recid;
            _height = 1;
            _entries = 1;
//            _update( _recid, this );
            return null;
        } else {
            Node.InsertResult insert = rootPage.insert( _height, key, value, replace );
            boolean dirty = false;
            if ( insert._overflow != null ) {
                // current root page overflowed, we replace with a new root page
                if ( DEBUG ) {
                    System.out.println( "ObjectIndex.insert() replace root Node due to overflow" );
                }
                rootPage = new Node( this, rootPage, insert._overflow );
                _root = rootPage._recid;
                _height += 1;
                dirty = true;
            }
            if ( insert._existing == null ) {
                _entries++;
                dirty = true;
            }
//            if ( dirty ) {
//                _update( _recid, this );
//            }
            // insert might have returned an existing value
            return insert._existing;
        }
    }

    /**
     * Remove an entry with the given key from the ObjectIndex.
     * 
     * @param key
     *            Removal key
     *            
     * @return Value associated with the key, or null if no entry with given key
     *         existed in the ObjectIndex.
     *         
     * @exception IllegalArgumentException
     *                if the key is null.
     *                
     * @exception IllegalArgumentException
     *                if the key is not found.
     */
    public Object remove(Integer key) {
        
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        Node rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }
        boolean dirty = false;
        Node.RemoveResult remove = rootPage.remove( _height, key );
        if ( remove._underflow && rootPage.isEmpty() ) {
            _height -= 1;
            dirty = true;

            // TODO:  check contract for BPages to be removed from recman.
            if ( _height == 0 ) {
                _root = 0;
            } else {
                _root = rootPage.loadChildNode( _pageSize-1 )._recid;
            }
        }
        if ( remove._value != null ) {
            _entries--;
            dirty = true;
        }
//        if ( dirty ) {
//            _update( _recid, this );
//        }
        return remove._value;
    }

    /**
     * Find the value associated with the given key.
     * 
     * @param key
     *            Lookup key.
     *            
     * @return Value associated with the key, or null if not found.
     */
    public Object find(Integer key) {
        
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        
        Node rootPage = getRoot();
        
        if ( rootPage == null ) {
            
            return null;
            
        }

        Tuple tuple = new Tuple( null, null );
        TupleBrowser browser = rootPage.find( _height, key );

        if ( browser.getNext( tuple ) ) {
            // find returns the matching key or the next ordered key, so we must
            // check if we have an exact match
            if ( key == tuple.getKey() ) {
                return null;
            } else {
                return tuple.getValue();
            }
        } else {
            return null;
        }
    }

    /**
     * Find the value associated with the given key, or the entry immediately
     * following this key in the ordered ObjectIndex.
     * 
     * @param key
     *            Lookup key.
     *            
     * @return Value associated with the key, or a greater entry, or null if no
     *         greater entry was found.
     */
    public Tuple findGreaterOrEqual(Integer key) {
        
        Tuple         tuple;
        TupleBrowser  browser;

        if ( key == null ) {
            // there can't be a key greater than or equal to "null"
            // because null is considered an infinite key.
            return null;
        }

        tuple = new Tuple( null, null );
        
        browser = browse( key );
        
        if ( browser.getNext( tuple ) ) {
            
            return tuple;
            
        } else {
            
            return null;
            
        }
        
    }

    /**
     * <p>
     * Get a browser initially positioned at the beginning of the ObjectIndex.
     * </p>
     * <p>
     * <b> WARNING: If you make structural modifications to the ObjectIndex during
     * browsing, you will get inconsistent browing results. </b>
     * </p>
     * 
     * @return Browser positionned at the beginning of the ObjectIndex.
     */
    public TupleBrowser browse() {
        
        Node rootPage = getRoot();
        
        if ( rootPage == null ) {
            
            return EmptyBrowser.INSTANCE;
            
        }
        
        TupleBrowser browser = rootPage.findFirst();
        
        return browser;
        
    }

    /**
     * <p>
     * Get a browser initially positioned at the beginning of the
     * {@link ObjectIndex}.
     * </p>
     * <p>
     * <b> WARNING: If you make structural modifications to the ObjectIndex
     * during browsing, you will get inconsistent browing results. </b>
     * </p>
     * 
     * @param key
     *            Key used to position the browser. If null, the browser will be
     *            positionned after the last entry of the ObjectIndex. (Null is
     *            considered to be an "infinite" key)
     * 
     * @return Browser positioned just before the given key.
     */
    public TupleBrowser browse(Integer key) {

        Node rootPage = getRoot();
        
        if ( rootPage == null ) {
            
            return EmptyBrowser.INSTANCE;
            
        }
        
        TupleBrowser browser = rootPage.find( _height, key );
        
        return browser;
        
    }

    /**
     * Deletes all nodes in the {@link ObjectIndex}.
     * 
     * @deprecated There is probably no use case for deleting an object index.
     */
    public void delete() {
     
        Node rootPage = getRoot();
        
        if (rootPage != null) {
            
            rootPage.delete();

        }
        
//        _delete(_recid);
        
    }

    /**
     * Return the number of entries (size) of the ObjectIndex.
     */
    public  long size()
    {
        
        return _entries;
        
    }

    /**
     * Return the persistent record identifier of the ObjectIndex.
     */
    public long getRecid()
    {
        
        return _recid;
    
    }

    /**
     * Return the size of a node in the btree.
     */
    public int getPageSize()
    {
    
        return _pageSize;
        
    }
    
    /**
     * Height of the B+Tree. This is the number of BPages you have to traverse
     * to get to a leaf Node, starting from the root.
     */
    public int getHeight()
    {
       
        return _height;
        
    }

    /**
     * Return the root Node, or null if it doesn't exist.
     */
    protected Node getRoot() {

        if ( _root == 0 ) {
            
            return null;
            
        }
        
        Node root = _fetch( _root );
        
        return root;
        
    }

    /*
    public void assert() throws IOException {
        Node root = getRoot();
        if ( root != null ) {
            root.assertRecursive( _height );
        }
    }
    */

    public void dump( PrintStream out ) {
        
        Node root = getRoot();
        
        if ( root != null ) {
            
        	root.dump( out, 0 );
            
            root.dumpRecursive( out, _height, 0 );
            
        }
        
    }

    /**
     * Used for debugging and testing only. Populates the 'out' list with the
     * recids of all child pages in the ObjectIndex.
     * 
     * @param out
     */
    void dumpChildPageRecIDs(List<Long> out) {

        Node root = getRoot();
        
        if ( root != null ) {
            
            out.add(new Long(root._recid));
            
            root.dumpChildPageRecIDs( out, _height);
            
        }
        
    }

    /**
     * PRIVATE INNER CLASS
     * 
     * Browser returning no element.
     */
    static class EmptyBrowser
        extends TupleBrowser
    {

        static TupleBrowser INSTANCE = new EmptyBrowser();

        public boolean getNext( Tuple tuple )
        {
            return false;
        }

        public boolean getPrevious( Tuple tuple )
        {
            return false;
        }
    }

    /*
     * Interface to the persistence API.
     */
    
    /**
     * Write the node onto the journal.
     * 
     * @param obj
     *            The object to be inserted into the store.
     * 
     * @return The persistent identifier assigned to the object.
     * 
     * @todo Writes are only performed on cache eviction and must also take
     *       responsibility for breaking hard references so that the node may be
     *       GC'd. Hard references exist from a parent to its children, from a
     *       child to the parent, and from leaf to leaf in a chain. Use a pool
     *       fo node and leaf buffers to minimize allocation and GC.
     */
    long _insert(Node obj) {

        final ByteBuffer buf;

        if (obj._isLeaf) {

            buf = ByteBuffer.allocate(_nodeSer.LEAF_SIZE);

            _nodeSer.putLeaf(buf, obj);

        } else {

            buf = ByteBuffer.allocate(_nodeSer.NODE_SIZE);

            _nodeSer.putNode(buf, obj);

        }

        long recid = _journal.write(buf).toLong();
        
        obj._recid = recid;
        
        return recid;

    }

    /**
     * Fetch a {@link Node} from the store.
     * 
     * @param recid
     *            The logical row id for the {@link Node}.
     * 
     * @return The {@link Node}.
     * 
     * @todo Nodes are fetched on a cache miss. The code must always check to
     *       see whether the reference is a hard reference or an
     *       {@link ISlotAllocation} encoded as a long. When it is an
     *       {@link ISlotAllocation} the data is read from the journal and
     *       unpacked into an {@link Node}. The node will be immutable. If a
     *       change is attempted, then the node (and its ancestors) are cloned
     *       into mutable nodes and the appropriate hard references are setup.
     *       (This implies that the index node cache size must exceed the index
     *       depth, probably significantly.)
     */
    Node _fetch(long recid) {

        ISlotAllocation slots = _journal.slotMath.toSlots(recid);
        
        ByteBuffer buf = _journal.read(slots, null);
        
        Node node = _nodeSer.getNodeOrLeaf(this, recid, buf);
        
        return node;

    }
    
    /**
     * Update a {@link Node}.
     * 
     * @param recid
     *            The logical row id for the {@link Node}.
     * 
     * @param bpage
     *            The {@link Node} that is being updated.
     * 
     * @todo Nodes are never updated in place (as this code does). Instead,
     *       nodes are either persistent read-only views or mutable copies not
     *       yet written on the journal.<br>
     *       Note: This code will fail an assertion if there is an attempt to
     *       write a leaf onto slots previously used for a non-leaf (and visa
     *       versa). This is detected based on the #of bytes in the slot
     *       allocation and whether or not the node being serialized is a leaf.
     *       This assertion will go away along with this method, but it provides
     *       a useful empirical test for conversion of a node to a leaf and visa
     *       versa.
     */
    void _update(Node node) {

        ISlotAllocation slots = _journal.slotMath.toSlots(node._recid);

        final ByteBuffer buf;

        if (node._isLeaf) {

            assert slots.getByteCount() == _nodeSer.LEAF_SIZE ; 
            
            buf = ByteBuffer.allocate(_nodeSer.LEAF_SIZE);

            _nodeSer.putLeaf(buf, node);

        } else {

            assert slots.getByteCount() == _nodeSer.NODE_SIZE ; 
            
            buf = ByteBuffer.allocate(_nodeSer.NODE_SIZE);

            _nodeSer.putNode(buf, node);

        }

        _journal.write(buf);

    }

    /**
     * Delete a {@link Node} from the store.
     * 
     * @param recid
     *            The recid of the {@link Node}.
     * 
     * @todo Deletes are just a special case of write, just like the MVCC policy
     *       of the journal, but I need to explore the specific cases for delete
     *       in order to gain a better understanding of what the correct action
     *       is in each case. Like writes, if the version was first written in a
     *       given tx, then the version is simply deallocated. Also like writes,
     *       if the version is historical then it is only removed from the
     *       isolation context - not from the base context. As an added twist,
     *       index nodes are transient when they are mutable and there will be
     *       cases when an index node has never been written.
     */
    void _delete(long recid) {

        ISlotAllocation slots = _journal.slotMath.toSlots(recid);

        _journal.delete(slots);

    }

}
