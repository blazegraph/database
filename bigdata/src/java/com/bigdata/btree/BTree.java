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

package com.bigdata.btree;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.CognitiveWeb.extser.DataInput;
import org.CognitiveWeb.extser.DataOutput;
import org.CognitiveWeb.extser.IStreamSerializer;
import org.CognitiveWeb.extser.Stateless;

import com.bigdata.btree.compression.CompressionProvider;
import com.bigdata.istore.IOM;

/**
 * <p>
 * B+Tree persistent indexing data structure. B+Trees are optimized for
 * block-based, random I/O storage because they store multiple keys on one tree
 * node (called <code>BPage</code>). In addition, the leaf nodes directly
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
 * key size, which impacts all non-leaf <code>BPage</code> objects.
 * </p>
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id$
 * 
 * FIXME Refactor so that the same code can support both the object index in the
 * journal and primary (clustered) and secondary indices at the application
 * level. This also requires abstracting away from whether we are using int32 or
 * int64 identifiers. (The journal always uses in32 identifiers internally, but
 * bigdata indices can span segments are are therefore int64.) <br>
 * The object index has to be intrinsically aware of isolation. Like versions,
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
 * necessary for the object index during validation.
 * 
 * FIXME Review use of the synchronization keyword. The journal is a single
 * threaded environment, so it does not require explicit synchronization.
 * Perhaps a wrapper class can be created for environments where there may be
 * concurrent threads, much like the {@link Collections} utility methods?
 */
public class BTree
//    implements Externalizable
{

    private static final boolean DEBUG = false;

    /**
     * Default page size (number of entries per node)
     */
    public static final int DEFAULT_SIZE = 16;

    /**
     * Used to manage persistence of the index nodes.
     */
    private transient IOM _recman;

    /**
     * This BTree's record ID in the PageManager.
     */
    private transient long _recid;

    /**
     * Comparator used to index entries.
     */
    protected Comparator _comparator;

    /**
     * Serializer used to serialize index keys (optional)
     */
    protected Serializer _keySerializer;

    /**
     * Serializer used to serialize index values (optional)
     */
    protected Serializer _valueSerializer;

    /**
     * Height of the B+Tree.  This is the number of BPages you have to traverse
     * to get to a leaf BPage, starting from the root.
     */
    private int _height;

    /**
     * Recid of the root BPage
     */
    private transient long _root;

    /**
     * Number of entries in each BPage.
     */
    protected int _pageSize;

    /**
     * Total number of entries in the BTree
     */
    protected long _entries;
    
    /**
    * Provides compressor and decompressor for the keys stored in the BTree
    */
    protected CompressionProvider _keyCompressionProvider;

//    /**
//     * Serializer used for the root BPage of this tree.
//     */
//    private transient BPage _bpageSerializer;

    /**
     * No-argument constructor used by serialization.
     */
    public BTree()
    {
        // empty
    }

    /**
     * Create a new persistent BTree, with 16 entries per node.
     * 
     * @param store
     *            Persistence API.
     * @param comparator
     *            Comparator used to order index entries
     */
    public BTree(IOM store, Comparator comparator) {

        this(store, comparator, null, null, null, DEFAULT_SIZE);

    }

    /**
     * Create a new persistent BTree, with 16 entries per node.
     * 
     * @param store
     *            Persistence API.
     * @param keySerializer
     *            Serializer used to serialize index keys (optional)
     * @param valueSerializer
     *            Serializer used to serialize index values (optional)
     * @param comparator
     *            Comparator used to order index entries
     */
    public BTree(IOM store, Comparator comparator, Serializer keySerializer,
            Serializer valueSerializer) {

        this(store, comparator, keySerializer, valueSerializer, null,
                DEFAULT_SIZE);

    }

    /**
     * Create a new persistent BTree with the given number of entries per node.
     * 
     * @param store
     *            Persistence API.
     * @param comparator
     *            Comparator used to order index entries
     * @param keySerializer
     *            Serializer used to serialize index keys (optional)
     * @param valueSerializer
     *            Serializer used to serialize index values (optional)
     * @param keyCompressionProvider
     *            Used to compress keys (optional)
     * @param pageSize
     *            Number of entries per page (must be even).
     */
    public BTree(IOM store, Comparator comparator, Serializer keySerializer,
            Serializer valueSerializer,
            CompressionProvider keyCompressionProvider, int pageSize) {

        if (store == null) {
            throw new IllegalArgumentException("Argument 'journal' is null");
        }

        if (comparator == null) {
            throw new IllegalArgumentException("Argument 'comparator' is null");
        }

        if (!(comparator instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "Argument 'comparator' must be serializable");
        }

        if (keySerializer != null && !(keySerializer instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "Argument 'keySerializer' must be serializable");
        }

        if (valueSerializer != null
                && !(valueSerializer instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "Argument 'valueSerializer' must be serializable");
        }

        // make sure there's an even number of entries per BPage
        if ((pageSize & 1) != 0) {
            throw new IllegalArgumentException(
                    "Argument 'pageSize' must be even");
        }

        _recman = store;
        _pageSize = pageSize;
        _comparator = comparator;
        _keySerializer = keySerializer;
        _valueSerializer = valueSerializer;
        _keyCompressionProvider = null;
        // btree._bpageSerializer = new BPage();
        // btree._bpageSerializer._btree = btree;
        _recid = _insert(this); // insert into store.
        // btree._bpageSerializer._btreeId = btree.getRecid();
    }

//    /**
//     * Load a persistent BTree.
//     * 
//     * @param journal
//     *            The journal.
//     * @param recid
//     *            Record id of the BTree
//     * 
//     * @deprecated This is not needed when using extser as long as the
//     *             de-serialization logic sets the recid and recman fields,
//     *             which it can do from its own state.
//     */
//    public static BTree load( Journal journal, long recid )
//        throws IOException
//    {
//        
////        BTree btree = (BTree) recman.fetch( recid );
//        BTree btree = (BTree) journal._readObject(recid);
//
//        btree._recid = recid;
//        btree._recman = journal;
////        btree._bpageSerializer = new BPage();
////        btree._bpageSerializer._btree = btree;
////        btree._bpageSerializer._btreeId = recid;
//        return btree;
//    }

    public CompressionProvider getKeyCompressionProvider()
    {

        return _keyCompressionProvider;
        
    }
    
    /**
     * <p>
     * Insert an entry in the BTree.
     * </p>
     * <p>
     * The BTree cannot store duplicate entries. An existing entry can be
     * replaced using the <code>replace</code> flag. If an entry with the same
     * key already exists in the BTree, its value is returned.
     * </p>
     * 
     * @param key
     *            Insert key
     * @param value
     *            Insert value
     * @param replace
     *            Set to true to replace an existing key-value pair.
     * @return Existing value, if any.
     */
    public synchronized Object insert( Object key, Object value,
                                       boolean replace )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        if ( value == null ) {
            throw new IllegalArgumentException( "Argument 'value' is null" );
        }

        BPage rootPage = getRoot();

        if ( rootPage == null ) {
            // BTree is currently empty, create a new root BPage
            if (DEBUG) {
                System.out.println( "BTree.insert() new root BPage" );
            }
            rootPage = new BPage( this, key, value );
            _root = rootPage._recid;
            _height = 1;
            _entries = 1;
            _update( _recid, this );
            return null;
        } else {
            BPage.InsertResult insert = rootPage.insert( _height, key, value, replace );
            boolean dirty = false;
            if ( insert._overflow != null ) {
                // current root page overflowed, we replace with a new root page
                if ( DEBUG ) {
                    System.out.println( "BTree.insert() replace root BPage due to overflow" );
                }
                rootPage = new BPage( this, rootPage, insert._overflow );
                _root = rootPage._recid;
                _height += 1;
                dirty = true;
            }
            if ( insert._existing == null ) {
                _entries++;
                dirty = true;
            }
            if ( dirty ) {
                _update( _recid, this );
            }
            // insert might have returned an existing value
            return insert._existing;
        }
    }

    /**
     * Remove an entry with the given key from the BTree.
     * 
     * @param key
     *            Removal key
     *            
     * @return Value associated with the key, or null if no entry with given key
     *         existed in the BTree.
     *         
     * @exception IllegalArgumentException
     *                if the key is null.
     *                
     * @exception IllegalArgumentException
     *                if the key is not found.
     */
    public synchronized Object remove( Object key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }

        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }
        boolean dirty = false;
        BPage.RemoveResult remove = rootPage.remove( _height, key );
        if ( remove._underflow && rootPage.isEmpty() ) {
            _height -= 1;
            dirty = true;

            // TODO:  check contract for BPages to be removed from recman.
            if ( _height == 0 ) {
                _root = 0;
            } else {
                _root = rootPage.childBPage( _pageSize-1 )._recid;
            }
        }
        if ( remove._value != null ) {
            _entries--;
            dirty = true;
        }
        if ( dirty ) {
            _update( _recid, this );
        }
        return remove._value;
    }

    /**
     * Find the value associated with the given key.
     * 
     * @param key
     *            Lookup key.
     * @return Value associated with the key, or null if not found.
     */
    public synchronized Object find( Object key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return null;
        }

        Tuple tuple = new Tuple( null, null );
        TupleBrowser browser = rootPage.find( _height, key );

        if ( browser.getNext( tuple ) ) {
            // find returns the matching key or the next ordered key, so we must
            // check if we have an exact match
            if ( _comparator.compare( key, tuple.getKey() ) != 0 ) {
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
     * following this key in the ordered BTree.
     * 
     * @param key
     *            Lookup key.
     *            
     * @return Value associated with the key, or a greater entry, or null if no
     *         greater entry was found.
     */
    public synchronized Tuple findGreaterOrEqual( Object key )
        throws IOException
    {
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
     * Get a browser initially positioned at the beginning of the BTree.
     * </p>
     * <p>
     * <b> WARNING: If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results. </b>
     * </p>
     * 
     * @return Browser positionned at the beginning of the BTree.
     */
    public synchronized TupleBrowser browse()
        throws IOException
    {
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return EmptyBrowser.INSTANCE;
        }
        TupleBrowser browser = rootPage.findFirst();
        return browser;
    }

    /**
     * <p>
     * Get a browser initially positioned at the beginning of the BTree.
     * </p>
     * <p>
     * <b> WARNING: If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results. </b>
     * </p>
     * 
     * @param key
     *            Key used to position the browser. If null, the browser will be
     *            positionned after the last entry of the BTree. (Null is
     *            considered to be an "infinite" key)
     *            
     * @return Browser positionned just before the given key.
     */
    public synchronized TupleBrowser browse( Object key )
        throws IOException
    {
        BPage rootPage = getRoot();
        if ( rootPage == null ) {
            return EmptyBrowser.INSTANCE;
        }
        TupleBrowser browser = rootPage.find( _height, key );
        return browser;
    }

    /**
     * Deletes all BPages in this BTree, then deletes the tree from the record
     * manager
     */
    public synchronized void delete() throws IOException {
     
        BPage rootPage = getRoot();
        
        if (rootPage != null) {
            
            rootPage.delete();

        }
        
        _delete(_recid);
        
    }

    /**
     * Return the number of entries (size) of the BTree.
     */
    public synchronized long size()
    {
        
        return _entries;
        
    }

    /**
     * Return the persistent record identifier of the BTree.
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
     * to get to a leaf BPage, starting from the root.
     */
    public int getHeight()
    {
       
        return _height;
        
    }
    
    /**
     * Return the {@link Comparator} in use by the {@link BTree}.
     */
    public Comparator getComparator()
    {
        
        return _comparator;
        
    }

    /**
     * Return the {@link Serializer} in use by the {@link BTree}
     * to (de-)serialize its keys.
     */
    public Serializer getKeySerializer()
    {
        
        return _keySerializer;
        
    }
    
    /**
     * Return the {@link Serializer} in use by the {@link BTree}
     * to (de-)serialize its values.
     */
    public Serializer getValueSerializer()
    {
    
        return _valueSerializer;
        
    }
    
    /**
     * Return the root BPage, or null if it doesn't exist.
     */
    protected BPage getRoot() throws IOException
    {

        if ( _root == 0 ) {
            
            return null;
            
        }
        
        BPage root = _fetch( _root );
        
        root._recid = _root;
        
        root._btree = this;
        
        root._btreeId = getRecid();
        
        return root;
        
    }

//    /**
//     * De-serialization for {@link Externalizable}.
//     */
//    public void readExternal( ObjectInput in )
//        throws IOException, ClassNotFoundException
//    {
//        _comparator = (Comparator) in.readObject();
//        _keySerializer = (Serializer) in.readObject();
//        _valueSerializer = (Serializer) in.readObject();
//        _height = in.readInt();
//        _root = in.readLong();
//        _pageSize = in.readInt();
//        _entries = in.readLong();
//        _keyCompressionProvider = (CompressionProvider) in.readObject();
//    }
//
//    /**
//     * Fat serialization using {@link Externalizable}.
//     */
//    public void writeExternal( ObjectOutput out )
//        throws IOException
//    {
//        out.writeObject( _comparator );
//        out.writeObject( _keySerializer );
//        out.writeObject( _valueSerializer );
//        out.writeInt( _height );
//        out.writeLong( _root );
//        out.writeInt( _pageSize );
//        out.writeLong( _entries );
//        out.writeObject( _keyCompressionProvider );
//    }

    /**
     * Stream-based serialization supporting transparent versioning.
     * 
     * @author thompsonbry
     */
    public static class Serializer0 implements IStreamSerializer, Stateless
    {

        private static final long serialVersionUID = -7590073906338661367L;

        public void serialize(DataOutput out, Object obj) throws IOException {
            
            BTree tmp = (BTree) obj;

            out.writePackedInt ( tmp._height );
            out.writePackedInt ( tmp._pageSize );
            
            out.writePackedOId( tmp._root );
            out.writePackedLong( tmp._entries );
            
            out.serialize( tmp._comparator );
            out.serialize( tmp._keySerializer );
            out.serialize( tmp._valueSerializer );
            out.serialize( tmp._keyCompressionProvider );
            
        }

        /**
         * @todo Set _recid and _journal on the deserialized BTre.
         */
        public Object deserialize(DataInput in, Object obj) throws IOException {
            
            BTree tmp = (BTree) obj;
            
            tmp._height   = in.readPackedInt();
            tmp._pageSize = in.readPackedInt();
            
            tmp._root     = in.readPackedOId();
            tmp._entries  = in.readPackedLong();
            
            tmp._comparator = (Comparator) in.deserialize();
            tmp._keySerializer = (Serializer) in.deserialize();
            tmp._valueSerializer = (Serializer) in.deserialize();
            tmp._keyCompressionProvider = (CompressionProvider) in.deserialize();
            
            return tmp;
            
        }
        
    }

    /*
    public void assert() throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
            root.assertRecursive( _height );
        }
    }
    */

    public void dump( PrintStream out ) throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
        	root.dump( out, 0 );
            root.dumpRecursive( out, _height, 0 );
        }
    }

    /**
     * Used for debugging and testing only. Populates the 'out' list with the
     * recids of all child pages in the BTree.
     * 
     * @param out
     * @throws IOException
     */
    void dumpChildPageRecIDs(List<Long> out) throws IOException{

        BPage root = getRoot();
        
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
     * Insert the object into the store (used for {@link BTree} and
     * {@link BPage}.
     * 
     * @param obj
     *            The object to be inserted into the store.
     * 
     * @return The persistent identifier assigned to the object.
     */

    long _insert(Object obj) {

        return _recman.insert(obj);

    }

    /**
     * Fetch a {@link BPage} from the store.
     * 
     * @param recid
     *            The logical row id for the {@link BPage}.
     * 
     * @return The {@link BPage}.
     */
    BPage _fetch(long recid) {

        return (BPage) _recman.read(recid);

    }
    
    /**
     * Update a {@link BPage} in the store using either a custom serializer or a
     * serializer registered with the extensible serialization handler.
     * 
     * @param recid
     *            The logical row id for the {@link BPage}.
     * 
     * @param bpage
     *            The {@link BPage} that is being updated.
     */
    void _update(long recid, Object obj) throws IOException {

       _recman.update(recid, obj);

    }

    /**
     * Delete a {@link BPage} from the store.
     * 
     * @param recid
     *            The recid of the {@link BPage}.
     */
    void _delete(long recid) {

        _recman.delete(recid);

    }

//    /*
//     * Version of the CRUD interface for use by the object index.  This has
//     * different semantics since update() returns a NEW identifier.
//     */
//    
//    /**
//     * Insert the object into the store (used for {@link BTree} and
//     * {@link BPage}.
//     * 
//     * @param obj
//     *            The object to be inserted into the store.
//     * 
//     * @return The persistent identifier assigned to the object.
//     */
//
//    long _insert(Object obj) {
//
//        return _recman._insertObject(obj);
//
//    }
//
//    /**
//     * Fetch a {@link BPage} from the store.
//     * 
//     * @param recid
//     *            The logical row id for the {@link BPage}.
//     * 
//     * @return The {@link BPage}.
//     */
//    BPage _fetch(long recid) {
//
//        return (BPage) _recman._readObject(recid);
//
//    }
//    
//    /**
//     * Delete a {@link BPage} from the store.
//     * 
//     * @param recid
//     *            The recid of the {@link BPage}.
//     */
//    void _delete(long recid) {
//
//        _recman._deleteObject(recid);
//
//    }

}
