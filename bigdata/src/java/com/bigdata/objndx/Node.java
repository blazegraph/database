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
import java.util.Arrays;
import java.util.List;

import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.Journal;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * <p>
 * Page of a Btree.
 * </p>
 * <p>
 * The page contains a number of key-value pairs. Keys are ordered to allow
 * dichotomic search.
 * </p>
 * <p>
 * If the page is a leaf page, the keys and values are user-defined and
 * represent entries inserted by the user.
 * </p>
 * <p>
 * If the page is non-leaf, each key represents the greatest key in the
 * underlying BPages and the values are recids pointing to the children BPages.
 * The only exception is the rightmost Node, which is considered to
 * have an "infinite" key value, meaning that any insert will be to the left of
 * this pseudo-key
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class Node
{

    /**
     * FIXME Temporary factor for node identifers.  This SHOULD NOT be static,
     * but a per-journal or index instance counter.
     */
    static private int nextId = 0;

    /**
     * Node identifier, perhaps persistent, certainly stable within a session.
     * 
     * FIXME Reconcile id and _recid. One should be the current slots on which
     * the node is written in the journal. The other should be the node
     * identifier.
     */
    transient int id = nextId++;
    
    /**
     * The {@link ISlotAllocation} for this node, encoded as a long integer.
     * 
     * @todo This is required to deallocate the storage when the index node is
     *       GC'd. The value is set when the node is serialized or deserialized.
     *       (Make this final and set to 0L during tests. It can actually be
     *       just the firstSlot or even an ISlotAllocation, or we can leave it
     *       as a long for consistency with non-object index btrees.)
     */
    /*final*/ transient long _recid;

    /**
     * Debug flag.
     */
    private static final boolean DEBUG = false;

    /**
     * Negative infinity for keys - zero (0). This value is never a valid key.
     * All valid keys sort larger than this value.
     */
    static transient final int NEGINF_KEY = 0;
    
    /**
     * The largest possible key. This value is never a valid key. All valid keys
     * sort less than this value.
     */
    static transient final int POSINF_KEY = Integer.MAX_VALUE;

    /**
     * Parent B+Tree.
     * 
     * @todo Reduce this to just the API for the node cache since that is the
     * only purpose to which the {@link ObjectIndex} is applied by this class.
     */
    final transient ObjectIndex _btree;

    /**
     * Flag indicating if this is a leaf Node.
     */
    final boolean _isLeaf;

    /**
     * The #of key/value positions available in this node (aka the branching
     * factor for the node or the maximum possible #of values for a leaf).
     */
    final int _pageSize;
    
    /**
     * Index of first used item at the page. Keys positions are consumed
     * backwards from the end of {@link #_keys}. Therefore when {@link #_first}
     * is equal to the #of keys positions available in a node, the node
     * {@link #isEmpty()} and when {@link #_first} is equal to zero(0) the node
     * is full.
     * 
     * @todo Document how this field is interpreted and how it relates to the
     *       #of defined keys for the node.
     */
    int _first;

    /**
     * Keys of children nodes. Key positions are consumed backwards from the end
     * of this array. This array is maintained in sorted order and, depending on
     * whether the node is a non-leaf or leaf-node, the values in
     * {@link #_children} (non-leaf nodes) or in {@link #_values} (leaf nodes)
     * are maintained in the corresponding order (so that values remain paired
     * with their key).
     * 
     * @todo Convert keys to int[].
     */
    Integer[] _keys;
    
    /**
     * Values associated with keys. (Only valid if leaf Node)
     * 
     * FIXME This needs to be strongly typed as {@link IObjectIndexEntry} or
     * even {@link IndexEntry}. This change triggers a cascade of changes that
     * touch on the semantics of the object index as it differs from a simple
     * btree. Those changes include tracking version counters, deallocating
     * slots for the prior version written within a transaction, noting that
     * there is a pre-existing version from outside of the transaction, marking
     * an object as deleted within a transaction, and reading through to the
     * base object index.
     */
    Object[] _values;

    /**
     * Children pages (recids) associated with keys. (Only valid if non-leaf
     * Node)
     */
    long[] _children;
    
    /**
     * Previous leaf Node (only if this Node is a leaf)
     */
    long _previous;

    /**
     * Next leaf Node (only if this Node is a leaf)
     */
    long _next;

    /**
     * Return the B+Tree that is the owner of this {@link Node}.
     */
    public ObjectIndex getBTree() {
        
        return _btree;
        
    }

    /**
     * Disallowed.
     */
    private Node() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Constructor used by {@link NodeSerializer} to de-serialize a node.
     * 
     * @param ndx
     *            The index (required).
     * @param recid
     *            The persistent identifier for the node (required).
     * @param pageSize
     *            The #of key positions in the node (aka branching factor).
     * @param first
     *            The index of the first key position used in the node. Keys
     *            position are filled backwards from the end of the node and are
     *            maintained in sorted order.
     * @param keys
     *            An array of keys (optional). The capacity of the array must
     *            equal <i>pageSize</i>. Keys are stored backwards from the end
     *            of the array and must be in sorted order.
     * @param children
     */
    public Node(ObjectIndex ndx, long recid, int pageSize,
            int first, int[] keys, long[] children) {
        
//        assert ndx != null; // @todo enable assertion
//        assert recid != 0L; // @todo enable assertion
        assert pageSize > 0 && (pageSize&1) == 0;
        assert first>=0 && first<pageSize;
        assert keys == null || keys.length == pageSize;
        assert children == null || children.length == pageSize;
        
        this._btree = ndx;
        this._recid = recid;
        this._pageSize = pageSize;
        this._isLeaf = false;
        this._first = first;
        if( keys != null ) {
            /*
             * @todo Copy only those keys defined by [first].
             * @todo Convert keys to int[].
             */
            this._keys = new Integer[keys.length];
            for( int i=0; i<keys.length; i++ ) {
                this._keys[i] = new Integer(keys[i]);
            }
        }
        this._children = children;
        this._values = null;
        this._previous = 0L;
        this._next = 0L;
    }

    /**
     * Constructor used by {@link NodeSerializer} to de-serialize a leaf.
     */
    public Node(ObjectIndex ndx, long recid, int pageSize,
            int first, int[] keys, IndexEntry[] values, long previous, long next) {
        
//      assert ndx != null; // @todo enable assertion
//      assert recid != 0L; // @todo enable assertion
        assert pageSize > 0 && (pageSize&1) == 0;
        assert first>=0 && first<pageSize;
        assert keys == null || keys.length == pageSize;
        assert values == null || values.length == pageSize;
        
        this._btree = ndx;
        this._recid = recid;
        this._pageSize = pageSize;
        this._isLeaf = true;
        this._first = first;
        if( keys != null ) {
            /*
             * @todo Copy only those keys defined by [first].
             * @todo Convert keys to int[].
             */
            this._keys = new Integer[keys.length];
            for( int i=0; i<keys.length; i++ ) {
                this._keys[i] = new Integer(keys[i]);
            }
        }
        this._children = null;
        this._values = values;
        this._previous = previous;
        this._next = next;
    }

    /**
     * Root page overflow constructor
     * 
     * @todo The constructor must not eagerly write the new node onto the
     *       journal.
     */
    Node( ObjectIndex btree, Node root, Node overflow )
    {

        _btree = btree;
//        _btreeId = btree.getRecid();

        _isLeaf = false;

        _pageSize = btree._pageSize;
        
        _first = _pageSize-2;

        _keys = new Integer[ _pageSize ];
        _keys[ _pageSize-2 ] = overflow.getLargestKey();
        _keys[ _pageSize-1 ] = root.getLargestKey();

        _children = new long[ _pageSize ];
        _children[ _pageSize-2 ] = overflow._recid;
        _children[ _pageSize-1 ] = root._recid;

        _recid = _btree._insert( this );
        
    }

    /**
     * Root page (first insert) constructor.
     * 
     * @todo The root page should be created when the journal is created rather
     *       than waiting for the first insert. That empty root page is then the
     *       rollback point until the first transaction successfully commits.
     * @todo The constructor must not eagerly write the new node onto the
     *       journal.  This should be handled by the {@link Journal} init.
     */
    Node( ObjectIndex btree, Integer key, ISlotAllocation currentVersion )
    {
        _btree = btree;
//        _btreeId = btree.getRecid();

        _isLeaf = true;

        _pageSize = btree._pageSize;

        _first = _pageSize-2;

        /*
         * @todo What is this about pageSize-2 and pageSize-1 to indicate the
         * root node? Is this just a means of specifying POSINF as a successor
         * of the first key?
         */
        
        _keys = new Integer[ _pageSize ];
        _keys[ _pageSize-2 ] = key;
        _keys[ _pageSize-1 ] = null;  // I am the root Node for now

        /*
         * @todo Verify assumption that there is no pre-existing version.
         */
        _values = new IndexEntry[ _pageSize ];
        _values[ _pageSize-2 ] = new IndexEntry(btree._journal.slotMath,(short)0,currentVersion.toLong(),0L);
        _values[ _pageSize-1 ] = null;  // I am the root Node for now

        _recid = _btree._insert( this );
    }

    /**
     * Overflow page constructor. Creates an empty node.
     * 
     * @param btree
     * @param isLeaf
     * 
     * @todo The constructor must not eagerly write the new node onto the
     *       journal.
     */
    Node( ObjectIndex btree, boolean isLeaf )
    {
        
        _btree = btree;
        
//        _btreeId = btree.getRecid();

        _pageSize = btree._pageSize;

        _isLeaf = isLeaf;

        /*
         * page will initially be half-full
         * 
         * @todo This is a bit odd. Check into the contexts in which this
         * constructor is used. I would think that adjusting _first would be the
         * responsibility of the caller, but it appears that the constructor
         * sets _first in advance for the post-condition of the overflow
         * operation.
         */
        _first = _pageSize/2;

        _keys = new Integer[ _pageSize ];
        
        if ( isLeaf ) {
            
            _values = new IndexEntry[ _pageSize ];
            
        } else {
            
            _children = new long[ _pageSize ];
            
        }

        _recid = _btree._insert( this );
        
    }

    /**
     * Get largest key under this Node. Null is considered to be the
     * greatest possible key.
     */
    Integer getLargestKey()
    {
        
        return _keys[ _pageSize-1 ];
        
    }

    /**
     * Return true if Node is empty.
     */
    boolean isEmpty()
    {

        if ( _isLeaf ) {
            
            return ( _first == _values.length-1 );
            
        } else {
            
            return ( _first == _children.length-1 );
            
        }
        
    }

    /**
     * Return true if Node is full.
     */
    boolean isFull() {

        return ( _first == 0 );
        
    }

    /**
     * Find the object associated with the given key.
     * 
     * @param height
     *            Height of the current Node (zero is leaf page).
     * @param key
     *            The key.
     *            
     * @return TupleBrowser positionned just before the given key, or before
     *         next greater key if key isn't found.
     */
    TupleBrowser find(int height, Integer key)
    {

        int index = findChildren( key );

        /*
        if ( DEBUG ) {
            System.out.println( "Node.find() current: " + this
                                + " height: " + height);
        }
        */

        height -= 1;

        if ( height == 0 ) {
            
            // leaf Node
            return new Browser( this, index );
            
        } else {
            
            // non-leaf Node
            Node child = loadChildNode( index );
            
            return child.find( height, key );
            
        }
        
    }

    /**
     * Find first entry and return a browser positioned before it.
     * 
     * @return TupleBrowser positioned just before the first entry.
     */
    TupleBrowser findFirst() {

        if ( _isLeaf ) {
            
            return new Browser( this, _first );
            
        } else {
            
            Node child = loadChildNode( _first );
            
            return child.findFirst();
            
        }
        
    }

    /**
     * Deletes this Node and all children pages.
     */
    void delete() {
        
        if (_isLeaf){
            if (_next != 0){
                Node nextBPage = loadNode(_next);
                if (nextBPage._previous == _recid){ // this consistency check can be removed in production code
                    nextBPage._previous = _previous;
                    _btree._update(nextBPage);
                } else {
                    throw new Error("Inconsistent data in ObjectIndex");
                }
            }
            if (_previous != 0){
                Node previousBPage = loadNode(_previous);
                if (previousBPage._next != _recid){ // this consistency check can be removed in production code
                    previousBPage._next = _next;
                    _btree._update(previousBPage);
                } else {
                    throw new Error("Inconsistent data in ObjectIndex");
                }
            }
        } else {
            int left = _first;
            int right = _pageSize-1;

            for (int i = left; i <= right; i++){
                Node childBPage = loadNode(_children[i]);
                childBPage.delete();
            }
        }
        
        _btree._delete(_recid);

    }
    
    /**
     * <p>
     * Insert the given key and value.
     * </p>
     * <p>
     * Since the Btree does not support duplicate entries, the caller must
     * specify whether to replace the existing value.
     * </p>
     * 
     * @param height
     *            Height of the current Node (zero is leaf page)
     * @param key
     *            Insert key
     * @param value
     *            Insert value
     * @param replace
     *            Set to true to replace the existing value, if one exists.
     * 
     * @return Insertion result containing existing value OR a
     *         {@link Node} if the key was inserted and provoked a
     *         {@link Node} overflow.
     */
    InsertResult insert(int height, Integer key, Object value, boolean replace) {

        InsertResult  result;
        long          overflow;

        int index = findChildren( key );

        height -= 1;
        if ( height == 0 )  {

            result = new InsertResult();

            // inserting on a leaf Node
            overflow = -1;
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() Insert on leaf Bpage key=" + key
                                    + " value=" + value + " index="+index);
            }
            if ( compare( key, _keys[ index ] ) == 0 ) {
                // key already exists
                if ( DEBUG ) {
                    System.out.println( "Bpage.insert() Key already exists." ) ;
                }
                result._existing = _values[ index ];
                assert replace = true; // @todo semantics not appropriate for object index.
                if ( replace ) {
                    /*
                     * FIXME There is a version for the persistent identifier recorded
                     * in this object index context.  We need to check preExisting and
                     * deallocation the previous version unless it was preExisting.
                     * 
                     * FIXME Deletes are handled separately, but make sure that they are
                     * handled!
                     */
                    _values [ index ] = value;
                    _btree._update( this );
                }
                // return the existing key
                return result;
            }
        } else {
            // non-leaf Node
            Node child = loadChildNode( index );
            result = child.insert( height, key, value, replace );

            if ( result._existing != null ) {
                // return existing key, if any.
                return result;
            }

            if ( result._overflow == null ) {
                // no overflow means we're done with insertion
                return result;
            }

            // there was an overflow, we need to insert the overflow page
            // on this Node
            if ( DEBUG ) {
                System.out.println( "Node.insert() Overflow page: " + result._overflow._recid );
            }
            key = result._overflow.getLargestKey();
            overflow = result._overflow._recid;

            // update child's largest key
            _keys[ index ] = child.getLargestKey();

            // clean result so we can reuse it
            result._overflow = null;
        }

        // if we get here, we need to insert a new entry on the Node
        // before _children[ index ]
        if ( !isFull() ) {
            if ( height == 0 ) {
                insertEntry( this, index-1, key, value );
            } else {
                insertChild( this, index-1, key, overflow );
            }
            _btree._update( this);
            return result;
        }

        // page is full, we must divide the page
        int half = _pageSize >> 1;
        Node newPage = new Node( _btree, _isLeaf );
        if ( index < half ) {
            // move lower-half of entries to overflow Node,
            // including new entry
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() move lower-half of entries to overflow Node, including new entry." ) ;
            }
            if ( height == 0 ) {
                copyEntries( this, 0, newPage, half, index );
                setEntry( newPage, half+index, key, value );
                copyEntries( this, index, newPage, half+index+1, half-index-1 );
            } else {
                copyChildren( this, 0, newPage, half, index );
                setChild( newPage, half+index, key, overflow );
                copyChildren( this, index, newPage, half+index+1, half-index-1 );
            }
        } else {
            // move lower-half of entries to overflow Node,
            // new entry stays on this Node
            if ( DEBUG ) {
                System.out.println( "Bpage.insert() move lower-half of entries to overflow Node. New entry stays" ) ;
            }
            if ( height == 0 ) {
                copyEntries( this, 0, newPage, half, half );
                copyEntries( this, half, this, half-1, index-half );
                setEntry( this, index-1, key, value );
            } else {
                copyChildren( this, 0, newPage, half, half );
                copyChildren( this, half, this, half-1, index-half );
                setChild( this, index-1, key, overflow );
            }
        }

        _first = half-1;

        // nullify lower half of entries
        for ( int i=0; i<_first; i++ ) {
            if ( height == 0 ) {
                setEntry( this, i, null, null );
            } else {
                setChild( this, i, null, -1 );
            }
        }

        if ( _isLeaf ) {
            // link newly created Node
            newPage._previous = _previous;
            newPage._next = _recid;
            if ( _previous != 0 ) {
                Node previous = loadNode( _previous );
                previous._next = newPage._recid;
                _btree._update( previous );
            }
            _previous = newPage._recid;
        }

        _btree._update( this );
        _btree._update( newPage );

        result._overflow = newPage;
        return result;
    }

    /**
     * Remove the entry associated with the given key.
     * 
     * @param height
     *            Height of the current Node (zero is leaf page)
     * @param key
     *            Removal key
     *            
     * @return Remove result object
     */
    RemoveResult remove(int height, Integer key) {

        RemoveResult result;

        int half = _pageSize / 2;
        int index = findChildren( key );

        height -= 1;
        if ( height == 0 ) {
            // remove leaf entry
            if ( compare( _keys[ index ], key ) != 0 ) {
                throw new IllegalArgumentException( "Key not found: " + key );
            }
            result = new RemoveResult();
            result._value = _values[ index ];
            removeEntry( this, index );

            // update this Node
            _btree._update( this );

        } else {
            // recurse into Btree to remove entry on a children page
            Node child = loadChildNode( index );
            result = child.remove( height, key );

            // update children
            _keys[ index ] = child.getLargestKey();
            _btree._update( this );

            if ( result._underflow ) {
                // underflow occured
                if ( child._first != half+1 ) {
                    throw new IllegalStateException( "Error during underflow [1]" );
                }
                if ( index < _children.length-1 ) {
                    // exists greater brother page
                    Node brother = loadChildNode( index+1 );
                    int bfirst = brother._first;
                    if ( bfirst < half ) {
                        // steal entries from "brother" page
                        int steal = ( half - bfirst + 1 ) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        if ( child._isLeaf ) {
                            copyEntries( child, half+1, child, half+1-steal, half-1 );
                            copyEntries( brother, bfirst, child, 2*half-steal, steal );
                        } else {
                            copyChildren( child, half+1, child, half+1-steal, half-1 );
                            copyChildren( brother, bfirst, child, 2*half-steal, steal );
                        }                            

                        for ( int i=bfirst; i<bfirst+steal; i++ ) {
                            if ( brother._isLeaf ) {
                                setEntry( brother, i, null, null );
                            } else {
                                setChild( brother, i, null, -1 );
                            }
                        }

                        // update child's largest key
                        _keys[ index ] = child.getLargestKey();

                        // no change in previous/next Node

                        // update BPages
                        _btree._update( this );
                        _btree._update( brother );
                        _btree._update( child );

                    } else {
                        // move all entries from page "child" to "brother"
                        if ( brother._first != half ) {
                            throw new IllegalStateException( "Error during underflow [2]" );
                        }

                        brother._first = 1;
                        if ( child._isLeaf ) {
                            copyEntries( child, half+1, brother, 1, half-1 );
                        } else {
                            copyChildren( child, half+1, brother, 1, half-1 );
                        }
                        _btree._update( brother );

                        // remove "child" from current Node
                        if ( _isLeaf ) {
                            copyEntries( this, _first, this, _first+1, index-_first );
                            setEntry( this, _first, null, null );
                        } else {
                            copyChildren( this, _first, this, _first+1, index-_first );
                            setChild( this, _first, null, -1 );
                        }
                        _first += 1;
                        _btree._update( this );

                        // re-link previous and next BPages
                        if ( child._previous != 0 ) {
                            Node prev = loadNode( child._previous );
                            prev._next = child._next;
                            _btree._update( prev );
                        }
                        if ( child._next != 0 ) {
                            Node next = loadNode( child._next );
                            next._previous = child._previous;
//                            _btree._recman.update( next._recid, next, this );
                            _btree._update( next );
                        }

                        // delete "child" Node
                        _btree._delete( child._recid );
                    }
                } else {
                    // page "brother" is before "child"
                    Node brother = loadChildNode( index-1 );
                    int bfirst = brother._first;
                    if ( bfirst < half ) {
                        // steal entries from "brother" page
                        int steal = ( half - bfirst + 1 ) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        if ( child._isLeaf ) {
                            copyEntries( brother, 2*half-steal, child,
                                         half+1-steal, steal );
                            copyEntries( brother, bfirst, brother,
                                         bfirst+steal, 2*half-bfirst-steal );
                        } else {
                            copyChildren( brother, 2*half-steal, child,
                                          half+1-steal, steal );
                            copyChildren( brother, bfirst, brother,
                                          bfirst+steal, 2*half-bfirst-steal );
                        }

                        for ( int i=bfirst; i<bfirst+steal; i++ ) {
                            if ( brother._isLeaf ) {
                                setEntry( brother, i, null, null );
                            } else {
                                setChild( brother, i, null, -1 );
                            }
                        }

                        // update brother's largest key
                        _keys[ index-1 ] = brother.getLargestKey();

                        // no change in previous/next Node

                        // update BPages
                        _btree._update( this );
                        _btree._update( brother );
                        _btree._update( child );

                    } else {
                        // move all entries from page "brother" to "child"
                        if ( brother._first != half ) {
                            throw new IllegalStateException( "Error during underflow [3]" );
                        }

                        child._first = 1;
                        if ( child._isLeaf ) {
                            copyEntries( brother, half, child, 1, half );
                        } else {
                            copyChildren( brother, half, child, 1, half );
                        }
                        _btree._update( child );

                        // remove "brother" from current Node
                        if ( _isLeaf ) {
                            copyEntries( this, _first, this, _first+1, index-1-_first );
                            setEntry( this, _first, null, null );
                        } else {
                            copyChildren( this, _first, this, _first+1, index-1-_first );
                            setChild( this, _first, null, -1 );
                        }
                        _first += 1;
                        _btree._update( this );

                        // re-link previous and next BPages
                        if ( brother._previous != 0 ) {
                            Node prev = loadNode( brother._previous );
                            prev._next = brother._next;
                            _btree._update( prev );
                        }
                        if ( brother._next != 0 ) {
                            Node next = loadNode( brother._next );
                            next._previous = brother._previous;
                            _btree._update( next );
                        }

                        // delete "brother" Node
                        _btree._delete( brother._recid );
                    }
                }
            }
        }

        // underflow if page is more than half-empty
        result._underflow = _first > half;

        return result;
    }

    /**
     * @todo Convert to int32 parameters.
     * @todo Write test cases to make sure that positive and negative infinity
     *       are handled correctly.
     * @todo roll this method inline
     */
    final int compare(Integer value1, Integer value2) {
        if (value1 == null) {
            return 1;
        }
        if (value2 == null) {
            return -1;
        }
        return value1.compareTo(value2);
    }

    /**
     * Find the first entry having a key equal or greater than the given key
     * using a binary search on the sorted keys.
     * 
     * @param key
     *            The given key.
     * 
     * @return The index of first entry having an equal or greater key.<br> *
     *         Note: The result when probing greater than the last key value is
     *         the last key position.
     * 
     * @todo The edge case when the probe is greater than the last key value is
     *       doubtless treated through logic everywhere that
     *       {@link #findChildren(Integer)} is invoked. It would perhaps be
     *       cleaner we changed the semantics to return a negative value that
     *       encodes the insertion index when the key is not present in the
     *       node per {@link Arrays#binarySearch(int[], int)}.
     * 
     * @todo Write test cases to make sure that positive and negative infinity
     *       are handled correctly.
     */
    int findChildren( Integer key )
    {
        int left = _first;
        int right = _pageSize-1;

        // binary search
        while ( left < right )  {
            int middle = ( left + right ) / 2;
            if ( compare( _keys[ middle ], key ) < 0 ) {
                left = middle+1;
            } else {
                right = middle;
            }
        }
        return right;
    }

    /**
     * Insert entry at given position.
     */
    private static void insertEntry( Node page, int index,
                                     Object key, Object value )
    {
        Object[] keys = page._keys;
        Object[] values = page._values;
        int start = page._first;
        int count = index-page._first+1;

        // shift entries to the left
        System.arraycopy( keys, start, keys, start-1, count );
        System.arraycopy( values, start, values, start-1, count );
        page._first -= 1;
        keys[ index ] = key;
        values[ index ] = value;
    }


    /**
     * Insert child at given position.
     */
    private static void insertChild( Node page, int index,
                                     Object key, long child )
    {
        Object[] keys = page._keys;
        long[] children = page._children;
        int start = page._first;
        int count = index-page._first+1;

        // shift entries to the left
        System.arraycopy( keys, start, keys, start-1, count );
        System.arraycopy( children, start, children, start-1, count );
        page._first -= 1;
        keys[ index ] = key;
        children[ index ] = child;
    }
    
    /**
     * Remove entry at given position.
     */
    private static void removeEntry( Node page, int index )
    {
        Object[] keys = page._keys;
        Object[] values = page._values;
        int start = page._first;
        int count = index-page._first;

        System.arraycopy( keys, start, keys, start+1, count );
        keys[ start ] = null;
        System.arraycopy( values, start, values, start+1, count );
        values[ start ] = null;
        page._first++;
    }


//    /**
//     * Remove child at given position.
//     */
/*    
    private static void removeChild( Node page, int index )
    {
        Object[] keys = page._keys;
        long[] children = page._children;
        int start = page._first;
        int count = index-page._first;

        System.arraycopy( keys, start, keys, start+1, count );
        keys[ start ] = null;
        System.arraycopy( children, start, children, start+1, count );
        children[ start ] = (long) -1;
        page._first++;
    }
*/
    
    /**
     * Set the entry at the given index (leaf node).
     */
    private static void setEntry( Node page, int index, Integer key, Object value )
    {
        page._keys[ index ] = key;
        page._values[ index ] = value;
    }


    /**
     * Set the child Node at the given index (non-leaf node).
     */
    private static void setChild( Node page, int index, Integer key, long recid )
    {
        page._keys[ index ] = key;
        page._children[ index ] = recid;
    }
    
    
    /**
     * Copy entries between two BPages
     */
    private static void copyEntries( Node source, int indexSource,
                                     Node dest, int indexDest, int count )
    {
        System.arraycopy( source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy( source._values, indexSource, dest._values, indexDest, count);
    }


    /**
     * Copy child Node recids between two BPages
     */
    private static void copyChildren( Node source, int indexSource,
                                      Node dest, int indexDest, int count )
    {
        System.arraycopy( source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy( source._children, indexSource, dest._children, indexDest, count);
    }
   
    /**
     * Return the child Node at given index.
     */
    Node loadChildNode(int index) {

        return loadNode(_children[index]);
        
    }

    /**
     * Load the Node at the given recid.
     */
    private Node loadNode(long recid) {

        return _btree._fetch(recid);
        
    }

    /**
     * Dump the structure of the tree on the screen.  This is used for debugging
     * purposes only.
     */
    void dump( PrintStream out, int height )
    {
        String prefix = "";
        for ( int i=0; i<height; i++ ) {
           prefix += "    ";
        }
        out.println( prefix + "-------------------------------------- Node recid=" + _recid);
        out.println( prefix + "first=" + _first );
        final int limit = ( _keys == null ? 0 : _keys.length );
        for ( int i=0; i<limit; i++ ) {
            if ( _isLeaf ) {
                out.println( prefix + "value [" + i + "] " + _keys[ i ] + " " + _values[ i ] );
            } else {
                out.println( prefix + "child [" + i + "] " + _keys[ i ] + " " + _children[ i ] );
            }
        }
        out.println( prefix + "--------------------------------------" );
    }

    /**
     * Recursively dump the state of the ObjectIndex on screen.  This is used for
     * debugging purposes only.
     */
    void dumpRecursive(PrintStream out, int height, int level) {
        
        height -= 1;
        
        level += 1;
        
        if ( height > 0 ) {
            
            for ( int i=_first; i<_pageSize; i++ ) {
                
                if ( _children[ i ] == ObjectIndex.NULL ) continue;
                
                Node child = loadChildNode( i );
                
                child.dump( out, level );
                
                child.dumpRecursive( out, height, level );
                
            }
            
        }
        
    }

    /**
     * Used for debugging and testing only. Recursively obtains the recids of
     * all child BPages and adds them to the 'out' list.
     * 
     * @param out
     * @param height
     */
    void dumpChildPageRecIDs(List<Long> out, int height) {
        
        height -= 1;
        
        if ( height > 0 ) {
            
            for ( int i=_first; i<_pageSize; i++ ) {
                
                if ( _children[ i ] == ObjectIndex.NULL ) continue;
                
                Node child = loadChildNode( i );
                
                out.add(new Long(child._recid));
                
                child.dumpChildPageRecIDs( out, height );
                
            }
            
        }
        
    }

    /**
     * Assert the ordering of the keys on the Node. This is used for
     * testing purposes only.
     */
    private void assertConsistency( PrintStream out )
    {
        for ( int i=_first; i<_pageSize-1; i++ ) {
            if ( compare( _keys[ i ], _keys[ i+1 ] ) >= 0 ) {
                dump( out, 0 );
                throw new Error( "Node not ordered" );
            }
        }
    }

    /**
     * Recursively assert the ordering of the Node entries on this page
     * and sub-pages.  This is used for testing purposes only.
     */
    void assertConsistencyRecursive(PrintStream out, int height) {
        
        assertConsistency( out );
        
        if ( --height > 0 ) {
            
            for ( int i=_first; i<_pageSize; i++ ) {
                
                if ( _keys[ i ] == null ) break;
            
                Node child = loadChildNode( i );
                
                if ( compare( _keys[ i ], child.getLargestKey() ) != 0 ) {
                    
                    dump( out, 0 );
                    
                    child.dump( out, 0 );
                    
                    throw new Error( "Invalid child subordinate key" );
                    
                }
                
                child.assertConsistencyRecursive( out, height );
                
            }
            
        }
        
    }

    /**
     * Result from insert() method call
     */
    static class InsertResult {

        /**
         * Overflow page.
         */
        Node _overflow;

        /**
         * Existing value for the insertion key.
         */
        Object _existing;

    }

    /** STATIC INNER CLASS
     *  Result from remove() method call
     */
    static class RemoveResult {

        /**
         * Set to true if underlying pages underflowed
         */
        boolean _underflow;

        /**
         * Removed entry value
         */
        Object _value;
    }


    /**
     * Browser to traverse leaf BPages.
     */
    static class Browser
        extends TupleBrowser
    {

        /**
         * Current page.
         */
        private Node _page;

        /**
         * Current index in the page.  The index positionned on the next
         * tuple to return.
         */
        private int _index;

        /**
         * Create a browser.
         *
         * @param page Current page
         * @param index Position of the next tuple to return.
         */
        Browser( Node page, int index )
        {
            _page = page;
            _index = index;
        }

        public boolean getNext( Tuple tuple )
        {
            if ( _index < _page._pageSize ) {
                if ( _page._keys[ _index ] == null ) {
                    // reached end of the tree.
                    return false;
                }
            } else if ( _page._next != 0 ) {
                // move to next page
                _page = _page.loadNode( _page._next );
                _index = _page._first;
            }
            tuple.setKey( _page._keys[ _index ] );
            tuple.setValue( _page._values[ _index ] );//.getCurrentVersionSlots() );
            _index++;
            return true;
        }

        public boolean getPrevious( Tuple tuple )
        {
            if ( _index == _page._first ) {

                if ( _page._previous != 0 ) {
                    _page = _page.loadNode( _page._previous );
                    _index = _page._pageSize;
                } else {
                    // reached beginning of the tree
                    return false;
                }
            }
            _index--;
            tuple.setKey( _page._keys[ _index ] );
            tuple.setValue( _page._values[ _index ] ); //.getCurrentVersionSlots() );
            return true;

        }
    }

}
