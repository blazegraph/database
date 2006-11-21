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
 * Created on Nov 20, 2006
 */

package com.bigdata.objndx;

import com.bigdata.journal.IRawStore;

/**
 * Interface for a B-Tree having integer keys and arbitrary values. The key
 * range is restricted to ({@link #NEGINF}:{@link #POSINF}). The interface
 * and its implementation provide specialized support for an object index from
 * int32 within segment persistent identifiers to the metadata required by the
 * concurrency control policy to track both the current state of the object,
 * detect write-write conflicts, and efficiently garbage collect historical
 * versions that are no longer accessible to any active transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBTree {

    /**
     * The persistence store.
     */
    public IRawStore getStore();

    /**
     * The branching factor for the btree.
     */
    public int getBrachingFactor();

    /**
     * The height of the btree. The height is the #of leaves minus one. A
     * btree with only a root leaf is said to have <code>height := 0</code>.
     * Note that the height only changes when we split the root node.
     */
    public int getHeight();

    /**
     * The #of non-leaf nodes in the btree. The is zero (0) for a new btree.
     */
    public int getNodeCount();

    /**
     * The #of leaf nodes in the btree.  This is one (1) for a new btree.
     */
    public int getLeafCount();

    /**
     * The #of entries (aka values) in the btree. This is zero (0) for a new
     * btree.
     */
    public int size();

    /**
     * The object responsible for (de-)serializing the nodes and leaves of the
     * {@link BTree}.
     */
    public NodeSerializer getNodeSerializer();

    /**
     * The object responsible for choosing the index at which to split a
     * {@link Node}.
     */
    public INodeSplitPolicy getNodeSplitPolicy();

    /**
     * The object responsible for choosing the index at which to split a
     * {@link Leaf}.
     */
    public ILeafSplitPolicy getLeafSplitPolicy();

    /**
     * Positive infinity for the external keys.
     */
    public static final int POSINF = Integer.MAX_VALUE;

    /**
     * Negative infinity for the external keys.
     */
    public static final int NEGINF = 0;

    /**
     * The root of the btree. This is initially a leaf until the leaf is
     * split, at which point it is replaced by a node. The root is also
     * replaced each time copy-on-write triggers a cascade of updates.
     */
    public AbstractNode getRoot();

    /**
     * Insert an entry under the external key.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     *            
     * @return The previous entry under that key or <code>null</code> if the
     *         key was not found.
     */
    public Object insert(int key, Object entry);

    /**
     * Lookup an entry for an external key.
     * 
     * @return The entry or null if there is no entry for that key.
     */
    public Object lookup(int key);

    /**
     * Remove the entry for the external key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The entry stored under that key and null if there was no
     *         entry for that key.
     */
    public Object remove(int key);

    /**
     * Commit dirty nodes using a post-order traversal that first writes any
     * dirty leaves and then (recursively) their parent nodes. The parent nodes
     * are guarenteed to be dirty if there is a dirty child so the commit never
     * triggers copy-on-write.
     * 
     * @return The persistent identity of the metadata record for the btree.
     */
    public long commit();

}
