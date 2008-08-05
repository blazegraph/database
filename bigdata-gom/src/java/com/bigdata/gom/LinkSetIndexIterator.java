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
 * Created on Aug 16, 2005
 */
package com.bigdata.gom;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.CognitiveWeb.generic.IGeneric;
import org.CognitiveWeb.generic.ILinkSetIndex;
import org.CognitiveWeb.generic.core.ILinkSetIndexIterator;
import org.CognitiveWeb.generic.core.LinkSetIndex;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.KeyBuilder;

/**
 * <p>
 * Wraps up a {@link TupleBrowser} and provides forward iteration within a key
 * range.
 * </p>
 * 
 * @todo Support traversal with concurrent modification and {@link #remove()}.
 */
public class LinkSetIndexIterator implements ILinkSetIndexIterator {

    // parameter to the constructor.
    private final LinkSetIndex m_linkSetIndex;

    // hard reference to the BTree (cached by constructor).
    private final BTree m_btree;

    // internally created.
    private final ITupleIterator itr;

//    // parameter to the constructor.
//    private final Object m_fromKey;
//
//    // parameter to the constructor.
//    private final Object m_toKey;

    // parameter to the constructor.
    private final boolean m_resolve;

    /**
     * The last key visited (this is the internal form of the key - internal
     * keys are always distinct).
     */
    private byte[] m_curKey;

    /**
     * The last value visited (an object identifier).
     */
    private Long m_curVal;

    /**
     * <p>
     * Creates an {@link Iterator}which supports forward traversal within a key
     * range. Both <i>fromKey </i> and <i>toKey </i> MUST be in the
     * <em>internalKey</em> form.
     * </p>
     * 
     * @param linkSetIndex
     *            The persistent index backed by a {@link BTree}.
     * 
     * @param fromKey
     *            The traversal will begin with the first key greater than or
     *            equal to <i>fromKey </i>. If <i>fromKey </i> is
     *            <code>null</code>, then traveral will begin with the first
     *            key in the index.
     * 
     * @param toKey
     *            The traversal will visits all keys from <i>fromKey </i> and up
     *            to but not including <i>toKey </i>. If <i>toKey </i> is
     *            <code>null</code>, then the traversal will visit all
     *            remaining keys.
     * 
     * @param resolve
     *            When true, the recid of the entry in the {@link BTree}is
     *            automatically resolved by {@link #next()}to the corresponding
     *            {@link IGeneric}. When false, it is not and {@link #next()}
     *            returns the recid of that generic instead (as a {@link Long}).
     *            This effects the behavior of {@link #next()}and may be used
     *            to provide more efficient traversal when you do not want to
     *            materialize the visited objects.
     */
    public LinkSetIndexIterator(LinkSetIndex linkSetIndex, Object fromKey,
            Object toKey, boolean resolve) {

        if (linkSetIndex == null) {

            throw new IllegalArgumentException(
                    "The linkSetIndex may not be null.");

        }

        m_btree = (BTree) linkSetIndex.getBTree().getNativeBTree();

        /*
         * Obtain an iterator that visits only entries in the desired key range.
         * 
         * Note: Explicitly convert keys as necessary to byte[]s.
         */
        itr = m_btree.rangeIterator //
            ( KeyBuilder.asSortKey(fromKey),//
              KeyBuilder.asSortKey(toKey)//
              );
        
        m_linkSetIndex = linkSetIndex;

//        m_fromKey = fromKey;
//
//        m_toKey = toKey;

        m_resolve = resolve;

    }

    public boolean hasNext() {

        // Make sure that the link set index is still valid.

        m_linkSetIndex.valid();

        // the source iterator will deliver more entries.
        
        if(!itr.hasNext()) return false;
        
        return true;
    }

    /**
     * <p>
     * If <i>resolve </i> was specified as <code>true</code> to the
     * constructor, then returns the next {@link IGeneric}in the {@link BTree}.
     * Otherwise returns OID of that object as a {@link Long}.
     * </p>
     */
    public Object next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        final ITuple tuple = itr.next();
        
        m_curVal = KeyBuilder.decodeLong(tuple.getValueBuffer().array(), 0);

        m_curKey = tuple.getKey();

        Object ret = m_curVal;

        if (m_resolve) {

            // Resolve the recid to the corresponding generic object.

            ret = m_linkSetIndex.fetch(((Long) m_curVal).longValue());

        }

        return ret;

    }

    public ILinkSetIndex getIndex() {
        
        return m_linkSetIndex;
        
    }
    
    public Object getInternalKey() {

        return m_curKey;

    }

    /**
     * <p>
     * Removes the current entry from the {@link BTree}.
     * </p>
     * 
     * @todo currently throws {@link UnsupportedOperationException}
     */
    public void remove() {

        // 	    // Make sure that the link set index is still valid.

        // 	    m_linkSetIndex.assertValid();

        throw new UnsupportedOperationException();

    }

    /**
     * NOP.
     */
    public void close() {
        // NOP.
    }
    
}
