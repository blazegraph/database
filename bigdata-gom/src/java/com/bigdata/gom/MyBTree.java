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
 * Created on May 9, 2006
 */
package com.bigdata.gom;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.CognitiveWeb.generic.core.AbstractBTree;
import org.CognitiveWeb.generic.core.ILinkSetIndexIterator;
import org.CognitiveWeb.generic.core.LinkSetIndex;
import org.CognitiveWeb.generic.core.ndx.Coercer;
import org.CognitiveWeb.generic.core.ndx.Successor;
import org.CognitiveWeb.generic.core.om.BaseObject;

import com.bigdata.btree.BTree;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.IJournal;
import com.bigdata.rawstore.Bytes;

/**
 * Persistent object wrapping a persistent {@link BTree} together with the
 * {@link Coercer}, {@link Successor}, and {@link Comparator} objects for use
 * with that btree.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
final public class MyBTree extends AbstractBTree {

    private static final long serialVersionUID = -4148994330379865469L;
    
    transient private BTree btree;
    
    /**
     * Deserialization constructor.
     */
    public MyBTree() {

        super();
        
    }

    public MyBTree(BaseObject container, BTree btree, Coercer coercer,
            Successor successor, Comparator comparator) {

        super(container, btree.getMetadata().getMetadataAddr(),
                false/* storeOidOnly */, coercer, successor, comparator);

        this.btree = btree;

    }

    /**
     * Returns the persistent {@link BTree} object, which is then cached using a
     * hard reference.
     */
    synchronized public Object getNativeBTree() {

        if (btree != null)
            return btree;

        IJournal journal = ((PersistenceStore) getNativeObjectManager()
                .getPersistenceStore()).getJournal();

        // Load btree from store.
        btree = BTree.load(journal, _nativeBTreeId);

        if (btree == null) {

            throw new RuntimeException("No such BTree: recid=" + _nativeBTreeId);

        }

        return btree;

    }

    /**
     * Type safe version of {@link #getNativeBTree()}.
     * 
     * @return The btree.
     */
    public BTree getBTree() {
        
        return (BTree) getNativeBTree();
        
    }
    
    /**
     * Note: when using indices that support transactional isolation the size is
     * an upper bound since deleted entries continue to exist in the index until
     * they are purged.
     */
    public long size() {

        return getBTree().getEntryCount();

    }

    public long find(final Object key) {

        if( key == null ) {

            throw new IllegalArgumentException();
            
        }
        
        final Long recid = (Long) getBTree().lookup(key);

        if (recid == null) {

            /*
             * The key was not found.
             */
            
            return 0L;

        }

        return recid.longValue();
        
    }

    public void insert(final Object internalKey, final long oid) {

        if( internalKey == null ) {

            throw new IllegalArgumentException();
            
        }
        
        if( oid == 0L ) {
            
            throw new IllegalArgumentException();
            
        }
        
        /*
         * The index entry value is always the object identifier of the indexed
         * generic object.
         */
        final Object val = new Long(oid);
        
        final Long oldValue = (Long) getBTree().insert(internalKey, val);

        if (oldValue != null) {

            // This is always an error.
            //
            // If duplicate keys are permitted, then those keys MUST
            // be made distinct creating a composite key from the
            // coerced key and the identity of the indexed object.
            //
            // If duplicate keys are not permitted, we just checked
            // for an existing entry for this key and got back a "no",
            // which implies that either the check or the insert
            // operation is broken.

            throw new IllegalStateException(
                    "Index already contains this entry" + ": index=" + this
                            + ", key='" + internalKey + "'" + ", oid=" + oid);

        }

    }

    public long remove(final Object internalKey) {

        // remove, returning the old value under that key.
        final Long oid = (Long) getBTree().remove(internalKey);

        if(oid ==null) {

            // nothing found under that key.
            
            throw new IllegalStateException();
            
        }
        
        return oid.longValue();
        
    }

    public Object lastKey(final LinkSetIndex ndx) throws NoSuchElementException {

        throw new UnsupportedOperationException();
        
        /*
        final TupleBrowser browser = getBTree().browse(null);

        final Tuple tuple = new Tuple();

        if (browser.getPrevious(tuple)) {

//                // This is the internal form of the key.
//
//                final Object lastKey = tuple.getKey();
//
//                // Return the external form for that internal key.
//
//                return ndx.getExternalKey(lastKey);

            // The object identifier of the last index entry.
            
            final long oid = ((Long) tuple.getValue()).longValue();
            
            Generic g = (Generic) ndx.getNativeObjectManager().fetch(oid);

            Object externalKey = g.get( ndx.getValuePropertyClass() );

            return externalKey;

        }

        throw new NoSuchElementException();
        */

    }

    public void remove() {

        BTree tmp = (BTree) getNativeBTree();
        
        tmp.removeAll();
        
        btree = null; // clear reference.
        
        _nativeBTreeId = 0L; // clear btree object identifier.

        super.remove();

    }

    public ILinkSetIndexIterator iterator(LinkSetIndex ndx, Object fromKey, Object toKey,
            boolean resolve) {

        return new LinkSetIndexIterator(ndx, fromKey, toKey, resolve);

    }

    /**
     * Forms the composite key as an unsigned byte[]. The coercedKey appears
     * first in the composite key. The object identifier is converted to an
     * unsigned byte[] and appended in order to break ties based on solely the
     * coercedKey.
     * 
     * @todo In the current implementation object identifiers are always 64-bit
     *       integers. Therefore you can extract the object identifier by
     *       applying {@link KeyBuilder#decodeLong(byte[], int)} to the last 8
     *       bytes in the composite key. However, in the general case when
     *       object identifiers may be arbitrary byte[]s allocated within some
     *       named index there is no easy way to extract the object identifier
     *       from the resulting composite key. In practice this should not be a
     *       problem since the value stored under that key in the link set index
     *       is the object identifier!
     */
    public Object newCompositeKey(Object coercedKey, long oid) {

        final byte[] in = (byte[]) coercedKey;
        
        final byte[] out = new byte[in.length /*+ 1*/ + Bytes.SIZEOF_LONG];

        // copy the coerced key
        System.arraycopy(in, 0, out, 0, in.length);
        
//        // nul delimiter.
//        out[out.length] = '\0';
        
        // append the object identifier.

        final byte[] oidbytes = BTree.unbox(oid);
        
        System.arraycopy(oidbytes, 0, out, in.length/*+1*/, oidbytes.length);
        
        return out;
        
    }

    /**
     * Serializers are specific to a class and are not automatically inherited
     * by subclasses therefore this serializer extends the {@link AbstractBTree}
     * serializer so as to inherit the serialization behavior of the
     * {@link AbstractBTree}.
     * 
     * @author thompsonbry
     * @version $Id$
     */
    public static class Serializer0 extends AbstractBTree.Serializer0 {

        private static final long serialVersionUID = -1303147677877289315L;

    }

}
