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

import java.util.Arrays;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.CognitiveWeb.generic.core.AbstractBTree;
import org.CognitiveWeb.generic.core.Generic;
import org.CognitiveWeb.generic.core.ILinkSetIndexIterator;
import org.CognitiveWeb.generic.core.LinkSetIndex;
import org.CognitiveWeb.generic.core.ndx.Coercer;
import org.CognitiveWeb.generic.core.ndx.Successor;
import org.CognitiveWeb.generic.core.om.BaseObject;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.IJournal;
import com.bigdata.rawstore.Bytes;

/**
 * Persistent object wrapping a persistent {@link BTree} together with the
 * {@link Coercer}, {@link Successor}, and {@link Comparator} objects for use
 * with that btree.
 * <h2>Implementation notes</h2>
 * <p>
 * The current approach uses an unnamed {@link BTree} and explicitly manages the
 * BTree on the journal. In particular, methods that change the state of the
 * BTree cause this object to be {@link #update() marked as dirty} so that we
 * will flush the {@link BTree} to the journal in {@link #saveBTree()} and have
 * the current {@link BTreeMetadata} record on hand when this object is
 * serialized. This works fine until we start using partitioned indices in the
 * journal - the problem in that case is that the low-level integration will not
 * be able to keep track of the partitioned indices.
 * </p>
 * <p>
 * An alternative approach is to use named scale-out indices for the link set
 * indices. A scale-out index is created for each link set index family (the
 * association property plus a value property). The keys for the scale-out index
 * are formed as:
 * </p>
 * 
 * <pre>
 *            [linkSetOid][attribute]([oid])?
 * </pre>
 * 
 * <p>
 * The [linkSetOid] is used as the prefix for the keys stored in the scale-out
 * index. In this manner the scale-out index is automatically broken down into
 * key ranges for each distinct link set.
 * </p>
 * <p>
 * The scale-out indices would be named by the concatenation of the association
 * class and value property class for the link set family. Either the object
 * identifiers for those property classes or the names of the property classes
 * may be used for this purpose. E.g., "___employee___name" might be the name of
 * the scale-out index for the association class "employee" and the value
 * property "name".
 * </p>
 * <p>
 * When using scale-out indices in this manner the add, rebuild, and drop index
 * operations will need to be modified such that they operation only on the
 * entries in the scale-out index which correspond to the link set for which the
 * operation was requested.
 * </p>
 * <p>
 * Note: the reason not to use a distinct named scale-out indices for each link
 * set index is that the total number of resulting link set indices could be
 * quite large. This would not be a problem for something like rdf-generic but
 * it could be a problem if there were 10k+ link set indices on a single journal
 * all of which were quite small. If those link set indices correspond to named
 * scale-out indices then each one needs to be examine when we overflow the
 * journal. It might be fine...but it might be a problem. Also, the named
 * indices would (of course) need to be placed within their own name space to
 * avoid, which is not a problem.
 * </p>
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

        // Note: implicit conversion to byte[].
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
        
        // Note: implicit conversion of the key to byte[].
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
                    "Index already contains this entry"
                    + ": index=" + this + ", key='"
                    + Arrays.toString((byte[]) internalKey) + "'" + ", oid="
                    + oid);

        }
        
        /*
         * Note: We have to mark this object as dirty since the btree will need
         * to be flushed to disk so that we can write out the new address of the
         * btree record when this object is serialized.
         */

        update();

    }

    public long remove(final Object internalKey) {

        /*
         * Remove, returning the old value under that key.
         * 
         * Note: implicit conversion of the key to byte[].
         */
        final Long oid = (Long) getBTree().remove(internalKey);

        if (oid == null) {

            // nothing found under that key.
            
            throw new IllegalStateException("key="+Arrays.toString((byte[])internalKey));
            
        }
        
        /*
         * Note: We have to mark this object as dirty since the btree will need
         * to be flushed to disk so that we can write out the new address of the
         * btree record when this object is serialized.
         */

        update();

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

        if (false) {
        
            /*
             * Note: We do NOT need to actually delete the entries in the BTree
             * since it will no longer be accessible and the journal is a write
             * only store. However, if we switch this class over to use a named
             * scale-out index then the appropriate key range of that index MUST
             * be cleared here!
             */
            
            BTree tmp = (BTree) getNativeBTree();

            tmp.removeAll();
            
        }
        
        btree = null; // clear reference.
        
        _nativeBTreeId = 0L; // clear btree object identifier.

        super.remove();

    }

    public ILinkSetIndexIterator iterator(LinkSetIndex ndx, Object fromKey, Object toKey,
            boolean resolve) {

        return new LinkSetIndexIterator(ndx, fromKey, toKey, resolve);

    }

    /**
     * Note: this is overriden completely since we need to handle the successor
     * of a String value in a special manner using
     * {@link IKeyBuilder#appendText(String, boolean, boolean)}.
     */
    protected Object getInternalKey(//
            final boolean indexEverything,//
            final boolean duplicateKeys,//
            final boolean coerce,//
            final boolean successor,//
            /*final*/ Object key,//
            final Generic what//
            ) {
        
        if (successor && _successor != null) {

            /*
             * Note: take the successor before we do anything else (but not for
             * keys that coerce to String which have special successor semantics
             * are are handled below).
             */

            key = _successor.successor(key);
            
        }

        final Object coercedKey = ( coerce && _coercer != null ? _coercer.coerce(key) : key );

        if (coercedKey == null && !indexEverything) {

            /*
             * Note: Unless we are indexing everything we will drop out keys
             * that coerce to [null].
             */

            return null;

        }

        Object internalKey = coercedKey;

        if (duplicateKeys) {

            /*
             * Note: If we are permitting duplicate external keys, then we
             * always wrap the coerced key as a composite key. This let's us
             * impose a total ordering over the index even when the external
             * (and coerced) keys are not unique.
             */

//            internalKey = newCompositeKey(coercedKey, (what == null ? 0L : what
//                    .getOID()));

            final long oid = (what == null ? 0L : what.getOID());
            
            if(coercedKey instanceof String) {

                synchronized(keyBuilder) {
                    
                    final String text = (String) coercedKey;
                    
                    keyBuilder.reset();
                    
                    // Note: handles successor of a String.
                    keyBuilder.appendText(text, true/*unicode*/, successor );
                    
                    keyBuilder.append(oid);
                    
                    final byte[] out = keyBuilder.getKey();
                    
                    return out;
                    
                }
                
            } else {
            
                final byte[] in = KeyBuilder.asSortKey(coercedKey);
                
                final byte[] out = new byte[in.length /* + 1 */+ Bytes.SIZEOF_LONG];

                // copy the coerced key
                System.arraycopy(in, 0, out, 0, in.length);

                // // nul delimiter.
                // out[out.length] = '\0';

                // append the object identifier.

                final byte[] oidbytes = KeyBuilder.asSortKey(oid);

                System.arraycopy(oidbytes, 0, out, in.length/* +1 */,
                        oidbytes.length);

                return out;

            }

        } else if(successor && _successor == null) {
            
            /*
             * Normally a string key is transparently converted to a byte[] by
             * the index access methods on the bigdata BTree object. However in
             * the case where we need to compute the successor of the key we
             * explicitly convert the String to a byte[] and form the successor
             * by appending a nul byte.
             */
            
            // convert to byte[] using collator.
            byte[] out = KeyBuilder.asSortKey((String)coercedKey);
            
            out = BytesUtil.successor(out);
            
            internalKey = out;
            
        }

        return internalKey;
        
    }

    /**
     * Forms the composite key as an unsigned byte[]. The coercedKey appears
     * first in the composite key. The object identifier is converted to an
     * unsigned byte[] and appended in order to break ties based on solely the
     * coercedKey.  Keys that are coerced to Strings are handled specially 
     * using {@link IKeyBuilder#appendText(String, boolean, boolean)}.
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

        return _newCompositeKey(KeyBuilder.asSortKey(coercedKey), oid);
        
    }
    
    // @todo should be a configurable instance, probably on the PersistenceStore or the OM.
    private static final IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance(); 

    public static byte[] _newCompositeKey(Object coercedKey,long oid) {

        if (coercedKey == null) {

            throw new IllegalArgumentException();
            
        }

        if(coercedKey instanceof String) {

            synchronized(keyBuilder) {
                
                final String text = (String) coercedKey;
                
                keyBuilder.reset();
                
                keyBuilder.appendText(text, true/*unicode*/, false/*successor*/);
                
                keyBuilder.append(oid);
                
                final byte[] out = keyBuilder.getKey();
                
                return out;
                
            }
            
        } else {
        
            final byte[] in = KeyBuilder.asSortKey(coercedKey);
            
            final byte[] out = new byte[in.length /* + 1 */+ Bytes.SIZEOF_LONG];

            // copy the coerced key
            System.arraycopy(in, 0, out, 0, in.length);

            // // nul delimiter.
            // out[out.length] = '\0';

            // append the object identifier.

            final byte[] oidbytes = KeyBuilder.asSortKey(oid);

            System.arraycopy(oidbytes, 0, out, in.length/* +1 */,
                    oidbytes.length);

            return out;

        }

    }

    /**
     * Note: unnamed bigdata {@link BTree} objects needs to be explicitly
     * written on the store since they will not not otherwise participate in the
     * commit protocol. This method invokes {@link BTree#handleCommit()} to
     * flush the {@link BTree} state to the store and obtain the address of the
     * new metadata record from which the btree's state may be reloaded.
     */
    protected void saveBTree() {

        if (btree != null) {

            final long oldAddress = _nativeBTreeId;
            
            _nativeBTreeId = btree.handleCommit();
            
            log.info("Saving BTree: " + this + ", old address="
                    + oldAddress + ", new address=" + _nativeBTreeId + "\n"
                    + btree.getStatistics());

        }
        
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
