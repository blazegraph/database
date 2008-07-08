/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on May 28, 2008
 */

package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.Journal;
import com.bigdata.service.IMetadataService;

/**
 * An interface that provides for the (de)-serialization of the value of a tuple
 * stored in an index and, when possible, the key under which that value is
 * stored.
 * <p>
 * The encoded key is always a variable length unsigned byte[]s. The purpose of
 * the encoded key is to determine the total order over the tuple in the B+Tree.
 * While some key encodings are reversable without less (e.g., int, long, float
 * or double), many key encodings are simply not decodable (including Unicode
 * keys). Applications that require the ability to decode complex keys may need
 * to resort to storing the unencoded key state redundently in the tuple value.
 * 
 * FIXME extSer integration. There are two broad setups. (1) Local: when running
 * without an {@link IMetadataService} and hence on a single
 * {@link AbstractJournal}, the serializer state should be stored in a record
 * whose address is available from the {@link CommitRecordIndex} of the
 * {@link Journal}. (2) Distributed: when running a distributed federation the
 * serializer state should be stored globally so that the serialized values can
 * be consistently interpreted as they are moved from {@link BTree} to
 * {@link IndexSegment} and from {@link IndexSegment} to {@link IndexSegment}.
 * <p>
 * I am inclined to store the serializer state for the distributed federation on
 * a per-scale-out index basis. This means that you must de-serialize the keys
 * and values in order to copy tuples from one scale-out index to another.
 * However, when copying tuples from one federation to another de-serialization
 * and re-serialization will be required regardless since the serializer state
 * will not be shared and, in fact, could be different for the two indices (this
 * is also true within a single federation or even within a scale-up instance).
 * (There should probably be a utility class to do index copies that handles
 * these various cases).
 * <p>
 * The critical point is being able to recover the reference to the (potentially
 * remote) extSer object from within both the {@link AbstractBTree} and the
 * various {@link ITupleIterator} implementations, including the
 * {@link AbstractChunkedRangeIterator}. Further, the recovered object must
 * cache the extSer state, must have a resonable life cycle so that it is
 * efficient, and must be linked into the commit protocol such that assigned
 * class identifiers are always made persistent if a commit could have included
 * data written using those class identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param K
 *            The generic type of the application key.
 * @param V
 *            The generic type of the application value.
 */
public interface ITupleSerializer<K extends Object, V extends Object> extends
        IKeyBuilderFactory, Serializable {

//    /**
//     * This method is invoked to notify the implementation of the backing store.
//     * This provides an opportunity for a reuse of thread-local buffers or the
//     * use of additional persistent state in a serialization protocol.
//     * <p>
//     * Note: There are three basic cases which can arise here depending on the
//     * {@link IRawStore} implementation.
//     * <dl>
//     * <dt>{@link ManagedJournal}</dt>
//     * <dd>This corresponds to a scale-out configuration. You can access the
//     * {@link IMetadataService} using
//     * {@link ManagedJournal#getMetadataService()}. </dd>
//     * <dt>{@link Journal}</dt>
//     * <dd>This corresponds to an scale-up configuration.</dd>
//     * <dt>{@link TemporaryRawStore}</dt>
//     * <dd>A temporary store.</dd>
//     * </dl>
//     * <p>
//     * Note: {@link ITupleSerializer}s are stored in the {@link IndexMetadata}.
//     * An instance therefore has a life-cycle corresponding more or less to that
//     * of a {@link BTree} or {@link FusedView}. It is often convenient to
//     * access the thread-local {@link KeyBuilder} or {@link DataOutputBuffer}
//     * associated with the live {@link AbstractJournal}.
//     * <p>
//     * Note: There are also a several simple {@link IRawStore} implementations.
//     * While the {@link BTree} can be used with any of them, they are mostly
//     * used in unit tests. E.g., {@link SimpleMemoryRawStore}.
//     * 
//     * @param store
//     *            The backing store.
//     * 
//     * @throws IllegalArgumentException
//     *             if <i>store</i> is <code>null</code>.
//     * @throws IllegalStateException
//     *             if the store has already been set on this object.
//     */
//    void setStore(IRawStore store);

//    /**
//     * Automatically invoked to set the {@link IKeyBuilder} configured for the
//     * {@link IIndex} when an {@link ITupleSerializer} is de-serialized from an
//     * {@link IndexMetadata} object.
//     * 
//     * @todo if defined, then I also need to do this when de-serializing the
//     *       {@link ITupleSerializer} for a {@link ResultSet}.
//     * 
//     * @todo an alternative is to define getKeyBuilderFactory() and a protocol
//     *       to make sure that the factory is set on the
//     *       {@link ITupleSerializer} if it is set on the owning
//     *       {@link IndexMetadata}. Then the factory will stick with the
//     *       {@link ITupleSerializer} without further troubles.
//     * 
//     * FIXME Best yet, have the factory on the {@link ITupleSerializer} directly
//     * and have the {@link IndexMetadata} just read it off of the
//     * {@link ITupleSerializer}! (Modify SPOTupleSerializer and DefaultTupleSerializer).
//     */
//    IKeyBuilderFactory getKeyBuilder();
//    void setKeyBuilderFactory(IKeyBuilderFactory keyBuilderFactory);

    /**
     * Factory for thread-safe {@link IKeyBuilder} objects for use by
     * {@link ITupleSerializer#serializeKey(Object)} and possibly others.
     * <p>
     * Note: A mutable B+Tree is always single-threaded. However, read-only
     * B+Trees allow concurrent readers. Therefore, thread-safety requirement
     * for this {@link IKeyBuilderFactory} is
     * <em>safe for either a single writers -or-
     * for concurrent readers</em>.
     */
    public IKeyBuilder getKeyBuilder();
    
    /**
     * Serialize a facet of an object's state that places the object into the
     * total sort order for the index. This method is automatically applied by
     * {@link ILocalBTree#insert(Object, Object)} and friends to convert the
     * <strong>key</strong> object into an <strong>unsigned</strong> variable
     * length byte[].
     * <p>
     * Note: This handles the conversion between an object and the
     * <strong>unsigned</strong> variable length byte[] representation of that
     * object which determines its place within the total index order. Since
     * this transform imposes the total order of the index, different techniques
     * are applied here than are applied to the serialization of the index
     * values.
     * 
     * @param obj
     *            A object (MAY NOT be <code>null</code>).
     * 
     * @return An <strong>unsigned</strong> byte[] which places the object into
     *         the total sort order for the index and never <code>null</code> (
     *         <code>null</code> keys are not allowed into an index).
     * 
     * @throws IllegalArgumentException
     *             if <i>obj</i> is <code>null</code>.
     */
    byte[] serializeKey(Object obj);

    /**
     * Serialize the persistent state of the object (the value stored in the
     * index under the key for that object). This method is automatically
     * applied by {@link ILocalBTree#insert(Object, Object)} and friends to
     * convert the <strong>value</strong> object into an byte[].
     * 
     * @param obj
     *            An object (MAY NOT be <code>null</code>).
     * 
     * @return A byte[] containing the serialized state of the object -or-
     *         <code>null</code> if no value will be stored under the
     *         serialized key.
     * 
     * @throws IllegalArgumentException
     *             if <i>obj</i> is <code>null</code>.
     */
    byte[] serializeVal(V obj);

    /**
     * De-serialize an object from an {@link ITuple}. This method is
     * automatically applied by methods on the {@link ILocalBTree} interface
     * that return the object stored under a key and by
     * {@link ITuple#getObject()}.
     * 
     * @param tuple
     *            The tuple.
     * 
     * @return The de-serialized object.
     * 
     * @throws IllegalArgumentException
     *             if <i>tuple</i> is <code>null</code>.
     */
    V deserialize(ITuple tuple);

    /**
     * De-serialize the application key from an {@link ITuple} (optional
     * operation).
     * <p>
     * Note: There is no general means to recover the application key from the
     * B+Tree. However, there are two approaches, either of which may work for
     * your data.
     * <ol>
     * <li>If the application key can be recovered from the application value
     * then this method can delegate to {@link #deserialize(ITuple)} and return
     * the appropriate field or synthetic property value. </li>
     * <li>Some kinds of application keys can be directly de-serialized using
     * {@link KeyBuilder}. For example, int, long, float, double, etc. However
     * this is NOT possible for Unicode {@link String}s.</li>
     * </ol>
     * <p>
     * Note: The B+Tree does NOT rely on this method. It is used to support the
     * materialization of the key required to allow {@link BigdataMap} and
     * {@link BigdataSet} to fullfill their respective APIs.
     * 
     * @throws UnsupportedOperationException
     *             if this operation is not implemented.
     */
    K deserializeKey(ITuple tuple);
    
}
