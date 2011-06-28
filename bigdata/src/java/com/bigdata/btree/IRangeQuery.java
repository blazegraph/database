/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 14, 2007
 */

package com.bigdata.btree;

import java.util.Iterator;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IndexSegment.IndexSegmentTupleCursor;
import com.bigdata.btree.filter.ITupleFilter;
import com.bigdata.btree.filter.TupleRemover;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.ndx.IClientIndex;

import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Interface for range count and range query operations.
 * <p>
 * Note: There are implementations of this interface for both local indices and
 * for distributed scale-out indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRangeQuery {

    /**
     * Return the #of tuples in the index.
     * <p>
     * Note: If the index supports deletion markers then the range count will be
     * an upper bound and may double count tuples which have been overwritten,
     * including the special case where the overwrite is a delete.
     * 
     * @return The #of tuples in the index.
     */
    public long rangeCount();
    
    /**
     * Return the #of tuples in a half-open key range. The fromKey and toKey
     * need not exist in the B+Tree.
     * <p>
     * Note: If the index supports deletion markers then the range count will be
     * an upper bound and may double count tuples which have been overwritten,
     * including the special case where the overwrite is a delete.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return The #of tuples in the half-open key range.
     * 
     * @todo ??? throw exception if LT but allow GTE? This will be zero if
     *       <i>toKey</i> is less than or equal to <i>fromKey</i> in the total
     *       ordering.
     */
    public long rangeCount(byte[] fromKey, byte[] toKey);

    /**
     * Return the exact #of tuples in a half-open key range. The fromKey and
     * toKey need not exist in the B+Tree.
     * <p>
     * Note: If the index supports deletion markers then this operation will
     * require a key-range scan.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return The exact #of tuples in the half-open key range.
     */
    public long rangeCountExact(byte[] fromKey, byte[] toKey);

    /**
     * Return the exact #of tuples in a half-open key range, including any
     * deleted tuples. The fromKey and toKey need not exist in the B+Tree.
     * <p>
     * When the view is just an {@link AbstractBTree} the result is the same as
     * for {@link IRangeQuery#rangeCount(byte[], byte[])}, which already
     * reports all tuples regardless of whether or not they are deleted.
     * <p>
     * When the index is a view with multiple sources, this operation requires a
     * key-range scan where both deleted and undeleted tuples are visited.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return The exact #of deleted and undeleted tuples in the half-open key
     *         range.
     * 
     * @see IRangeQuery#rangeCountExact(byte[], byte[])
     */
    public long rangeCountExactWithDeleted(final byte[] fromKey,
            final byte[] toKey);
    
    /**
     * Flag specifies no data (the #of scanned index entries matching the optional
     * filter will still be reported).
     */
    public static final int NONE = 0;
    
    /**
     * Flag specifies that keys in the key range will be returned. The keys are
     * guaranteed to be made available via {@link ITupleIterator#getKey()} only
     * when this flag is given.
     */
    public static final int KEYS = 1 << 0;

    /**
     * Flag specifies that values in the key range will be returned. The values
     * are guaranteed to be made available via {@link ITupleIterator#next()} and
     * {@link ITupleIterator#getValue()} only when this flag is given.
     */
    public static final int VALS = 1 << 1;

    /**
     * Flag specifies that deleted index entries for a key are visited by the
     * iterator (by default the iterator will hide deleted index entries).
     */
    public static final int DELETED = 1 << 2;

    /**
     * The flags that should be used by default [{@link #KEYS}, {@link #VALS}]
     * in contexts where the flags are not explicitly specified by the
     * application such as {@link #rangeIterator(byte[], byte[])}.
     */
    public static final int DEFAULT = KEYS | VALS;

    /**
     * Shorthand for [{@link #KEYS}, {@link #VALS}, {@link #DELETED}].
     */
    public static final int ALL = (KEYS | VALS | DELETED);
    
    /**
     * Flag specifies that the iterator, including any {@link ITupleFilter}s,
     * will not write on the index. Various optimizations may be applied when
     * this flag is present. (Read only can be inferred if {@link #CURSOR} flag
     * is NOT specified AND there are NO {@link ITupleFilter}s).
     */
    public static final int READONLY = 1 << 3;

    /**
     * Flag specifies that entries visited by the iterator in the key range will
     * be <em>removed</em> from the index. This flag may be combined with
     * {@link #KEYS} or {@link #VALS} in order to return the keys and/or values
     * for the deleted entries. When a {@link IFilter} is specified, the filter
     * stack will be applied first and then {@link #REMOVEALL} will cause a
     * {@link TupleRemover} to be layered on top. You can achieve other stacked
     * iterator semantics using {@link IFilter}, including causing
     * {@link ITuple}s to be removed at a different layer in the stack. Note
     * however, that removal for a local {@link BTree} will require that the
     * {@link TupleRemover} is stacked directly over an {@link ITupleCursor}.
     * <p>
     * Note: This semantics of this flag require that the entries are atomically
     * removed within the isolation level of the operation. In particular, if
     * the iterator is running against an {@link IDataService} using an
     * unisolated view then the entries MUST be buffered and removed as the
     * {@link ResultSet} is populated.
     * <p>
     * Note: The {@link BigdataFileSystem#deleteHead(String, int)} relies on
     * this atomic guarantee.
     * 
     * @todo define rangeRemove(fromKey,toKey,filter)? This method would return
     *       the #of items matching the optional filter that were deleted. It
     *       will be a parallelizable operation since it does not specify a
     *       limit on the #of items to be removed and does not return any data
     *       or metadata (other than the count) for the deleted items.
     *       <p>
     *       Note: We still need {@link #REMOVEALL} since it provides an atomic
     *       remove with return of an optionally limited #of matching index
     *       entries. This makes it ideal for creating certain kinds of queue
     *       constructions.
     */
    public static final int REMOVEALL = 1 << 4;
    
    /**
     * Flag specifies that the base iterator will support the full
     * {@link ITupleCursor} API, including traversal with concurrent
     * modification, bi-directional tuple navigation and random seeks within the
     * key range. There are several pragmatic reasons why you would or would not
     * specify this flag.
     * <ol>
     * <li>The original {@link Striterator} construction for the {@link BTree}
     * is <em>faster</em> than the newer {@link AbstractBTreeTupleCursor}. It
     * is used by default when this flag is NOT specified and the iterator is
     * running across a {@link BTree}. (The {@link IndexSegmentTupleCursor} is
     * used for {@link IndexSegment}s regardless of the value of this flag
     * since it exploits the double-linked leaves of the {@link IndexSegment}
     * and is therefore MORE efficient than the {@link Striterator} based
     * construct.)</li>
     * <li> This flag enables traversal with concurrent modification (i.e.,
     * {@link Iterator#remove()}) when used with a local {@link BTree}.
     * Scale-out iterators always support traversal with concurrent modification
     * since they heavily buffer the iterator with {@link ResultSet}s.</li>
     * </ol>
     */
    public static final int CURSOR = 1 << 5;

    /**
     * Flag specifies that the entries will be visited using a reverse scan. The
     * first tuple to be visited will be the tuple having the largest key
     * strictly less than the optional upper bound for the key range. The
     * iterator will then visit the previous tuple(s) until it has visited the
     * tuple having the smallest key greater than or equal to the optional lower
     * bound for the key range.
     * <p>
     * This flag may be used to realize a number of interesting constructions,
     * including atomic operations on the tail of a queue and obtaining the last
     * key in the key range.
     */
    public static final int REVERSE = 1 << 6;
    
    /**
     * There are two ways in which the successor of an unsigned byte[] key may
     * be computed. The default is to append an unsigned zero byte to the key.
     * This forms the next possible key for the natural unsigned byte[] sort
     * order.
     * <p>
     * The other choice is to treat the unsigned byte[] as a fixed length bit
     * string and to add ONE (1) with rollover. This works in some special
     * circumstances, primarily because you are actually seeking to skip over
     * all keys that have a given key as their prefix and continue the iterator
     * at the next possible prefix having the same length.
     * <p>
     * Note: This option is applied by {@link AbstractChunkedTupleIterator},
     * which is responsible for issuing continuation queries.
     */
    public static final int FIXED_LENGTH_SUCCESSOR = 1 << 7;

    /**
     * Flag indicates that the iterator may process multiple index partitions in
     * parallel. While tuples are visited in each index partition in the natural
     * ordering, this flag breaks the total ordering guarantee of the iterator
     * if multiple index partitions are visited as tuples from each index
     * partition will be visited concurrently and appear in an interleaved
     * ordering. To maintain efficiency, the tuples are interleaved in chunks so
     * processing which benefits from order effects can exploit the within chunk
     * ordering of the tuples. The semantics of this flag are realized by the
     * {@link IClientIndex} implementation. This flag has no effect on an
     * {@link ILocalBTreeView} and is not passed through to the
     * {@link IDataService}.
     * 
     * @todo This flag is not supported in combination with {@link #CURSOR}?
     * @todo This flag is not supported in combination with {@link #REVERSE}?
     */
    public static final int PARALLEL = 1 << 8;
    
    /**
     * Visits all tuples in key order. This is identical to
     * 
     * <pre>
     * rangeIterator(null, null)
     * </pre>
     * 
     * @return An iterator that will visit all entries in key order.
     */
    public ITupleIterator rangeIterator();
    
    /**
     * Return an iterator that visits the entries in a half-open key range. When
     * <i>toKey</i> <em>EQ</em> <i>fromKey</i> nothing will be visited. It
     * is an error if <i>toKey</i> <em>LT</em> <i>fromKey</i>.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive lower bound).
     *            When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive upper
     *            bound). When <code>null</code> there is no upper bound.
     * 
     * @throws RuntimeException
     *             if <i>fromKey</i> is non-<code>null</code> and orders LT
     *             the inclusive lower bound for an index partition.
     * 
     * @throws RuntimeException
     *             if <i>toKey</i> is non-<code>null</code> and orders GTE
     *             the exclusive upper bound for an index partition.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see EntryFilter, which may be used to filter the entries visited by the
     *      iterator.
     * 
     * @todo define behavior when the toKey is less than the fromKey.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey);

    /**
     * Designated variant (the one that gets overridden) for an iterator that
     * visits the entries in a half-open key range. When <i>toKey</i>
     * <em>EQ</em> <i>fromKey</i> nothing will be visited. It is an error if
     * <i>toKey</i> <em>LT</em> <i>fromKey</i>.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive lower bound).
     *            When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive upper
     *            bound). When <code>null</code> there is no upper bound.
     * @param capacity
     *            The #of entries to buffer at a time. This is a hint and MAY be
     *            zero (0) to use an implementation specific <i>default</i>
     *            capacity. A non-zero value may be used if you know that you
     *            want at most N results or if you want to override the default
     *            #of results to be buffered before sending them across a
     *            network interface. (Note that you can control the default
     *            value using
     *            {@link IBigdataClient.Options#DEFAULT_CLIENT_RANGE_QUERY_CAPACITY}).
     * @param flags
     *            A bitwise OR of {@link #KEYS}, {@link #VALS}, etc.
     * @param filterCtor
     *            An optional object used to construct a stacked iterator. When
     *            {@link #CURSOR} is specified in <i>flags</i>, the base
     *            iterator will implement {@link ITupleCursor} and the first
     *            filter in the stack can safely cast the source iterator to an
     *            {@link ITupleCursor}. If the outermost filter in the stack
     *            does not implement {@link ITupleIterator}, then it will be
     *            wrapped an {@link ITupleIterator}.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see IFilterConstructor, which may be used to construct an iterator stack
     *      performing filtering or other operations.
     */
    public ITupleIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IFilter filterCtor);

//    /**
//     * An iterator that is mapped over a set of key ranges.
//     * 
//     * @param fromKeys
//     *            An array of inclusive lower bounds with one entry for each key
//     *            range over which the iterator will be mapped. The elements of
//     *            the array MUST be sorted. The first element MAY be a
//     *            <code>null</code> to indicate that there is no lower bound
//     *            for the first key range.
//     * @param toKeys
//     *            An array of exclusive upper bounds with one entry for each key
//     *            range over which the iterator will be mapped. Each toKey MUST
//     *            be GTE the corresponding fromKey. The last element MAY be a
//     *            <code>null</code> to indicate that there is no exclusive
//     *            upper bound bound for the last key range.
//     * @param capacity
//     *            The #of entries to buffer at a time. This is a hint and MAY be
//     *            zero (0) to use an implementation specific <i>default</i>
//     *            capacity. The capacity is intended to limit the burden on the
//     *            heap imposed by the iterator if it needs to buffer data, e.g.,
//     *            before sending it across a network interface.
//     * @param flags
//     *            A bitwise OR of {@link #KEYS}, {@link #VALS}, etc.
//     */
//    public ITupleIterator rangeIterator(byte[][] fromKeys, byte[][] toKeys,
//            int capacity, int flags);
    
    // removeAll() could be added, but the problem is that we often want
    // the keys or values of the deleted entries, at which point you have to
    // use the rangeIterator anyway.
    
//    /**
//     * Removes all entries in the key range from the index. When running on a
//     * scale-out index, this operation is atomic for each index partition. The
//     * operation may be used to build queue-like constructed by atomic delete of
//     * the first item in a key range. This operation is parallelized across
//     * index partitions when no limit is specified and serialized across index
//     * partitions when a limit is specified.
//     * 
//     * @param fromKey
//     *            The first key that will be visited (inclusive). When
//     *            <code>null</code> there is no lower bound.
//     * @param toKey
//     *            The first key that will NOT be visited (exclusive). When
//     *            <code>null</code> there is no upper bound.
//     * @param limit
//     *            When non-zero, this is the maximum #of entries that will be
//     *            removed. When zero (0L) all index entries in the key range
//     *            will be removed.
//     * 
//     * @return The #of index entries that were removed.
//     */
//    public long removeAll(byte[] fromKey,byte[] toKey, long limit);

}
