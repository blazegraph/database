/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.btree;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.btree.AbstractBTree.IBTreeCounters;
import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.Instrument;

/**
 * A helper class that collects statistics on an {@link AbstractBTree}.
 * <p>
 * Note: This class DOES NOT have a hard reference to the {@link AbstractBTree}.
 * Holding an instance of this class WILL NOT force the {@link AbstractBTree} to
 * remain strongly reachable.
 * <p>
 * Note: Counters for mutation are plain fields. Counters for read-only
 * operations are {@link AtomicLong} or {@link AtomicInteger} objects since read
 * operations may involve high concurrently and could otherwise lead to lost
 * counter updates.
 * 
 * @todo {@link #add(BTreeCounters)}, {@link #subtract(BTreeCounters)} and the
 *       copy ctor are not atomic. I have not seen any problems resulting from
 *       this.
 * 
 * @todo instrument the key search time
 * 
 * @todo instrument bloom filter time when the bloom filter is used (this should
 *       be done separately on the bloom filter object).
 * 
 * @todo (I should recheck these costs now that that refactor has been done). At
 *       present, deserialization is much slower than serialization, primarily
 *       because of object creation. Changing to a raw record format for nodes
 *       and leaves would likely reduce the deserialization costs significantly.
 *       The time could also be reduced by lazy decompression of the values and
 *       lazy deserialization of the values such that tuple scans for keys only
 *       do not require value deserialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
final public class BTreeCounters implements Cloneable, ICounterSetAccess {

    public BTreeCounters() {
        
    }

    /**
     * Equal iff they are the same instance.
     */
    @Override
    public boolean equals(final Object o) {
        
        return this == o;
        
    }
    
    /**
     * Copy constructor.
     * 
     * @param c
     */
    public BTreeCounters(final BTreeCounters c) {
        
        add(c);
        
    }
    
    @Override
    public BTreeCounters clone() {
        
        final BTreeCounters tmp = new BTreeCounters();
        
        tmp.add(this);
        
        return tmp;
        
    }
    
//    /**
//     * Returns a "mark". The mark is a copy of the counters taken at the moment
//     * requested. You can compute the delta between the last mark or any mark
//     * using {@link #subtract(BTreeCounters)}.
//     */
//    public BTreeCounters mark() {
//       
//        final BTreeCounters mark = new BTreeCounters();
//        
//        mark.add(this);
//        
//        return mark;
//        
//    }

    /**
     * Adds the values from another {@link BTreeCounters} object to this one.
     * 
     * @param o
     */
    public void add(final BTreeCounters o) {
        
        if (o == null)
            throw new IllegalArgumentException();
        
        // IKeySearch
//        nfinds.addAndGet(o.nfinds.get());
        ninserts.addAndGet(o.ninserts.get());
        nremoves.addAndGet(o.nremoves.get());
        // ILinearList
        nindexOf.add(o.nindexOf.get()); // Note: also does key search.
        ngetKey.add(o.ngetKey.get());
        ngetValue.add(o.ngetValue.get());
//        // IRangeQuery
        nrangeCount.add(o.nrangeCount.get());
        nrangeIterator.add(o.nrangeIterator.get());
        // Structural mutation.
        rootsSplit += o.rootsSplit;
        rootsJoined += o.rootsJoined;
        nodesSplit += o.nodesSplit;
        nodesJoined += o.nodesJoined;
        leavesSplit += o.leavesSplit;
        leavesJoined += o.leavesJoined;
        headSplit += o.headSplit;
        tailSplit += o.tailSplit;
        nodesCopyOnWrite += o.nodesCopyOnWrite;
        leavesCopyOnWrite += o.leavesCopyOnWrite;
        // tuple mutation.
        ntupleInsertValue += o.ntupleInsertValue;
        ntupleInsertDelete += o.ntupleInsertDelete;
        ntupleUpdateValue += o.ntupleUpdateValue;
        ntupleUpdateDelete += o.ntupleUpdateDelete;
        ntupleRemove += o.ntupleRemove;
        // IO reads
        cacheTests.add(o.cacheTests.get());
        cacheMisses.add(o.cacheMisses.get());
        nodesRead.add(o.nodesRead.get());
        leavesRead.add(o.leavesRead.get());
        bytesRead.add(o.bytesRead.get());
        readNanos.add(o.readNanos.get());
        deserializeNanos.add(o.deserializeNanos.get());
        rawRecordsRead.add(o.rawRecordsRead.get());
        rawRecordsBytesRead.add(o.rawRecordsBytesRead.get());
        // IO writes.
        nodesWritten.add(o.nodesWritten.get());
        leavesWritten.add(o.leavesWritten.get());
        bytesWritten.add(o.bytesWritten.get());
        bytesReleased.add(o.bytesReleased.get());
        writeNanos.add(o.writeNanos.get());
        serializeNanos.add(o.serializeNanos.get());
        rawRecordsWritten.add(o.rawRecordsWritten.get());
        rawRecordsBytesWritten.add(o.rawRecordsBytesWritten.get());
//        // touch()
//        syncTouchNanos.add(o.syncTouchNanos.get());
//        touchNanos.add(o.touchNanos.get());
        // write eviction queue
        
    }
    
    /**
     * Subtracts the given counters from the current counters, returning a new
     * counter object containing their difference.
     * 
     * @param o
     */
    public BTreeCounters subtract(final BTreeCounters o) {
        
        if (o == null)
            throw new IllegalArgumentException();
        
        // make a copy of the current counters.
        final BTreeCounters t = new BTreeCounters(this);
        
        /*
         * Subtract out the given counters.
         */
        
        // IKeySearch
//        t.nfinds.addAndGet(-o.nfinds.get());
        t.ninserts.addAndGet(-o.ninserts.get());
        t.nremoves.addAndGet(-o.nremoves.get());
        // ILinearList
        t.nindexOf.add(-o.nindexOf.get()); // Note: also does key search.
        t.ngetKey.add(-o.ngetKey.get());
        t.ngetValue.add(-o.ngetValue.get());
//        // IRangeQuery
        t.nrangeCount.add(-o.nrangeCount.get());
        t.nrangeIterator.add(-o.nrangeIterator.get());
        // Structural mutation.
        t.rootsSplit -= o.rootsSplit;
        t.rootsJoined -= o.rootsJoined;
        t.nodesSplit -= o.nodesSplit;
        t.nodesJoined -= o.nodesJoined;
        t.leavesSplit -= o.leavesSplit;
        t.leavesJoined -= o.leavesJoined;
        t.headSplit -= o.headSplit;
        t.tailSplit -= o.tailSplit;
        t.nodesCopyOnWrite -= o.nodesCopyOnWrite;
        t.leavesCopyOnWrite -= o.leavesCopyOnWrite;
        // tuple mutation.
        t.ntupleInsertValue -= o.ntupleInsertValue;
        t.ntupleInsertDelete -= o.ntupleInsertDelete;
        t.ntupleUpdateValue -= o.ntupleUpdateValue;
        t.ntupleUpdateDelete -= o.ntupleUpdateDelete;
        t.ntupleRemove -= o.ntupleRemove;
        // IO reads
        t.cacheTests.add(-o.cacheTests.get());
        t.cacheMisses.add(-o.cacheMisses.get());
        t.nodesRead.add(-o.nodesRead.get());
        t.leavesRead.add(-o.leavesRead.get());
        t.bytesRead.add(-o.bytesRead.get());
        t.readNanos.add(-o.readNanos.get());
        t.deserializeNanos.add(-o.deserializeNanos.get());
        t.rawRecordsRead.add(-o.rawRecordsRead.get());
        t.rawRecordsBytesRead.add(-o.rawRecordsBytesRead.get());
        // IO writes.
        t.nodesWritten.add(-o.nodesWritten.get());
        t.leavesWritten.add(-o.leavesWritten.get());
        t.bytesWritten.add(-o.bytesWritten.get());
        t.bytesReleased.add(-o.bytesReleased.get());
        t.serializeNanos.add(-o.serializeNanos.get());
        t.writeNanos.add(-o.writeNanos.get());
        t.rawRecordsWritten.add(-o.rawRecordsWritten.get());
        t.rawRecordsBytesWritten.add(-o.rawRecordsBytesWritten.get());
//        // touch()
//        syncTouchNanos.add(-o.syncTouchNanos.get());
//        touchNanos.add(-o.touchNanos.get());
        
        return t;
        
    }
    
    /**
     * #of keys looked up in the tree by contains/lookup(key) (does not count
     * those rejected by the bloom filter before they are tested against the
     * B+Tree).
     */
    /*
     * Note: This is a hot spot with concurrent readers and does not provide
     * terribly useful information so I have taken it out. BBT 1/24/2010.
     */
//    public final AtomicLong nfinds = new AtomicLong();
    public final AtomicLong ninserts = new AtomicLong();
    public final AtomicLong nremoves = new AtomicLong();
    // ILinearList
    public final CAT nindexOf = new CAT();
    public final CAT ngetKey = new CAT();
    public final CAT ngetValue = new CAT();

    /*
     * Note: These counters are hot spots with concurrent readers and do not
     * provide terribly useful information so I have taken them out. BBT
     * 1/26/2010.
     */
    // IRangeQuery
    public final CAT nrangeCount = new CAT();
    public final CAT nrangeIterator = new CAT();

    // Structural change (single-threaded, so plain variables are Ok).
    public int rootsSplit = 0;
    public int rootsJoined = 0;
    public int nodesSplit = 0;
    public int nodesJoined = 0;
    public int leavesSplit = 0;
    public int leavesJoined = 0;
    public int tailSplit = 0;
    public int headSplit = 0;
    public int nodesCopyOnWrite = 0;
    public int leavesCopyOnWrite = 0;
    
    /*
     * Tuple counters.
     * 
     * Note: The tuple level counters can be used to determine the degree of
     * insert, vs update, vs delete activity on a B+Tree. How you count
     * "deletes" depends on whether or not delete markers are in use - when they
     * are in use deletes are in fact updates (or inserts if the key was not
     * pre-existing). When delete markers are not in use, deletes actually
     * remove the tuple.
     * 
     * Note: You CAN NOT determine the actual net change in the #of tuples in
     * the B+Tree that is part of a fused view using these counters since the
     * same tuple may have been inserted, deleted and updated and may or may not
     * exist in the other sources of the fused view. However, these counters MAY
     * be used as an indication that a compacting merge might reduce the #of
     * tuples in the view (e.g., a lot of insertDelete or updateDelete activity)
     * and that might lead you to choose to perform a compacting merge before
     * deciding whether or not to split an index partition.
     */
    
    /**
     * #of non-deleted tuples that were inserted into the B+Tree (rather than
     * updating the value for an existing tuple).
     */
    public long ntupleInsertValue = 0;

    /**
     * #of deleted tuples that were inserted into the B+Tree (rather than
     * deleting the value for an existing tuple). Note that delete markers MAY
     * be written into a B+Tree when there is no tuple for that key in the
     * BTree. Those cases arise when the B+Tree is used to isolate a
     * transaction's write set and are counted here.
     */
    public long ntupleInsertDelete = 0;
    
    /**
     * #of pre-existing tuples whose value was updated to a non-deleted value
     * (includes update of a deleted tuple to a non-deleted tuple by overwrite
     * of the tuple).
     */
    public long ntupleUpdateValue = 0;

    /**
     * #of pre-existing un-deleted tuples whose delete marker was set (we don't
     * count re-deletes of an already deleted tuple).
     */
    public long ntupleUpdateDelete = 0;
    
    /**
     * #of pre-existing tuples that were removed from the B+Tree (only non-zero
     * when the B+Tree does not support delete markers).
     */
    public long ntupleRemove = 0;

    /*
     * IO times.
     * 
     * Note: The nanosecond time fields are for nodes + leaves.
     */
    
    // IO reads (concurrent)
    /** #of tests of the BTree cache (getChild()). See BLZG-1657. Should correlate to #of getChild() calls. */
    public final CAT cacheTests = new CAT();
    /** #of misses when testing the BTree cache (getChild()). See BLZG-1657. Should correlate to nodesRead+leavesRead. */
    public final CAT cacheMisses = new CAT();
    /** #of node read operations. */
    public final CAT nodesRead = new CAT();
    /** #of leaf read operations. */
    public final CAT leavesRead = new CAT();
    /** Total bytes read for nodes and leaves (but not raw records). */;
    public final CAT bytesRead = new CAT();
    /** Read time for nodes and leaves (but not raw records). */
    public final CAT readNanos = new CAT();
    /** De-serialization time for nodes and leaves. */
    public final CAT deserializeNanos = new CAT();
    /** The #of raw record read operations. */
    public final CAT rawRecordsRead = new CAT();
    /** Total bytes read for raw records. */
    public final CAT rawRecordsBytesRead = new CAT();

    // IO writes (multi-threaded since BLZG-1665)
    public CAT nodesWritten = new CAT();
    public CAT leavesWritten = new CAT();
    public CAT bytesWritten = new CAT();
    public CAT bytesReleased = new CAT();
    public CAT writeNanos = new CAT();
    public CAT serializeNanos = new CAT();
    public CAT rawRecordsWritten = new CAT();
    public CAT rawRecordsBytesWritten = new CAT();

	/*
	 * Note: The introduction of these performance counters caused a significant
	 * performance regression for both load and query.  See BLZG-1693.
	 */
//    // touch()
//	/**
//	 * Nanoseconds inside of doSyncTouch().
//	 * 
//     * @see BLZG-1664
//	 */
//    public final CAT syncTouchNanos = new CAT();
//    /**
//     * Nanoseconds inside of doTouch() (this is also invoked from within
//     * doSyncTouch()).
//     * 
//     * @see BLZG-1664
//     */
//    public final CAT touchNanos = new CAT();
//    /**
//     * doTouch() call counter.
//     * 
//     * @see BLZG-1664
//     */
//    public final CAT touchCount = new CAT();

    //
    // write eviction queue
    //
    
    /*
     * Note: The introduction of these performance counters might have caused
     * some performance regression for both load and query.  See BLZG-1693.
     */
//    /**
//     * The #of node or leaf references evicted from the write retention queue.
//     */
//    public final AtomicLong queueEvict = new AtomicLong();
//    /**
//     * The #of node or leave references evicted from the write retention queue
//     * that are no longer referenced on that queue. If the reference is dirty,
//     * it will be serialized and written on the backing store.
//     */
//    public final AtomicLong queueEvictNoRef = new AtomicLong();
//    /**
//     * The #of node or leave references evicted from the write retention queue
//     * that are no longer referenced on that queue and are dirty and thus will
//     * be immediately serialized and written on the backing store.
//     */
//    public final AtomicLong queueEvictDirty = new AtomicLong();

	/**
	 * The #of bytes in the unisolated view of the index which are being used to
	 * store raw records.
	 * 
	 * @todo not correctly tracked in scale-out (overflow handling).
	 */
    public final AtomicLong bytesOnStore_rawRecords = new AtomicLong();

	/**
	 * The #of bytes in node and leaf) records on the backing store for the
	 * unisolated view of the index. This value grows and shrinks as nodes
	 * (leaves) are added to and removed from the index. The actual space
	 * occupied by the index on the backing store depends on whether and when
	 * persistence store reclaims the storage associated with deleted nodes and
	 * leaves.
	 * 
	 * @todo break out for nodes and leaves separately.
	 * 
	 * @todo not correctly tracked in scale-out (overflow handling).
	 */
	public final AtomicLong bytesOnStore_nodesAndLeaves = new AtomicLong();

    /**
     * Return a score whose increasing value is correlated with the amount of
     * read/write activity on an index as reflected in these
     * {@link BTreeCounters}.
     * <p>
     * The score is the serialization / deserialization time plus the read /
     * write time. Time was chosen since it is a common unit and since it
     * reflects the combination of CPU time, memory time (for allocations and
     * garbage collection - the latter can be quite significant), and the disk
     * wait time. The other main component of time is key search, but that is
     * not instrumented right now.
     * <p>
     * Serialization and deserialization are basically a CPU activity and drive
     * memory to the extent that allocations are made, especially during
     * deserialization.
     * <p>
     * The read/write time is strongly dominated by actual DISK IO and by
     * garbage collection time (garbage collection can cause threads to be
     * suspended at any time). For deep B+Trees, DISK READ time dominates DISK
     * WRITE time since increasing numbers of random reads are required to
     * materialize any given leaf.
     * <p>
     * The total read-write cost (in seconds) for a BTree is one of the factors
     * that is considered when choosing which index partition to move. Comparing
     * the total read/write cost for BTrees across a database can help to reveal
     * which index partitions have the heaviest load. If only a few index
     * partitions for a given scale-out index have a heavy load, then those
     * index partitions are hotspots for that index.
     * 
     * @return The computed score.
     */
    public double computeRawReadWriteScore() {
        
        return //
            (serializeNanos.get() + deserializeNanos.get()) + //
            (writeNanos.get() + readNanos.get())//
            ;
        
    }

    /**
     * Return a score whose increasing value is correlated with the amount of
     * read activity on an index as reflected in these {@link BTreeCounters}.
     * The score is {@link #deserializeNanos} + {@link #readNanos}.
     * 
     * @see #computeRawReadWriteScore()
     */
    public double computeRawReadScore() {
        
        return deserializeNanos.get() + readNanos.get();
        
    }

    /**
     * Return a score whose increasing value is correlated with the amount of
     * write activity on an index as reflected in these {@link BTreeCounters}.
     * The score is {@link #serializeNanos} plus {@link #writeNanos}.
     * 
     * @see #computeRawReadWriteScore()
     */
    public double computeRawWriteScore() {
        
        return serializeNanos.get() + writeNanos.get();
        
    }
    
    /**
     * Normalizes a raw score in the context of totals for some data service.
     * 
     * @param rawScore
     *            The raw score.
     * @param totalRawScore
     *            The raw score computed from the totals.
     * 
     * @return The normalized score.
     */
    static public double normalize(final double rawScore,
            final double totalRawScore) {
        
        if (totalRawScore == 0d) {

            return 0d;

        }

        return rawScore / totalRawScore;
        
    }
    
    /**
     * The #of nodes written on the backing store.
     */
    final public long getNodesWritten() {
        
        return nodesWritten.get();
        
    }
    
    /**
     * The #of leaves written on the backing store.
     */
    final public long getLeavesWritten() {
        
        return leavesWritten.get();
        
    }
    
    /**
     * The number of bytes read from the backing store.
     */
    final public long getBytesRead() {
        
        return bytesRead.get();
        
    }
    
    /**
     * The number of bytes written onto the backing store.
     */
    final public long getBytesWritten() {
        
        return bytesWritten.get();
        
    }

    /**
     * The number of bytes released from the backing store.
     */
    final public long getBytesReleased() {
        
        return bytesReleased.get();
        
    }

    /**
     * Return a {@link CounterSet} reporting on the various counters tracked in
     * the instance fields of this class.
     */
//    synchronized 
    @Override
    public CounterSet getCounters() {
        
//        if(counterSet == null) {
            
        final CounterSet counterSet = new CounterSet();

            /*
             * ISimpleBTree
             *
             * @todo instrument key search time.
             */
            {
                
                final CounterSet tmp = counterSet.makePath(IBTreeCounters.KeySearch);

//                Note: This is a hotspot since it is used by concurrent readers.
//                tmp.addCounter("find", new Instrument<Long>() {
//                    protected void sample() {
//                        setValue(nfinds.get());
//                    }
//                });

                // Note: mutation counters are Ok since mutation is single-threaded.
                tmp.addCounter("insert", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ninserts.get());
                    }
                });

                tmp.addCounter("remove", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nremoves.get());
                    }
                });
            }
            
            /*
             * ILinearList API.
             */
            {
                
                final CounterSet tmp = counterSet.makePath(IBTreeCounters.LinearList);

                tmp.addCounter("indexOf", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nindexOf.get());
                    }
                });

                tmp.addCounter("getKey", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ngetKey.get());
                    }
                });

                tmp.addCounter("getValue", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ngetValue.get());
                    }
                });
                
            }
            
            /*
             * IRangeQuery
             * 
             * @todo Instrument the IRangeQuery API for times (must aggregate
             * across hasNext() and next()). Note that rangeCount and rangeCopy
             * both depend on rangeIterator so that is the only one for which we
             * really need timing data. The iterator should report the
             * cumulative service time when it is finalized.
             */
            {
                
                final CounterSet tmp = counterSet.makePath(IBTreeCounters.RangeQuery);

                tmp.addCounter("rangeCount", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nrangeCount.get());
                    }
                });
                
                tmp.addCounter("rangeIterator", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nrangeIterator.get());
                    }
                });
                
            }
            
            /*
             * Structural mutation statistics.
             * 
             * @todo also merge vs redistribution of keys on remove (and insert
             * if b*-tree)
             */
            {
               
                final CounterSet tmp = counterSet.makePath(IBTreeCounters.Structure);
                
                tmp.addCounter("rootSplit", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(rootsSplit);
                    }
                });

                tmp.addCounter("rootJoined", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(rootsJoined);
                    }
                });

                tmp.addCounter("nodeSplit", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(nodesSplit);
                    }
                });

                tmp.addCounter("nodeJoined", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(nodesJoined);
                    }
                });

                tmp.addCounter("leafSplit", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(leavesSplit);
                    }
                });

                tmp.addCounter("leafJoined", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(leavesJoined);
                    }
                });

                tmp.addCounter("headSplit", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(headSplit);
                    }
                });

                tmp.addCounter("tailSplit", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(tailSplit);
                    }
                });

                tmp.addCounter("leafCopyOnWrite", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(leavesCopyOnWrite);
                    }
                });

                tmp.addCounter("nodeCopyOnWrite", new Instrument<Integer>() {
                    @Override
                    protected void sample() {
                        setValue(nodesCopyOnWrite);
                    }
                });

            }
         
            /*
             * Tuple stats.
             */
            {
                
                final CounterSet tmp = counterSet.makePath(IBTreeCounters.Tuples);

                tmp.addCounter("insertValue", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntupleInsertValue);
                    }
                });
                tmp.addCounter("insertDelete", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntupleInsertDelete);
                    }
                });

                tmp.addCounter("updateValue", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntupleUpdateValue);
                    }
                });

                tmp.addCounter("updateDelete", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntupleUpdateDelete);
                    }
                });

                tmp.addCounter("tupleRemove", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(ntupleRemove);
                    }
                });
            }

            /*
             * IO stats.
             */
            {

                final CounterSet tmp = counterSet.makePath(IBTreeCounters.IO);

                /*
                 * Cache.
                 */

                /** #of tests of the BTree cache (getChild()). See BLZG-1657. Should correlate to #of getChild() calls. */
                tmp.addCounter("cacheTests", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(cacheTests.get());
                    }
                });

                /** #of misses when testing the BTree cache (getChild()). See BLZG-1657. Should correlate to nodesRead+leavesRead. */
                tmp.addCounter("cacheMisses", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(cacheMisses.get());
                    }
                });

                tmp.addCounter("cacheHits", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        final long cacheHits = cacheTests.get() - cacheMisses.get();
                        setValue(cacheHits);
                    }
                });

                tmp.addCounter("cacheHitRatio", new Instrument<Double>() {
                    @Override
                    protected void sample() {
                        final double _cacheTests = cacheTests.get();
                        if(_cacheTests==0) return; // avoid divide-by-zero.
                        final double _cacheHits = _cacheTests - cacheMisses.get();
                        final double cacheHitRatio = _cacheHits / _cacheTests;
                        setValue(cacheHitRatio);
                    }
                });

                /*
                 * bytes on store.
                 */
				tmp.addCounter("bytesOnStoreNodesAndLeaves", new Instrument<Long>() {
				    @Override
					protected void sample() {
						setValue(bytesOnStore_nodesAndLeaves.get());
					}
				});
				
				tmp.addCounter("bytesOnStoreRawRecords", new Instrument<Long>() {
				    @Override
					protected void sample() {
						setValue(bytesOnStore_rawRecords.get());
					}
				});

				tmp.addCounter("bytesOnStoreTotal", new Instrument<Long>() {
				    @Override
					protected void sample() {
						setValue(bytesOnStore_nodesAndLeaves.get()
								+ bytesOnStore_rawRecords.get());
					}
				});

                /*
                 * nodes/leaves read/written
                 */

                tmp.addCounter("leafReadCount", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(leavesRead.get());
                    }
                });

                tmp.addCounter("nodeReadCount", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nodesRead.get());
                    }
                });

                tmp.addCounter("leafWriteCount", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(leavesWritten.get());
                    }
                });

                tmp.addCounter("nodeWriteCount", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(nodesWritten.get());
                    }
                });

                /*
                 * store reads (bytes)
                 */

                tmp.addCounter("bytesRead", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(bytesRead.get());
                    }
                });

                tmp.addCounter("readSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double elapsedReadSecs = (readNanos.get() / 1000000000.);
                        setValue(elapsedReadSecs);
                    }
                });

                tmp.addCounter("bytesReadPerSec",
                        new Instrument<Double>() {
            	    @Override
                            public void sample() {
                                final double readSecs = (readNanos.get() / 1000000000.);
                                final double bytesReadPerSec = (readSecs == 0L ? 0d
                                        : (bytesRead.get() / readSecs));
                                setValue(bytesReadPerSec);
                            }
                        });

                /*
                 * store writes (bytes)
                 */
                tmp.addCounter("bytesWritten", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(bytesWritten.get());
                    }
                });

                tmp.addCounter("writeSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double writeSecs = (writeNanos.get() / 1000000000.);
                        setValue(writeSecs);
                    }
                });

                tmp.addCounter("bytesWrittenPerSec",
                        new Instrument<Double>() {
                    @Override
                            public void sample() {
                                final double writeSecs = (writeNanos.get() / 1000000000.);
                                final double bytesWrittenPerSec = (writeSecs == 0L ? 0d
                                        : (bytesWritten.get() / writeSecs));
                                setValue(bytesWrittenPerSec);
                            }
                        });
                
                /*
                 * node/leaf (de-)serialization times.
                 */
                
                tmp.addCounter("serializeSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double serializeSecs = (serializeNanos.get() / 1000000000.);
                        setValue(serializeSecs);
                    }
                });

                tmp.addCounter("serializeLatencyNanos",
                        new Instrument<Double>() {
                    @Override
                            public void sample() {
                                final long nwritten = nodesWritten.get() + leavesWritten.get();
                                final double serializeLatencyNanos = (nwritten == 0L ? 0d
                                        : (serializeNanos.get() / nwritten));
                                setValue(serializeLatencyNanos);
                            }
                        });

                tmp.addCounter("serializePerSec",
                        new Instrument<Double>() {
                    @Override
                            public void sample() {
                                final long nwritten = nodesWritten.get() + leavesWritten.get();
                                final double serializeSecs = (serializeNanos.get() / 1000000000.);
                                final double serializePerSec = (serializeSecs== 0L ? 0d
                                        : (nwritten / serializeSecs));
                                setValue(serializePerSec);
                            }
                        });

                tmp.addCounter("deserializeSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double deserializeSecs = (deserializeNanos.get() / 1000000000.);
                        setValue(deserializeSecs);
                    }
                });

                tmp.addCounter("deserializeLatencyNanos",
                        new Instrument<Double>() {
                    @Override
                            public void sample() {
                                final long nread = nodesRead.get() + leavesRead.get();
                                final double deserializeLatencyNanos = (nread == 0L ? 0d
                                        : (deserializeNanos.get() / nread));
                                setValue(deserializeLatencyNanos);
                            }
                        });

                tmp.addCounter("deserializePerSec",
                        new Instrument<Double>() {
                    @Override
                            public void sample() {
                                final long nread = nodesRead.get() + leavesRead.get();
                                final double deserializeSecs = (deserializeNanos.get() / 1000000000.);
                                final double deserializePerSec = (deserializeSecs== 0L ? 0d
                                        : (nread / deserializeSecs));
                                setValue(deserializePerSec);
                            }
                        });
                
                /*
                 * Aggregated read / write times for IO and (de-)serialization.
                 */
                
                /*
                 * Sum of readSecs + writeSecs + deserializeSecs + serializeSecs.
                 */
                tmp.addCounter("totalReadWriteSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double secs = (computeRawReadWriteScore()/ 1000000000.);
                        setValue(secs);
                    }
                });
                
                /*
                 * Sum of readSecs + deserializeSecs.
                 */
                tmp.addCounter("totalReadSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double secs = (computeRawReadScore()/ 1000000000.);
                        setValue(secs);
                    }
                });
                
                /*
                 * Sum of writeSecs + serializeSecs.
                 */
                tmp.addCounter("totalWriteSecs", new Instrument<Double>() {
                    @Override
                    public void sample() {
                        final double secs = (computeRawWriteScore()/ 1000000000.);
                        setValue(secs);
                    }
                });

                /*
                 * RawRecords
                 */
                tmp.addCounter("rawRecordsRead", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(rawRecordsRead.get());
                    }
                });
                tmp.addCounter("rawRecordsBytesRead", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(rawRecordsBytesRead.get());
                    }
                });
                tmp.addCounter("rawRecordsWritten", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(rawRecordsWritten.get());
                    }
                });
                tmp.addCounter("rawRecordsBytesWritten", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(rawRecordsBytesWritten.get());
                    }
                });
                
                tmp.addCounter("bytesReleased", new Instrument<Long>() {
                    @Override
                    protected void sample() {
                        setValue(bytesReleased.get());
                    }
                });

            }

//            /*
//             * touch() stats.
//             */
//            {
//
//                final CounterSet tmp = counterSet.makePath(IBTreeCounters.TOUCH);
//                
//                tmp.addCounter("touchCount", new Instrument<Long>() {
//                    @Override
//                    public void sample() {
//                        setValue(touchCount.get());
//                    }
//                });
//
//                tmp.addCounter("syncTouchSecs", new Instrument<Double>() {
//                    @Override
//                    public void sample() {
//                        final double secs = (syncTouchNanos.get() / 1000000000.);
//                        setValue(secs);
//                    }
//                });
//
//                tmp.addCounter("syncTouchLatencyNanos",
//                        new Instrument<Double>() {
//                    @Override
//                            public void sample() {
//                                final long n = touchCount.get();
//                                final double latencyNanos = (n == 0L ? 0d : (syncTouchNanos.get() / n));
//                                setValue(latencyNanos);
//                            }
//                        });
//
//                tmp.addCounter("syncTouchPerSec",
//                        new Instrument<Double>() {
//                    @Override
//                            public void sample() {
//                                final long n = touchCount.get();
//                                final double secs = (syncTouchNanos.get() / 1000000000.);
//                                final double perSec = (secs == 0L ? 0d : (n / secs));
//                                setValue(perSec);
//                            }
//                        });
//
//                tmp.addCounter("touchSecs", new Instrument<Double>() {
//                    @Override
//                    public void sample() {
//                        final double secs = (touchNanos.get() / 1000000000.);
//                        setValue(secs);
//                    }
//                });
//
//                tmp.addCounter("touchLatencyNanos",
//                        new Instrument<Double>() {
//                    @Override
//                            public void sample() {
//                                final long n = touchCount.get();
//                                final double latencyNanos = (n == 0L ? 0d : (touchNanos.get() / n));
//                                setValue(latencyNanos);
//                            }
//                        });
//
//                tmp.addCounter("touchPerSec",
//                        new Instrument<Double>() {
//                    @Override
//                            public void sample() {
//                                final long n = touchCount.get();
//                                final double secs = (touchNanos.get() / 1000000000.);
//                                final double perSec = (secs == 0L ? 0d : (n / secs));
//                                setValue(perSec);
//                            }
//                        });
//
//            }

//            // writeRetentionQueue
//            {
//                
//                final CounterSet tmp = counterSet.makePath(IBTreeCounters.WriteRetentionQueue);
//                
//                tmp.addCounter("evictCount", new Instrument<Long>() {
//                    @Override
//                    public void sample() {
//                        setValue(queueEvict.get());
//                    }
//                });
//
//                tmp.addCounter("evictCountNoRef", new Instrument<Long>() {
//                    @Override
//                    public void sample() {
//                        setValue(queueEvictNoRef.get());
//                    }
//                });
//
//                tmp.addCounter("evictCountDirty", new Instrument<Long>() {
//                    @Override
//                    public void sample() {
//                        setValue(queueEvictDirty.get());
//                    }
//                });
//
//            }
            
//        }
        
        return counterSet;
        
    }
//    private CounterSet counterSet;
    
    @Override
    public String toString() {

        // in XML.
        return getCounters().asXML(null/* filter */);
        
    }

}
