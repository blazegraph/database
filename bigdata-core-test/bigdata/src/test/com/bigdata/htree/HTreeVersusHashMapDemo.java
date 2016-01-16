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
/*
 * Created on Oct 19, 2011
 */

package com.bigdata.htree;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.util.Bytes;
import com.bigdata.util.PseudoRandom;

/**
 * A simple demonstration which may be used to compare the {@link HTree}
 * performance against a Java collections class such as {@link HashMap} or
 * {@link LinkedHashMap}. This demonstration is focused on the performance curve
 * when inserting a large number of keys into a collection. The Java collection
 * classes are faster for small numbers of keys, but the {@link HTree} rapidly
 * out performs them as the #of keys grows larger.
 * <p>
 * The {@link HTree} is run against the {@link MemoryManager}. This means that
 * the data for the {@link HTree} is mostly stored on the Java native process
 * heap using {@link java.nio.ByteBuffer#allocateDirect(int)}. Thus even a very
 * large {@link HTree} instance can be run with a very small JVM object heap and
 * creates nearly no GC pressure.
 * <p>
 * The {@link HTree} permits multiple entries for the same key. While variable
 * length keys are supported, the key for the {@link HTree} is typically a 32
 * (or 64) bit hash code for the value stored in the {@link HTree}. Normally,
 * the application will lookup all tuples having the same hash code and then
 * enforce {@link Set} or {@link Map} semantics by scanning those tuples for the
 * presence of the same application object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn
 *         Cutcher</a>
 * @version $Id$
 */
public class HTreeVersusHashMapDemo {

    private static final Logger log = Logger
            .getLogger(HTreeVersusHashMapDemo.class);

    /**
     * Provision and return an {@link HTree} instance.
     * 
     * @param store
     *            The backing store.
     * @param addressBits
     *            The #of address bits (10 is typical and gives you directory
     *            pages with 2^10 slots, which is a 1024 fan-out).
     * @param rawRecords
     *            <code>true</code> iff raw record support will be enabled.
     * @param writeRetentionQueueCapacity
     *            The write retention queue capacity controls how long mutable
     *            htree nodes will be buffered on the JVM heap before being
     *            incrementally evicted to the backing store.
     * 
     * @return The {@link HTree} instance.
     */
    static private HTree getHTree(final IRawStore store, final int addressBits,
            final boolean rawRecords, final int writeRetentionQueueCapacity) {

        final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                // new FrontCodedRabaCoder(),// TODO FrontCodedRaba
                new SimpleRabaCoder(),// keys
                new SimpleRabaCoder() // vals
//                EmptyRabaValueCoder.INSTANCE // if no values.
                );

		final HTreeIndexMetadata metadata = new HTreeIndexMetadata(
				UUID.randomUUID());

        if (rawRecords) {
            metadata.setRawRecords(true);
            metadata.setMaxRecLen(0);
        }

        metadata.setAddressBits(addressBits);

        metadata.setTupleSerializer(tupleSer);

        /*
         * Note: A low retention queue capacity will drive evictions, which is
         * good from the perspective of stressing the persistence store
         * integration.
         */
        metadata.setWriteRetentionQueueCapacity(writeRetentionQueueCapacity);
        metadata.setWriteRetentionQueueScan(10); // Must be LTE capacity.

        return HTree.create(store, metadata);

    }

    static final int REPORT_INTERVAL = 100000; // report interval.

    /**
     * Interface for reporting on progress.
     */
    interface IReport {

        /**
         * Report hook.
         * 
         * @param nops
         *            The #of operations performed.
         * @param elapsed
         *            The elapsed time in milliseconds.
         * @param store
         *            The backing store for the {@link HTree}.
         */
        void report(long nops, long elapsed, final IMemoryManager mmgr,
                IRawStore store);
        
    }

    private static class ReportListener implements IReport {

        public ReportListener() {
            System.out
                    .println("inserts\telapsed(ms)\tinserts/sec\tfreeMemory\ttotalMemory\tuserBytes\tmmgrBytes");
        }

        @Override
        public void report(long nops, long elapsed, final IMemoryManager mmgr,
                final IRawStore store) {

            final long insertsPerSec = (long) (((double) nops) / (elapsed / 1000d));

            final long freeMemory = Runtime.getRuntime().freeMemory();

            final long totalMemory = Runtime.getRuntime().totalMemory();

            // #of application bytes in the store.
            final long userBytes = store == null ? 0L : store.size();

            // total extent of the store (1M increments).
            final long mmgrBytes = mmgr == null ? 0L : ((MemoryManager) mmgr)
                    .getExtent();

            System.out.println(nops + "\t" + elapsed + "\t" + insertsPerSec
                    + "\t" + freeMemory + "\t" + totalMemory + "\t" + userBytes
                    + "\t" + mmgrBytes);

        }

    }

    /**
     * Version stuff the keys into a Java collection.
     */
    private static class JavaCollectionDemo implements Runnable {

        private final int nkeys;
        private final int vectorSize;
        private final IGenerator gen;
        private final Map<Object,Object> c;
        private final IReport report;

        /**
         * 
         * @param nkeys
         *            The #of keys to insert.
         * @param vectorSize
         *            The #of keys which are sorted (vectored) as per the
         *            {@link HTree} variant.
         * @param seed
         *            The random generator seed.
         * @param c
         *            The Java collection.
         */
        JavaCollectionDemo(final IReport report, final int nkeys, final int vectorSize,
                final IGenerator gen,
                final Map<Object,Object> c) {

            this.report = report;
            this.nkeys = nkeys;
            this.vectorSize = vectorSize;
            this.gen = gen;
            this.c = c;

        }
        
        public void run() {

            final long start = System.currentTimeMillis();

            final IKeyBuilder keyBuilder = new KeyBuilder();
            
            final int[] keys = new int[vectorSize];
            int alen;

            for (int i = 0; i < nkeys;) {

                alen = Math.min(vectorSize, nkeys - i);

                for (int j = 0; j < alen; j++) {

                    final int rnd = gen.next();

                    keys[j] = rnd;

                }

                i += alen;

                // Vector the chunk.
                Arrays.sort(keys, 0, alen);

                for (int j = 0; j < alen; j++) {

                    final Integer key = keys[j];
//                    final byte[] key = keyBuilder.reset().append(keys[j])
//                            .getKey();

//                    if (!c.containsKey(key)) {
//                        /*
//                         * Do not store duplicate entries since we will compare
//                         * the performance to a Set.
//                         */
                        c.put(key,key);
//                    }

                    final long nops = i + j;

                    if (report != null && (nops % REPORT_INTERVAL) == 0L) {

                        final long elapsed = System.currentTimeMillis() - start;

                        report.report(nops, elapsed, null/* mmgr */, null/* mmgr */);

                    }

                }

            }

            final long load = System.currentTimeMillis();

            if (log.isInfoEnabled()) {

                log.info("\nEntries: " + c.size() + "; Load took "
                        + (load - start) + "ms, Generator="
                        + gen.getClass().getSimpleName() + ", class="
                        + this.getClass().getSimpleName());

            }

        }
        
    }

    /**
     * Version stuff the keys into an {@link HTree}.
     */
    private static class HTreeDemo implements Runnable {

        private final int nkeys;
        private final int vectorSize;
        private final IGenerator gen;
        private final int addressBits;
        private final int writeRetentionQueueCapacity;
        private final IReport report;

        /**
         * 
         * @param nkeys
         *            The #of keys to insert.
         * @param vectorSize
         *            The #of keys which are sorted (vectored) to improve IO
         *            efficiency.
         * @param seed
         *            The random generator seed.
         * @param addressBits
         *            The address bits for the {@link HTree}.
         * @param writeRetentionQueueCapacity
         *            The capacity of the write retention queue.
         */
        HTreeDemo(final IReport report, final int nkeys, final int vectorSize,
                final IGenerator gen,
                final int addressBits,
                final int writeRetentionQueueCapacity) {

            this.report = report;
            this.nkeys = nkeys;
            this.vectorSize = vectorSize;
            this.gen = gen;
            this.addressBits = addressBits;
            this.writeRetentionQueueCapacity = writeRetentionQueueCapacity;

        }
        
        public void run() {

            final long start = System.currentTimeMillis();

            final MemStore store = new MemStore(DirectBufferPool.INSTANCE);

            try {

                final HTree htree = getHTree(store, addressBits,
                        false/* rawRecords */, writeRetentionQueueCapacity);

                final IKeyBuilder keyBuilder = new KeyBuilder();

//                final byte[] val = null; // no value stored under the key.
                
                final int[] keys = new int[vectorSize];
                int alen;

                for (int i = 0; i < nkeys; ) {

                    alen = Math.min(vectorSize, nkeys - i);

                    for (int j = 0; j < alen; j++) {

                      final int rnd = gen.next(); 
                        
                      keys[j] = rnd;
                      
                    }

                    i += alen;
                    
                    // Vector the chunk.
                    Arrays.sort(keys, 0, alen);

                    for (int j = 0; j < alen; j++) {

                        final int rnd = keys[j];

                        final byte[] key = keyBuilder.reset().append(rnd)
                                .getKey();

//                        if (!htree.contains(key)) {
//                            /*
//                             * Do not store duplicate entries since we will
//                             * compare the performance to a Set.
//                             */
                            htree.insert(key, key);
//                        }

                        final long nops = i + j;

                        if (report != null && (nops % REPORT_INTERVAL) == 0L) {

                            final long elapsed = System.currentTimeMillis()
                                    - start;

                            report.report(nops, elapsed,
                                    store.getMemoryManager(), store);

                        }

                    }
                    
                }

                final long load = System.currentTimeMillis();
                
                final BTreeCounters counters = htree.getBtreeCounters();
                
                if (log.isInfoEnabled()) {

                    log.info("\nEntries: "+htree.nentries+", Leaves: " + htree.nleaves + ", Evicted: "
                            + counters.leavesWritten + ", Nodes: "
                            + htree.nnodes + ", Evicted: "
                            + counters.nodesWritten + "; Load took "
                            + (load - start) + "ms, Generator="
                            + gen.getClass().getSimpleName() + ", class="
                            + this.getClass().getSimpleName());

                }

//                htree.writeCheckpoint();
                
            } finally {

                store.destroy();

            }
        }
        
    }

    private interface IGenerator {
        int next();
    }
    
    /**
     * Sequential numbers starting from zero.
     */
    static private class SequentialGenerator implements IGenerator {
        private int next = 0;
        public int next() {
            return next++;
        }
    }

    /**
     * Random numbers in a half-open range (does not cover all 32-bit values (no
     * negative values)).
     */
    static private class RandomGenerator implements IGenerator {
        private int next = 0;
        private final long seed;
        private final int range;
        private final Random r;
        public RandomGenerator(final long seed, final int range) {
            this.seed = seed;
            this.range = range;
            this.r = new Random(seed);
        }
        public int next() {
            return r.nextInt(range);
        }
    }

    /** Pseudo random numbers without replacement covering a half-open range. */
    static private class PseudoRandomGenerator implements IGenerator {
        private int range;
        private PseudoRandom pr;
        /**
         * 
         * @param range The half-open range (0:range].
         */
        public PseudoRandomGenerator(final int range) {
            this(range, 0/*next*/);
        }
        /**
         * 
         * @param range The half-open range (0:range].
         * @param next The next value to visit.
         */
        public PseudoRandomGenerator(final int range, final int next) {
            this.range = range;
            this.pr = new PseudoRandom(range, next);
        }
        public int next() {
            return pr.next();
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {

//      final int rnd = r.nextInt(); // random, not random w/o replacement.
//      final int rnd = r.nextInt(nkeys); // random, not random w/o replacement.
//      final int rnd = i+j; // sequential
//      final int rnd = Integer.reverse(i+j); // sequential

        final int nkeys = 2 * Bytes.megabyte32;
        final int vectorSize = 1;//0000;
        final IGenerator gen = true? new SequentialGenerator() : new PseudoRandomGenerator(nkeys);
//        final IGenerator gen = new SequentialGenerator();
//        final IGenerator gen = new PseudoRandomGenerator(nkeys);
//        final IGenerator gen = new RandomGenerator(-91L/*seed*/,nkeys);
        final int addressBits = 8; // pages with 2^10 slots.
        final int writeRetentionQueueCapacity = 50;

        if (false) {
            new HTreeDemo(new ReportListener(), nkeys, vectorSize, gen,
                    addressBits, writeRetentionQueueCapacity).run();
        } else {
            new JavaCollectionDemo(new ReportListener(), nkeys, vectorSize,
                    gen, new HashMap<Object,Object>(nkeys)).run();
        }

    }

}
