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
 * Created on Nov 4, 2008
 */

package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.CognitiveWeb.extser.LongPacker;
import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;

/**
 * Encapsulates the actual implementation class and provides the protocol for
 * (de-)serialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Compare to the latest DSI code for the bloom filter. Has it been made
 *       faster?
 */
public class BloomFilter implements IBloomFilter, Externalizable {

    protected static final transient Logger log = Logger.getLogger(BloomFilter.class);

    protected static final transient boolean INFO = log.isInfoEnabled();

    protected static final transient boolean DEBUG = log.isDebugEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -4011582802868293737L;

    /**
     * The implementation object. This is cleared by {@link #disable()}.
     * 
     * @serial
     */
    private it.unimi.dsi.util.BloomFilter2 filter;

    /**
     * The natural logarithm of 2, used in the computation of the number of
     * bits.
     */
    private transient final static double LN2 = Math.log(2);

    /**
     * The expected #of index entries (from the ctor).
     */
    private int n;
    
    /**
     * The target error rate when there are {@link #n} index entries (from the
     * ctor).
     */
    private double p;
    
    /**
     * The #of index entries at which the filter will have reached its maximum
     * error rate (from the ctor).
     * <p>
     * Note: The value applied by {@link BTree} and normally calculated by the
     * {@link BloomFilterFactory}.
     */
    private int maxN;

    /**
     * The expected #of index entries (from the ctor).
     */
    final public int getN() {

        return n;
        
    }

    /**
     * The target error rate when there are {@link #getN()} index entries (from
     * the ctor).
     * <p>
     * Note: This class does not know the actual false positive error rate.
     * However, that is tracked by the {@link AbstractBTree#btreeCounters}.
     */
    final public double getP() {
        
        return p;
        
    }

    /**
     * The false positive error rate estimated as <code>2^-d</code>, where
     * <i>d</i> is the #of hash functions. This will be close to but typically
     * not exactly the same as the value of {@link #getP()} specified to the
     * ctor.
     * <p>
     * Note: This class does not know the actual false positive error rate.
     * However, that is tracked by the {@link AbstractBTree#btreeCounters}.
     */
    public double getErrorRate() {

        return Math.pow(2, -filter.d());
    
    }
    
    /**
     * The #of index entries at which the filter will have reached its maximum
     * error rate (from the ctor).
     */
    final public int getMaxN() {
        
        return maxN;
        
    }

    /**
     * De-serialization ctor.
     */
    public BloomFilter() {
        
    }
    
    /**
     * Ctor specifies <code>maxN := n * 2</code>.
     * 
     * @param n
     *            The expected #of index entries.
     * @param p
     *            The target error rate.
     */
    public BloomFilter(final int n, final double p) {
    
        this(n, p, n * 2);
        
    }
    
    /**
     * 
     * @param n
     *            The expected #of index entries.
     * @param p
     *            The target error rate.
     * @param maxN
     *            The #of index entries at which the filter will have reached
     *            its maximum error rate. (The value is normally calculated by
     *            the {@link BloomFilterFactory}.)
     * 
     * @throws IllegalArgumentException
     *             if <i>n</i> is non-positive.
     * @throws IllegalArgumentException
     *             unless <i>p</i> lies in (0:1).
     * @throws IllegalArgumentException
     *             if <i>maxN</i> is LT <i>n</i>.
     * 
     */
    public BloomFilter(final int n, final double p, final int maxN) {

        if (n < 1) {

            throw new IllegalArgumentException();

        }

        if (p <= 0.0 || p > 1.0) {

            throw new IllegalArgumentException();

        }

        if (maxN < n) {

            throw new IllegalArgumentException();

        }

        final int d = getHashFunctionCount(p);
        
        filter = new it.unimi.dsi.util.BloomFilter2(n, d);

        if (DEBUG)
            log.debug("n=" + n + ", p=" + p + ", d=" + d + ", m=" + filter.m());

        this.n = n;
        
        this.p = p;
        
        this.maxN = maxN;
        
    }
    
    /**
     * The #of hash functions used by the filter.
     */
    final public int getHashFunctionCount() {
        
        return filter.d();

    }

    /**
     * The bit length of the filter.
     */
    final public long getBitLength() {

        return filter.m();

    }

    /**
     * Return the #of hash functions required to achieve the specified error
     * rate. If <code>p</code> is the probability of a false positive and
     * <code>d</code> is the #of hash functions, then
     * 
     * <pre>
     * p = pow(2, -d)
     * </pre>
     * 
     * and
     * 
     * <pre>
     * d = ceil(-ln(p) / ln(2))
     * </pre>
     */
    public static int getHashFunctionCount(final double errorRate) {

        if (errorRate <= 0.0 || errorRate > 1.0) {

            throw new IllegalArgumentException();

        }

        final double p = errorRate;

        final double d2 = -(Math.log(p) / LN2);

        final int d = (int) Math.ceil(d2);

        return d;

    }

    /**
     * Return the bit length required to provision a filter having the specified
     * #of hash functions and the target capacity. This is
     * 
     * <pre>
     * 
     * bitLength = ceil(nentries * (hashFunctionCount / ln(2)))
     * 
     * </pre>
     * 
     * @param hashFunctionCount
     *            The #of hash functions.
     * @param nentries
     *            The target capacity.
     * 
     * @return The required bit length.
     */
    public static long getBitLength(final int hashFunctionCount,
            final int nentries) {

        final long bitLength = (long) Math.ceil(nentries
                * (hashFunctionCount / LN2));

        return bitLength;

    }

    /**
     * This returns the #of index entries at which the bloom filter will have
     * the specified <em>expected</em> error rate. The probability of a false
     * positive is
     * 
     * <pre>
     * p = (1 - (1 - 1 / m) &circ; kn) &circ; k
     * </pre>
     * 
     * where m is the bit length of the filter, k is the #of hash functions, and
     * n is the #of items that have been inserted into the filter.
     * 
     * or approximately
     * 
     * <pre>
     * p = (1 - e &circ; -kn / m) &circ; k
     * </pre>
     * 
     * solving for <code>n</code> we obtain
     * 
     * <pre>
     * n = -m ln( 1 - p&circ;(1/k) ) / k
     * </pre>
     * 
     * @param k
     *            The #of hash functions.
     * @param m
     *            The bit length of the filter.
     * @param p
     *            The expected error rate.
     * 
     * @return The #of index entries at which the expected bloom filter error
     *         rate will be the specified value.
     * 
     * @see http://en.wikipedia.org/wiki/Bloom_filter
     */
    static public int getEntryCountForErrorRate(final int k, final long m,
            final double p) {

        final double n = -m * Math.log(1 - Math.pow(p, 1d / k)) / k;

        if(DEBUG)
            log.debug("p=" + p + ", m=" + m + ", k=" + k + ", n=" + n);

        return (int) n;

    }

    /**
     * @throws IllegalStateException
     *             if the filter has been {@link #disable()}d
     */
    public boolean add(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        if (!enabled)
            throw new IllegalStateException();

        if (filter.add(key)) {

            // filter state was modified.
            dirty = true;

            counters.nbloomAdd++;
            
            return true;

        }

        return false;

    }

    /**
     * @throws IllegalStateException
     *             if the filter has been {@link #disable()}d
     */
    public boolean contains(final byte[] key) {

        if (key == null)
            throw new IllegalArgumentException();

        if (!enabled)
            throw new IllegalStateException();

        counters.nbloomTest++;
        
        if(!filter.contains(key)) {
            
            counters.nbloomRejects++;
            
            return false;
            
        }
        
        return true;

    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append("BloomFilter");

        sb.append("{ minSize=" + filter.size());
        
        sb.append(", n=" + n);

        sb.append(", p=" + p);

        sb.append(", maxN=" + maxN);

        sb.append(", bitLength=" + filter.m());

        sb.append(", hashFunctionCount=" + filter.d());

        sb.append(", errorRate=" + getErrorRate());

        if (dirty)
            sb.append(", dirty");
        if (!enabled)
            sb.append(", disabled");
        if (addr != 0L)
            sb.append(", addr=" + addr);

        sb.append("}");

        return sb.toString();

    }

    /*
     * Persistence protocol.
     */

    /**
     * Address that can be used to read this object from the store.
     * <p>
     * Note: This is not persisted since we do not have the address until after
     * we have written out the state of this record. However the value is
     * written into each {@link Checkpoint} record.
     */
    private transient long addr;

    /**
     * Presumed clean until {@link #add(byte[])} indicates that the filter state
     * was changed.
     */
    private transient boolean dirty = false;

    /**
     * Address that can be used to read this object from the store.
     * <p>
     * Note: This is not a persistent property. However the value is set when
     * the record is read from, or written on, the store.
     */
    public final long getAddr() {

        return addr;

    }

    /**
     * Read a bloom filter record from the store.
     * 
     * @param store
     *            the store.
     * @param addr
     *            the address of the bloom filter record.
     * 
     * @return the de-serialized bloom filter record. The address from which it
     *         was loaded is set on the bloom filter as a side-effect.
     */
    public static BloomFilter read(final IRawStore store, final long addr) {

        final BloomFilter filter = (BloomFilter) SerializerUtil
                .deserialize(store.read(addr));

        // save the address from which the record was loaded.
        filter.addr = addr;

        if (INFO)
            log.info("Read bloom filter: bytesOnDisk="
                    + store.getByteCount(addr) + ": " + filter);

        return filter;

    }

    /**
     * Return <code>true</code> iff the state of the filter has been modified
     * but not yet written onto the store. The filter is presumed clean when
     * created or when it is read from the store. The filter will remain clean
     * until {@link #add(byte[])} returns <code>true</code>, indicating that
     * the state of the filter has been changed.
     */
    final public boolean isDirty() {

        return dirty;

    }

    //    /**
    //     * Marks the filter as dirty.
    //     */
    //    final protected void setDirty() {
    //        
    //        dirty = true;
    //        
    //    }

    /**
     * Writes the bloom filter on the store and clears the {@link #isDirty()}
     * flag.
     * <p>
     * Note: This also sets the address on {@link #addr} as a side-effect, but
     * the address is NOT written into the store since it is not available until
     * after the record has been serialized.
     * <p>
     * Note: This method DOES NOT test {@link #isDirty()}.
     * 
     * @param store
     *            The store.
     * 
     * @return The address on which it was written.
     * 
     * @throws IllegalStateException
     *             if the filter is not dirty.
     * @throws IllegalStateException
     *             if the filter is not enabled.
     */
    public long write(final IRawStore store) {

        if (!dirty)
            throw new IllegalStateException();

        if (!enabled)
            throw new IllegalStateException();

        addr = store.write(ByteBuffer.wrap(SerializerUtil.serialize(this)));

        dirty = false;

        if (INFO)
            log.info("Wrote bloom filter: bytesOnDisk="
                    + store.getByteCount(addr) + ": " + filter);

        return addr;

    }

    /**
     * Disables the bloom filter associated with the index. A disabled bloom
     * filter can not be persisted and will not respond to queries or permit
     * mutations.
     * <p>
     * Note: This method is invoked by {@link BTree#insert(byte[], byte[])} when
     * the #of index entries exceeds the maximum allowed for the bloom filter.
     * At that point the {@link BTree} is dirty. {@link Checkpoint} will notice
     * that the bloom filter is disabled will write its address as 0L so the
     * bloom filter is no longer reachable from the post-checkpoint record.
     */
    final public void disable() {

        if (enabled) {

            enabled = false;

            // release the filter impl. this is often 1-10M of data!
            filter = null;

            if (log.isInfoEnabled())
                log.info("disabled.");

        }

    }

    private transient boolean enabled = true;

    /**
     * Return <code>true</code> unless the bloom filter has been disabled.
     * <p>
     * Note: A bloom filter may be disabled is the #of index entries has
     * exceeded the maximum desired error rate for the bloom filter.
     * 
     * @return iff the bloom filter is enabled.
     */
    final public boolean isEnabled() {

        return enabled;

    }

    private final static transient int VERSION0 = 0x0;
    
    /**
     * Note: On read, the {@link #addr} is set to <code>0L</code>, the
     * {@link #dirty} flag is cleared, and {@link #enabled} flag is set. It's
     * necessary to override serialization otherwise java will default the
     * {@link #enabled} flag to <code>false</code> when an object is
     * de-serialized - whoops!
     * 
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final int version = (int) LongPacker.unpackLong(in);

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        n = (int) LongPacker.unpackLong(in);

        maxN = (int) LongPacker.unpackLong(in);

        p = in.readDouble();
        
        filter = (it.unimi.dsi.util.BloomFilter2) in.readObject();

        dirty = false;

        addr = 0L;

        enabled = true;

    }

    /**
     * 
     * @param out
     * @throws IOException
     */
    public void writeExternal(ObjectOutput out) throws IOException {

        LongPacker.packLong(out, VERSION0);

        LongPacker.packLong(out, n);

        LongPacker.packLong(out, maxN);

        out.writeDouble( p );
        
        out.writeObject(filter);

    }

    public void falsePos() {
        
        counters.nbloomFalsePos++;
        
    }
    
    /**
     * Counters are not persistent.
     */
    public transient BloomFilterCounters counters = new BloomFilterCounters();

    /**
     * Counters for bloom filter access and notification of false positives.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo use long here and in {@link BTreeCounters}?
     */
    public static class BloomFilterCounters {

        /**
         * #of keys added to the bloom filter.
         */
        public int nbloomAdd = 0;
        
        /** #of keys tested by the bloom filter in contains/lookup(key). */
        public int nbloomTest = 0;

        /** #of keys rejected by the bloom filter in contains/lookup(key). */
        public int nbloomRejects = 0;

        /** #of false positives from the bloom filter in contains/lookup(key). */
        public int nbloomFalsePos = 0;

        /**
         * @todo summarize when the {@link BTreeCounters} are summarized.
         * 
         * @param o
         */
        public void add(BloomFilterCounters o) {

            nbloomAdd += o.nbloomAdd;
            nbloomTest += o.nbloomTest;
            nbloomRejects += o.nbloomRejects;
            nbloomFalsePos += o.nbloomFalsePos;
            
        }

        /**
         * The effective acceptance rate for the bloom filter (<code>1 - rejectRate</code>).
         * This is the rate at which the bloom filter reports that the key is in
         * the index. False positives occur when the filter accepts a key and
         * the index is consulted but the key is not found in the index.
         * 
         * @return The bloom filter acceptance rate.
         */
        public double getBloomAcceptRate() {

            return 1 - getBloomRejectionRate();
            
        }
        
        /**
         * The effective rejection rate (correct rejection rate) for the bloom
         * filter. Bloom filters do not make false negative errors, so any time
         * the filter rejects a key we assume that it was a correct rejection.
         * 
         * @return The bloom filter correct rejection rate.
         */
        public double getBloomRejectionRate() {

            if (nbloomTest == 0)
                return 0d;

            return (nbloomRejects / (double) nbloomTest);

        }

        /**
         * The effective error rate (false positive rate) for the bloom filter.
         * A false positive is an instance where the bloom filter reports that
         * the key is in the index but a read against the index demonstrates
         * that the key does not exist in the index. False positives are in the
         * nature of bloom filters and arise because keys may be hash equivalent
         * for the bloom filter.
         * 
         * @return The bloom filter error rate.
         */
        public double getBloomErrorRate() {

            if (nbloomTest == 0)
                return 0d;

            return (nbloomFalsePos / (double) nbloomTest);

        }

        /**
         * Return a {@link CounterSet} reporting on the various counters tracked
         * in the instance fields of this class.
         * 
         * FIXME Integrate with {@link BTreeCounters}. This needs to happen when we
         * setup the bloom filter, so that is in _reopen() for both
         * {@link BTree} and {@link IndexSegment}.
         */
        synchronized public ICounterSet getCounters() {

            if (counterSet == null) {

                counterSet = new CounterSet();

                counterSet.addCounter("#add", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nbloomAdd);
                    }
                });

                counterSet.addCounter("#test", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nbloomTest);
                    }
                });

                counterSet.addCounter("#reject", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nbloomRejects);
                    }
                });

                counterSet.addCounter("#falsePos", new Instrument<Integer>() {
                    protected void sample() {
                        setValue(nbloomFalsePos);
                    }
                });

                counterSet.addCounter("rejectRate", new Instrument<Double>() {
                    protected void sample() {
                        setValue(getBloomRejectionRate());
                    }
                });

                counterSet.addCounter("errorRate", new Instrument<Double>() {
                    protected void sample() {
                        setValue(getBloomErrorRate());
                    }
                });

            }
            
            return counterSet;
            
        }
        private CounterSet counterSet;
        
        /**
         * XML representation of the {@link CounterSet}.
         */
        public String toString() {

            // in XML.
            return getCounters().asXML(null/* filter */);

        }

        /**
         * Returns a human readable representation of the bloom filter
         * performance, including the correct rejection rate and the false
         * positive rate to date (or at least since the bloom filter was
         * read from the store).
         */
        public String getBloomFilterPerformance() {

            return "bloom filter" + //
            ": nadd=" + nbloomAdd + //
            ", ntest=" + nbloomTest + //
            ", nreject=" + nbloomRejects + //
            ", nfalsePos=" + nbloomFalsePos + //
            ", rejectRate=" + getBloomRejectionRate() + //
            ", errorRate=" + getBloomErrorRate()//
            ;

        }

    }

}
