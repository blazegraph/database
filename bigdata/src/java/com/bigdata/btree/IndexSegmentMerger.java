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
 * Created on Jan 5, 2007
 */

package com.bigdata.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegmentBuilder.NOPNodeFactory;
import com.bigdata.btree.IndexSegmentBuilder.SimpleLeafData;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Striterator;

/**
 * Class supporting a compacting merge of two btrees into a series of ordered
 * leaves written on a {@link TemporaryRawStore} in support of a compacting
 * merge of mutable {@link BTree}s and/or immutable {@link IndexSegment}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support merge rule that knows how to process timestamped entries and
 *       deletion markers and that removes deletion markers iff a full
 *       compacting merge is requested.
 * 
 * @todo Support deletion based on history policy (requires timestamps in the
 *       keys and explicit awareness of column store nature).
 * 
 * @todo factor out the merge rule into the {@link BTreeMetadata}. consider
 *       whether it should be maintained on the {@link IndexSegmentMetadata} as
 *       well.
 * 
 * @see {@link ReadOnlyFusedView}, which provides a dynamic view of two or more btrees.
 *      However, this class is more efficient when we are going to do a bulk
 *      merge operation since it performs the merge and computes the #of output
 *      entries in one pass.
 * 
 * @todo parameterize recordCompressor.
 */
public class IndexSegmentMerger {

    /**
     * Logger for building {@link IndexSegment}s.
     */
    protected static final Logger log = Logger
            .getLogger(IndexSegmentMerger.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * @todo parameterize useChecksum.
     */
    final boolean useChecksum = false;
    
    /**
     * Compacting merge of two btrees, writing the results onto a file. The file
     * data format is simply a sequence of leaves using the specified branching
     * factor. Leaves are filled from entries in an entry scan of the source
     * btrees. Each leaf is treated more or less as if it were a root leaf in
     * that it may be deficient and in that there is no node structure over the
     * leaves.
     * 
     * @param m
     *            The branching factor used by the leaves written on the
     *            temporary store.
     * @param in1
     *            A btree.
     * @param in2
     *            Another btree.
     */
    public IndexSegmentMerger(int m, AbstractBTree in1, AbstractBTree in2) throws IOException {
        
        if (m < AbstractBTree.MIN_BRANCHING_FACTOR)
            throw new IllegalArgumentException();
        
        if( in1 == null )
            throw new IllegalArgumentException();
        
        if( in2 == null )
            throw new IllegalArgumentException();
        
        tmpStore = new TemporaryRawStore();
        
        // reads leaves from the 1st btree.
//        itr1 = in1.leafIterator();
        itr1 = new SourceLeafIterator(in1);
        
        // reads leaves from the 2nd btree.
//        itr2 = in2.leafIterator();
        itr2 = new SourceLeafIterator(in2);

        // output leaf - reused for each leaf written.
        leaf = new SimpleLeafData(0, m);
        leaf.reset(m);
        
        // Used to serialize the leaves.
        nodeSer = new NodeSerializer(NOPNodeFactory.INSTANCE,
                m,
                0 /*initialBufferCapacity will be estimated*/,
                new IndexSegment.CustomAddressSerializer(WormAddressManager.INSTANCE),
                in1.nodeSer.keySerializer,
                in1.nodeSer.valueSerializer,
                null, //new RecordCompressor(),
                useChecksum
                );

    }

    /**
     * FIXME Support n-way merge.
     * 
     * @param m
     *            The output branching factor.
     * @param srcs
     *            The source indices in reverse timestamp order (by increasing
     *            age).
     *            
     * @throws IOException
     */
    public IndexSegmentMerger(int m, AbstractBTree[] srcs) throws IOException {
    
        // @todo validate sources for the same indexUUID.
        
        throw new UnsupportedOperationException();
    
    }

    /**
     * Used to buffer the output of the merge process.
     */
    final TemporaryRawStore tmpStore;
    
    /**
     * The address at which each leaf in written in the {@link #tmpStore}. The
     * entries in this list are ordered. The first entry is the first leaf
     * written, the second entry is the second leaf written, etc.
     */
    final Vector<Long> addrs = new Vector<Long>();
    
    final NodeSerializer nodeSer;

    final SourceLeafIterator itr1;
    final SourceLeafIterator itr2;
    
//    final Iterator itr1; // reads leaves from the 1st btree.
//    Leaf leaf1 = null; // current leaf in 1st btree.
//    int index1 = 0; // current entry index in current leaf of 1st btree.
//    boolean exhausted1 = false; // true iff the 1st iterator is exhausted.
//
//    final Iterator itr2; // reads leaves from the 2nd btree.
//    Leaf leaf2 = null; // current leaf in 2nd btree.
//    int index2 = 0; // current entry index in current leaf of 2nd btree.
//    boolean exhausted2 = false; // true iff the 2nd iterator is exhausted.

    /**
     * @todo this will need to be modified to use a key range so that we can
     *       evict an index partition worth of data at a time from a btree on
     *       the journal. Once we do that, is there any point to operating at
     *       the leaf iterator level vs just using an {@link EntryIterator}?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SourceLeafIterator {

        final Iterator itr; // reads leaves from the source btree.
        Leaf leaf = null; // current leaf in source btree.
        int index = 0; // current entry index in current leaf of source btree.
        boolean exhausted = false; // true iff the source btree is exhausted.

        public SourceLeafIterator(AbstractBTree src) {
            
            itr = src.leafIterator();
            
        }

        public Leaf next() {
            
            return (Leaf)itr.next();
            
        }
        
        /**
         * If the current leaf is not fully consumed then return immediately.
         * Otherwise read the next leaf from the source btree into {@link #leaf}.
         * If there are no more leaves available then {@link #exhausted} is set
         * to false. {@link #index} is reset to zero(0) in either case.
         * 
         * @return true unless this source btree is exhausted.
         */
        protected boolean nextLeaf() {
            if (index < leaf.nkeys)
                return !exhausted;
            index = 0;
            if (itr.hasNext()) {
                leaf = (Leaf) itr.next();
            } else {
                leaf = null;
                exhausted = true;
            }
            return !exhausted;
        }

    }
    
    /**
     * The output leaf. We reuse this for each leaf that we write. When the
     * output leaf is full we write it onto the {@link #tmpStore} and then reset
     * it so that it is ready to accept more keys.
     */
    final SimpleLeafData leaf;
    
    /**
     * The #of entries in the merged output file.
     */
    int nentries;

    /**
     * The #of leaves in the merged output file.
     */
    int nleaves;

    /**
     * The size of the largest serialized leaf in bytes.
     */
    int maxLeafBytes = 0;
    
    /**
     * The #of entries in the merged output file.
     */
    public int nentries() {
        
        return nentries;
        
    }
    
    /**
     * Do compacting merge. This scans leaves one at a time from each source
     * tree (note that the source trees may have different branching factors so
     * the leaves are not processed one for one). For the current entry in each
     * source leaf, a determination is made whether to skip the entry, output
     * the entry, or defer the entry in favor of the current entry in the other
     * source leaf -- all based on a caller supplied merge rule. As the output
     * {@link #leaf} fills up it is periodically written onto the output channel
     * and reset so that it may receive a new set of entries (note that there is
     * no correspondence required or assumed between the branching factor of the
     * output leaves and the branching factor of the inputs btrees). Once both
     * source trees are exhausted, the final contents (if any) of the output
     * {@link #leaf} are written out onto the output channel.
     * 
     * until either iterator is exhausted
     * 
     * if either source leaf is null, get the next leaf from that btree.
     * 
     * compare the current entry for leaf1 and leaf2. if the leaf1 has a lessor
     * key then drop the entry (if deleted or expired) or output the entry. if
     * leaf2 has a lessor key then drop the entry (if deleted or expired) or
     * output the entry. if the keys are the same, then "merge" the entry.
     * depending on the policy the merge will either keep the most recent entry,
     * keep the entry in the first source btree (which can be by convention the
     * more recent), or keep both entries (iff they are timestamped and the
     * expiration policy does not reject either version of that key).
     * 
     * once one of the iterators is exhausted, copy the remaining non-deleted
     * non-expired entries from the other iterator.
     * 
     * @param m
     *            The branching factor used in the leaves written onto the
     *            output file.
     * @param keyType
     *            The key type used by the source btrees and the output btree.
     * 
     * @return Iterator that visits the {@link ILeafData leaves} in the merged
     *         file in key order.
     * 
     * @todo in order to support expiration of old entries we will need to
     *       buffer entries for the same key. the buffer will need to be sorted
     *       by timestamp. either the buffer is flexible in size or its size
     *       (supports history policies based on age) is determined by the
     *       maximum #of retained entries for a key.
     */
    public MergedLeafIterator merge() throws IOException {

        final long begin = System.currentTimeMillis();
        
        /*
         * read the first leaf from both source btrees (it is an error if either
         * tree does not define a source leaf). until one tree or the other is
         * exhausted, apply the merge rule to the current entry for each tree.
         */
        
        itr1.leaf = itr1.next();
        
        itr2.leaf = itr2.next();
        
        while( !itr1.exhausted && ! itr2.exhausted ) {
            
            applyMergeRule();
            
        }
        
        // copy anything remaining in the 1st btree.
        while(!itr1.exhausted) {

            outputKey(itr1.leaf,itr1.index++);
            
            itr1.nextLeaf();
            
        }

        // copy anything remaining in the 2nd btree.
        while(!itr2.exhausted) {

            outputKey(itr2.leaf,itr2.index++);
            
            itr2.nextLeaf();
            
        }

        /*
         * if there is anything in the output leaf then we write it out now.
         */
        if (leaf.keys.getKeyCount() > 0) {
            
            writeLeaf(leaf);
            
        }

        /*
         * reporting.
         */
        {

            final long elapsed = System.currentTimeMillis() - begin;

            final long length = tmpStore.size();

            NumberFormat cf = NumberFormat.getNumberInstance();

            cf.setGroupingUsed(true);

            NumberFormat fpf = NumberFormat.getNumberInstance();

            fpf.setGroupingUsed(false);

            fpf.setMaximumFractionDigits(2);

            log.info("merge: " + elapsed + "ms, "
                    + cf.format(nentries) + " entries, "
                    + fpf.format(((double) length / Bytes.megabyte32)) + "MB");
            
        }

        return new MergedLeafIterator(tmpStore, addrs, leaf.m, nentries,
                nleaves, maxLeafBytes, nodeSer);
        
    }

    /**
     * A simple merge rule that combines entries in key order and when keys
     * compare as equal the value choosen is from the <em>first</em> btree
     * specified to the constructor.
     * <p>
     * The inputs to the merge rule are {@link #leaf1} at {@link #index1} and
     * {@link #leaf2} at {@link #index2}. The outputs are inserted into
     * {@link #leaf}, which is written onto the output channel using
     * {@link #writeLeaf(SimpleLeafData)} each time it fills up and then
     * {@link SimpleLeafData#reset(int)} to accept more entries.
     * 
     * @todo choose the more recent version for isolation purposes (greater
     *       timestamp).
     * 
     * @todo do a column store version in which the timestamp of interest is
     *       part of the key and retain N such keys (or keys no older than N
     *       units).
     */
    protected void applyMergeRule() throws IOException {
        
        assert !itr1.exhausted && !itr2.exhausted;

        /*
         * @todo this does unnecessary allocation if the IKeyBuffers are
         * ImmutableKeyBuffers.  If that is the case (and I think that it
         * is) then we should write a special purpose comparator that can
         * do less allocation and less search for this case.
         */ 
        byte[] key1 = itr1.leaf.keys.getKey(itr1.index);
        
        byte[] key2 = itr2.leaf.keys.getKey(itr2.index);
        
        int ret = BytesUtil.compareBytes(key1,key2);
        
        if(ret==0) {
            
            /* 
             * prefer source one in case of a tie.
             */
            
            outputKey(itr1.leaf,itr1.index++);
            
            itr2.index++;
            
            itr1.nextLeaf();
            itr2.nextLeaf();

        } else if(ret<0) {

            outputKey(itr1.leaf,itr1.index++);
            
            itr1.nextLeaf();
            
        } else {

            outputKey(itr2.leaf,itr2.index++);
            
            itr2.nextLeaf();
            
        }
        
    }
    
//    /**
//     * If the current leaf is not fully consumed then return immediately.
//     * Otherwise read the next leaf from the 1nd source btree into
//     * {@link #leaf1}. If there are no more leaves available then
//     * {@link #exhausted1} is set to false. {@link #index1} is reset to zero(0)
//     * in either case.
//     * 
//     * @return true unless this source btree is exhausted.
//     */
//    protected boolean nextLeaf1() {
//        if(index1<leaf1.nkeys) return !exhausted1;
//        index1 = 0;
//        if (itr1.hasNext()) {
//            leaf1 = (Leaf) itr1.next();
//        } else {
//            leaf1 = null;
//            exhausted1 = true;
//        }
//        return !exhausted1;
//    }
//
//    /**
//     * If the current leaf is not fully consumed then return immediately.
//     * Otherwise read the next leaf from the 2nd source btree into
//     * {@link #leaf2}. If there are no more leaves available then
//     * {@link #exhausted2} is set to false. {@link #index2} is reset to zero(0)
//     * in either case.
//     * 
//     * @return true unless this source btree is exhausted.
//     */
//    protected boolean nextLeaf2() {
//        if(index2<leaf2.nkeys) return !exhausted2;
//        index2 = 0;
//        if (itr2.hasNext()) {
//            leaf2 = (Leaf) itr2.next();
//        } else {
//            leaf2 = null;
//            exhausted2 = true;
//        }
//        return !exhausted2;
//    }

    /**
     * Output the current key from the specified source leaf onto the output
     * {@link #leaf}. If the output leaf becomes full then it is written onto
     * the output channel.
     * 
     * @param src
     *            the source leaf.
     * 
     * @param srcpos
     *            the index of the entry in that source leaf to be output
     *            (origin zero, relative to the first entry in the leaf).
     */
    protected void outputKey(ILeafData src,int srcpos) throws IOException {

        if(DEBUG) log.debug("#leavesWritten=" + nleaves + ", src="
                + (src == itr1.leaf ? "leaf1" : "leaf2") + ", srcpos=" + srcpos);
        
        MutableKeyBuffer keys = (MutableKeyBuffer) leaf.keys;
        
        assert keys.nkeys < leaf.max;
        
        /*
         * copy source key into next position on the output leaf.
         * 
         * @todo This makes a copy of the key, but we are also making a copy of
         * the key to support comparison of keys in applyMergeRule(). Factor out
         * that redundency.
         */
        keys.keys[keys.nkeys] = src.getKeys().getKey(srcpos);
        //leaf.copyKey(keys.nkeys, src, srcpos);
        
        leaf.vals[keys.nkeys] = src.getValues()[srcpos];
        
        keys.nkeys++;

        if( keys.nkeys == leaf.max ) {

            // write the leaf onto the output channel.
            writeLeaf(leaf);

            // reset the leaf to receive more entries.
            leaf.reset(leaf.m);
            
        }
     
        nentries++;
        
    }
    
    /** 
     * write the leaf onto the output channel.
     */
    protected void writeLeaf(SimpleLeafData leaf) throws IOException {
        
        ByteBuffer buf = nodeSer.putNodeOrLeaf( leaf );

        // write the record
        
        final int nbytes = buf.limit();
        
        final long addr = tmpStore.write(buf);

        addrs.add(addr);
        
        System.err.print(">"); // wrote a leaf.

        nleaves++;
        
        if( nbytes > maxLeafBytes ) {
            
            maxLeafBytes = nbytes;
            
        }
        
    }

    /**
     * Iterator visits the leaves in the merged file in key order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MergedLeafIterator implements Iterator<ILeafData> {

        public final TemporaryRawStore tmpStore;
        public final Vector<Long> addrs;
        public final int m;
        public final int nentries;
        public final int nleaves;
        public final int maxLeafBytes;
        protected final NodeSerializer nodeSer;
        
        private int leafIndex = 0;
        
        /**
         * Used to note when the iterator is exhausted and delete the temporary
         * file.
         */
        private boolean exhausted;
        
        /**
         * @param tmpStore
         * @param addrs
         *            An ordered list of the addresses at which the leaves may
         *            be found in <i>tmpStore</i>.
         * @param m
         * @param nentries
         * @param nleaves
         * @param maxLeafBytes
         * @param nodeSer
         */
        public MergedLeafIterator(TemporaryRawStore tmpStore,
                Vector<Long> addrs, int m, int nentries, int nleaves,
                int maxLeafBytes, NodeSerializer nodeSer) throws IOException {
            
            this.tmpStore = tmpStore;
            this.addrs = addrs;
            this.m = m;
            this.nentries = nentries;
            this.nleaves = nleaves;
            this.maxLeafBytes = maxLeafBytes;
            this.nodeSer = new NodeSerializer(new SimpleNodeFactory(), m,
                    maxLeafBytes, nodeSer.addrSerializer,
                    nodeSer.keySerializer, nodeSer.valueSerializer,
                    nodeSer.recordCompressor, nodeSer.useChecksum);
            
        }
       
        /**
         * Closes and deletes the backing store.
         * <p>
         * This is invoked automatically when the iterator is exhausted. 
         * 
         * @throws IOException
         */
        public void close() {

            log.info("Closing temporary store");
            
            tmpStore.close();

        }

        /**
         * Test whether more leaves are available. Automatically closes out the
         * backing buffer when all leaves have been processed.
         */
        public boolean hasNext() {
        
            if(exhausted) return false;
            
            exhausted = leafIndex >= nleaves;

            if(exhausted) {
                
                close();
                
            }
            
            return ! exhausted;
            
        }

        public ILeafData next() {

            if (!hasNext())
                throw new NoSuchElementException();

            // the address of the next leaf.
            final long addr = addrs.get(leafIndex);

            /*
             * Note: this is using a nodeSer whose node factory does not require
             * a non-null btree reference.
             */

            ByteBuffer buf = tmpStore.read(addr);

            ILeafData leaf = nodeSer.getLeaf(null/* btree */, addr, buf);

            leafIndex++;

            System.err.print("<"); // read a leaf.

            return leaf;

        }

        /**
         * Not supported.
         */
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        /**
         * Factory does not support node or leaf creation.
         */
        protected static class SimpleNodeFactory implements INodeFactory {

            public static final INodeFactory INSTANCE = new SimpleNodeFactory();

            private SimpleNodeFactory() {
            }

            public ILeafData allocLeaf(IIndex btree, long addr,
                    int branchingFactor, IKeyBuffer keys, Object[] values) {

                return new LeafData(branchingFactor, keys, values);

            }

            public INodeData allocNode(IIndex btree, long addr,
                    int branchingFactor, int nentries, IKeyBuffer keys,
                    long[] childAddr, int[] childEntryCounts) {

                throw new UnsupportedOperationException();

            }

        }

    }

    /**
     * A class that can be used to (de-)serialize the data for a leaf without
     * any of the logic for operations on the leaf.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    final private static class LeafData implements ILeafData {

        final int m;
        /**
         * Note: this is redundent with {@link IKeyBuffer#getKeyCount()} but is
         * final and is initialized in the constructor.
         */
        final int nkeys;
        final IKeyBuffer keys;
        final Object[] vals;

        public int getBranchingFactor() {
            return m;
        }

        public int getKeyCount() {
            return nkeys;
        }

        public IKeyBuffer getKeys() {
            return keys;
        }

        public LeafData(int m, IKeyBuffer keys, Object[] vals) {

            this.m = m;
            this.nkeys = keys.getKeyCount();
            this.keys = keys;
            this.vals = vals;
            
        }
        
        public int getValueCount() {
            
            return nkeys;
            
        }

        public Object[] getValues() {
            
            return vals;
            
        }

        public boolean isLeaf() {
            
            return true;
            
        }

        public int getEntryCount() {
            
            return nkeys;
            
        }

    }
    
    /**
     * Exposes a {@link MergedLeafIterator} as an {@link IEntryIterator}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MergedEntryIterator extends Striterator implements IEntryIterator {
        
        /**
         * The key-value for each entry are set as a side-effect on a private
         * {@link Tuple} field so that this class can implement
         * {@link IEntryIterator}.
         * 
         * @see EntryIterator#EntryIterator(Leaf, Tuple)
         */
        final Tuple tuple = new Tuple();
        
        public MergedEntryIterator(MergedLeafIterator postOrderIterator) {
            
            super(postOrderIterator);
            
            addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /**
                 * Expand the entries in each leaf.
                 */
                protected Iterator expand(Object childObj) {

                    LeafData leaf = (LeafData) childObj;

                    if (leaf.nkeys == 0) {

                        return EmptyEntryIterator.INSTANCE;

                    }
                     
                    return new LeafDataEntryIterator(leaf,tuple);

                }

            });
        
        }

        public MergedEntryIterator(MergedLeafIterator postOrderIterator,
                final byte[] fromKey, final byte[] toKey) {
            
            super(postOrderIterator);
            
            addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /**
                 * Expand the entries in each leaf.
                 */
                protected Iterator expand(Object childObj) {

                    /*
                     * A child of this node.
                     */
                    LeafData leaf = (LeafData) childObj;

                    if (leaf.nkeys == 0) {

                        return EmptyEntryIterator.INSTANCE;

                    }
                     
                    return new LeafDataEntryIterator(leaf,tuple,fromKey,toKey);

                }

            });
        
        }

        public byte[] getKey() {
            
            return tuple.key;
            
        }

        public Object getValue() {
            
            return tuple.val;
            
        }

    }
    
    /**
     * Visits the values of a {@link Leaf} in the external key ordering. There is
     * exactly one value per key for a leaf node.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class LeafDataEntryIterator implements IEntryIterator {

        private final LeafData leaf;

        private final Tuple tuple;

        private int index = 0;

        private int lastVisited = -1;

//        private final byte[] fromKey;
//        
//        private final byte[] toKey;

        // first index to visit.
        private final int fromIndex;

        // first index to NOT visit.
        private final int toIndex;
        
        public LeafDataEntryIterator(LeafData leaf) {

            this( leaf, null );
            
        }

        public LeafDataEntryIterator(LeafData leaf, Tuple tuple ) {

            this(leaf,tuple,null,null);
            
        }
        
        /**
         * 
         * @param leaf
         *            The leaf whose entries will be traversed.
         * @param tuple
         *            Used to hold the output values.
         * @param fromKey
         *            The first key whose entry will be visited or <code>null</code>
         *            if the lower bound on the key traversal is not constrained.
         * @param toKey
         *            The first key whose entry will NOT be visited or
         *            <code>null</code> if the upper bound on the key traversal is
         *            not constrained.
         * 
         * @exception IllegalArgumentException
         *                if fromKey is given and is greater than toKey.
         */
        public LeafDataEntryIterator(LeafData leaf, Tuple tuple, byte[] fromKey, byte[] toKey) {

            assert leaf != null;

            this.leaf = leaf;
            
            this.tuple = tuple; // MAY be null.

//            this.fromKey = fromKey; // may be null (no lower bound).
//            
//            this.toKey = toKey; // may be null (no upper bound).

            { // figure out the first index to visit.

                int fromIndex;

                if (fromKey != null) {

                    fromIndex = leaf.keys.search(fromKey);

                    if (fromIndex < 0) {

                        fromIndex = -fromIndex - 1;

                    }

                } else {

                    fromIndex = 0;

                }

                this.fromIndex = fromIndex;

            }

            { // figure out the first index to NOT visit.

                int toIndex;

                if (toKey != null) {

                    toIndex = leaf.keys.search(toKey);

                    if (toIndex < 0) {

                        toIndex = -toIndex - 1;

                    }

                } else {

                    toIndex = leaf.nkeys;

                }

                this.toIndex = toIndex;

            }

            if (fromIndex > toIndex) {
                
                throw new IllegalArgumentException("fromKey > toKey");
                
            }
            
            // starting index is the lower bound.
            index = fromIndex;
            
        }

        public boolean hasNext() {

            return index >= fromIndex && index < toIndex;

        }

        public Object next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            lastVisited = index++;
            
            if( tuple != null ) {

                /*
                 * eagerly set the key/value on the tuple for a side-effect style
                 * return.
                 */
                tuple.key = leaf.keys.getKey(lastVisited);
                
                tuple.val = leaf.vals[lastVisited];
                
                return tuple.val;
                
            }
            
            return leaf.vals[lastVisited];
            
        }

        public Object getValue() {
            
            if( lastVisited == -1 ) {
                
                throw new IllegalStateException();
                
            }
            
            return leaf.vals[lastVisited];
            
        }
        
        public byte[] getKey() {
            
            if( lastVisited == -1 ) {
                
                throw new IllegalStateException();
                
            }
            
            return leaf.keys.getKey(lastVisited);
            
        }
        
        /**
         * @exception UnsupportedOperationException
         */
        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

}
