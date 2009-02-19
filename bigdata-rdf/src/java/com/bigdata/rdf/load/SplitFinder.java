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
 * Created on Feb 19, 2009
 */

package com.bigdata.rdf.load;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.LexiconKeyOrder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.resources.IPartitionIdFactory;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.Split;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.util.NV;

/**
 * Utility class pre-parses a bunch of RDF data into a "terms" and "preds" index
 * and then recommends {@link Split}s for the {@link AbstractTripleStore}
 * indices. This approach takes advantage of the "schema" for the RDF DB to
 * distribute index partitions across a federation in something which approaches
 * an optimal manner. A utility method is defined to create an
 * {@link AbstractTripleStore} using the generated {@link Split}s where the
 * index partitions are distributed across the discovered services in the
 * federation.
 * <p>
 * The RDF DB is comprised of 5 core indices. There are two for the lexicon
 * (TERM2ID and ID2TERM), and three statement indices: SPO, POS, and OSP. The
 * "pre-parse" loads data into a {@link LocalTripleStore} backed by a
 * {@link BufferMode#Temporary} journal which is then used to compute the
 * separator keys for each index partition. With the separator keys, we are then
 * able to create a {@link ScaleOutTripleStore} whose indices are pre-split and
 * allocated on the desired #of discovered {@link DataService}s.
 * <p>
 * The split points are computed as follows:
 * <dl>
 * 
 * <dt>TERM2ID</dt>
 * 
 * <dd>The {@link Split}s for the "terms" index directly give the separator
 * keys for the TERM2ID index.</dd>
 * 
 * <dt>POS</dt>
 * 
 * <dd>The POS split points need to be choosen based on the weighted
 * distribution. Note that we already know the #of distinct statements in the
 * {@link LocalTripleStore}. We want to have N index partitions. So we want to
 * assign 1/N statements to each POS index partition.
 * <p>
 * We scan the POS index, counting the of #of statements visited for each
 * distinct predicate. This data is stored in an list of
 * (term,termId,partId,rangeCount) tuples. Once the scan is complete we resolve
 * the term for each termId and then sort the array lexically by term. Next, for
 * each element of the array, we resolve the partition identifier of the TERM2ID
 * index partition which <em>would</em> assign the term identifier for that
 * predicate (based on the splits that we choose for the TERM2ID index).
 * <p>
 * Finally, we choose the separator keys for the POS index finding a set of N
 * ordered buckets of predicates whose range counts are roughly equal. If the
 * last POS index partition would have too small a range count then we generate
 * N-1 separator keys instead.
 * 
 * <dt>ID2TERM</dt>
 * 
 * <dd>The index partition identifier is the high word of the term identifiers
 * assigned by the TERM2ID index. The ith ID2TERM separator key is formed from
 * the partition identifier of i+1 TERM2ID index partition as:
 * 
 * <pre>
 * separatorKey[i] = keyBuilder.reset().append(partitionId(i + 1)).append(0)
 *         .getKey();
 * </pre>
 * 
 * Note that the partitionId plus the 0 (for the partition local counter value)
 * give us one 64-bit "term identifier".
 * 
 * </dd>
 * 
 * <dt>SPO</dt>
 * 
 * <dd> This is handled in much the same manner as ID2TERM.
 * 
 * <pre>
 * separatorKey[i] = keyBuilder.reset().append(partitionId(i + 1)).append(0)
 *         .append(0L).append(0L).getKey();
 * </pre>
 * 
 * Note that the partitionId plus the 0 (for the partition local counter value)
 * give us one 64-bit "term identifier". We append two more ZEROs (0L) in order
 * to get the full SPO key.
 * 
 * </dd>
 * 
 * <dt>OSP</dt>
 * 
 * <dd>Uses the same separator keys as the SPO index.</dd>
 * 
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo handle the full text index.
 * 
 * @todo I rather doubt that the justifications mechanism can scale-out. Instead
 *       do a magic sets integration which will eliminate the requirement for
 *       the justifications index and allow an option for eager, incremental, or
 *       query time closure.
 */
public class SplitFinder {

    protected static final Logger log = Logger.getLogger(SplitFinder.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * The desired #of index partitions.
     */
    private final int nsplits;
    
    /**
     * Used to collect the sample data on which we will base the split
     * computation.
     */
    private final LocalTripleStore tempStore;

    /**
     * Mock implementation assigns index partitions from a counter beginning
     * with ZERO (0), which is the first legal index partition identifier. This
     * assumes that each operation is for the same index (the name parameter is
     * ignored).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MockPartitionIdFactory implements IPartitionIdFactory {

        private int i = 0;

        public int nextPartitionId(String nameIsIgnored) {

            return i++;

        }

    }
    
    /**
     * Note: This class is designed for use with scale-out federations. In order
     * for the generated splits to be correct it MUST use the same properties
     * that will be configured for the scale-out triple store, which is the
     * class is tightly integrated with the {@link JiniFederation}.
     * 
     * @param properties
     *            The properties for the scale-out triple store.
     * @param nsplits
     *            The desired number of index partitions.
     * @param npartitions
     * @throws IOException
     */
    public SplitFinder(final Properties properties, final int nsplits)
            throws IOException {

        if (properties == null)
            throw new IllegalArgumentException();

        if (nsplits <= 0)
            throw new IllegalArgumentException();

        this.nsplits = nsplits;

        {

            // protect caller's object against writes.
            final Properties p = new Properties(properties);
            
            p.setProperty(com.bigdata.journal.Options.FILE, File
                    .createTempFile("SplitFinder", ".jnl").toString());
            
            p.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                    BufferMode.Temporary.toString());

            // we get by with just the SPO index and the lexicon.
            p.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");

            /*
             * Turn off various options that we do not need for the tempStore.
             * We will not need these options even if they are in use for the
             * scale-out triple store.
             */
            p.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS,
                    "false");
            p.setProperty(AbstractTripleStore.Options.TEXT_INDEX, "false");
            p.setProperty(AbstractTripleStore.Options.STORE_BLANK_NODES,
                    "false");
            p.setProperty(AbstractTripleStore.Options.JUSTIFY, "false");

            // create our transient triple store.
            tempStore = new LocalTripleStore( p );
            
        }
        
    }

    /**
     * Encapsulates the set of {@link Split}s choosen for each index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Splits {
       
        public Split[] term2IdSplits;
        public Split[] id2TermSplits;
        public Split[] spoSplits;
        public Split[] ospSplits;
        public Split[] posSplits;
        
    }
    
    /**
     * Find the splits for each index based on the loaded data.
     */
    public Splits findSplits() {

        final Splits splits = new Splits();
        
        splits.term2IdSplits = findSplitsTerm2ID();
        if (INFO) {
            for (Split split : splits.term2IdSplits) {

                log.info(split.toString());

            }
        }
        
        splits.id2TermSplits = findSplitsID2Term(splits.term2IdSplits);
        if (INFO) {
            for (Split split : splits.id2TermSplits) {

                log.info(split.toString());

            }
        }
        
        splits.spoSplits = findSplitsSPO(splits.term2IdSplits);
        if (INFO) {
            for (Split split : splits.spoSplits) {

                log.info(split.toString());

            }
        }
        
        // The OSP splits are exactly the same as the SPO splits.
        splits.ospSplits = splits.spoSplits;
        
        splits.posSplits = findSplitsPOS(splits.term2IdSplits);
        if (INFO) {
            for (Split split : splits.posSplits) {

                log.info(split.toString());

            }
        }

        return splits;
        
    }

    protected Split[] findSplitsTerm2ID() {

        /*
         * Grab the unisolated index (the object is otherwise wrapped with an
         * UnisolatedReadWriteIndex which provides thread safety).
         */ 
        final BTree ndx = (BTree) tempStore.getIndexManager().getIndex(
                tempStore.getNamespace() + "."
                        + LexiconRelation.NAME_LEXICON_RELATION + "."
                        + LexiconKeyOrder.TERM2ID);

        /*
         * Note: We need the pmd for the split handler since it checks this
         * stuff. However, the actual pmd when we create the index partitions
         * needs to be the one for the scale-out index.
         */
        ndx
                .getIndexMetadata()
                .setPartitionMetadata(
                        newMockPartitionMetadata(0/* pid */,
                                new byte[0]/* leftSeparator */, null/* rightSeparator */));
        
        final long rangeCount = ndx.getEntryCount();

        final ISplitHandler adjustedSplitHandler = ((DefaultSplitHandler) ndx
                .getIndexMetadata().getSplitHandler())
                .getAdjustedSplitHandlerForEqualSplits(nsplits, rangeCount);

        return adjustedSplitHandler
                .getSplits(new MockPartitionIdFactory(), ndx);
        
    }

    /**
     * Helper for creating {@link LocalPartitionMetadata} instances which exist
     * solely to define the split points - this IS NOT used when we create the
     * partition metadata to register the scale-out indices.
     */
    LocalPartitionMetadata newMockPartitionMetadata(final int partitionId,
            final byte[] leftSeparator, final byte[] rightSeparator) {

        return new LocalPartitionMetadata(//
                partitionId,
                -1, // sourcePartitionId (iff move)
                leftSeparator,
                rightSeparator,
                new IResourceMetadata[]{
                        tempStore.getIndexManager().getResourceMetadata()
                },
                "{NoHistory}"
                );
        
    }
    
    /**
     * A {@link Split} is created for each TERM2ID {@link Split}. For ID2TERM
     * index partition <code>i</code>, then rightSeparator is based on the
     * partitionId assigned to the <code>i+1</code> TERM2ID index partition.
     * The leftSeparator of the first index partition is always an empty byte[].
     * The rightSeparator of the last index partition is always
     * <code>null</code>.
     * 
     * @param term2IdSplits
     *            The TERM2ID index partition splits.
     * 
     * @return The ID2TERM index partition splits.
     */
    protected Split[] findSplitsID2Term(final Split[] term2IdSplits) {

        int partitionId = 0;
        
        final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

        final Split[] splits = new Split[term2IdSplits.length];

        byte[] leftSeparator = new byte[0];

        for (int i = 0; i < term2IdSplits.length - 1; i++) {

            // the partitionId for the _next_ TERM2ID index partition.
            final int nextPartitionId = term2IdSplits[i + 1].pmd
                    .getPartitionId();

            /*
             * The key is just the term identifier. The high word of the term
             * identifier is the partition identifier of the TERM2ID index
             * partition which assigned that term identifier. The low word is a
             * 32-bit index partition local counter.
             */
            final byte[] rightSeparator = keyBuilder.reset().append(
                    nextPartitionId).append(0/* partitionLocalCounter */)
                    .getKey();

            splits[i] = new Split(newMockPartitionMetadata(partitionId++,
                    leftSeparator, rightSeparator));

            leftSeparator = rightSeparator;

        }

        // the last index partition.
        splits[term2IdSplits.length - 1] = new Split(newMockPartitionMetadata(
                ++partitionId, leftSeparator, null/* rightSeparator */));
        
        return splits;
        
    }

    /**
     * Note: This is very similar to {@link #findSplitsID2Term(Split[])}.
     * <p>
     * Note: Since the SPO and OSP index partitions are defined in exactly the
     * same manner we use the value returned by this method for both of them.
     * 
     * @param term2IdSplits
     *            The TERM2ID index partition splits.
     * 
     * @return The ID2TERM index partition splits.
     */
    protected Split[] findSplitsSPO(final Split[] term2IdSplits) {

        int partitionId = 0;
        
        final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

        final Split[] splits = new Split[term2IdSplits.length];

        byte[] leftSeparator = new byte[0];

        for (int i = 0; i < term2IdSplits.length - 1; i++) {

            // the partitionId for the _next_ TERM2ID index partition.
            final int nextPartitionId = term2IdSplits[i + 1].pmd
                    .getPartitionId();

            /*
             * The key is the {s,p,o} (or the {o,s,p} for the OSP index). The
             * separator is formed by combining the partitionId for the next
             * TERM2ID index partition with a local counter value of ZERO (0)
             * and then appending ZERO (0L) for each of the other two long
             * values in the key.
             */
            final byte[] rightSeparator = keyBuilder.reset().append(
                    nextPartitionId).append(0/* partitionLocalCounter */)
                    .append(0L/* p */).append(0L/* o */).getKey();

            splits[i] = new Split(newMockPartitionMetadata(++partitionId,
                    leftSeparator, rightSeparator));

            leftSeparator = rightSeparator;

        }

        // the last index partition.
        splits[term2IdSplits.length - 1] = new Split(newMockPartitionMetadata(
                ++partitionId, leftSeparator, null/* rightSeparator */));
        
        return splits;

    }

    /**
     * Helper class for {@link SplitFinder#findSplitsSPO(Split[])}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class PredStat {
        
        /**
         * The term identifier for the predicate as assigned by the
         * {@link LocalTripleStore}.
         */
        public final long termId;

        /**
         * The #of statements that we encounter in a POS index scan of the
         * {@link LocalTripleStore} which use this predicate (have the same term
         * identifier in the predicate position).  This is the "sample" for that
         * predicate.
         */
        public int rangeCount;
        
        /**
         * The lexical form of the predicate (it will always be a URI). This is
         * resolved after the POS index scan.
         */
        public URI term;
        
        /**
         * The unsigned byte[] sort key generated from the {@link #term}.
         */
        public byte[] sortKey;
        
        public void setTerm(final URI term, final byte[] sortKey) {
            
            if (term == null)
                throw new IllegalArgumentException();

            if (sortKey == null)
                throw new IllegalArgumentException();

            if (this.term != null)
                throw new IllegalArgumentException();

            this.term = term;
            
            this.sortKey = sortKey;
            
        }
        
        /**
         * The partition identifier of the TERM2ID index partition which WOULD
         * assign the term identifier to this predicate based on its lexical
         * form and the {@link Split}s which we have already computed for the
         * TERM2ID index.
         * <p>
         * Note: we are going to make split choices for the POS index at the
         * granuality of this partition identifier. One consequence of this is
         * that each POS index partition will begin and end right on a predicate
         * boundary. Another consequence is that some POS index partitions may
         * accept more statements, potentially many more, than others. That is
         * Ok. The POS index partitions will be re-split, moved around, etc.
         * when the scale-out triple store is loaded with its data. We are just
         * making a set of good initial choices here.
         */
        public int partId;
        
        public PredStat(final long termId) {

            this.termId = termId;

            this.hashCode = (int) (termId ^ (termId >>> 32));
            
        }
        
        public String toString() {
            
            return getClass().getSimpleName()+//
            "{ termId="+termId+//
            ", partId="+partId+//
            ", rangeCount="+rangeCount+//
            ", term="+term+//
            ", key="+BytesUtil.toString(sortKey)+//
            "}";
            
        }
        
        public boolean equals(Object o) {
            
            return this.termId == ((PredStat)o).termId;
            
        }
        
        public int hashCode() {
            
            return hashCode; 
            
        }
        private final int hashCode;

    };

    /**
     * Places {@link PredStat}s into order by descending frequency.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class FrequencyComparator implements Comparator<PredStat> {

        public int compare(PredStat arg0, PredStat arg1) {

            if (arg0.rangeCount > arg1.rangeCount) {
                return -1;
            } else if (arg0.rangeCount < arg1.rangeCount) {
                return 1;
            } else {
                return 0;
            }
            
        }
        
    }

    /**
     * Places {@link PredStat}s into unsigned byte[] order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class SortKeyComparator implements Comparator<PredStat> {

        public int compare(PredStat arg0, PredStat arg1) {

            return BytesUtil.compareBytes(arg0.sortKey, arg1.sortKey);
            
        }
        
    }
    
    /**
     * Return the index of the {@link Split} that spans the <i>key</i>.
     * 
     * @param key
     *            An unsigned byte[] key.
     * @param splits
     *            An ordered array of {@link Split}s.
     * @param lastIndex
     *            The last index returned by this method -or- ZERO (0) to search
     *            from the beginning of the <i>splits</i> array.
     * 
     * @return The index of split into which the key would fall.
     */
    private int find(final byte[] key, final Split[] splits, final int lastIndex) {

        if (key == null)
            throw new IllegalArgumentException();
        
        if (splits == null)
            throw new IllegalArgumentException();
        
        if (splits.length == 0)
            throw new IllegalArgumentException();

        for (int i = lastIndex; i < splits.length; i++) {
            
            final IPartitionMetadata pmd = splits[i].pmd;
            
            final byte[] leftSeparator = pmd.getLeftSeparatorKey();

            final byte[] rightSeparator = pmd.getRightSeparatorKey();

            if (BytesUtil.compareBytes(key, leftSeparator) < 0) {

                /*
                 * The key MUST be GTE the leftSeparator. There is something
                 * wrong if it is LT then leftSeparator. For example, the search
                 * index was too high or the Splits[] are not ordered, etc.
                 */

                throw new AssertionError();

            }

            if (rightSeparator == null) {

                /*
                 * The last index partition has a null for its right separator
                 * and accepts any key GTE its leftSeparator.
                 */
                
                if (i != splits.length - 1) {

                    // this is not the last of the Split[]s.
                    throw new AssertionError();

                }

                // accept the key.
                return i;

            }
            
            if (BytesUtil.compareBytes(key, rightSeparator) >= 0) {

                /*
                 * The key is GTE to the rightSeparastor for this index
                 * partition so we need to look in the next index partition.
                 */

                if (INFO)
                    log.info("Advancing splitIndex=" + (i + 1));
                
                continue;
                
            }
            
            return i;
            
        }
        
        // the last rightSeparator was probably not null.
        throw new AssertionError();
        
    }

    /**
     * Choose the POS index split points.
     * <p>
     * The POS split points need to be choosen based on the weighted
     * distribution. Note that we already know the #of distinct statements in
     * the {@link LocalTripleStore}. We want to have N index partitions. So we
     * want to assign 1/N statements to each POS index partition.
     * <p>
     * We scan the POS index, counting the of #of statements visited for each
     * distinct predicate. This data is stored in a set of {@link PredStat}
     * tuples. Once the scan is complete we resolve the term for each termId and
     * then sort the array lexically by term. Next, for each element of the
     * array, we resolve the partition identifier of the TERM2ID index partition
     * which <em>would</em> assign the term identifier for that predicate
     * (based on the splits that we choose for the TERM2ID index).
     * <p>
     * Finally, we choose the separator keys for the POS index finding a set of
     * N ordered buckets of predicates whose range counts are roughly equal. If
     * the last POS index partition would have too small a range count then we
     * generate N-1 separator keys instead.
     * 
     * @param term2IdSplits
     *            The {@link Split} already choosen for the TERM2ID index.
     * 
     * @return The choosen {@link Split}s.
     */
    protected Split[] findSplitsPOS(final Split[] term2IdSplits) {

        final int nsplits = term2IdSplits.length;
        
        // @todo configure capacity
        final Long2ObjectMap<PredStat> preds = new Long2ObjectOpenHashMap<PredStat>(
                500000/* initialCapacity */);

        /*
         * Find the distinct predicates and their range counts.
         */
        PredStat maxFreq = null;
        long maxRangeCount = 0;
        {

            // this will be an SPO index scan.
            final IChunkedOrderedIterator<ISPO> itr = tempStore
                    .getSPORelation().getAccessPath(0, 0, 0).iterator();

            while (itr.hasNext()) {

                final ISPO spo = itr.next();

                final long p = spo.p();

                PredStat stat = preds.get(spo.p());

                if (stat == null) {

                    stat = new PredStat(p);

                    preds.put(p, stat);

                }

                stat.rangeCount++;
                
                if (stat.rangeCount > maxRangeCount) {

                    maxRangeCount = stat.rangeCount;
                    
                    maxFreq = stat;
                    
                }

            }

            if (INFO)
                log
                        .info("Scan reveals "
                                + preds.size()
                                + " distinct predicates.  The most frequent predicate occurs "
                                + maxRangeCount + " times");
            
        }

        // Convert to an array.
        final PredStat[] a = preds.values().toArray(new PredStat[0]);

        /*
         * Efficiently resolve the predicate term identifiers to the
         * corresponding URIs
         */
        {

            final Term2IdTupleSerializer tupleSer = (Term2IdTupleSerializer) tempStore
                    .getLexiconRelation().getTerm2IdIndex().getIndexMetadata()
                    .getTupleSerializer();
            
            final ArrayList<Long> ids = new ArrayList<Long>(preds.size());
            
            for(PredStat t : preds.values()) {
                
                ids.add(t.termId);

            }

            final Map<Long/* id */, BigdataValue/* Term */> terms = tempStore
                    .getLexiconRelation().getTerms(ids);

            for (PredStat t : preds.values()) {

                final URI term = (URI) terms.get(t.termId);

                final byte[] sortKey = tupleSer.serializeKey(term);

                t.setTerm(term, sortKey);

                if (t.term == null) {

                    throw new AssertionError("No such term? termId=" + t.termId);

                }

            }

            if (INFO)
                log.info("The most frequent predicate is: " + maxFreq.term);

            if (DEBUG) {

                /*
                 * Show in descending frequency order.
                 * 
                 */
                
                // note: clone to avoid side effects!
                final PredStat[] b = a.clone();
                
                Arrays.sort(b, new FrequencyComparator());

                for (int i = 0; i < b.length; i++) {

                    log.info(b[i]);
                    
                }

            }

        }

        /*
         * Put the array into ascending sort key order and assign the
         * partitionId which would be used to assign each predicate its term
         * identifier.
         */
        {

            // put into sort key order.
            Arrays.sort(a, new SortKeyComparator());

            int lastSplitIndex = 0;
            
            for(PredStat t : a) {
                
                final int splitIndex = find(t.sortKey, term2IdSplits, lastSplitIndex);
                
                t.partId = term2IdSplits[splitIndex].pmd.getPartitionId();
                
                lastSplitIndex = splitIndex;
                
            }
            
            if (INFO) {

                /*
                 * Show in the current order (sort key order).
                 */
                
                for (int i = 0; i < a.length; i++) {

                    log.info(a[i]);
                    
                }
                
            }
            
        }

        /*
         * Finally, we choose the separator keys for the POS index finding a set
         * of N ordered buckets of predicates whose range counts are roughly
         * equal. If the last POS index partition would have too small a range
         * count then we generate N-1 separator keys instead.  This is similar
         * to the DefaultSplitHandler.
         */
        {
            
            // it will in fact be an int32 value since it is a BTree.
            final int entryCount = (int) tempStore.getStatementCount();

            // The desired #of tuples per split.
            final int entryCountPerSplit = entryCount / nsplits;

            // minimum #of predicates instances to let into a split.
            final int minEntryCountPerSplit = (int) (entryCountPerSplit * .8);

            // maximum #of predicates instances to let into a split.
            final int maxEntryCountPerSplit = (int) (entryCountPerSplit * 1.2);
            
            if (INFO)
                log.info("nsplits=" + nsplits + ", entryCount=" + entryCount
                        + ", entryCountPerSplit=" + entryCountPerSplit
                        + ", minEntryCountPerSplit=" + minEntryCountPerSplit
                        + ", maxEntryCountPerSplit=" + maxEntryCountPerSplit);

            final List<Split> splits = new ArrayList<Split>(nsplits);

            // #of predicates in the current partition.
            int predCount = 0;
            // #of instances of those predicates in the current partition.
            long rangeCount = 0;
            // the left separator of the current partition.
            byte[] leftSeparator = new byte[0];
            // the first predicate which enters the current partition.
            PredStat firstPred = null;
            // the partition identifier for the current partition.
            int partitionId = 0;

            for (int i = 0; i < a.length; i++) {

                // iff the last predicate.
                final boolean last = (i + 1) == a.length;

                final PredStat t = a[i];
                final PredStat nextPred = last ? null : a[i + 1];

                if (firstPred == null) {

                    firstPred = t;
                    
                }
                
                // accept this predicate into the split.
                predCount++;
                rangeCount += t.rangeCount;

                // iff rangeCount is enough to qualify for a split.
                final boolean sufficient = rangeCount > minEntryCountPerSplit;

                // iff adding the next predicate to this split would make it too
                // large.
                final boolean wouldOverflow;
                long nextRangeCount = -1;
                if (last) {

                    /*
                     * There are no more predicates.
                     * 
                     * Note: The choice [true] here interacts with the handling
                     * of the rightSeparator for the last split, which must be
                     * [null].
                     */
                    wouldOverflow = true;

                } else {

                    nextRangeCount = (rangeCount + nextPred.rangeCount);
                    
                    wouldOverflow = nextRangeCount > maxEntryCountPerSplit;
                    
                }

                if (DEBUG)
                    log.debug("predCount=" + predCount + ", i=" + i + ", last="
                            + last + ", rangeCount=" + rangeCount
                            + ", nextRangeCount=" + nextRangeCount
                            + ", wouldOverflow=" + wouldOverflow
                            + ", firstPred=" + firstPred.term + ", thisPred="
                            + t.term);
                
                if ((!sufficient && !last) || (sufficient && !wouldOverflow)) {

                    /*
                     * Allow the next predicate into this split (but not if this
                     * is is the last predicate!)
                     * 
                     * @todo this can result in the last index partition for POS
                     * holding no more than a single predicate. It would be
                     * better in such cases to put the predicate into the
                     * previous split. We should do that as a touch up on the
                     * last index partition below. This is relatively important
                     * since the load on the host which gets that index
                     * partition can otherwise be quite unbalanced.
                     */
                    
                    continue;
                    
                }

                /*
                 * Generate a split.
                 */
                
                final byte[] rightSeparator;
                if (last) {

                    // the last index partition always has a [null]
                    // rightSeparator.
                    rightSeparator = null;

                } else {

                    rightSeparator = t.sortKey;
                    
                }
                
                splits.add(new Split(newMockPartitionMetadata(partitionId,
                        leftSeparator, rightSeparator)));

                if (INFO) {

                    log.info("Placed " + predCount
                            + " predicates with rangeCount=" + rangeCount
                            + " into partitionId=" + partitionId
                            + ", leftSeperatorPred=" + firstPred.term
                            + ", rightSeparatorPred=" + t.term);
                    
                }
                
                // start another split.
                leftSeparator = rightSeparator;
                predCount = 0;
                rangeCount = 0;
                firstPred = null;
                partitionId++;
                
            }
        
            if (INFO)
                log.info("The predicates index will have " + splits.size()
                        + " partitions");
            
            return splits.toArray(new Split[0]);

        }

    }
    
    /**
     * Write out the choosen index partition splits.
     * 
     * @param w
     *            The writer.
     */
    public void writeSplits(PrintWriter w) {

        throw new UnsupportedOperationException();

    }

    /**
     * Accepts anything recognized as RDF.
     */
    final static public FilenameFilter filenameFilter = new FilenameFilter() {

        public boolean accept(File dir, String name) {

            if(new File(dir,name).isDirectory()) {
                
                // visit subdirectories.
                return true;
                
            }

            return RDFFormat.forFileName(name)!=null;

        }
        
    };

    /**
     * Loads the files using the {@link DataLoader}.
     * <p>
     * Note: this does not handle recursion into subdirectories.
     * 
     * @param dir
     *            The directory containing the files.
     */
    public void loadSingleThreaded(final File dir, final RDFFormat rdfFormat,
            final FilenameFilter filter) {

        System.out.println("Will load files: dir=" + dir);

        final long statementCountBefore = tempStore.getStatementCount();

        final long termCountBefore = tempStore.getTermCount();

        final long begin = System.currentTimeMillis();

        final DataLoader dataLoader = tempStore.getDataLoader();

        // load data files (loads subdirs too).
        try {

//            final LoadStats loadStats = 
                dataLoader.loadFiles(dir, null/* baseURI */, rdfFormat, filter);
            
// System.out.println(loadStats);

        } catch (IOException ex) {
            
            throw new RuntimeException("Problem loading file(s): " + ex, ex);
            
        }

        final long elapsed1 = System.currentTimeMillis() - begin;
        
        final long statementCountAfter = tempStore.getStatementCount();
        
        final long termCountAfter = tempStore.getTermCount();

        final long statementsLoaded = statementCountAfter
                - statementCountBefore;

        final long termsLoaded = termCountAfter - termCountBefore;
        
        final long statementsPerSecond = (long) (statementsLoaded * 1000d / elapsed1);

        System.out.println("Loaded data files: loadTime(ms)=" + elapsed1
                + ", loadRate(tps)=" + statementsPerSecond + ", toldTriples="
                + statementsLoaded+", #terms="+termsLoaded);

    }

    /**
     * Create a triple store using the computed splits for the various indices.
     * 
     * @return The new triple store.
     */
    public AbstractTripleStore createTripleStore(
            final AbstractScaleOutFederation fed, 
            final Properties properties, final String namespace, final Splits splits) {

        if (INFO)
            log.info("Creating tripleStore: " + namespace);

        final AbstractTripleStore tripleStore = new ScaleOutTripleStore(fed,
                namespace, ITx.UNISOLATED, properties);

        // create the triple store.
        tripleStore.create();

        // show #of axioms.
        System.out.println("axiomCount=" + tripleStore.getStatementCount());

        if (INFO)
            log.info("Created tripleStore: " + namespace);

        return tripleStore;

    }
    
    /**
     * Configuration options for {@link SplitFinder}. These options are in the
     * {@link #COMPONENT} namespace.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

        /**
         * The namespace for the options in this class.
         */
        String COMPONENT = SplitFinder.class.getName();
        
        /**
         * The namespace of the triple store to be created.
         */
        String NAMESPACE = "namespace";

        /**
         * When <code>true</code> the named triple store will be created using
         * the computed split points and its various index partitions will be
         * registered on the discovered data services.
         */
        String CREATE = "create";
        
        /**
         * The desired number of discovered data services to use when allocating
         * the index partitions. If there are fewer data services in the
         * federation then the index partitions will be allocated on all
         * discovered data services. If there are more data services in the
         * federation, then only this many data services will be used.
         */
        String NSERVICES = "nservices";

        /**
         * The desired number index partitions to generate.
         */
        String NSPLITS = "nsplits";
        
//        /**
//         * An acceleration factor for splitting the index partitions.  Using
//         * this option you can pre-process less data and still generate the
//         * desired splits. 
//         */
//        String ACCELERATE_SPLIT_THRESHOLD = "accelerateSplitThreshold";
        
        /**
         * The properties that will be used to configured the
         * {@link AbstractTripleStore} expressed as an {@link NV}[].
         */
        String PROPERTIES = "properties";
        
        /**
         * The directory from which the data will be read.
         */
        String DATA_DIR = "dataDir";

        /**
         * An additional file or directory whose data will be loaded into the KB
         * when it is created. If it is a directory, then all data in that
         * directory will be loaded.
         * <p>
         * Note: This is intended to load ontologies pertaining to the data to
         * be loaded.
         */
        String ONTOLOGY = "ontology";
        
    }
    
    /**
     * Command line utility will process an identified set of the files and
     * directories and write and compute the ideal split points for the index
     * partitions and the allocation of the index partitions to the discovered
     * data services and optionally create a specified triple store based on
     * those decisions.
     * 
     * @param args
     *            {@link Configuration} and overrides.
     * 
     * @throws ConfigurationException 
     * @throws IOException 
     * 
     * @see ConfigurationOptions
     */
    static public void main(final String[] args) throws ConfigurationException,
            IOException {

        final JiniFederation fed = new JiniClient(args).connect();

        try {
            
            final Configuration config = fed.getClient().getConfiguration();

            final Properties properties = JiniClientConfig.getProperties(
                    ConfigurationOptions.COMPONENT, config);

            final int nsplits = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NSPLITS, Integer.TYPE);
            
            final int nservices = (Integer) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NSERVICES, Integer.TYPE);
            
            final String namespace = (String) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.NAMESPACE, String.class);

            final boolean create = (Boolean) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.CREATE, Boolean.TYPE);

            // optional.
            final File ontology = (File) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.ONTOLOGY, File.class, null);

            // required.
            final File dataDir = (File) config.getEntry(
                    ConfigurationOptions.COMPONENT,
                    ConfigurationOptions.DATA_DIR, File.class);

            // @todo configure
            final RDFFormat rdfFormat = RDFFormat.RDFXML;

            properties.list(System.out);

            final SplitFinder s = new SplitFinder(properties, nsplits);

            if (ontology != null) {

                s.loadSingleThreaded(ontology, rdfFormat, filenameFilter);

            }

            s.loadSingleThreaded(dataDir, rdfFormat, filenameFilter);

            final Splits splits = s.findSplits();

            if (create) {

                s.createTripleStore(fed, properties, namespace, splits);
                
            }

        } finally {

            fed.shutdown();

        }

    }

}
