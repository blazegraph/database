/*
 * Created on Jul 4, 2008
 */

package com.bigdata.rdf.lexicon;

import java.io.IOException;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.PrefixFilter;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.KeyBuilder.StrengthEnum;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBufferHandler;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.cache.LRUCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer.LexiconKeyBuilder;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Term2IdWriteProcConstructor;
import com.bigdata.rdf.load.AssignedSplits;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.TermIdComparator2;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataSolutionResolverator;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.TokenBuffer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.Split;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * The {@link LexiconRelation} handles all things related to the indices mapping
 * RDF {@link Value}s onto internal 64-bit term identifiers.
 * <p>
 * The term2id index has all the distinct terms ever asserted. Those "terms"
 * include {s:p:o} keys for statements IFF statement identifiers are in use.
 * However, {@link BNode}s are NOT stored in the forward index, even though the
 * forward index is used to assign globally unique term identifiers for blank
 * nodes. See {@link BigdataValueFactoryImpl#createBNode()}.
 * <p>
 * The id2term index only has {@link URI}s and {@link Literal}s. It CAN NOT
 * used to resolve either {@link BNode}s or statement identifiers. In fact,
 * there is NO means to resolve either a statement identifier or a blank node.
 * Both are always assigned (consistently) within a context in which their
 * referent (if any) is defined. For a statement identifier the referent MUST be
 * defined by an instance of the statement itself. The RIO parser integration
 * and the {@link IStatementBuffer} implementations handle all of this stuff.
 * <p>
 * See {@link KeyBuilder.Options} for properties that control how the sort keys
 * are generated for the {@link URI}s and {@link Literal}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconRelation extends AbstractRelation<BigdataValue> {

    final protected static Logger log = Logger.getLogger(LexiconRelation.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    private final Set<String> indexNames;

    /**
     * Note: The term:id and id:term indices MUST use unisolated write operation
     * to ensure consistency without write-write conflicts. The only exception
     * would be a read-historical view.
     * 
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     * 
     */
    public LexiconRelation(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        {

            this.textIndex = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.TEXT_INDEX,
                    AbstractTripleStore.Options.DEFAULT_TEXT_INDEX));
         
            /*
             * Explicitly disable overwrite for the full text index associated
             * with the lexicon. By default, the full text index will replace
             * the existing tuple for a key. We turn this property off because
             * the RDF values are immutable as is the mapping from an RDF value
             * to a term identifier. Hence if we observe the same key there is
             * no need to update the index entry - it will only cause the
             * journal size to grow but will not add any information to the
             * index.
             */
            if (textIndex)
                properties.setProperty(AbstractTripleStore.Options.OVERWRITE,
                        "false");

        }
        
        this.storeBlankNodes = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.STORE_BLANK_NODES,
                AbstractTripleStore.Options.DEFAULT_STORE_BLANK_NODES));
        
        {

            final String defaultValue;
            if (indexManager instanceof IBigdataFederation
                    && ((IBigdataFederation) indexManager).isScaleOut()) {

                defaultValue = AbstractTripleStore.Options.DEFAULT_TERMID_BITS_TO_REVERSE;

            } else {

                // false unless this is a scale-out deployment.
                defaultValue = "0";

            }

            termIdBitsToReverse = Integer
                    .parseInt(getProperty(
                            AbstractTripleStore.Options.TERMID_BITS_TO_REVERSE,
                            defaultValue));
            
            if (termIdBitsToReverse < 0 || termIdBitsToReverse > 31) {

                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.TERMID_BITS_TO_REVERSE
                                + "=" + termIdBitsToReverse);
                
            }
            
        }

        {

            final Set<String> set = new HashSet<String>();

            set.add(getFQN(LexiconKeyOrder.TERM2ID));

            set.add(getFQN(LexiconKeyOrder.ID2TERM));

            if(textIndex) {
                
                set.add(getNamespace() + "."+FullTextIndex.NAME_SEARCH);
                
            }
            
            // @todo add names as registered to base class? but then how to
            // discover?  could be in the global row store.
            this.indexNames = Collections.unmodifiableSet(set);

        }

        /*
         * cache hard references to the indices.
         */

        term2id = super.getIndex(LexiconKeyOrder.TERM2ID);

        id2term = super.getIndex(LexiconKeyOrder.ID2TERM);

        if(textIndex) {
            
            getSearchEngine();
            
        }

        // lookup/create value factory for the lexicon's namespace.
        valueFactory = BigdataValueFactoryImpl.getInstance(namespace);
        
    }
    
    /**
     * The canonical {@link BigdataValueFactoryImpl} reference (JVM wide) for the
     * lexicon namespace.
     */
    public BigdataValueFactoryImpl getValueFactory() {
        
        return valueFactory;
        
    }
    final private BigdataValueFactoryImpl valueFactory;
    
    public boolean exists() {

        return term2id != null && id2term != null;

    }

    public void create() {
        
        create( null );
        
    }
    
    /**
     * 
     * @param assignedSplits
     *            An map providing pre-assigned separator keys describing index
     *            partitions and optionally the data service {@link UUID}s on
     *            which to register the index partitions. The keys of the map
     *            identify the index whose index partitions are described by the
     *            corresponding value. You may specify one or all of the
     *            indices. This parameter is optional and when <code>null</code>,
     *            the default assignments will be used.
     */
    public void create(final Map<IKeyOrder, AssignedSplits> assignedSplits) {

        final IResourceLock resourceLock = acquireExclusiveLock();
        
        try {

            super.create();

            final IIndexManager indexManager = getIndexManager();

            // register the indices.

            {
                final IndexMetadata md = getTerm2IdIndexMetadata(getFQN(LexiconKeyOrder.TERM2ID));

                final AssignedSplits splits = assignedSplits == null ? null
                        : assignedSplits.get(LexiconKeyOrder.TERM2ID);

                if (splits != null) {

                    splits.registerIndex(indexManager, md);

                } else {

                    indexManager.registerIndex(md);

                }
            }

            {

                final IndexMetadata md = getId2TermIndexMetadata(getFQN(LexiconKeyOrder.ID2TERM));

                final AssignedSplits splits = assignedSplits == null ? null
                        : assignedSplits.get(LexiconKeyOrder.ID2TERM);

                if (splits != null) {

                    splits.registerIndex(indexManager, md);

                } else {

                    indexManager.registerIndex(md);

                }
            }

            if (textIndex) {

                // create the full text index
                
                final FullTextIndex tmp = getSearchEngine();

                tmp.create();

            }

            term2id = super.getIndex(LexiconKeyOrder.TERM2ID);

            id2term = super.getIndex(LexiconKeyOrder.ID2TERM);

            assert term2id != null;

            assert id2term != null;

        } finally {

            unlock(resourceLock);

        }

    }

    public void destroy() {

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            super.destroy();

            final IIndexManager indexManager = getIndexManager();

            indexManager.dropIndex(getFQN(LexiconKeyOrder.ID2TERM));
            id2term = null;

            indexManager.dropIndex(getFQN(LexiconKeyOrder.TERM2ID));
            term2id = null;

            if (textIndex) {

                getSearchEngine().destroy();

                searchEngineRef.clear();

            }

            // discard the value factory for the lexicon's namespace.
            BigdataValueFactoryImpl.remove(getNamespace());
            
        } finally {

            unlock(resourceLock);

        }

    }
    
    private IIndex id2term;
    private IIndex term2id;
    private final boolean textIndex;
    final boolean storeBlankNodes;
//    private final boolean scaleOutTermIds;
    final int termIdBitsToReverse;

    /**
     * <code>true</code> iff blank nodes are being stored in the lexicon's
     * forward index.
     * 
     * @see AbstractTripleStore.Options#STORE_BLANK_NODES
     */
    final public boolean isStoreBlankNodes() {
        
        return storeBlankNodes;
        
    }
    
    /**
     * <code>true</code> iff the full text index is enabled.
     * 
     * @see AbstractTripleStore.Options#TEXT_INDEX
     */
    final public boolean isTextIndex() {
        
        return textIndex;
        
    }
    
    /**
     * Overriden to return the hard reference for the index.
     */
    @Override
    public IIndex getIndex(IKeyOrder<? extends BigdataValue> keyOrder) {

        if (keyOrder == LexiconKeyOrder.ID2TERM) {
     
            return getId2TermIndex();
            
        } else if (keyOrder == LexiconKeyOrder.TERM2ID) {
            
            return getTerm2IdIndex();
            
        } else {
            
            throw new AssertionError("keyOrder=" + keyOrder);
            
        }

    }

    final public IIndex getTerm2IdIndex() {

        if (term2id == null)
            throw new IllegalStateException();

        return term2id;

    }

    final public IIndex getId2TermIndex() {

        if (id2term == null)
            throw new IllegalStateException();

        return id2term;

    }

    /**
     * A factory returning the softly held singleton for the
     * {@link FullTextIndex}.
     * 
     * @todo replace with the use of the {@link IResourceLocator} since it
     *       already imposes a canonicalizing mapping within for the index name
     *       and timestamp inside of a JVM.
     */
    public FullTextIndex getSearchEngine() {

        if (!textIndex)
            return null;
        
        synchronized(this) {
        
            FullTextIndex view = searchEngineRef == null ? null
                    : searchEngineRef.get();
            
            if(view == null) {
                
                view = new FullTextIndex(getIndexManager(), getNamespace(),
                        getTimestamp(), getProperties());
                
                searchEngineRef = new SoftReference<FullTextIndex>(view);
                
            }
            
            return view; 
        
        }
        
    }
    private SoftReference<FullTextIndex> searchEngineRef;

    protected IndexMetadata getTerm2IdIndexMetadata(String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new Term2IdTupleSerializer(getProperties()));
        
        return metadata;

    }

    protected IndexMetadata getId2TermIndexMetadata(String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new Id2TermTupleSerializer(
                getNamespace()));

        return metadata;

    }

    public Set<String> getIndexNames() {

        return indexNames;

    }

    /**
     * @todo Not implemented yet. This could be used for high-level query, but
     *       there are no rules written so far that join against the
     *       {@link LexiconRelation}.
     * 
     * @throws UnsupportedOperationException
     */
    public IAccessPath<BigdataValue> getAccessPath(
            IPredicate<BigdataValue> predicate) {

        throw new UnsupportedOperationException();
        
    }

//    /**
//     * @todo Not implemented yet. Exact counts are not very useful on this
//     *       relation.
//     * 
//     * @throws UnsupportedOperationException
//     */
//    public long getElementCount(boolean exact) {
//
//        throw new UnsupportedOperationException();
//        
//    }

    /**
     * Note : this method is part of the mutation api. it is primarily (at this
     * point, only) invoked by the rule execution layer and, at present, no
     * rules can entail terms into the lexicon.
     * 
     * @throws UnsupportedOperationException
     */
    public BigdataValue newElement(IPredicate<BigdataValue> predicate, IBindingSet bindingSet) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Note : this method is part of the mutation api. it is primarily (at this
     * point, only) invoked by the rule execution layer and, at present, no
     * rules can entail terms into the lexicon.
     * 
     * @throws UnsupportedOperationException
     */
    public long delete(IChunkedOrderedIterator<BigdataValue> itr) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Note : this method is part of the mutation api. it is primarily (at this
     * point, only) invoked by the rule execution layer and, at present, no
     * rules can entail terms into the lexicon.
     * 
     * @throws UnsupportedOperationException
     */
    public long insert(IChunkedOrderedIterator<BigdataValue> itr) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * A scan of all literals having the given literal as a prefix.
     * 
     * @param lit
     *            A literal.
     * 
     * @return An iterator visiting the term identifiers for the matching
     *         {@link Literal}s.
     */
    public Iterator<Long> prefixScan(final Literal lit) {

        if (lit == null)
            throw new IllegalArgumentException();
        
        return prefixScan(new Literal[] { lit });
        
    }
    
    /**
     * A scan of all literals having any of the given literals as a prefix.
     * 
     * @param lits
     *            An array of literals.
     * 
     * @return An iterator visiting the term identifiers for the matching
     *         {@link Literal}s.
     * 
     * @todo The prefix scan can be refactored as an {@link IElementFilter}
     *       applied to the lexicon. This would let it be used directly from
     *       {@link IRule}s. (There is no direct dependency on this class other
     *       than for access to the index, and the rules already provide that).
     */
    @SuppressWarnings("unchecked")
    public Iterator<Long> prefixScan(final Literal[] lits) {

        if (lits == null || lits.length == 0)
            throw new IllegalArgumentException();

        if(log.isInfoEnabled()) {

            log.info("#lits="+lits.length);
            
        }

        /*
         * The KeyBuilder used to form the prefix keys.
         * 
         * Note: The prefix keys are formed with IDENTICAL strength. This is
         * necessary in order to match all keys in the index since it causes the
         * secondary characteristics to NOT be included in the prefix key even
         * if they are present in the keys in the index.
         */
        final LexiconKeyBuilder keyBuilder;
        {

            Properties properties = new Properties();

            properties.setProperty(KeyBuilder.Options.STRENGTH,
                    StrengthEnum.Primary.toString());

            keyBuilder = new Term2IdTupleSerializer(
                    new DefaultKeyBuilderFactory(properties)).getLexiconKeyBuilder();

        }

        /*
         * Formulate the keys[].
         * 
         * Note: Each key is encoded with the appropriate bytes to indicate the
         * kind of literal (plain, languageCode, or datatype literal).
         * 
         * Note: The key builder was choosen to only encode the PRIMARY
         * characteristics so that we obtain a prefix[] suitable for the
         * completion scan.
         */

        final byte[][] keys = new byte[lits.length][];

        for (int i = 0; i < lits.length; i++) {

            final Literal lit = lits[i];

            if (lit == null)
                throw new IllegalArgumentException();

            keys[i] = keyBuilder.value2Key(lit);

        }

        final IIndex ndx = getTerm2IdIndex();

        final Iterator<Long> termIdIterator = new Striterator(
                ndx
                        .rangeIterator(
                                null/* fromKey */,
                                null/* toKey */,
                                0/* capacity */,
                                IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
                                // prefix filter.
                                new FilterConstructor<BigdataValue>()
                                        .addFilter(new PrefixFilter<BigdataValue>(
                                                keys))))
                .addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    /**
                     * Decode the value, which is the term identifier.
                     */
                    @Override
                    protected Object resolve(Object arg0) {

                        final ITuple tuple = (ITuple) arg0;

                        final long tid;
                        try {

                            tid = tuple.getValueStream().readLong();
//                            tid = LongPacker.unpackLong(tuple.getValueStream());

                        } catch (IOException e) {

                            throw new RuntimeException(e);

                        }

                        return tid;

                    }
                });

        return termIdIterator;
            
    }
    
    /**
     * Generate the sort keys for the terms.
     * 
     * @param keyBuilder
     *            The object used to generate the sort keys.
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @return An array of correlated key-value-object tuples. * Note that
     *         {@link KVO#val} is <code>null</code> until we know that we need
     *         to write it on the reverse index.
     * 
     * @see LexiconKeyBuilder
     */
    @SuppressWarnings("unchecked")
    final public KVO<BigdataValue>[] generateSortKeys(
            final LexiconKeyBuilder keyBuilder, final BigdataValue[] terms,
            final int numTerms) {

        final KVO<BigdataValue>[] a = (KVO<BigdataValue>[]) new KVO[numTerms];
        
        for (int i = 0; i < numTerms; i++) {

            final BigdataValue term = terms[i];

            a[i] = new KVO<BigdataValue>(keyBuilder.value2Key(term),
                    null/* val */, term);

        }
        
        return a;

    }


    /**
     * Places {@link KVO}s containing {@link BigdataValue} references into an
     * ordering determined by the assigned term identifiers}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see BigdataValue#getTermId()
     */
    final static protected class KVOTermIdComparator implements
            Comparator<KVO<BigdataValue>> {

        public static final transient Comparator<KVO<BigdataValue>> INSTANCE = new KVOTermIdComparator();

        /**
         * Note: comparison avoids possible overflow of <code>long</code> by
         * not computing the difference directly.
         */
        public int compare(final KVO<BigdataValue> term1,
                final KVO<BigdataValue> term2) {

            final long id1 = term1.obj.getTermId();
            final long id2 = term2.obj.getTermId();

            if (id1 < id2)
                return -1;
            if (id1 > id2)
                return 1;
            return 0;

        }

    }
    
    /**
     * Batch insert of terms into the database.
     * <p>
     * Note: Duplicate {@link BigdataValue} references and {@link BigdataValue}s
     * that already have an assigned term identifiers are ignored by this
     * operation.
     * <p>
     * Note: This implementation is designed to use unisolated batch writes on
     * the terms and ids index that guarentee consistency.
     * <p>
     * If the full text index is enabled, then the terms will also be inserted
     * into the full text index.
     * 
     * @param terms
     *            An array whose elements [0:nterms-1] will be inserted.
     * @param numTerms
     *            The #of terms to insert.
     * @param readOnly
     *            When <code>true</code>, unknown terms will not be inserted
     *            into the database. Otherwise unknown terms are inserted into
     *            the database.
     */
    @SuppressWarnings("unchecked")
    public void addTerms(final BigdataValue[] terms, final int numTerms,
            final boolean readOnly) {

        if (DEBUG)
            log.debug("numTerms=" + numTerms + ", readOnly=" + readOnly);
        
        if (numTerms == 0)
            return;

        final long begin = System.currentTimeMillis();
        
        final WriteTaskStats stats = new WriteTaskStats();

        final KVO<BigdataValue>[] a;
        try {
            // write on the forward index (sync RPC)
            a = new Term2IdWriteTask(this, readOnly, numTerms, terms, stats)
                    .call();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        /*
         * Note: [a] is dense and its elements are distinct. it will be in sort
         * key order for the Values.
         */
        final int ndistinct = a.length;

        if (ndistinct == 0) {

            // Nothing left to do.
            return;
            
        }
        
        if(!readOnly) {
            
            {
    
                /*
                 * Sort terms based on their assigned termId (when interpreted
                 * as unsigned long integers).
                 * 
                 * Note: We sort before the index writes since we will co-thread
                 * the reverse index write and the full text index write.
                 * Sorting first let's us read from the same array.
                 */
    
                final long _begin = System.currentTimeMillis();
    
                Arrays.sort(a, 0, ndistinct, KVOTermIdComparator.INSTANCE);
                
                stats.sortTime += System.currentTimeMillis() - _begin;
    
            }

            /*
             * Write on the reverse index and the full text index.
             */
            {

                final long _begin = System.currentTimeMillis();

                final List<Callable<Long>> tasks = new LinkedList<Callable<Long>>();

                tasks.add(new ReverseIndexWriterTask(getId2TermIndex(),
                        valueFactory, a, ndistinct));

                if (textIndex) {

                    /*
                     * Note: terms[] is in termId order at this point and can
                     * contain both duplicates and terms that already have term
                     * identifiers and therefore are already in the index.
                     * 
                     * Therefore, instead of terms[], we use an iterator that
                     * resolves the distinct terms in a[] (it is dense) to do
                     * the indexing.
                     */

                    final Iterator<BigdataValue> itr = new Striterator(
                            new ChunkedArrayIterator(ndistinct, a, null/* keyOrder */))
                            .addFilter(new Resolver() {

                        private static final long serialVersionUID = 1L;

                        @Override
                        protected Object resolve(Object obj) {
                        
                            return ((KVO<BigdataValue>) obj).obj;
                            
                        }
                        
                    });

                    tasks.add(new FullTextIndexWriterTask(
                            ndistinct/* capacity */, itr));

                }

                /*
                 * Co-thread the reverse index writes and the search index
                 * writes.
                 */
                try {

                    final List<Future<Long>> futures = getExecutorService()
                            .invokeAll(tasks);

                    stats.reverseIndexTime = futures.get(0).get();
                    
                    if (textIndex)
                        stats.fullTextIndexTime = futures.get(1).get();
                    else 
                        stats.fullTextIndexTime = 0L;

                } catch (Throwable t) {

                    throw new RuntimeException(t);

                }

                stats.indexTime += System.currentTimeMillis() - _begin;

            }
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (INFO && readOnly && stats.nunknown.get() > 0) {
         
            log.info("There are " + stats.nunknown + " unknown terms out of "
                    + numTerms + " given");
            
        }
        
        if (numTerms > 1000 || elapsed > 3000) {

            if(INFO)
            log.info("Processed " + numTerms + " in " + elapsed
                        + "ms; keygen=" + stats.keyGenTime + "ms, sort=" + stats.sortTime
                        + "ms, insert=" + stats.indexTime + "ms" + " {forward="
                        + stats.forwardIndexTime + ", reverse=" + stats.reverseIndexTime
                        + ", fullText=" + stats.fullTextIndexTime + "}");

        }

    }

    /**
     * Index terms for keyword search.
     */
    private class FullTextIndexWriterTask implements Callable<Long> {

        private final int capacity;
        
        private final Iterator<BigdataValue> itr;

        public FullTextIndexWriterTask(final int capacity,
                final Iterator<BigdataValue> itr) {
        
            this.capacity = capacity;
            
            this.itr = itr;
            
        }
        
        /**
         * Elapsed time for this operation.
         */
        public Long call() throws Exception {

            final long _begin = System.currentTimeMillis();

            indexTermText(capacity, itr);

            final long elapsed = System.currentTimeMillis() - _begin;
            
            return elapsed;
    
        }
        
    }
    
    /**
     * Assign unique statement identifiers to triples.
     * <p>
     * Each distinct {@link StatementEnum#Explicit} {s,p,o} is assigned a unique
     * statement identifier using the {@link LexiconKeyOrder#TERM2ID} index. The
     * assignment of statement identifiers is <i>consistent</i> using an
     * unisolated atomic write operation similar to
     * {@link #addTerms(BigdataValue[], int, boolean)}
     * <p>
     * Note: Statement identifiers are NOT inserted into the reverse (id:term)
     * index. Instead, they are written into the values associated with the
     * {s,p,o} in each of the statement indices. That is handled by
     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)},
     * which is also responsible for invoking this method in order to have the
     * statement identifiers on hand before it writes on the statement indices.
     * <p>
     * Note: The caller's {@link ISPO}[] is sorted into SPO order as a
     * side-effect.
     * <p>
     * Note: The statement identifiers are assigned to the {@link ISPO}s as a
     * side-effect.
     */
    public void addStatementIdentifiers(final ISPO[] a, final int n) {

        //        * @throws UnsupportedOperationException
//        *             if {@link Options#STATEMENT_IDENTIFIERS} was not specified.
//        * 
//        if (!statementIdentifiers)
//            throw new UnsupportedOperationException();

        if (n == 0)
            return;

        final long begin = System.currentTimeMillis();
        final long keyGenTime; // time to convert {s,p,o} to byte[] sort keys.
        final long sortTime; // time to sort terms by assigned byte[] keys.
        final long insertTime; // time to insert terms into the term:id index.

        /*
         * Sort the caller's array into SPO order. This order will correspond to
         * the total order of the term:id index.
         * 
         * Note: This depends critically on SPOComparator producing the same
         * total order as we would obtain by an unsigned byte[] sort of the
         * generated sort keys.
         * 
         * Note: the keys for the term:id index are NOT precisely the keys used
         * by the SPO index since there is a prefix code used to mark the keys
         * are Statements (vs Literals, BNodes, or URIs).
         */
        {

            final long _begin = System.currentTimeMillis();

            Arrays.sort(a, 0, n, SPOComparator.INSTANCE);

            sortTime = System.currentTimeMillis() - _begin;

        }

        /*
         * Insert into the forward index (term -> id). This will either assign a
         * statement identifier or return the existing statement identifier if
         * the statement is already in the lexicon (the statement identifier is
         * in a sense a term identifier since it is assigned by the term:id
         * index).
         * 
         * Note: Since we only assign statement identifiers for explicit
         * statements the caller's SPO[] can not be directly correlated to the
         * keys[]. We copy the references into b[] so that we can keep that
         * correlation 1:1.
         */
        final byte[][] keys = new byte[n][];
        final ISPO[] b = new ISPO[n];

        /*
         * Generate the sort keys for the term:id index.
         */
        int nexplicit = 0;
        {

            final long _begin = System.currentTimeMillis();

            // local instance, no unicode support.
            final IKeyBuilder keyBuilder = KeyBuilder.newInstance(1
                    + IRawTripleStore.N * Bytes.SIZEOF_LONG);

            for (int i = 0; i < n; i++) {

                final ISPO spo = a[i];

                if (!spo.isExplicit())
                    continue;
                
                if (!spo.isFullyBound())
                    throw new IllegalArgumentException("Not fully bound: "
                            + spo.toString(/*this*/));

                keys[nexplicit] = keyBuilder.reset() //
                        .append(ITermIndexCodes.TERM_CODE_STMT)//
                        .append(spo.s()).append(spo.p()).append(spo.o())//
                        .getKey()//
                ;

                // Note: keeps correlation between key and SPO.
                b[nexplicit] = spo;

                nexplicit++;

            }

            keyGenTime = System.currentTimeMillis() - _begin;

        }

        /*
         * Execute a remote unisolated batch operation that assigns the
         * statement identifier.
         */
        {

            final long _begin = System.currentTimeMillis();

            final IIndex termIdIndex = getTerm2IdIndex();

            // run the procedure.
            if (nexplicit > 0) {

                termIdIndex.submit(0/* fromIndex */, nexplicit/* toIndex */,
                        keys, null/* vals */, new Term2IdWriteProcConstructor(
                                false/* readOnly */, storeBlankNodes, //scaleOutTermIds,
                                termIdBitsToReverse),
                        new IResultHandler<Term2IdWriteProc.Result, Void>() {

                            /**
                             * Copy the assigned / discovered statement
                             * identifiers onto the corresponding elements of
                             * the SPO[].
                             */
                            public void aggregate(Term2IdWriteProc.Result result,
                                    Split split) {

                                for (int i = split.fromIndex, j = 0; i < split.toIndex; i++, j++) {

                                    b[i].setStatementIdentifier(result.ids[j]);

                                }

                            }

                            public Void getResult() {

                                return null;

                            }

                        });

            }

            insertTime = System.currentTimeMillis() - _begin;

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (INFO && n > 1000 || elapsed > 3000) {

            log.info("Wrote " + n + " in " + elapsed + "ms; keygen="
                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
                    + insertTime + "ms");

        }

    }

    /**
     * <p>
     * Add the terms to the full text index so that we can do fast lookup of the
     * corresponding term identifiers. Literals that have a language code
     * property are parsed using a tokenizer appropriate for the specified
     * language family. Other literals and URIs are tokenized using the default
     * {@link Locale}.
     * </p>
     * 
     * @param itr Iterator visiting the terms to be indexed.
     * 
     * @see #textSearch(String, String)
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     */
    protected void indexTermText(final int capacity, final Iterator<BigdataValue> itr) {
        
        final FullTextIndex ndx = getSearchEngine();

        final TokenBuffer buffer = new TokenBuffer(capacity, ndx);

        int n = 0;

        while(itr.hasNext()) {

            final BigdataValue val = itr.next();

            if (!(val instanceof Literal))
                continue;
            
            final Literal lit = (Literal)val;

            // do not index datatype literals in this manner.
            if (lit.getDatatype() != null)
                continue;

            final String languageCode = lit.getLanguage();

            // Note: May be null (we will index plain literals).
            // if(languageCode==null) continue;

            final String text = lit.getLabel();

            /*
             * Note: The OVERWRITE option is turned off to avoid some of the
             * cost of re-indexing each time we see a term.
             */

            final long termId = val.getTermId();
            
            assert termId != IRawTripleStore.NULL; // the termId must have been assigned.

            ndx.index(buffer, termId, 0/* fieldId */, languageCode,
                    new StringReader(text));

            n++;

        }

        // flush writes to the text index.
        buffer.flush();

        if (INFO)
            log.info("indexed " + n + " new terms");

    }

    /**
     * Batch resolution of term identifiers to {@link BigdataValue}s.
     * 
     * @param ids
     *            An collection of term identifiers.
     * 
     * @return A map from term identifier to the {@link BigdataValue}. If a
     *         term identifier was not resolved then the map will not contain an
     *         entry for that term identifier.
     * 
     * @todo write unit tests getTerms(ids) - it's mainly used by the
     *       {@link BigdataStatementIteratorImpl} and
     *       {@link BigdataSolutionResolverator}.
     * 
     * @todo performance tuning for statement pattern scans with resolution to
     *       terms, e.g., LUBM Q6.
     */
    final public Map<Long,BigdataValue> getTerms(final Collection<Long> ids) {

        if (ids == null)
            throw new IllegalArgumentException();

        // Maximum #of distinct term identifiers.
        final int n = ids.size();
        
        if (n == 0) {

            return Collections.EMPTY_MAP;
            
        }

        final long begin = System.currentTimeMillis();
        
        final ConcurrentHashMap<Long/* id */, BigdataValue/* term */> ret = new ConcurrentHashMap<Long, BigdataValue>(
                n);

        /*
         * An array of any term identifiers that were not resolved in this first
         * stage of processing. We will look these up in the index.
         */
        final Long[] notFound = new Long[n];

        int numNotFound = 0;
        
        for(Long id : ids ) {
            
            final BigdataValue value = _getTermId(id.longValue());
            
            if (value != null) {
            
                // resolved.
                ret.put(id, valueFactory.asValue(value));
                
                continue;

            }

            /*
             * We will need to test the index for this term identifier.
             */
            notFound[numNotFound++] = id;
            
        }

        /*
         * batch lookup.
         */
        if (numNotFound > 0) {

            if (INFO)
                log.info("nterms=" + n + ", numNotFound=" + numNotFound
                        + (termCache!=null?(", cacheSize=" + termCache.size() + "\n"
                        + termCache.getStatistics()):""));
            
            /*
             * sort term identifiers into index order.
             * 
             * FIXME 10/12ths of the cost of this sort is the TermIdComparator2
             * class and the sort is 1/3 of the cost of this method overall. Try
             * to use a native long[] and see if we can shave some time.
             */
            Arrays.sort(notFound, 0, numNotFound, TermIdComparator2.INSTANCE);
            
            final IKeyBuilder keyBuilder = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);

            final byte[][] keys = new byte[numNotFound][];

            for (int i = 0; i < numNotFound; i++) {

                // Note: shortcut for keyBuilder.id2key(id)
                keys[i] = keyBuilder.reset().append(notFound[i]).getKey();
                
            }

            // the id:term index.
            final IIndex ndx = getId2TermIndex();

            /*
             * Note: This parameter is not terribly sensitive.
             */
            final int MAX_CHUNK = 4000;
            if (numNotFound < MAX_CHUNK) {

                /*
                 * Resolve everything in one go.
                 */
                
                new ResolveTermTask(ndx, 0/* fromIndex */,
                        numNotFound/* toIndex */, keys, notFound, ret).call();
                
            } else {
                
                /*
                 * Break it down into multiple chunks and resolve those chunks
                 * in parallel.
                 */

                // #of elements.
                final int N = numNotFound;
                // target maximum #of elements per chunk.
                final int M = MAX_CHUNK;
                // #of chunks
                final int nchunks = (int) Math.ceil((double)N / M);
                // #of elements per chunk, with any remainder in the last chunk.
                final int perChunk = N / nchunks;
                
//                System.err.println("N="+N+", M="+M+", nchunks="+nchunks+", perChunk="+perChunk);
                
                final List tasks = new ArrayList<Callable>(nchunks);

                int fromIndex = 0;
                int remaining = numNotFound;

                for(int i=0; i<nchunks; i++) {

                    final boolean lastChunk = i + 1 == nchunks;
                    
                    final int chunkSize = lastChunk ? remaining : perChunk;

                    final int toIndex = fromIndex + chunkSize;

                    remaining -= chunkSize;
                    
//                    System.err.println("chunkSize=" + chunkSize
//                            + ", fromIndex=" + fromIndex + ", toIndex="
//                            + toIndex + ", remaining=" + remaining);
                    
                    tasks.add(new ResolveTermTask(ndx, fromIndex, toIndex,
                            keys, notFound, ret));

                    fromIndex = toIndex;
                    
                }
                
                final List<Future> futures;
                try {
                    futures = getExecutorService().invokeAll(tasks);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                
                for(Future f : futures) {
                    
                    // verify task executed Ok.
                    try {
                        f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }                    
                }

                final long elapsed = System.currentTimeMillis() - begin;
                
                if (INFO)
                    log.info("resolved " + numNotFound + " terms in "
                            + tasks.size() + " chunks and " + elapsed + "ms");
                
            }

        }

        return ret;
        
    }
    
    /**
     * Task resolves a chunk of terms against the lexicon.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ResolveTermTask implements Callable<Void> {
        
        final IIndex ndx;

        final int fromIndex;

        final int toIndex;

        final byte[][] keys;

        final Long[] notFound;

        final ConcurrentHashMap<Long, BigdataValue> map;
        
        /**
         * 
         * @param ndx
         *            The index that will be used to resolve the term
         *            identifiers.
         * @param fromIndex
         *            The first index in <i>keys</i> to resolve.
         * @param toIndex
         *            The first index in <i>keys</i> that will not be resolved.
         * @param keys
         *            The serialized term identifiers.
         * @param notFound
         *            An array of term identifiers whose corresponding
         *            {@link BigdataValue} must be resolved against the index.
         *            The indices in this array are correlated 1:1 with those
         *            for <i>keys</i>.
         * @param map
         *            Terms are inserted into this map under using their term
         *            identifier as the key.
         */
        ResolveTermTask(final IIndex ndx, final int fromIndex,
                final int toIndex, final byte[][] keys, final Long[] notFound,
                ConcurrentHashMap<Long, BigdataValue> map) {

            this.ndx = ndx;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.keys = keys;
            this.notFound = notFound;
            this.map = map;

        }

        public Void call() {
            
            // aggregates results if lookup split across index partitions.
            final ResultBufferHandler resultHandler = new ResultBufferHandler(
                    toIndex, ndx.getIndexMetadata().getTupleSerializer()
                            .getLeafValueSerializer());

            // batch lookup
            ndx.submit(fromIndex, toIndex/* toIndex */, keys, null/* vals */,
                    BatchLookupConstructor.INSTANCE, resultHandler);

            // the aggregated results.
            final ResultBuffer results = resultHandler.getResult();

            /*
             * synchronize on the term cache before updating it.
             * 
             * Note: This is synchronized to guard against a race condition
             * where concurrent threads resolve the term against the database.
             * If both threads attempt to insert their resolved term
             * definitions, which are distinct objects, into the cache then one
             * will get an IllegalStateException since the other's object will
             * already be in the cache.
             * 
             * @todo move de-serialization out of the synchronized block and the
             * putAll() into the term cache?
             */
            {

                for (int i = fromIndex; i < toIndex; i++) {

                    final Long id = notFound[i];

                    final byte[] data = results.getResult(i);

                    if (data == null) {

                        log.warn("No such term: " + id);

                        continue;

                    }

                    /*
                     * Note: This automatically sets the valueFactory reference
                     * on the de-serialized value.
                     */
                    final BigdataValue value = valueFactory
                            .getValueSerializer().deserialize(data);

                    if (termCache != null) {

                        synchronized (termCache) {

                            if (termCache.get(id) == null) {

                                termCache.put(id, value, false/* dirty */);

                            }
                            
                        }
                        
                    }

                    /*
                     * The term identifier was set when the value was
                     * de-serialized. However, this will throw an
                     * IllegalStateException if the value somehow was assigned
                     * the wrong term identifier (paranoia test).
                     */
                    value.setTermId(id);

                    // save in local map.
                    map.put(id, value);

                }

            }

            return null;
            
        }
        
    }
    
//    /**
//     * Batch resolution of term identifiers to {@link BigdataValue}s.
//     * <p>
//     * Note: This is an alternative implementation using a native long hash map.
//     * It is designed to reduce costs for {@link Long} creation and for sorting
//     * {@link Long}s.
//     * 
//     * @param ids
//     *            An collection of term identifiers.
//     * 
//     * @return A map from term identifier to the {@link BigdataValue}. If a
//     *         term identifier was not resolved then the map will not contain an
//     *         entry for that term identifier.
//     */
//    final public Long2ObjectOpenHashMap<BigdataValue> getTerms(final LongOpenHashSet ids) {
//
//        if (ids == null)
//            throw new IllegalArgumentException();
//
//        final int n = ids.size();
//        
//        final Long2ObjectOpenHashMap<BigdataValue/* term */> ret = new Long2ObjectOpenHashMap<BigdataValue>(n);
//        
//        if (n == 0)
//            return ret;
//        
//        /*
//         * An array of any term identifiers that were not resolved in this first
//         * stage of processing. We will look these up in the index.
//         */
//        final long[] notFound = new long[n];
//
//        int numNotFound = 0;
//        
//        {
//        
//            final LongIterator itr = ids.iterator();
//        
//            while(itr.hasNext()) {
//                
//                long id = itr.nextLong();
//            
//// for(Long id : ids ) {
//            
//            final BigdataValue value = _getTermId(id);//.longValue());
//            
//            if (value != null) {
//            
//                // resolved.
//                ret.put(id, valueFactory.asValue(value));
//                
//                continue;
//
//            }
//
//            /*
//             * We will need to test the index for this term identifier.
//             */
//            notFound[numNotFound++] = id;
//            
//        }
//        
//    }
//
//        /*
//         * batch lookup.
//         */
//        if (numNotFound > 0) {
//
//            if (log.isInfoEnabled())
//                log.info("Will resolve " + numNotFound
//                        + " term identifers against the index.");
//            
//            // sort term identifiers into index order.
//            Arrays.sort(notFound, 0, numNotFound);
//            
//            final IKeyBuilder keyBuilder = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);
//
//            final byte[][] keys = new byte[numNotFound][];
//
//            for(int i=0; i<numNotFound; i++) {
//
//                // Note: shortcut for keyBuilder.id2key(id)
//                keys[i] = keyBuilder.reset().append(notFound[i]).getKey();
//                
//            }
//
//            // the id:term index.
//            final IIndex ndx = getId2TermIndex();
//
//            // aggregates results if lookup split across index partitions.
//            final ResultBufferHandler resultHandler = new ResultBufferHandler(
//                    numNotFound, ndx.getIndexMetadata().getTupleSerializer()
//                            .getLeafValueSerializer());
//
//            // batch lookup
//            ndx.submit(0/* fromIndex */, numNotFound/* toIndex */, keys,
//                    null/* vals */, BatchLookupConstructor.INSTANCE,
//                    resultHandler);
//        
//            // the aggregated results.
//            final ResultBuffer results = resultHandler.getResult();
//            
//            /*
//             * synchronize on the term cache before updating it.
//             * 
//             * @todo move de-serialization out of the synchronized block?
//             */
//            synchronized (termCache) {
//
//                for (int i = 0; i < numNotFound; i++) {
//                    
//                    final long id = notFound[i];
//
//                    final byte[] data = results.getResult(i);
//
//                    if (data == null) {
//
//                        log.warn("No such term: " + id );
//
//                        continue;
//
//                    }
//
//                    /*
//                     * Note: This automatically sets the valueFactory reference
//                     * on the de-serialized value. 
//                     */
//                    final BigdataValue value = valueFactory
//                            .getValueSerializer().deserialize(data);
//                    
//                    /*
//                     * Note: This code block is synchronized to address a
//                     * possible race condition where concurrent threads resolve
//                     * the term against the database. If both threads attempt to
//                     * insert their resolved term definitions, which are
//                     * distinct objects, into the cache then one will get an
//                     * IllegalStateException since the other's object will
//                     * already be in the cache.
//                     */
//
//                    Long tmp = id;// note: creates Long from long
//                    if (termCache.get(tmp) == null) {
//
//                        termCache.put(tmp, value, false/* dirty */);
//
//                    }
//
//                    /*
//                     * The term identifier was set when the value was
//                     * de-serialized. However, this will throw an
//                     * IllegalStateException if the value somehow was assigned
//                     * the wrong term identifier (paranoia test).
//                     */
//                    value.setTermId( id );
//
//                    // save in local map.
//                    ret.put(id, value);
//                    
//                }
//
//            }
//            
//        }
//        
//        return ret;
//        
//    }

    /**
     * Recently resolved term identifers are cached to improve performance when
     * externalizing statements.
     * 
     * @todo consider using this cache in the batch API as well or simply modify
     *       the {@link StatementBuffer} to use a term cache in order to
     *       minimize the #of terms that it has to resolve against the indices -
     *       this especially matters for the scale-out implementation.
     */
    private LRUCache<Long, BigdataValue> termCache = null;
//    private LRUCache<Long, BigdataValue> termCache = new LRUCache<Long, BigdataValue>(10000);

    /**
     * Constant for the {@link LexiconRelation} namespace component.
     * <p>
     * Note: To obtain the fully qualified name of an index in the
     * {@link LexiconRelation} you need to append a "." to the relation's
     * namespace, then this constant, then a "." and then the local name of the
     * index.
     * 
     * @see AbstractRelation#getFQN(IKeyOrder)
     */
    public static final transient String NAME_LEXICON_RELATION = "lex";

    /**
     * Handles {@link IRawTripleStore#NULL}, blank node identifiers, statement
     * identifiers, and the {@link #termCache}.
     * 
     * @param id
     *            A term identifier.
     * 
     * @return The corresponding {@link BigdataValue} if the term identifier is
     *         a blank node identifier, a statement identifier, or found in the
     *         {@link #termCache} and <code>null</code> otherwise.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</i> is {@link IRawTripleStore#NULL}.
     */
    private BigdataValue _getTermId(final long id) {

        if (id == IRawTripleStore.NULL)
            throw new IllegalArgumentException();

        if (AbstractTripleStore.isStatement(id)) {

            /*
             * Statement identifiers are not stored in the reverse lexicon (or
             * the cache).
             * 
             * A statement identifier is externalized as a BNode. The "S" prefix
             * is a syntactic marker for those in the know to indicate that the
             * BNode corresponds to a statement identifier.
             */

            final BigdataBNodeImpl stmt = valueFactory.createBNode("S" + Long.toString(id));

            // set the term identifier on the object.
            stmt.setTermId(id);

            // mark as a statement identifier.
            stmt.statementIdentifier = true;

            return stmt;
            
        }

        if (AbstractTripleStore.isBNode(id)) {

            /*
             * BNodes are not stored in the reverse lexicon (or the cache). The
             * "B" prefix is a syntactic marker for a real blank node.
             */

            final BigdataBNode bnode = valueFactory.createBNode("B" + Long.toString(id));

            // set the term identifier on the object.
            bnode.setTermId( id );

            return bnode;

        }

        // test the term cache (promotes to Long since not native long map).
        if (termCache != null) {

            return termCache.get(id);

        }

        return null;

    }
    
    /**
     * Note: {@link BNode}s are not stored in the reverse lexicon and are
     * recognized using {@link AbstractTripleStore#isBNode(long)}.
     * <p>
     * Note: Statement identifiers (when enabled) are not stored in the reverse
     * lexicon and are recognized using
     * {@link AbstractTripleStore#isStatement(long)}. If the term identifier is
     * recognized as being, in fact, a statement identifier, then it is
     * externalized as a {@link BNode}. This fits rather well with the notion
     * in a quad store that the context position may be either a {@link URI} or
     * a {@link BNode} and the fact that you can use {@link BNode}s to "stamp"
     * statement identifiers.
     * <p>
     * Note: Handles both unisolatable and isolatable indices.
     * <P>
     * Note: Sets {@link BigdataValue#getTermId()} as a side-effect.
     * <p>
     * Note: this always mints a new {@link BNode} instance when the term
     * identifier identifies a {@link BNode} or a {@link Statement}.
     * 
     * @return The {@link BigdataValue} -or- <code>null</code> iff there is no
     *         {@link BigdataValue} for that term identifier in the lexicon.
     */
    final public BigdataValue getTerm(final long id) {
        
        // handle NULL, bnodes, statement identifiers, and the termCache.
        BigdataValue value = _getTermId( id );
        
        if(value != null) return value;
        
        final IIndex ndx = getId2TermIndex();

        final Id2TermTupleSerializer tupleSer = (Id2TermTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.id2key(id);

        final byte[] data = ndx.lookup(key);

        if (data == null) {

            return null;

        }

        value = valueFactory.getValueSerializer().deserialize(data);

        if (termCache != null)
            synchronized (termCache) {

            /*
             * Note: This code block is synchronized to address a possible race
             * condition where concurrent threads resolve the term against the
             * database. It both threads attempt to insert their resolved term
             * definitions, which are distinct objects, into the cache then one
             * will get an IllegalStateException since the other's object will
             * already be in the cache.
             */

            if (termCache.get(id) == null) {

                termCache.put(id, value, false/* dirty */);

            }

        }

        // @todo modify unit test to verify that this field is being set.

        value.setTermId( id );

//        value.known = true;

        return value;

    }

    /**
     * Note: If {@link BigdataValue#getTermId()} is set, then returns that value
     * immediately. Otherwise looks up the termId in the index and
     * {@link BigdataValue#setTermId(long) sets the term identifier} as a
     * side-effect.
     */
    final public long getTermId(final Value value) {

        if (value == null) {

            return IRawTripleStore.NULL;

        }

        if (value instanceof BigdataValue
                && ((BigdataValue) value).getTermId() != IRawTripleStore.NULL) {

            return ((BigdataValue) value).getTermId();

        }

        final IIndex ndx = getTerm2IdIndex();

        final byte[] key;
        {
        
            final Term2IdTupleSerializer tupleSer = (Term2IdTupleSerializer) ndx
                    .getIndexMetadata().getTupleSerializer();

            // generate key iff not on hand.
            key = tupleSer.getLexiconKeyBuilder().value2Key(value);
        
        }
        
        // lookup in the forward index.
        final byte[] tmp = ndx.lookup(key);

        if (tmp == null) {

            return IRawTripleStore.NULL;

        }

        // decode the term identifier.
        final long termId;
        try {

            termId = new DataInputBuffer(tmp).readLong();
//            termId = new DataInputBuffer(tmp).unpackLong();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        /*
         * @todo now that we have the termId and the term, we should stick them
         * in the termCache, right?
         */

//        if (termCache.get(id) == null) {
//
//            termCache.put(id, value, false/* dirty */);
//
//        }

        if(value instanceof BigdataValue) {

            // set as side-effect.
            ((BigdataValue)value).setTermId(termId);
            
        }
        
//        if((value instanceof _Value)) {
//
//            // was found in the forward mapping (@todo if known means in both
//            // mappings then DO NOT set it here)
//
//            ((_Value)value).known = true;
//            
//        }

        return termId;

    }

    /**
     * Iterator visits all terms in order by their assigned <strong>term
     * identifiers</strong> (efficient index scan, but the terms are not in
     * term order).
     * 
     * @see #termIdIndexScan()
     * 
     * @see #termIterator()
     */
    @SuppressWarnings("unchecked")
    public Iterator<Value> idTermIndexScan() {

        final IIndex ndx = getId2TermIndex();

        return new Striterator(ndx.rangeIterator(null, null, 0/* capacity */,
                IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */))
                .addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param obj
             *            the serialized term.
             */
            protected Object resolve(Object obj) {

//                final ITuple<BigdataValue> tuple =  obj;

//                BigdataValue term = _Value.deserialize(tuple.getValueStream());

                final BigdataValue value = ((ITuple<BigdataValue>) obj)
                        .getObject();
                
                return value;

            }

        });

    }

    /**
     * Iterator visits all term identifiers in order by the <em>term</em> key
     * (efficient index scan).
     */
    @SuppressWarnings("unchecked")
    public Iterator<Long> termIdIndexScan() {

        final IIndex ndx = getTerm2IdIndex();

        return new Striterator(ndx.rangeIterator(null, null, 0/* capacity */,
                IRangeQuery.VALS, null/* filter */)).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * Deserialize the term identifier (packed long integer).
             * 
             * @param val
             *            The serialized term identifier.
             */
            protected Object resolve(final Object val) {

                final ITuple tuple = (ITuple) val;

                final long id;

                try {

//                    id = tuple.getValueStream().unpackLong();
                    id = tuple.getValueStream().readLong();

                } catch (final IOException ex) {

                    throw new RuntimeException(ex);

                }

                return id;

            }

        });

    }

    /**
     * Visits all terms in <strong>term key</strong> order (random index
     * operation).
     * <p>
     * Note: While this operation visits the terms in their index order it is
     * significantly less efficient than {@link #idTermIndexScan()}. This is
     * because the keys in the term:id index are formed using an un-reversable
     * technique such that it is not possible to re-materialize the term from
     * the key. Therefore visiting the terms in term order requires traversal of
     * the term:id index (so that you are in term order) plus term-by-term
     * resolution against the id:term index (to decode the term). Since the two
     * indices are not mutually ordered, that resolution will result in random
     * hits on the id:term index.
     */
    @SuppressWarnings("unchecked")
    public Iterator<Value> termIterator() {

        // visit term identifiers in term order.
        final Iterator<Long> itr = termIdIndexScan();

        // resolve term identifiers to terms.
        return new Striterator(itr).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the term identifer (Long).
             */
            protected Object resolve(Object val) {

                // the term identifier.
                long termId = (Long) val;

                // resolve against the id:term index (random lookup).
                return getTerm(termId);

            }

        });

    }

}
