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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.omg.CORBA.portable.ValueFactory;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.filter.PrefixFilter;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBufferHandler;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.cache.ConcurrentWeakValueCacheWithBatchedUpdates;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Term2IdWriteProcConstructor;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.TermIdComparator2;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.store.AbstractTripleStore;
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
    public LexiconRelation(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

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

            if (textIndex) {
                /*
                 * Also index datatype literals?
                 */
                textIndexDatatypeLiterals = Boolean
                        .parseBoolean(getProperty(
                                AbstractTripleStore.Options.TEXT_INDEX_DATATYPE_LITERALS,
                                AbstractTripleStore.Options.DEFAULT_TEXT_INDEX_DATATYPE_LITERALS));
            } else {
                textIndexDatatypeLiterals = false;
            }

        }
        
        this.storeBlankNodes = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.STORE_BLANK_NODES,
                AbstractTripleStore.Options.DEFAULT_STORE_BLANK_NODES));
        
        {

            final String defaultValue;
            if (indexManager instanceof IBigdataFederation<?>
                    && ((IBigdataFederation<?>) indexManager).isScaleOut()) {

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
                
                set.add(getNamespace() + "." + FullTextIndex.NAME_SEARCH);

            }
            
            // @todo add names as registered to base class? but then how to
            // discover?  could be in the global row store.
            this.indexNames = Collections.unmodifiableSet(set);

        }

        /*
         * Note: I am deferring resolution of the indices to minimize the
         * latency and overhead required to "locate" the relation. In scale out,
         * resolving the index will cause a ClientIndexView to spring into
         * existence for the appropriate timestamp, and we often do not need
         * that view for each index of the relation during query.
         */
        
//        /*
//         * cache hard references to the indices.
//         */
//
//        term2id = super.getIndex(LexiconKeyOrder.TERM2ID);
//
//        id2term = super.getIndex(LexiconKeyOrder.ID2TERM);
//
//        if(textIndex) {
//            
//            getSearchEngine();
//            
//        }

        /*
         * Lookup/create value factory for the lexicon's namespace.
         * 
         * Note: The same instance is used for read-only tx, read-write tx,
         * read-committed, and unisolated views of the lexicon for a given
         * triple store.
         */
        valueFactory = BigdataValueFactoryImpl.getInstance(namespace);

        /*
         * @todo This should be a high concurrency LIRS or similar cache in
         * order to prevent the cache being flushed by the materialization of
         * low frequency terms.
         */
        {
            
            final int termCacheCapacity = Integer.parseInt(getProperty(
                    AbstractTripleStore.Options.TERM_CACHE_CAPACITY,
                    AbstractTripleStore.Options.DEFAULT_TERM_CACHE_CAPACITY));

            termCache = new ConcurrentWeakValueCacheWithBatchedUpdates<Long, BigdataValueImpl>(//
                    termCacheCapacity, // queueCapacity
                    .75f, // loadFactor (.75 is the default)
                    16 // concurrency level (16 is the default)
            );
            
        }
        
    }
    
    /**
     * The canonical {@link BigdataValueFactoryImpl} reference (JVM wide) for the
     * lexicon namespace.
     */
    public BigdataValueFactoryImpl getValueFactory() {
        
        return valueFactory;
        
    }
    final private BigdataValueFactoryImpl valueFactory;
    
    /**
     * Strengthens the return type.
     */
    @Override
    public AbstractTripleStore getContainer() {
        
        return (AbstractTripleStore) super.getContainer();
        
    }
    
    public boolean exists() {

        for(String name : getIndexNames()) {
            
            if (getIndex(name) == null)
                return false;
            
        }
        
        return true;

    }

    public void create() {
        
        final IResourceLock resourceLock = acquireExclusiveLock();
        
        try {

            super.create();

            final IIndexManager indexManager = getIndexManager();

            // register the indices.

            indexManager
                    .registerIndex(getTerm2IdIndexMetadata(getFQN(LexiconKeyOrder.TERM2ID)));

            indexManager
                    .registerIndex(getId2TermIndexMetadata(getFQN(LexiconKeyOrder.ID2TERM)));

            if (textIndex) {

                // create the full text index
                
                final FullTextIndex tmp = getSearchEngine();

                tmp.create();

            }

            /*
             * Note: defer resolution of the newly created index objects.
             */
            
//            term2id = super.getIndex(LexiconKeyOrder.TERM2ID);
//
//            id2term = super.getIndex(LexiconKeyOrder.ID2TERM);
//
//            assert term2id != null;
//
//            assert id2term != null;

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
            
            if (termCache != null) {

                termCache.clear();
                
            }
            
        } finally {

            unlock(resourceLock);

        }

    }
    
    volatile private IIndex id2term;
    volatile private IIndex term2id;
    private final boolean textIndex;
    private final boolean textIndexDatatypeLiterals;
    final boolean storeBlankNodes;
    final int termIdBitsToReverse;

    /**
     * The #of low bits from the term identifier that are reversed and
     * rotated into the high bits when it is assigned.
     * 
     * @see AbstractTripleStore.Options#TERMID_BITS_TO_REVERSE
     */
    final public int getTermIdBitsToReverse() {
        
        return termIdBitsToReverse;
        
    }
    
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
     * Overridden to return the hard reference for the index.
     */
    @Override
    public IIndex getIndex(final IKeyOrder<? extends BigdataValue> keyOrder) {

        if (keyOrder == LexiconKeyOrder.ID2TERM) {
     
            return getId2TermIndex();
            
        } else if (keyOrder == LexiconKeyOrder.TERM2ID) {
            
            return getTerm2IdIndex();
            
        } else {
            
            throw new AssertionError("keyOrder=" + keyOrder);
            
        }

    }

    final public IIndex getTerm2IdIndex() {

        if (term2id == null) {

            synchronized (this) {

                if (term2id == null) {

                    term2id = super.getIndex(LexiconKeyOrder.TERM2ID);

                    if (term2id == null)
                        throw new IllegalStateException();

                }

            }
            
        }

        return term2id;

    }

    final public IIndex getId2TermIndex() {

        if (id2term == null) {

            synchronized (this) {
                
                if (id2term == null) {

                    id2term = super.getIndex(LexiconKeyOrder.ID2TERM);
                
                    if (id2term == null)
                        throw new IllegalStateException();

                }

            }

        }

        return id2term;

    }

    /**
     * A factory returning the softly held singleton for the
     * {@link FullTextIndex}.
     * 
     * @see Options#TEXT_INDEX
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

    protected IndexMetadata getTerm2IdIndexMetadata(final String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new Term2IdTupleSerializer(getProperties()));
        
        return metadata;

    }

    protected IndexMetadata getId2TermIndexMetadata(final String name) {

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

    /**
     * Note : this method is part of the mutation api. it is primarily (at this
     * point, only) invoked by the rule execution layer and, at present, no
     * rules can entail terms into the lexicon.
     * 
     * @throws UnsupportedOperationException
     */
    public BigdataValue newElement(IPredicate<BigdataValue> predicate,
            IBindingSet bindingSet) {

        throw new UnsupportedOperationException();

    }

    public Class<BigdataValue> getElementClass() {

        return BigdataValue.class;

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

        if (log.isInfoEnabled()) {

            log.info("#lits=" + lits.length);

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

            final Properties properties = new Properties();

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
         * Note: The key builder was chosen to only encode the PRIMARY
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
                    protected Object resolve(final Object arg0) {

                        return ((ITuple) arg0).getValueBuffer()
                                .getLong(0/* pos */);

//                        try {
//
//                            tid = tuple.getValueStream().readLong();
////                            tid = LongPacker.unpackLong(tuple.getValueStream());
//
//                        } catch (IOException e) {
//
//                            throw new RuntimeException(e);
//
//                        }
//
//                        return tid;

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
     * @return An array of correlated key-value-object tuples.
     *         <p>
     *         Note that {@link KVO#val} is <code>null</code> until we know
     *         that we need to write it on the reverse index.
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
     * Batch insert of terms into the database.
     * <p>
     * Note: Duplicate {@link BigdataValue} references and {@link BigdataValue}s
     * that already have an assigned term identifiers are ignored by this
     * operation.
     * <p>
     * Note: This implementation is designed to use unisolated batch writes on
     * the terms and ids index that guarantee consistency.
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

        if (log.isDebugEnabled())
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
                        valueFactory, a, ndistinct, storeBlankNodes));

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
                        protected Object resolve(final Object obj) {
                        
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

        if (log.isInfoEnabled() && readOnly && stats.nunknown.get() > 0) {
         
            log.info("There are " + stats.nunknown + " unknown terms out of "
                    + numTerms + " given");
            
        }
        
        if (numTerms > 1000 || elapsed > 3000) {

            if(log.isInfoEnabled())
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
     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)}
     * , which is also responsible for invoking this method in order to have the
     * statement identifiers on hand before it writes on the statement indices.
     * <p>
     * Note: The caller's {@link ISPO}[] is sorted into SPO order as a
     * side-effect.
     * <p>
     * Note: The statement identifiers are assigned to the {@link ISPO}s as a
     * side-effect.
     * <p>
     * Note: SIDs are NOT supported for quads, so this code is never executed
     * for quads.
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
            final IKeyBuilder keyBuilder = KeyBuilder
                    .newInstance(1/* statement byte */+ (3/* triple */* Bytes.SIZEOF_LONG));

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

//                                    if (b[i].c() != 0L
//                                            && b[i].c() != result.ids[j]) {
//                                        System.err.println("spo="
//                                                + getContainer().toString(b[i])
//                                                + ", sid="
//                                                + getContainer().toString(
//                                                        result.ids[j]));
//                                    }

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

        if (log.isInfoEnabled() && n > 1000 || elapsed > 3000) {

            log.info("Wrote " + n + " in " + elapsed + "ms; keygen="
                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
                    + insertTime + "ms");

        }

    }

    /**
     * <p>
     * Add the terms to the full text index so that we can do fast lookup of the
     * corresponding term identifiers. Only literals are tokenized. Literals
     * that have a language code property are parsed using a tokenizer
     * appropriate for the specified language family. Other literals and URIs
     * are tokenized using the default {@link Locale}.
     * </p>
     * 
     * @param itr
     *            Iterator visiting the terms to be indexed.
     * 
     * @see #textSearch(String, String)
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     * 
     * @todo Provide a lucene integration point as an alternative to the
     *       {@link FullTextIndex}. Integrate for query and search of course.
     */
    protected void indexTermText(final int capacity,
            final Iterator<BigdataValue> itr) {

        final FullTextIndex ndx = getSearchEngine();

        final TokenBuffer buffer = new TokenBuffer(capacity, ndx);

        int n = 0;

        while (itr.hasNext()) {

            final BigdataValue val = itr.next();

            if (!(val instanceof Literal)) {

                /*
                 * Note: If you allow URIs to be indexed then the code which is
                 * responsible for free text search for quads must impose a
                 * filter on the subject and predicate positions to ensure that
                 * free text search can not be used to materialize literals or
                 * URIs from other graphs. This matters when the named graphs
                 * are used as an ACL mechanism. This would also be an issue if
                 * literals were allowed into the subject position.
                 */
                continue;
                
            }

            final Literal lit = (Literal) val;

            if (!textIndexDatatypeLiterals && lit.getDatatype() != null) {

                // do not index datatype literals in this manner.
                continue;

            }

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

        if (log.isInfoEnabled())
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
     */
    final public Map<Long,BigdataValue> getTerms(final Collection<Long> ids) {

        if (ids == null)
            throw new IllegalArgumentException();

        // Maximum #of distinct term identifiers.
        final int n = ids.size();
        
        if (n == 0) {

            return Collections.emptyMap();
            
        }

        final long begin = System.currentTimeMillis();

        /*
         * Note: A concurrent hash map is used since the request may be split
         * across shards, in which case updates on the map may be concurrent.
         */
        final ConcurrentHashMap<Long/* id */, BigdataValue/* term */> ret = new ConcurrentHashMap<Long, BigdataValue>(
                n);

        /*
         * An array of any term identifiers that were not resolved in this first
         * stage of processing. We will look these up in the index.
         */
        final Long[] notFound = new Long[n];

        int numNotFound = 0;
        
        for (Long lid : ids) {

            final BigdataValueImpl value = _getTermId(lid);
            
            if (value != null) {

                assert value.getValueFactory() == valueFactory;

                // resolved.
                ret.put(lid, value);//valueFactory.asValue(value));
                
                continue;

            }

            /*
             * We will need to test the index for this term identifier.
             */
            notFound[numNotFound++] = lid;
            
        }

        /*
         * batch lookup.
         */
        if (numNotFound > 0) {

            if (log.isInfoEnabled())
                log.info("nterms=" + n + ", numNotFound=" + numNotFound
                        + (termCache!=null?(", cacheSize=" + termCache.size() + "\n"
//                        + termCache.getStatistics()
                        ): ""));

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
                
                final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(nchunks);

                int fromIndex = 0;
                int remaining = numNotFound;

                for (int i = 0; i < nchunks; i++) {

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
                
                final List<Future<Void>> futures;
                try {
                    futures = getExecutorService().invokeAll(tasks);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                
                for(Future<?> f : futures) {
                    
                    // verify task executed Ok.
                    try {
                        f.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }                    
                }

                final long elapsed = System.currentTimeMillis() - begin;
                
                if (log.isInfoEnabled())
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
         *            identifier as the key. This is a concurrent map because
         *            the operation may have been split across multiple shards,
         *            in which case the updates to the map can be concurrent.
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
                            .getLeafValuesCoder());

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
             */
            {

                final IRaba vals = results.getValues();
                
                for (int i = fromIndex; i < toIndex; i++) {

                    final Long lid = notFound[i];

                    final byte[] data = vals.get(i);

                    if (data == null) {

                        log.warn("No such term: " + lid);

                        continue;

                    }

                    /*
                     * Note: This automatically sets the valueFactory reference
                     * on the de-serialized value.
                     */
                    BigdataValueImpl value = valueFactory.getValueSerializer()
                            .deserialize(data);
                    
                    // Set the term identifier.
                    value.setTermId(lid.longValue());

                    if (termCache != null) {

//                        synchronized (termCache) {
//
//                            if (termCache.get(id) == null) {
//
//                                termCache.put(id, value, false/* dirty */);
//
//                            }
//                            
//                        }
                        
                        final BigdataValueImpl tmp = termCache.putIfAbsent(lid,
                                value);

                        if (tmp != null) {

                            value = tmp;

                        }
                            
                    }

                    /*
                     * The term identifier was set when the value was
                     * de-serialized. However, this will throw an
                     * IllegalStateException if the value somehow was assigned
                     * the wrong term identifier (paranoia test).
                     */
                    assert value.getTermId() == lid : "expecting id=" + lid
                            + ", but found " + value.getTermId();
                    assert ((BigdataValueImpl) value).getValueFactory() == valueFactory;

                    // save in caller's concurrent map.
                    map.put(lid, value);

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
     * Recently resolved term identifiers are cached to improve performance when
     * externalizing statements.
     * 
     * @todo consider using this cache in the batch API as well or simply modify
     *       the {@link StatementBuffer} to use a term cache in order to
     *       minimize the #of terms that it has to resolve against the indices -
     *       this especially matters for the scale-out implementation.
     *       <p>
     *       Or perhaps this can be rolled into the {@link ValueFactory} impl
     *       along with the reverse bnodes mapping?
     */
    private ConcurrentWeakValueCacheWithBatchedUpdates<Long, BigdataValueImpl> termCache;

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
     * Handles {@link IRawTripleStore#NULL}, blank nodes (unless the told bnodes
     * mode is being used), statement identifiers, and tests the
     * {@link #termCache}. When told bnodes are not being used, then if the term
     * identifier is a blank node the corresponding {@link BigdataValue} will be
     * dynamically generated and returned. If the term identifier is a SID, then
     * the corresponding {@link BigdataValue} will be dynamically generated and
     * returned. Finally, if the term identifier is found in the
     * {@link #termCache}, then the cached {@link BigdataValue} will be
     * returned.
     * 
     * @param lid
     *            A term identifier (passed as a {@link Long} to avoid creation
     *            of a {@link Long} inside of this method).
     * 
     * @return The corresponding {@link BigdataValue} if the term identifier is
     *         a blank node identifier, a statement identifier, or found in the
     *         {@link #termCache} and <code>null</code> otherwise.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</i> is {@link IRawTripleStore#NULL}.
     */
    private BigdataValueImpl _getTermId(final Long lid) {

        final long id = lid.longValue();
        
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

            final BigdataBNodeImpl stmt = valueFactory.createBNode("S"
                    + Long.toString(id));

            // set the term identifier on the object.
            stmt.setTermId(id);

            // mark as a statement identifier.
            stmt.statementIdentifier = true;

            return stmt;

        }

        if (!storeBlankNodes && AbstractTripleStore.isBNode(id)) {

            /*
             * Except when the "told bnodes" mode is enabled, blank nodes are
             * not stored in the reverse lexicon (or the cache). The "B" prefix
             * is a syntactic marker for a real blank node.
             * 
             * Note: In a told bnodes mode, we need to store the blank nodes in
             * the lexicon and enter them into the term cache since their
             * lexical form will include the specified ID, not the term
             * identifier.
             * 
             * @see TestAddTerms
             */

            final BigdataBNodeImpl bnode = valueFactory.createBNode("B"
                    + Long.toString(id));

            // set the term identifier on the object.
            bnode.setTermId(id);

            return bnode;

        }

        // test the term cache.
        if (termCache != null) {

            // Note: passing the Long from the caller as the cache key.
            return termCache.get(lid);

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
        
        // Convert to Long object.
        final Long lid = Long.valueOf(id);
        
        // handle NULL, bnodes, statement identifiers, and the termCache.
        BigdataValueImpl value = _getTermId(lid);
        
        if (value != null)
            return value;
        
        final IIndex ndx = getId2TermIndex();

        final Id2TermTupleSerializer tupleSer = (Id2TermTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.id2key(id);

        final byte[] data = ndx.lookup(key);

        if (data == null) {

            return null;

        }

        // This also sets the value factory.
        value = valueFactory.getValueSerializer().deserialize(data);
        
        // This sets the term identifier.
        value.setTermId(id);

        if (termCache != null) {

//            synchronized (termCache) {
//
//            /*
//             * Note: This code block is synchronized to address a possible race
//             * condition where concurrent threads resolve the term against the
//             * database. It both threads attempt to insert their resolved term
//             * definitions, which are distinct objects, into the cache then one
//             * will get an IllegalStateException since the other's object will
//             * already be in the cache.
//             */
//
//            if (termCache.get(id) == null) {
//
//                termCache.put(id, value, false/* dirty */);
//
//            }
            
            // Note: passing the Long object as the key.
            final BigdataValueImpl tmp = termCache.putIfAbsent(lid, value);

            if (tmp != null) {

                value = tmp;

            }

        }

        assert value.getTermId() == id : "expecting id=" + id + ", but found "
                + value.getTermId();
        //        value.setTermId( id );

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
        final long termId = new FixedByteArrayBuffer(tmp, 0/* off */, 8/* len */)
                .getLong(0);
//        final long termId;
//        try {
//
//            termId = new DataInputBuffer(tmp).readLong();
////            termId = new DataInputBuffer(tmp).unpackLong();
//
//        } catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }

        if(value instanceof BigdataValue) {

            final BigdataValueImpl impl = (BigdataValueImpl)value;
            
            // set as side-effect.
            impl.setTermId(termId);

            /*
             * Note that we have the termId and the term, we stick the value
             * into in the term cache IFF it has the correct value factory, but
             * do not replace the entry if there is one already there.
             */

            if (termCache != null && impl.getValueFactory() == valueFactory) {

                if (storeBlankNodes || !AbstractTripleStore.isBNode(termId)) {

                    // if (termCache.get(id) == null) {
                    //
                    // termCache.put(id, value, false/* dirty */);
                    //
                    // }

                    termCache.putIfAbsent(termId, impl);

                }
                
            }

        }

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
            protected Object resolve(final Object obj) {

                return ((ITuple<BigdataValue>) obj).getObject();

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
             * Deserialize the term identifier (long integer).
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
             *            the term identifier (Long).
             */
            protected Object resolve(final Object val) {

                // resolve against the id:term index (random lookup).
                return getTerm(((Long) val).longValue());

            }

        });

    }

    /**
     * Dumps the lexicon in a variety of ways.
     */
    public StringBuilder dumpTerms() {

        final StringBuilder sb = new StringBuilder();
        
        /*
         * Note: it is no longer true that all terms are stored in the reverse
         * index (BNodes are not). Also, statement identifiers are stored in the
         * forward index, so we can't really write the following assertion
         * anymore.
         */
//        // Same #of terms in the forward and reverse indices.
//        assertEquals("#terms", store.getIdTermIndex().rangeCount(null, null),
//                store.getTermIdIndex().rangeCount(null, null));
        
        /**
         * Dumps the forward mapping.
         */
        {

            sb.append("---- terms index (forward mapping) ----\n");

            final IIndex ndx = getTerm2IdIndex();

            final ITupleIterator<?> itr = ndx.rangeIterator(null, null);

            while (itr.hasNext()) {

                final ITuple<?> tuple = itr.next();
                
//                // the term identifier.
//                Object val = itr.next();

                /*
                 * The sort key for the term. This is not readily decodable. See
                 * RdfKeyBuilder for specifics.
                 */
                final byte[] key = tuple.getKey();

                /*
                 * deserialize the term identifier (packed long integer).
                 */
                final long id;
                
                try {

                    id = tuple.getValueStream().readLong();
//                    id = tuple.getValueStream().unpackLong();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

                sb.append(BytesUtil.toString(key) + " : " + id+"\n");

            }

        }

        /**
         * Dumps the reverse mapping.
         */
        {

            sb.append("---- ids index (reverse mapping) ----\n");

            final IIndex ndx = getId2TermIndex();

            final ITupleIterator<BigdataValue> itr = ndx.rangeIterator(null, null);

            while (itr.hasNext()) {

                final ITuple<BigdataValue> tuple = itr.next();
                
                final BigdataValue term = tuple.getObject();
                
                sb.append(term.getTermId()+ " : " + term+"\n");

            }

        }
        
        /**
         * Dumps the term:id index.
         */
        sb.append("---- term->id ----\n");
        for( Iterator<Long> itr = termIdIndexScan(); itr.hasNext(); ) {
            
            sb.append(itr.next());
            
            sb.append("\n");
            
        }

        /**
         * Dumps the id:term index.
         */
        sb.append("---- id->term ----\n");
        for( Iterator<Value> itr = idTermIndexScan(); itr.hasNext(); ) {
            
            sb.append(itr.next());
            
            sb.append("\n");
            
        }

        /**
         * Dumps the terms in term order.
         */
        sb.append("---- terms in term order ----\n");
        for( Iterator<Value> itr = termIterator(); itr.hasNext(); ) {
            
            sb.append(itr.next().toString());
            
            sb.append("\n");
            
        }
        
        return sb;
        
    }
    
}
