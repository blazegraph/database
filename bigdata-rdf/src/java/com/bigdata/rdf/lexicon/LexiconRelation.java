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
 * Created on Jul 4, 2008
 */

package com.bigdata.rdf.lexicon;

import java.io.IOException;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.ASCIIKeyBuilderFactory;
import com.bigdata.btree.BTree;
import com.bigdata.btree.DefaultKeyBuilderFactory;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBufferHandler;
import com.bigdata.btree.AbstractTupleFilterator.CompletionScan;
import com.bigdata.btree.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.KeyBuilder.StrengthEnum;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.Id2TermWriteProc.Id2TermWriteProcConstructor;
import com.bigdata.rdf.lexicon.Term2IdTupleSerializer.LexiconKeyBuilder;
import com.bigdata.rdf.lexicon.Term2IdWriteProc.Term2IdWriteProcConstructor;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator2;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.IKeyOrder;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.TokenBuffer;
import com.bigdata.service.Split;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * The {@link LexiconRelation} handles all things related to the indices mapping
 * RDF {@link Value}s onto internal 64-bit term identifiers.
 * <p>
 * The term2id index has all the distinct terms ever asserted and those "terms"
 * include SPO keys for statements if statement identifiers are in use.
 * <p>
 * The id2term index only has URIs and Literals (it can not used to resolve
 * either blank nodes or statement identifiers -- there is in fact no means to
 * resolve either a statement identifier or a blank node. Both are always
 * assigned (consistently) within a context in which their referent (if any) is
 * defined. For a statement identifier the referent MUST be defined by an
 * instance of the statement itself. The RIO parser integration and
 * {@link StatementBuffer} handles all of this stuff).
 * 
 * FIXME The only reason to enter {@link BNode} into the foward (term->id) index
 * is during data load. We can do without bnodes in the forward index also as
 * long as we (a) keep a hash or btree whose scope is the data load (a btree
 * would be required for very large files); and (b) we hand the {@link BNode}s
 * to {@link Term2IdWriteProc}s but it only assigns a one up term identifier
 * and does NOT enter the bnode into the index. The local (file load scope only)
 * bnode resolution is the only context in which it is possible for two
 * {@link BNode} {@link BNode#getID() ID}s to be interpreted as the same ID and
 * therefore assigned the same term identifier. In all other cases we will
 * assign a new term identifier. The assignment of the term identifier for a
 * BNode ID can be from ANY key-range partition of the term:id index.
 * <P>
 * This can be done by creating a {@link UUID} "seed" for bnodes in each
 * document loaded by the {@link StatementBuffer} and then issuing one-up bnode
 * IDs by appending a counter. At that point we do not need to store the Bnodes
 * in the forward index either and all bnode ids for a given document will hit
 * the same index partition, which will improve load performance. (Also do this
 * for the {@link BigdataValueFactory}.)
 * 
 * FIXME clean up the lexicon. it is some of the oldest code. the
 * {@link OptimizedValueFactory} has some very special support for batch writes
 * on the indices but it would be nice to refactor that so that efficient writes
 * could be performed using {@link BigdataValueImpl}s, e.g., by moving the
 * <code>unsigned byte[]</code> keys into a correlated data structure.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconRelation extends AbstractRelation<BigdataValue> {

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

        // Explicitly disable overwrite for the lexicon (@todo iff create)
        properties.setProperty(Options.OVERWRITE, "false");

        // Note: the full text index is not allowed unless the lexicon is
        // enabled.
        {

            this.textIndex = Boolean.parseBoolean(properties.getProperty(
                    Options.TEXT_INDEX, Options.DEFAULT_TEXT_INDEX));
         
            log.info(Options.TEXT_INDEX + "=" + textIndex);
            
        }
        
        {

            branchingFactor = Integer
                    .parseInt(properties.getProperty(Options.BRANCHING_FACTOR,
                            Options.DEFAULT_BRANCHING_FACTOR));

            if (branchingFactor < BTree.MIN_BRANCHING_FACTOR) {

                throw new IllegalArgumentException(Options.BRANCHING_FACTOR
                        + " must be at least " + BTree.MIN_BRANCHING_FACTOR);

            }

            log.info(Options.BRANCHING_FACTOR + "=" + branchingFactor);

        }
        
        {

            final Set<String> set = new HashSet<String>();

            set.add(getFQN(LexiconKeyOrder.TERM2ID));

            set.add(getFQN(LexiconKeyOrder.ID2TERM));

            if(textIndex) {
                
                set.add(getNamespace() + FullTextIndex.NAME_SEARCH);
                
            }
            
            // @todo add names as registered to base class? but then how to
            // discover?  could be in the global row store.
            this.indexNames = Collections.unmodifiableSet(set);

        }

        term2id = indexManager.getIndex(getFQN(LexiconKeyOrder.TERM2ID),
                getTimestamp());

        id2term = indexManager.getIndex(getFQN(LexiconKeyOrder.ID2TERM),
                getTimestamp());

        if(textIndex) {
            
            getSearchEngine();
            
        }
        
    }

    public boolean exists() {

        return term2id != null && id2term != null;

    }

    public void create() {

        final IResourceLock resourceLock = getIndexManager()
                .getResourceLockManager().acquireExclusiveLock(getNamespace());

        try {

            super.create();

            final IndexMetadata id2TermMetadata = getId2TermIndexMetadata(getFQN(LexiconKeyOrder.ID2TERM));

            final IndexMetadata term2IdMetadata = getTerm2IdIndexMetadata(getFQN(LexiconKeyOrder.TERM2ID));

            final IIndexManager indexManager = getIndexManager();

            indexManager.registerIndex(term2IdMetadata);

            indexManager.registerIndex(id2TermMetadata);

            term2id = indexManager.getIndex(getFQN(LexiconKeyOrder.TERM2ID),
                    getTimestamp());

            id2term = indexManager.getIndex(getFQN(LexiconKeyOrder.ID2TERM),
                    getTimestamp());

            if (textIndex) {

                FullTextIndex tmp = getSearchEngine();

                tmp.create();

            }

        } finally {

            resourceLock.unlock();

        }

    }

    public void destroy() {

        final IResourceLock resourceLock = getIndexManager()
                .getResourceLockManager().acquireExclusiveLock(getNamespace());

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

        } finally {

            resourceLock.unlock();

        }

    }
    
    private IIndex id2term;
    private IIndex term2id;
    private final boolean textIndex;
    private final int branchingFactor;

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
     */
    public FullTextIndex getSearchEngine() {

        if(!textIndex) return null;
        
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

    /**
     * Shared {@link IndexMetadata} configuration.
     * 
     * @param name
     *            The index name.
     * 
     * @return A new {@link IndexMetadata} object for that index.
     */
    protected IndexMetadata getIndexMetadata(String name) {

        final IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());

        metadata.setBranchingFactor(branchingFactor);
        
        /*
         * Note: Mainly used for torture testing.
         */
        if(false){
            
            // An override that makes a split very likely.
            final ISplitHandler splitHandler = new DefaultSplitHandler(
                    10 * Bytes.kilobyte32, // minimumEntryCount
                    50 * Bytes.kilobyte32, // entryCountPerSplit
                    1.5, // overCapacityMultiplier
                    .75, // underCapacityMultiplier
                    20 // sampleRate
            );
            
            metadata.setSplitHandler(splitHandler);
            
        }
                
        return metadata;

    }

    protected IndexMetadata getTerm2IdIndexMetadata(String name) {

        final IndexMetadata metadata = getIndexMetadata(name);

        metadata.setTupleSerializer(new Term2IdTupleSerializer(getProperties()));
        
        return metadata;

    }

    protected IndexMetadata getId2TermIndexMetadata(String name) {

        final IndexMetadata metadata = getIndexMetadata(name);

        metadata.setTupleSerializer(new Id2TermTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG)));

        return metadata;

    }

    @Override
    public String getFQN(IKeyOrder<? extends BigdataValue> keyOrder) {

        return getNamespace() + ((LexiconKeyOrder)keyOrder).getIndexName();
        
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
     * @todo Not implemented yet. Exact counts are not very useful on this
     *       relation.
     * 
     * @throws UnsupportedOperationException
     */
    public long getElementCount(boolean exact) {

        throw new UnsupportedOperationException();
        
    }

    /**
     * Note : this method is part of the mutation api. it is primarily (at this
     * point, only) invoked by the rule execution layer and, at present, no
     * rules can entail terms into the lexicon.
     * 
     * @throws UnsupportedOperationException
     */
    public Object newElement(IPredicate predicate, IBindingSet bindingSet) {
        
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
     * A completion scan of all literals having any of the given literal as a
     * prefix.
     * 
     * @param lit
     *            A literal.
     * 
     * @return An iterator visiting the term identifiers for the matching
     *         {@link Literal}s.
     */
    public Iterator<Long> completionScan(Literal lit) {

        if (lit == null)
            throw new IllegalArgumentException();
        
        return completionScan(new Literal[] { lit });
        
    }
    
    /**
     * A completion scan of all literals having any of the given literals as a
     * prefix.
     * 
     * @param lits An array of literals.
     * 
     * @return An iterator visiting the term identifiers for the matching
     *         {@link Literal}s.
     * 
     * FIXME Re-factor into an operation that can run against the scale-out
     * indices and then migrate into the {@link AbstractTripleStore}.
     * <p>
     * The {@link #completionScan(Literal[])} presumes that it has a local
     * {@link BTree}, but the rest of the code should already scale-out.
     */
    @SuppressWarnings("unchecked")
    public Iterator<Long> completionScan(Literal[] lits) {

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

        final BTree ndx = (BTree) getTerm2IdIndex();

        final Iterator<Long> termIdIterator = new Striterator(new CompletionScan(//
                (ITupleCursor) ndx
                                .rangeIterator(null/* fromKey */,
                                        null/* toKey */, 0/* capacity */,
                                        IRangeQuery.DEFAULT
                                                | IRangeQuery.CURSOR, null/* filter */), //
                keys)).addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    /**
                     * Decode the value, which is the term identifier.
                     */
            @Override
            protected Object resolve(Object arg0) {

                final ITuple tuple = (ITuple)arg0;
                
                final long tid;
                try {

                    tid = LongPacker.unpackLong(tuple.getValueStream());
                    
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
     *            The object used to generate the sort keys - <em>this is not
     *            safe for concurrent writers</em>
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @see LexiconKeyBuilder
     */
    final public void generateSortKeys(LexiconKeyBuilder keyBuilder,
            _Value[] terms, int numTerms) {

        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }

    /**
     * This implementation is designed to use unisolated batch writes on the
     * terms and ids index that guarentee consistency.
     * 
     * @param terms
     * @param numTerms
     * @param readOnly
     *            When <code>true</code>, unknown terms will not be inserted
     *            into the database. Otherwise unknown terms are inserted into
     *            the database.
     * 
     * @todo "known" terms should be filtered out of this operation.
     *       <p>
     *       A term is marked as "known" within a client if it was successfully
     *       asserted against both the terms and ids index (or if it was
     *       discovered on lookup against the ids index).
     * 
     * FIXME We should also check the {@link AbstractTripleStore#termCache} for
     * known terms and NOT cause them to be defined in the database (the
     * {@link _Value#known} flag needs to be set so that we are assured that the
     * term is in both the forward and reverse indices). Also, evaluate the
     * impact on the LRU {@link #termCache} during bulk load operations. Perhaps
     * that cache should be per thread? (Review the {@link WeakValueCache} and
     * see if perhaps it can be retrofitted to use {@link ConcurrentHashMap} and
     * to support full concurrency).
     */
    public void addTerms(final _Value[] terms, final int numTerms, boolean readOnly) {

        if (numTerms == 0)
            return;

        final long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort
                                // keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and
                                // reverse index.
        long indexerTime = 0; // time to insert terms into the text indexer.

        /*
         * Insert into the forward index (term -> id). This will either assign a
         * termId or return the existing termId if the term is already in the
         * lexicon.
         */
        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            {

                long _begin = System.currentTimeMillis();
                
                final Term2IdTupleSerializer tupleSer = (Term2IdTupleSerializer) getIndex(
                        LexiconKeyOrder.TERM2ID).getIndexMetadata()
                        .getTupleSerializer();

//                // per-thread key builder.
//                final RdfKeyBuilder keyBuilder = getKeyBuilder();

                generateSortKeys(tupleSer.getLexiconKeyBuilder(), terms, numTerms);

                keyGenTime = System.currentTimeMillis() - _begin;

            }

            /*
             * Sort terms by their assigned sort key. This places them into the
             * natural order for the term:id index.
             */
            {

                long _begin = System.currentTimeMillis();

                Arrays.sort(terms, 0, numTerms,
                        _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;

            }

            /*
             * For each term that does not have a pre-assigned term identifier,
             * add it to a remote unisolated batch operation that assigns term
             * identifiers.
             */
            {

                final long _begin = System.currentTimeMillis();

                final IIndex termIdIndex = getTerm2IdIndex();

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys().
                 */
                final byte[][] keys = new byte[numTerms][];
                {

                    for (int i = 0; i < numTerms; i++) {

                        keys[i] = terms[i].key;

                    }

                }

                // run the procedure.
                termIdIndex.submit(0/* fromIndex */, numTerms/* toIndex */,
                        keys, null/* vals */,
                        (readOnly ? Term2IdWriteProcConstructor.READ_ONLY
                                : Term2IdWriteProcConstructor.READ_WRITE),
                        new IResultHandler<Term2IdWriteProc.Result, Void>() {

                            /**
                             * Copy the assigned/discovered term identifiers
                             * onto the corresponding elements of the terms[].
                             */
                            public void aggregate(Term2IdWriteProc.Result result,
                                    Split split) {

                                for (int i = split.fromIndex, j = 0; i < split.toIndex; i++, j++) {

                                    terms[i].termId = result.ids[j];

                                }

                            }

                            public Void getResult() {

                                return null;

                            }

                        });

                insertTime += System.currentTimeMillis() - _begin;

            }

        }

        if(!readOnly) {
                
            {
    
                /*
                 * Sort terms based on their assigned termId (when interpreted as
                 * unsigned long integers).
                 */
    
                long _begin = System.currentTimeMillis();
    
                Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);
    
                sortTime += System.currentTimeMillis() - _begin;
    
            }
    
            {
    
                /*
                 * Add terms to the reverse index, which is the index that we use to
                 * lookup the RDF value by its termId to serialize some data as
                 * RDF/XML or the like.
                 * 
                 * Note: Every term asserted against the forward mapping [terms]
                 * MUST be asserted against the reverse mapping [ids] EVERY time.
                 * This is required in order to guarentee that the reverse index
                 * remains complete and consistent. Otherwise a client that writes
                 * on the terms index and fails before writing on the ids index
                 * would cause those terms to remain undefined in the reverse index.
                 */
    
                final long _begin = System.currentTimeMillis();
    
                final IIndex idTermIndex = getId2TermIndex();
    
                /*
                 * Create a key buffer to hold the keys generated from the term
                 * identifers and then generate those keys. The terms are already in
                 * sorted order by their term identifiers from the previous step.
                 * 
                 * Note: We DO NOT write BNodes on the reverse index.
                 */
                final byte[][] keys = new byte[numTerms][];
                final byte[][] vals = new byte[numTerms][];
                int nonBNodeCount = 0; // #of non-bnodes.
                {
    
                    // thread-local key builder removes single-threaded constraint.
                    final IKeyBuilder tmp = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);
    
                    // buffer is reused for each serialized term.
                    final DataOutputBuffer out = new DataOutputBuffer();
    
                    for (int i = 0; i < numTerms; i++) {
    
                        if (terms[i] instanceof BNode) {
    
                            terms[i].known = true;
    
                            continue;
    
                        }
    
                        keys[nonBNodeCount] = tmp.reset().append(terms[i].termId)
                                .getKey();
    
                        // Serialize the term.
                        vals[nonBNodeCount] = terms[i].serialize(out.reset());
    
                        nonBNodeCount++;
    
                    }
    
                }
    
                // run the procedure on the index.
                if (nonBNodeCount > 0) {
    
                    idTermIndex.submit(0/* fromIndex */,
                            nonBNodeCount/* toIndex */, keys, vals,
                            Id2TermWriteProcConstructor.INSTANCE,
                            new IResultHandler<Void, Void>() {
    
                                /**
                                 * Since the unisolated write succeeded the client
                                 * knows that the term is now in both the forward
                                 * and reverse indices. We codify that knowledge by
                                 * setting the [known] flag on the term.
                                 */
                                public void aggregate(Void result, Split split) {
    
                                    for (int i = split.fromIndex, j = 0; i < split.toIndex; i++, j++) {
    
                                        terms[i].known = true;
    
                                    }
    
                                }
    
                                public Void getResult() {
    
                                    return null;
    
                                }
                            });
    
                }
    
                insertTime += System.currentTimeMillis() - _begin;
    
            }
    
            /*
             * Index the terms for keyword search : @todo parallelize w/ reverse index write.
             */
            if (textIndex && getSearchEngine() != null) {
    
                final long _begin = System.currentTimeMillis();
    
                indexTermText(terms, numTerms);
    
                indexerTime = System.currentTimeMillis() - _begin;
    
            }
    
        } // if(!readOnly)

        final long elapsed = System.currentTimeMillis() - begin;

        if (numTerms > 1000 || elapsed > 3000) {

            log.info("Processed " + numTerms + " in " + elapsed
                            + "ms; keygen=" + keyGenTime + "ms, sort="
                            + sortTime + "ms, insert=" + insertTime + "ms"
                            + ", indexerTime=" + indexerTime + "ms");

        }

    }

    /**
     * Assign unique statement identifiers to triples.
     * <p>
     * Each distinct {@link StatementEnum#Explicit} {s,p,o} is assigned a unique
     * statement identifier using the {@link IRawTripleStore#getTerm2IdIndex()}.
     * The assignment of statement identifiers is <i>consistent</i> using an
     * unisolated atomic write operation similar to
     * {@link #addTerms(_Value[], int)}.
     * <p>
     * Note: Statement identifiers are NOT inserted into the reverse (id:term)
     * index. Instead, they are written into the values associated with the
     * {s,p,o} in each of the statement indices. That is handled by
     * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)},
     * which is also responsible for invoking this method in order to have the
     * statement identifiers on hand before it writes on the statement indices.
     * <p>
     * Note: The caller's {@link SPO}[] is sorted into SPO order as a
     * side-effect.
     * <p>
     * Note: The statement identifiers are assigned to the {@link SPO}s as a
     * side-effect.
     * 
     * @todo use #termCache to shortcut recently used statements?
     */
    public void addStatementIdentifiers(final SPO[] a, final int n) {

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
        final SPO[] b = new SPO[n];

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

                SPO spo = a[i];

                if (!spo.isExplicit())
                    continue;
                
                if (!spo.isFullyBound())
                    throw new IllegalArgumentException("Not fully bound: "
                            + spo.toString(/*this*/));

                keys[nexplicit] = keyBuilder.reset() //
                        .append(ITermIndexCodes.TERM_CODE_STMT)//
                        .append(spo.s).append(spo.p).append(spo.o)//
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
                        keys, null/* vals */, Term2IdWriteProcConstructor.READ_WRITE,
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

        if (n > 1000 || elapsed > 3000) {

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
     * @see #textSearch(String, String)
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     */
    protected void indexTermText(_Value[] terms, int numTerms) {

        final FullTextIndex ndx = getSearchEngine();

        final TokenBuffer buffer = new TokenBuffer(numTerms, ndx);

        int n = 0;

        for (int i = 0; i < numTerms; i++) {

            _Value val = terms[i];

            if (!(val instanceof Literal))
                continue;

            // do not index datatype literals in this manner.
            if (((_Literal) val).datatype != null)
                continue;

            final String languageCode = ((_Literal) val).language;

            // Note: May be null (we will index plain literals).
            // if(languageCode==null) continue;

            final String text = ((_Literal) val).term;

            /*
             * Note: The OVERWRITE option is turned off to avoid some of the
             * cost of re-indexing each time we see a term.
             */

            assert val.termId != 0L; // the termId must have been assigned.

            ndx.index(buffer, val.termId, 0/* fieldId */, languageCode,
                    new StringReader(text));

            n++;

        }

        // flush writes to the text index.
        buffer.flush();

        log.info("indexed " + n + " new terms");

    }

    /**
     * Batch resolution of term identifiers to {@link BigdataValue}s.
     * <p>
     * Note: the returned values will be {@link BigdataValueImpl}s that play well
     * with Sesame 2.x
     * 
     * @param ids
     *            An collection of term identifiers.
     * 
     * @return A map from term identifier to the {@link BigdataValue}. If a
     *         term identifier was not resolved then the map will not contain an
     *         entry for that term identifier.
     * 
     * FIXME write unit tests getTerms(ids) - it's mainly used by the
     *       {@link BigdataStatementIteratorImpl}
     */
    final public Map<Long,BigdataValue> getTerms(Collection<Long> ids) {

        if (ids == null)
            throw new IllegalArgumentException();

        // Maximum #of distinct term identifiers.
        final int n = ids.size();
        
        if (n == 0) {

            return Collections.EMPTY_MAP;
            
        }

        final Map<Long/* id */, BigdataValue/* term */> ret = new HashMap<Long, BigdataValue>( n );

        /*
         * An array of any term identifiers that were not resolved in this first
         * stage of processing. We will look these up in the index.
         */
        final Long[] notFound = new Long[n];

        int numNotFound = 0;
        
        for(Long id : ids ) {
            
            final _Value value = _getTermId(id.longValue());
            
            if (value != null) {
            
                // resolved.
                ret.put(id, OptimizedValueFactory.INSTANCE.toSesameObject(value));
                
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

            if(log.isInfoEnabled())
            log.info("Will resolve "+numNotFound+" term identifers against the index.");
            
            // sort term identifiers into index order.
            Arrays.sort(notFound, 0, numNotFound, TermIdComparator2.INSTANCE);
            
            final IKeyBuilder keyBuilder = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);

            final byte[][] keys = new byte[numNotFound][];

            for(int i=0; i<numNotFound; i++) {

                // Note: shortcut for keyBuilder.id2key(id)
                keys[i] = keyBuilder.reset().append(notFound[i]).getKey();
                
            }

            // the id:term index.
            final IIndex ndx = getId2TermIndex();

            // aggregates results if lookup split across index partitions.
            final ResultBufferHandler resultHandler = new ResultBufferHandler(
                    numNotFound, ndx.getIndexMetadata().getLeafValueSerializer());

            // batch lookup
            ndx.submit(0/* fromIndex */, numNotFound/* toIndex */, keys,
                    null/* vals */, BatchLookupConstructor.INSTANCE,
                    resultHandler);
        
            // the aggregated results.
            final ResultBuffer results = resultHandler.getResult();
            
            // synchronize on the term cache before updating it.
            synchronized (termCache) {

                for (int i = 0; i < numNotFound; i++) {
                    
                    final Long id = notFound[i];

                    final byte[] data = results.getResult(i);

                    if (data == null) {

                        log.warn("No such term: " + id );

                        continue;

                    }

                    final _Value value = _Value.deserialize(data);

                    /*
                     * Note: This code block is synchronized to address a
                     * possible race condition where concurrent threads resolve
                     * the term against the database. It both threads attempt to
                     * insert their resolved term definitions, which are
                     * distinct objects, into the cache then one will get an
                     * IllegalStateException since the other's object will
                     * already be in the cache.
                     */

                    if (termCache.get(id) == null) {

                        termCache.put(id, value, false/* dirty */);

                    }

                    // @todo modify unit test to verify that these fields are
                    // being set.

                    value.termId = id;

                    value.known = true;

                    // convert to object that complies with the sesame APIs.
                    ret.put(id, OptimizedValueFactory.INSTANCE.toSesameObject(value));
                    
                }

            }
            
        }
        
        return ret;
        
    }
    
    /**
     * Recently resolved term identifers are cached to improve performance when
     * externalizing statements.
     * 
     * @todo consider using this cache in the batch API as well or simply modify
     *       the {@link StatementBuffer} to use a term cache in order to
     *       minimize the #of terms that it has to resolve against the indices -
     *       this especially matters for the scale-out implementation.
     */
    protected LRUCache<Long, _Value> termCache = new LRUCache<Long, _Value>(
            10000);

    /**
     * Handles {@link IRawTripleStore#NULL}, blank node identifiers, statement
     * identifiers, and the {@link #termCache}.
     * 
     * @param id
     *            A term identifier.
     * 
     * @return The corresponding {@link _Value} if the term identifier is a
     *         blank node identifier, a statement identifier, or found in the
     *         {@link #termCache}.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</i> is {@link IRawTripleStore#NULL}.
     */
    private _Value _getTermId(long id) {
        
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

            _BNode stmt = (_BNode) OptimizedValueFactory.INSTANCE
                    .createBNode("S" + id);

            // set the term identifier on the object.
            stmt.termId = id;

            return stmt;
            
        }

        if (AbstractTripleStore.isBNode(id)) {

            /*
             * BNodes are not stored in the reverse lexicon (or the cache). The
             * "B" prefix is a syntactic marker for a real blank node.
             */

            _BNode bnode = (_BNode) OptimizedValueFactory.INSTANCE
                    .createBNode("B" + id);

            // set the term identifier on the object.
            bnode.termId = id;

            return bnode;

        }

        // test the term cache.
        final _Value value = termCache.get(id);

        return value;

    }
    
    /**
     * Note: {@link BNode}s are not stored in the reverse lexicon and are
     * recognized using {@link #isBNode(long)}.
     * <p>
     * Note: Statement identifiers (when enabled) are not stored in the reverse
     * lexicon and are recognized using {@link #isStatement(long)}. If the term
     * identifier is recognized as being, in fact, a statement identifier, then
     * it is externalized as a {@link BNode}. This fits rather well with the
     * notion in a quad store that the context position may be either a
     * {@link URI} or a {@link BNode} and the fact that you can use
     * {@link BNode}s to "stamp" statement identifiers.
     * <p>
     * Note: This specializes the return to {@link _Value}. This keeps the
     * {@link ITripleStore} interface cleaner while imposes the actual semantics
     * on all implementation of this class.
     * <p>
     * Note: Handles both unisolatable and isolatable indices.
     * <P>
     * Note: Sets {@link _Value#termId} and {@link _Value#known} as
     * side-effects.
     * 
     * @todo this always mints a new {@link BNode} instance when the term
     *       identifier is identifies a {@link BNode} or a statement. Should
     *       there be some cache effect to provide the same instance?
     * 
     * @todo this should return a BigdataValueImpl to be polite, r.g., as
     *       variant of {@link #getTerms(Collection)}.
     */
    final public _Value getTerm(long id) {
        
        // handle NULL, bnodes, statement identifiers, and the termCache.
        _Value value = _getTermId( id );
        
        if(value != null) return value;
        
        final IIndex ndx = getId2TermIndex();

        final Id2TermTupleSerializer tupleSer = (Id2TermTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.id2key(id);

        final byte[] data = ndx.lookup(key);

        if (data == null) {

            return null;

        }

        value = _Value.deserialize(data);

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

        // @todo modify unit test to verify that these fields are being set.

        value.termId = id;

        value.known = true;

        return value;

    }

    /**
     * Note: If {@link _Value#termId} is set, then returns that value
     * immediately. Otherwise looks up the termId in the index and sets
     * {@link _Value#termId} as a side-effect.
     * <p>
     * Note: If {@link _Value#key} is set, then that key is used. Otherwise the
     * key is computed and set as a side effect.
     */
    final public long getTermId(Value value) {

        if (value == null) {

            return IRawTripleStore.NULL;

        }

        if (value instanceof BigdataValue
                && ((BigdataValue) value).getTermId() != IRawTripleStore.NULL) {

            return ((BigdataValue) value).getTermId();

        }

        final IIndex ndx = getTerm2IdIndex();

        byte[] key;
        
        if (value instanceof _Value && ((_Value)value).key != null) {

            key = ((_Value)value).key;
            
        } else {
        
            final Term2IdTupleSerializer tupleSer = (Term2IdTupleSerializer) ndx
                    .getIndexMetadata().getTupleSerializer();

            // generate key iff not on hand.
            key = tupleSer.getLexiconKeyBuilder().value2Key(value);
        
            if(value instanceof _Value) {
            
                // save computed key.
                ((_Value)value).key = key;
                
            }
            
        }
        
        // lookup in the forward index.
        final byte[] tmp = ndx.lookup(key);

        if (tmp == null) {

            return IRawTripleStore.NULL;

        }

        // decode the term identifier.
        final long termId;
        try {

            termId = new DataInputBuffer(tmp).unpackLong();

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
        
        if((value instanceof _Value)) {

            // was found in the forward mapping (@todo if known means in both
            // mappings then DO NOT set it here)

            ((_Value)value).known = true;
            
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
                IRangeQuery.VALS, null/* filter */)).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the serialized term.
             */
            protected Object resolve(Object val) {

                ITuple tuple = (ITuple) val;

                _Value term = _Value.deserialize(tuple.getValueStream());

                return term;

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

                    id = tuple.getValueStream().unpackLong();

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
