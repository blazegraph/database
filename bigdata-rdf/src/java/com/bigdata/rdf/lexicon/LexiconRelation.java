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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.omg.CORBA.portable.ValueFactory;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBufferHandler;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.cache.ConcurrentWeakValueCacheWithBatchedUpdates;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtensionFactory;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.LexiconConfiguration;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSDStringExtension;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.ArrayAccessPath;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IRule;
import com.bigdata.search.FullTextIndex;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.CanonicalFactory;
import com.bigdata.util.NT;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * The {@link LexiconRelation} handles all things related to the indices mapping
 * external RDF {@link Value}s onto {@link IV}s (internal values)s and provides
 * methods for efficient materialization of external RDF {@link Value}s from
 * {@link IV}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconRelation extends AbstractRelation<BigdataValue> 
        implements IDatatypeURIResolver {

    final private static Logger log = Logger.getLogger(LexiconRelation.class);

    private final Set<String> indexNames;

    private final List<IKeyOrder<BigdataValue>> keyOrders;
    
    private final AtomicReference<ITextIndexer> viewRef = new AtomicReference<ITextIndexer>();

    /**
     * Note: This is a stateless class.
     */
    private final TermsIndexHelper h = new TermsIndexHelper();

	@SuppressWarnings("unchecked")
    protected Class<BigdataValueFactory> determineValueFactoryClass() {

        final String className = getProperty(
                AbstractTripleStore.Options.VALUE_FACTORY_CLASS,
                AbstractTripleStore.Options.DEFAULT_VALUE_FACTORY_CLASS);
        final Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + AbstractTripleStore.Options.VALUE_FACTORY_CLASS, e);
        }

        if (!BigdataValueFactory.class.isAssignableFrom(cls)) {
            throw new RuntimeException(
                    AbstractTripleStore.Options.VALUE_FACTORY_CLASS
                            + ": Must implement: "
                            + BigdataValueFactory.class.getName());
        }

        return (Class<BigdataValueFactory>) cls;

	}

    @SuppressWarnings("unchecked")
    protected Class<ITextIndexer> determineTextIndexerClass() {

        final String className = getProperty(
                AbstractTripleStore.Options.TEXT_INDEXER_CLASS,
                AbstractTripleStore.Options.DEFAULT_TEXT_INDEXER_CLASS);
        
        final Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + AbstractTripleStore.Options.TEXT_INDEXER_CLASS, e);
        }

        if (!ITextIndexer.class.isAssignableFrom(cls)) {
            throw new RuntimeException(
                    AbstractTripleStore.Options.TEXT_INDEXER_CLASS
                            + ": Must implement: "
                            + ITextIndexer.class.getName());
        }

        return (Class<ITextIndexer>) cls;

    }
    
    @SuppressWarnings("unchecked")
    protected Class<IExtensionFactory> determineExtensionFactoryClass() {

        final String className = getProperty(
                AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS,
                AbstractTripleStore.Options.DEFAULT_EXTENSION_FACTORY_CLASS);
        
        final Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, e);
        }

        if (!IExtensionFactory.class.isAssignableFrom(cls)) {
            throw new RuntimeException(
                    AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS
                            + ": Must implement: "
                            + IExtensionFactory.class.getName());
        }

        return (Class<IExtensionFactory>) cls;

    }

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
                    AbstractTripleStore.Options.DEFAULT_INLINE_TEXT_INDEX));
         
            if (textIndex) {
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
                properties
                        .setProperty(FullTextIndex.Options.OVERWRITE, "false");
//                /*
//                 * Explicitly set the class which knows how to handle IVs in the
//                 * keys of the full text index.
//                 */
//                properties.setProperty(
//                        FullTextIndex.Options.DOCID_FACTORY_CLASS,
//                        IVDocIdExtension.class.getName());

            }
            
        }
        
        this.storeBlankNodes = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.STORE_BLANK_NODES,
                AbstractTripleStore.Options.DEFAULT_STORE_BLANK_NODES));
        
        {

            final Set<String> set = new HashSet<String>();

            set.add(getFQN(LexiconKeyOrder.TERMS));

            if(textIndex) {
                
                set.add(getNamespace() + "." + FullTextIndex.NAME_SEARCH);

            }
            
            // @todo add names as registered to base class? but then how to
            // discover?  could be in the global row store.
            this.indexNames = Collections.unmodifiableSet(set);

            this.keyOrders = Arrays
                    .asList((IKeyOrder<BigdataValue>[]) new IKeyOrder[] { //
                            LexiconKeyOrder.TERMS //
                            });

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
//        terms = super.getIndex(LexiconKeyOrder.TERM2ID);
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
//        valueFactory = BigdataValueFactoryImpl.getInstance(namespace);
        try {
			final Class<BigdataValueFactory> vfc = determineValueFactoryClass();
			final Method gi = vfc.getMethod("getInstance", String.class);
			this.valueFactory = (BigdataValueFactory) gi.invoke(null, namespace);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(
					AbstractTripleStore.Options.VALUE_FACTORY_CLASS, e);
		} catch (InvocationTargetException e) {
			throw new IllegalArgumentException(
					AbstractTripleStore.Options.VALUE_FACTORY_CLASS, e);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(
					AbstractTripleStore.Options.VALUE_FACTORY_CLASS, e);
		}

        /*
         * @todo This should be a high concurrency LIRS or similar cache in
         * order to prevent the cache being flushed by the materialization of
         * low frequency terms.
         */
        {
            
            final int termCacheCapacity = Integer.parseInt(getProperty(
                    AbstractTripleStore.Options.TERM_CACHE_CAPACITY,
                    AbstractTripleStore.Options.DEFAULT_TERM_CACHE_CAPACITY));

            final Long commitTime = getCommitTime();
            
            if (commitTime != null && TimestampUtility.isReadOnly(timestamp)) {

                /*
                 * Shared for read-only views from sample commit time. Sharing
                 * allows us to reuse the same instances of the term cache for
                 * queries reading from the same commit point. The cache size is
                 * automatically increased to take advantage of the fact that it
                 * is a shared resource.
                 * 
                 * Note: Sharing is limited to the same commit time to prevent
                 * life cycle issues across drop/create sequences for the triple
                 * store.
                 */
                termCache = termCacheFactory.getInstance(new NT(namespace,
                        commitTime.longValue()), termCacheCapacity * 2);

            } else {

                /*
                 * Unshared for any other view of the triple store.
                 */
                termCache = new ConcurrentWeakValueCacheWithBatchedUpdates<IV, BigdataValue>(//
                        termCacheCapacity, // queueCapacity
                        .75f, // loadFactor (.75 is the default)
                        16 // concurrency level (16 is the default)
                );

            }
            
        }
        
        {
            
            inlineLiterals = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.INLINE_XSD_DATATYPE_LITERALS,
                    AbstractTripleStore.Options.DEFAULT_INLINE_XSD_DATATYPE_LITERALS));

            inlineTextLiterals = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.INLINE_TEXT_LITERALS,
                    AbstractTripleStore.Options.DEFAULT_INLINE_TEXT_LITERALS));
            
            maxInlineTextLength = Integer.parseInt(getProperty(
                    AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH,
                    AbstractTripleStore.Options.DEFAULT_MAX_INLINE_STRING_LENGTH));
            
            inlineBNodes = storeBlankNodes && Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.INLINE_BNODES,
                    AbstractTripleStore.Options.DEFAULT_INLINE_BNODES));
            
            inlineDateTimes = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.INLINE_DATE_TIMES,
                    AbstractTripleStore.Options.DEFAULT_INLINE_DATE_TIMES));
            
            inlineDateTimesTimeZone = TimeZone.getTimeZone(getProperty(
                    AbstractTripleStore.Options.INLINE_DATE_TIMES_TIMEZONE,
                    AbstractTripleStore.Options.DEFAULT_INLINE_DATE_TIMES_TIMEZONE));
            
            rejectInvalidXSDValues = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.REJECT_INVALID_XSD_VALUES,
                    AbstractTripleStore.Options.DEFAULT_REJECT_INVALID_XSD_VALUES));
            
            try {
                
            	/*
            	 * Setup the extension factory.
            	 */
                final Class<IExtensionFactory> xfc = 
                    determineExtensionFactoryClass();

                final IExtensionFactory xFactory = xfc.newInstance();

                /*
                 * Setup the lexicon configuration.
                 */
                lexiconConfiguration = new LexiconConfiguration<BigdataValue>(
                        inlineLiterals, inlineTextLiterals, maxInlineTextLength, inlineBNodes,
                        inlineDateTimes, rejectInvalidXSDValues, xFactory,
                        getContainer().getVocabulary());

            } catch (InstantiationException e) {
                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, e);
            }

        }
        
    }
    
    /**
     * The canonical {@link BigdataValueFactoryImpl} reference (JVM wide) for the
     * lexicon namespace.
     */
	public BigdataValueFactory getValueFactory() {
        
        return valueFactory;
        
    }

	final private BigdataValueFactory valueFactory;
    
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

    @Override
    public LexiconRelation init() {

    	super.init();
    	
		/*
		 * Allow the extensions to resolve their datatype URIs into term
		 * identifiers.
		 */
    	lexiconConfiguration.initExtensions(this);
    	
    	return this;
        
    }
    
    @Override
    public void create() {
        
        final IResourceLock resourceLock = acquireExclusiveLock();
        
        try {

            super.create();

			if (textIndex && inlineTextLiterals
					&& maxInlineTextLength > (4 * Bytes.kilobyte32)) {
				/*
				 * Log message if full text index is enabled and we are inlining
				 * textual literals and MAX_INLINE_TEXT_LENGTH is GT some
				 * threshold value (e.g., 4096). This combination represents an
				 * unresonable configuration due to the data duplication in the
				 * full text index. (The large literals will be replicated
				 * within the full text index for each token extracted from the
				 * literal by the text analyzer.)
				 */
				log
						.error("Configuration will duplicate large literals within the full text index"
								+ //
								": "
								+ AbstractTripleStore.Options.TEXT_INDEX
								+ "="
								+ textIndex
								+ //
								", "
								+ AbstractTripleStore.Options.INLINE_TEXT_LITERALS
								+ "="
								+ inlineTextLiterals
								+ //
								", "
								+ AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH
								+ "=" + maxInlineTextLength//
						);

			}

            final IIndexManager indexManager = getIndexManager();

            // register the indices.
            final String termsName = getFQN(LexiconKeyOrder.TERMS);
            IIndex terms = null;
            if (indexManager instanceof IJournal) {
                terms = ((IJournal) indexManager).registerIndex(termsName,
                        getTermsIndexMetadata(termsName));
            } else {
                /*
                 * Scale-out an other non-transactional contexts.
                 */
                indexManager.registerIndex(getTermsIndexMetadata(termsName));
            }

			/*
			 * Insert a tuple for the NullIV mapping it to a null value in the
			 * TERMS index.
			 */
			{

                if (terms == null) {
                    /*
                     * Scale-out (will not have been reported by registerIndex
                     * but we are not running a transaction so we can look it up
                     * now).
                     */
                    terms = getIndex(getFQN(LexiconKeyOrder.TERMS));
                }

                /*
                 * Insert a tuple for each kind of VTE having a ZERO hash code
                 * and a ZERO counter and thus qualifying it as a NullIV. Each
                 * of these tuples is mapped to a null value in the index. This
                 * reserves the possible distinct NullIV keys so they can not be
                 * assigned to real Values.
                 * 
                 * Note: The hashCode of "" is ZERO, so an empty Literal would
                 * otherwise be assigned the same key as mockIV(VTE.LITERAL).
                 */

                final IKeyBuilder keyBuilder = h.newKeyBuilder();

                final byte[][] keys = new byte[][] {
                        TermId.mockIV(VTE.URI).encode(keyBuilder.reset()).getKey(), //
                        TermId.mockIV(VTE.BNODE).encode(keyBuilder.reset()).getKey(), //
                        TermId.mockIV(VTE.LITERAL).encode(keyBuilder.reset()).getKey(), //
                        TermId.mockIV(VTE.STATEMENT).encode(keyBuilder.reset()).getKey(), //
                };
                
                // Sort the keys for efficient insert.
                Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);
                
                final byte[][] vals = new byte[][] { null, null, null, null };
                
                // submit the task and wait for it to complete.
                terms.submit(0/* fromIndex */, keys.length/* toIndex */, keys, vals,
                        BatchInsertConstructor.RETURN_NO_VALUES, null/* aggregator */);
                
			}

            if (textIndex) {

                // create the full text index

				final ITextIndexer tmp = getSearchEngine();

                tmp.create();

            }

            /*
             * Note: defer resolution of the newly created index objects. This
             * is mostly about efficiency since the scale-out API does not
             * return the IIndex object when we register the index. 
             */
            
//            terms = super.getIndex(LexiconKeyOrder.TERMS);
//            assert terms != null;

            /*
    		 * Allow the extensions to resolve their datatype URIs into term
    		 * identifiers.
    		 */
        	lexiconConfiguration.initExtensions(this);
            
        } finally {

            unlock(resourceLock);

        }

    }

    public void destroy() {

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            super.destroy();

            final IIndexManager indexManager = getIndexManager();

            indexManager.dropIndex(getFQN(LexiconKeyOrder.TERMS));
            terms = null;

            if (textIndex) {

                getSearchEngine().destroy();

                viewRef.set(null);

            }

            // discard the value factory for the lexicon's namespace.
            valueFactory.remove(getNamespace());

            termCache.clear();

        } finally {

            unlock(resourceLock);

        }

    }
    
    /**
     * The reference to the TERMS index.
     */
    volatile private IIndex terms;
    
    /**
     * When <code>true</code> a full text index is maintained.
     * 
     * @see AbstractTripleStore.Options#TEXT_INDEX
     */
    private final boolean textIndex;
    
    /**
     * When <code>true</code> the kb is using told blank nodes semantics.
     * 
     * @see AbstractTripleStore.Options#STORE_BLANK_NODES
     */
    private final boolean storeBlankNodes;
    
    /**
     * Are xsd datatype primitive and numeric literals being inlined into the statement indices.
     * 
     * {@link AbstractTripleStore.Options#INLINE_XSD_DATATYPE_LITERALS}
     */
    final private boolean inlineLiterals;
    
    /**
     * Are textual literals being inlined into the statement indices.
     * 
     * {@link AbstractTripleStore.Options#INLINE_TEXT_LITERALS}
     */
    final private boolean inlineTextLiterals;

    /**
     * The maximum length of <code>xsd:string</code> literals which will be
     * inlined into the statement indices. The {@link XSDStringExtension} is
     * registered when GT ZERO.
     */
    final private int maxInlineTextLength;
    
    /**
     * Are bnodes being inlined into the statement indices.
     * 
     * {@link AbstractTripleStore.Options#INLINE_BNODES}
     */
    final private boolean inlineBNodes;
    
    /**
     * Are xsd:dateTime litersls being inlined into the statement indices.
     * 
     * {@link AbstractTripleStore.Options#INLINE_DATE_TIMES}
     */
    final private boolean inlineDateTimes;

    /**
     * When <code>true</code>, XSD datatype literals which do not validate
     * against their datatype will be rejected rather than inlined.
     * 
     * {@link AbstractTripleStore.Options#REJECT_INVALID_XSD_VALUES}
     */
    final private boolean rejectInvalidXSDValues;
    
	/**
	 * The default time zone to be used for decoding inline xsd:datetime
	 * literals from the statement indices. Will use the current timezeon 
	 * unless otherwise specified using
	 * {@link AbstractTripleStore.Options#DEFAULT_INLINE_DATE_TIMES_TIMEZONE}.
	 */
    final private TimeZone inlineDateTimesTimeZone;
    
    /**
     * Return <code>true</code> if datatype literals are being inlined into
     * the statement indices.
     */
    final public boolean isInlineLiterals() {
        
        return inlineLiterals;
        
    }

    /**
     * Return the maximum length a string value which may be inlined into the
     * statement indices.
     */
    final public int getMaxInlineStringLength() {
        
        return maxInlineTextLength;
        
    }

    /**
     * Return <code>true</code> if xsd:datetime literals are being inlined into
     * the statement indices.
     */
    final public boolean isInlineDateTimes() {
        
        return inlineDateTimes;
        
    }

    /**
     * Return the default time zone to be used for inlining.
     */
    final public TimeZone getInlineDateTimesTimeZone() {
        
        return inlineDateTimesTimeZone;
        
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
	 * Overridden to use local cache of the index reference.
	 */
    @Override
    public IIndex getIndex(final IKeyOrder<? extends BigdataValue> keyOrder) {

        if (keyOrder != LexiconKeyOrder.TERMS)
            throw new AssertionError("keyOrder=" + keyOrder);

        return getTermsIndex();
        
    }

    final public IIndex getTermsIndex() {

        if (terms == null) {

            synchronized (this) {

                if (terms == null) {

                    final long timestamp = getTimestamp();
                    
                    if (TimestampUtility.isReadWriteTx(timestamp)) {
                        /*
                         * We always use the unisolated view of the lexicon
                         * indices for mutation and the lexicon indices do NOT
                         * set the [isolatable] flag even if the kb supports
                         * full tx isolation. This is because we use an
                         * eventually consistent strategy to write on the
                         * lexicon indices.
                         * 
                         * Note: It appears that we have already ensured that
                         * we will be using the unisolated view of the lexicon
                         * relation in AbstractTripleStore#getLexiconRelation()
                         * so this code path should not be evaluated.
                         */
                        terms = AbstractRelation
                                .getIndex(getIndexManager(),
                                        getFQN(LexiconKeyOrder.TERMS),
                                        ITx.UNISOLATED);
                    } else {
                        terms = super.getIndex(LexiconKeyOrder.TERMS);
                    }

                    if (terms == null)
                        throw new IllegalStateException();

                }

            }
            
        }

        return terms;

    }
    
    /**
     * A factory returning the softly held singleton for the
     * {@link FullTextIndex}.
     * 
     * @see AbstractTripleStore.Options#TEXT_INDEX
     * 
     * @todo replace with the use of the {@link IResourceLocator} since it
     *       already imposes a canonicalizing mapping within for the index name
     *       and timestamp inside of a JVM.
     */
    public ITextIndexer<?> getSearchEngine() {

        if (!textIndex)
            return null;

        /*
         * Note: Double-checked locking pattern requires [volatile] variable or
         * AtomicReference. This uses the AtomicReference since that gives us a
         * lock object which is specific to this request.
         */
        if (viewRef.get() == null) {

            synchronized (viewRef) {

                if (viewRef.get() == null) {

                    final ITextIndexer<?> tmp;
                    try {
                        final Class<?> vfc = determineTextIndexerClass();
                        final Method gi = vfc.getMethod("getInstance",
                                IIndexManager.class, String.class, Long.class,
                                Properties.class);
                        tmp = (ITextIndexer<?>) gi.invoke(null/* object */,
                                getIndexManager(), getNamespace(),
                                getTimestamp(), getProperties());
                        if(tmp instanceof ILocatableResource<?>) {
                        	((ILocatableResource<?>)tmp).init();
                        }
                        viewRef.set(tmp);
                    } catch (Throwable e) {
                        throw new IllegalArgumentException(
                                AbstractTripleStore.Options.TEXT_INDEXER_CLASS,
                                e);
                    }

                }

            }

        }

        return viewRef.get();

    }

	/**
	 * Return the {@link IndexMetadata} for the TERMS index.
	 * 
	 * @param name
	 *            The name of the index.
	 *            
	 * @return The {@link IndexMetadata}.
	 */
    protected IndexMetadata getTermsIndexMetadata(final String name) {

        final IndexMetadata metadata = new IndexMetadata(getIndexManager(),
                getProperties(), name, UUID.randomUUID());

        metadata.setTupleSerializer(new TermsTupleSerializer(getNamespace(),
                valueFactory));

		// enable raw record support.
		metadata.setRawRecords(true);

		return metadata;

    }

    public Set<String> getIndexNames() {

        return indexNames;

    }

    public Iterator<IKeyOrder<BigdataValue>> getKeyOrders() {
        
        return keyOrders.iterator();
        
    }

    public LexiconKeyOrder getPrimaryKeyOrder() {
        
    	return LexiconKeyOrder.TERMS;
        
    }

    /**
     * Note : this method is part of the mutation api. it is primarily (at this
     * point, only) invoked by the rule execution layer and, at present, no
     * rules can entail terms into the lexicon.
     * 
     * @throws UnsupportedOperationException
     */
    public BigdataValue newElement(List<BOp> a, IBindingSet bindingSet) {

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
    public Iterator<IV> prefixScan(final Literal lit) {

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
     * 
     *       FIXME TERMS REFACTOR : This was originally written to the TERM2ID
     *       index. Perhaps it can be restated as an operation against the full
     *       text index? [Very likely. The prefix match on the full text index
     *       will match any token appearing anywhere in a literal which starts
     *       with one of the tokens from the given literals. In order to have
     *       the same semantics we must also verify that (a) the prefix match is
     *       at the start of the literal; and (b) the match is contiguous.]
     * 
     */
    @SuppressWarnings("unchecked")
    public Iterator<IV> prefixScan(final Literal[] lits) {

        throw new UnsupportedOperationException();
        
//        if (lits == null || lits.length == 0)
//            throw new IllegalArgumentException();
//
//        if (log.isInfoEnabled()) {
//
//            log.info("#lits=" + lits.length);
//
//        }
//
//        /*
//         * The KeyBuilder used to form the prefix keys.
//         * 
//         * Note: The prefix keys are formed with IDENTICAL strength. This is
//         * necessary in order to match all keys in the index since it causes the
//         * secondary characteristics to NOT be included in the prefix key even
//         * if they are present in the keys in the index.
//         */
//        final LexiconKeyBuilder keyBuilder;
//        {
//
//            final Properties properties = new Properties();
//
//            properties.setProperty(KeyBuilder.Options.STRENGTH,
//                    StrengthEnum.Primary.toString());
//
//            keyBuilder = new Term2IdTupleSerializer(
//                    new DefaultKeyBuilderFactory(properties)).getLexiconKeyBuilder();
//
//        }
//
//        /*
//         * Formulate the keys[].
//         * 
//         * Note: Each key is encoded with the appropriate bytes to indicate the
//         * kind of literal (plain, languageCode, or datatype literal).
//         * 
//         * Note: The key builder was chosen to only encode the PRIMARY
//         * characteristics so that we obtain a prefix[] suitable for the
//         * completion scan.
//         */
//
//        final byte[][] keys = new byte[lits.length][];
//
//        for (int i = 0; i < lits.length; i++) {
//
//            final Literal lit = lits[i];
//
//            if (lit == null)
//                throw new IllegalArgumentException();
//
//            keys[i] = keyBuilder.value2Key(lit);
//
//        }
//
//        final IIndex ndx = getTerm2IdIndex();
//
//        final Iterator<IV> termIdIterator = new Striterator(
//                ndx
//                        .rangeIterator(
//                                null/* fromKey */,
//                                null/* toKey */,
//                                0/* capacity */,
//                                IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
//                                // prefix filter.
//                                new PrefixFilter<BigdataValue>(keys)))
//                .addFilter(new Resolver() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    /**
//                     * Decode the value, which is the term identifier.
//                     */
//                    @Override
//                    protected Object resolve(final Object arg0) {
//
//                        final byte[] bytes = ((ITuple) arg0).getValue(); 
//                        
//                        return IVUtility.decode(bytes);
//
//                    }
//                });
//
//        return termIdIterator;
            
    }

    /**
     * See {@link IDatatypeURIResolver}.
     */
    public BigdataURI resolve(final URI uri) {
        
        final BigdataURI buri = valueFactory.asValue(uri);
        
        if (buri.getIV() == null) {
            
            // will set tid on buri as a side effect
            final TermId<?> tid = getTermId(buri);
        
            if (tid == null) {
            
                try {
                
                    // will set tid on buri as a side effect
                    _addTerms(new BigdataValue[] { buri }, 1, false);
                    
                } catch (Exception ex) {
                    
                    // might be in a read-only transaction view
                    log.warn("unable to resolve term: " + uri);
                    
                }

            }
            
        }
        
        return buri.getIV() != null ? buri : null;
        
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
//    @SuppressWarnings("unchecked")
    public void addTerms(final BigdataValue[] terms, final int numTerms,
            final boolean readOnly) {
        
        /*
         * Very strange that we need to pass in the numTerms.  The
         * semantics are that we should only consider terms from 
         * terms[0] to terms[numTerms-1] (ignoring terms in the array past
         * numTerms-1).  The inline method will respect those semantics but
         * then return a dense array of terms that definitely should be added 
         * to the lexicon indices - that is, terms in the original array at 
         * index less than numTerms and not inlinable. MRP  [BBT This is not as efficient!]
         */
        final BigdataValue[] terms2 = addInlineTerms(terms, numTerms);
        final int numTerms2 = terms2.length;
        
//      this code is bad because it modifies the original array
//      // keep track of how many terms we need to index (non-inline terms)
//      int numTerms2 = numTerms;
//      for (int i = 0; i < numTerms; i++) {
//          // try to get an inline internal value
//          final IV inline = getInlineIV(terms[i]);
//          
//          if (inline != null) {
//              // take this term out of the terms array
//              terms[i] = null;
//              // decrement the number of terms to index
//              numTerms2--;
//          }
//      }
//
//      /* 
//       * We nulled out the inline terms from the original terms array, so
//       * now we need to shift the remaining terms down to fill in the gaps.
//       */
//      if (numTerms2 < numTerms) {
//          int j = 0;
//          for (int i = 0; i < numTerms2; i++) {
//              if (terms[i] != null) {
//                  j++;
//              } else {
//                  while (terms[i] == null) {
//                      j++;
//                      terms[i] = terms[j];
//                      terms[j] = null;
//                  }
//              }
//          }
//      }

        // write the non-inline terms to the indices
        _addTerms(terms2, numTerms2, readOnly);
        
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
    private void _addTerms(final BigdataValue[] terms, final int numTerms,
            final boolean readOnly) {

        if (log.isDebugEnabled())
            log.debug("numTerms=" + numTerms + ", readOnly=" + readOnly);
        
        if (numTerms == 0)
            return;

        final long begin = System.currentTimeMillis();
        
        final WriteTaskStats stats = new WriteTaskStats();

        final KVO<BigdataValue>[] a;
        try {
            // write on the TERMS index (rync sharded RPC in scale-out)
            a = new TermsWriteTask(getTermsIndex(), valueFactory, readOnly,
                    storeBlankNodes, numTerms, terms, stats).call();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
      
        /*
         * Note: [a] is dense and its elements are distinct. It will be in TERMS
         * index order.
         */
        final int ndistinct = a.length;

        if (ndistinct == 0) {

            // Nothing left to do.
            return;

        }
        
        if (!readOnly && textIndex) {

            /*
             * Write on the full text index.
             */
            final long _begin = System.currentTimeMillis();

            try {
                /*
                 * Note: a[] is in TERMS index order at this point and can
                 * contain both duplicates and terms that already have term
                 * identifiers and therefore are already in the index.
                 * 
                 * [TODO: Is it true that it can have duplicates? This is in
                 * direct conflict with the comments above. Figure out what is
                 * what and update as appropriate.]
                 * 
                 * Therefore, instead of a[], we use an iterator that resolves
                 * the distinct terms in a[] (it is dense) to do the indexing.
                 */
                @SuppressWarnings("unchecked")
                final Iterator<BigdataValue> itr = new Striterator(
                        new ChunkedArrayIterator(ndistinct, a, null/* keyOrder */))
                        .addFilter(new Resolver() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            protected Object resolve(final Object obj) {

                                return ((KVO<BigdataValue>) obj).obj;

                            }

                        });

                stats.fullTextIndexTime = new FullTextIndexWriterTask(
                        ndistinct/* capacity */, itr).call();
                
            } catch (Exception e) {
                
                throw new RuntimeException(e);
                
            }

            stats.indexTime += System.currentTimeMillis() - _begin;

        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled() && readOnly && stats.nunknown.get() > 0) {
         
            log.info("There are " + stats.nunknown + " unknown terms out of "
                    + numTerms + " given");
            
        }
        
        if (numTerms > 1000 || elapsed > 3000) {

            if (log.isInfoEnabled())
                log.info(stats.toString());

        }
//        System.err.println(stats.toString());

    }

    /**
     * Called by the add terms method, this method is meant to filter out inline
     * terms from the supplied terms array (up to position numTerms-1). It will
     * return a dense array of values that could not be inlined and thus need to
     * be written to the indices.
     * 
     * @param terms
     *            the original terms
     * @param numTerms
     *            the number of original terms to consider
     *            
     * @return the terms that were in consideration but could not be inlined
     */
    private BigdataValue[] addInlineTerms(final BigdataValue[] terms,
            final int numTerms) {

        // these are the terms that will need to be indexed (not inline terms)
        final List<BigdataValue> terms2 = new LinkedList<BigdataValue>(/* numTerms */);

        for (int i = 0; i < numTerms; i++) {

            // Try to get an inline internal value for the term (sets the IV as
            // a side effect if not null)
            if (getInlineIV(terms[i]) == null)
                terms2.add(terms[i]);
            
        }
        
        final int nremaining = terms2.size();
        
        if(nremaining == 0) 
        	return EMPTY_VALUE_ARRAY;

        // dense array having terms that need further processing.
        return terms2.toArray(new BigdataValue[terms2.size()]);

    }

	private static transient final BigdataValue[] EMPTY_VALUE_ARRAY = new BigdataValue[0];

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
     * @throws UnsupportedOperationException
     *             unless full text indexing was enabled.
     * 
     * @see ITextIndexer
     * @see AbstractTripleStore.Options#TEXT_INDEX
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     * 
     * @todo Provide a lucene integration point as an alternative to the
     *       {@link FullTextIndex}. Integrate for query and search of course.
     */
    protected void indexTermText(final int capacity,
            final Iterator<BigdataValue> itr) {

		final ITextIndexer<?> ndx = getSearchEngine();
		
		if(ndx == null) {
		    
		    throw new UnsupportedOperationException();
		    
		}
		
        ndx.index(capacity, itr);

    }

    /**
     * Utility method to (re-)build the full text index. This is a high latency
     * operation for a database of any significant size. You must be using the
     * unisolated view of the {@link AbstractTripleStore} for this operation.
     * {@link AbstractTripleStore.Options#TEXT_INDEX} must be enabled. This
     * operation is only supported when the {@link ITextIndexer} uses the
     * {@link FullTextIndex} class.
     */
    @SuppressWarnings("unchecked")
    public void rebuildTextIndex() {

        if (getTimestamp() != ITx.UNISOLATED)
            throw new UnsupportedOperationException();
        
        if(!textIndex)
            throw new UnsupportedOperationException();
        
        final ITextIndexer textIndexer = getSearchEngine();
        
        // destroy the existing text index.
        textIndexer.destroy();
        
        // create a new index.
        textIndexer.create();

        // the index to scan for the RDF Literals.
        final IIndex terms = getTermsIndex();

        // used to decode the
        final ITupleSerializer tupSer = terms.getIndexMetadata()
                .getTupleSerializer();

        /*
         * Visit all plain, language code, and datatype literals in the lexicon.
         * 
         * Note: This uses a filter on the ITupleIterator in order to filter out
         * non-literal terms before they are shipped from a remote index shard.
         */
        final Iterator<BigdataValue> itr = new Striterator(terms
                .rangeIterator(null/* fromKey */, null/* toKey */,
                        0/* capacity */, IRangeQuery.DEFAULT,
                        new TupleFilter<BigdataValue>() {
                            private static final long serialVersionUID = 1L;
                            protected boolean isValid(
                                    final ITuple<BigdataValue> obj) {
                                final IV iv = (IV) tupSer.deserializeKey(obj);
                                if (iv != null && iv.isLiteral()) {
                                    return true;
                                }
                                return false;
                            }
                        })).addFilter(new Resolver() {
            private static final long serialVersionUID = 1L;

            protected Object resolve(final Object obj) {
                final BigdataLiteral lit = (BigdataLiteral) tupSer
                        .deserialize((ITuple) obj);
//                System.err.println("lit: "+lit);
                return lit;
            }
        });

        final int capacity = 10000;

        while (itr.hasNext()) {

            indexTermText(capacity, itr);

        }

    }
    
    /**
     * Batch resolution of internal values to {@link BigdataValue}s.
     * 
     * @param ivs
     *            An collection of internal values
     * 
     * @return A map from internal value to the {@link BigdataValue}. If an
     *         internal value was not resolved then the map will not contain an
     *         entry for that internal value.
     */
    @SuppressWarnings("unchecked")
    final public Map<IV, BigdataValue> getTerms(final Collection<IV> ivs) {

        if (ivs == null)
            throw new IllegalArgumentException();

        // Maximum #of distinct term identifiers.
        final int n = ivs.size();
        
        if (n == 0) {

            return Collections.emptyMap();
            
        }

        final long begin = System.currentTimeMillis();

        /*
         * Note: A concurrent hash map is used since the request may be split
         * across shards, in which case updates on the map may be concurrent.
         */
        final ConcurrentHashMap<IV/* iv */, BigdataValue/* term */> ret = 
            new ConcurrentHashMap<IV, BigdataValue>(n);

        final Collection<TermId> termIds = new LinkedList<TermId>();
        
        /*
         * Filter out the inline values first and those that have already
         * been materialized and cached.
         */
        for (IV iv : ivs) {
            
    		if (iv.hasValue()) {
                
            	if (log.isDebugEnabled()) {
            		log.debug("already materialized: " + iv.getValue());
            	}
            	
            	// already materialized
            	ret.put(iv, iv.getValue());
            	
            } else if (iv.isInline()) {
                
                // translate it into a value directly
                ret.put(iv, iv.asValue(this));
                
            } else {
                
                termIds.add((TermId) iv);
                
            }
            
        }
        
        /*
         * An array of any term identifiers that were not resolved in this first
         * stage of processing. We will look these up in the index.
         */
        final TermId[] notFound = new TermId[termIds.size()];

        int numNotFound = 0;
        
        for (TermId tid : termIds) {

			final BigdataValue value = _getTermId(tid);
            
            if (value != null) {

                assert value.getValueFactory() == valueFactory;

                // resolved.
                ret.put(tid, value);//valueFactory.asValue(value));
                
                continue;

            }

            /*
             * We will need to test the index for this term identifier.
             */
            notFound[numNotFound++] = tid;
            
        }

        /*
         * batch lookup.
         */
        if (numNotFound > 0) {

            if (log.isInfoEnabled())
                log.info("nterms=" + n + ", numNotFound=" + numNotFound
                        + ", cacheSize=" + termCache.size());

            /*
             * sort term identifiers into index order.
             */
            Arrays.sort(notFound, 0, numNotFound);
                        
            final IKeyBuilder keyBuilder = KeyBuilder.newInstance();

            final byte[][] keys = new byte[numNotFound][];

            for (int i = 0; i < numNotFound; i++) {

                // Note: shortcut for keyBuilder.id2key(id)
                keys[i] = notFound[i].encode(keyBuilder.reset()).getKey();
                
            }

//            final IIndex ndx = getId2TermIndex();
            final IIndex ndx = getTermsIndex();

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
     */
    private class ResolveTermTask implements Callable<Void> {
        
        final IIndex ndx;

        final int fromIndex;

        final int toIndex;

        final byte[][] keys;

        final TermId[] notFound;

        final ConcurrentHashMap<IV, BigdataValue> map;

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
        @SuppressWarnings("unchecked")
        ResolveTermTask(final IIndex ndx, final int fromIndex,
                final int toIndex, final byte[][] keys, final TermId[] notFound,
                ConcurrentHashMap<IV, BigdataValue> map) {

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
             * Update the termCache.
             */
            {

                final IRaba vals = results.getValues();
                
                for (int i = fromIndex; i < toIndex; i++) {

                    final TermId tid = notFound[i];

                    final byte[] data = vals.get(i);

                    if (data == null) {

                        log.warn("No such term: " + tid);

                        continue;

                    }

                    /*
                     * Note: This automatically sets the valueFactory reference
                     * on the de-serialized value.
                     */
					BigdataValue value = valueFactory.getValueSerializer()
                            .deserialize(data);
                    
                    // Set the term identifier.
                    value.setIV(tid);

                    final BigdataValue tmp = termCache.putIfAbsent(tid, value);

                    if (tmp != null) {

                        value = tmp;

                    }

                    /*
                     * The term identifier was set when the value was
                     * de-serialized. However, this will throw an
                     * IllegalStateException if the value somehow was assigned
                     * the wrong term identifier (paranoia test).
                     */
                    assert value.getIV().equals(tid) : "expecting tid=" + tid
                            + ", but found " + value.getIV();
					assert (value).getValueFactory() == valueFactory;

                    // save in caller's concurrent map.
                    map.put(tid, value);

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
    @SuppressWarnings("unchecked")
    final private ConcurrentWeakValueCacheWithBatchedUpdates<IV, BigdataValue> termCache;
    
    /**
     * Factory used for {@link #termCache} for read-only views of the lexicon.
     */
    @SuppressWarnings("unchecked")
    static private CanonicalFactory<NT/* key */, ConcurrentWeakValueCacheWithBatchedUpdates<IV, BigdataValue>, Integer/* state */> termCacheFactory = new CanonicalFactory<NT, ConcurrentWeakValueCacheWithBatchedUpdates<IV, BigdataValue>, Integer>(
            1/* queueCapacity */) {
        @Override
        protected ConcurrentWeakValueCacheWithBatchedUpdates<IV, BigdataValue> newInstance(
                NT key, Integer termCacheCapacity) {
            return new ConcurrentWeakValueCacheWithBatchedUpdates<IV, BigdataValue>(//
                    termCacheCapacity.intValue(),// backing hard reference LRU queue capacity.
                    .75f, // loadFactor (.75 is the default)
                    16 // concurrency level (16 is the default)
            );
        }
    };
    
    /**
     * The {@link ILexiconConfiguration} instance, which will determine how
     * terms are encoded and decoded in the key space.
     */
    private final ILexiconConfiguration<BigdataValue> lexiconConfiguration;

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
     * @param TermId
     *         a term identifier
     * 
     * @return The corresponding {@link BigdataValue} if the term identifier is
     *         a blank node identifier, a statement identifier, or found in the
     *         {@link #termCache}, and <code>null</code> 
     *         otherwise.
     * 
     * @throws IllegalArgumentException
     *             if iv is null, or if the iv is a {@link TermId} and it's 
     *             <i>id</i> is {@link TermId#NULL}
     */
	@SuppressWarnings("unchecked")
    private BigdataValue _getTermId(final TermId tid) {

        if (tid == null)
            throw new IllegalArgumentException();
        
        if(tid.isNullIV())
            throw new IllegalArgumentException();
        
        if (tid.isStatement()) {

            throw new IllegalArgumentException("sids should be inline");

        }

        if (!storeBlankNodes && tid.isBNode()) {

            /*
             * Except when the "told bnodes" mode is enabled, blank nodes are
             * not stored in the reverse lexicon (or the cache). The "B" prefix
             * is a syntactic marker for a real blank node.
             * 
             * Note: In a told bnodes mode, we need to store the blank nodes in
             * the lexicon and enter them into the term cache since their
             * lexical form will include the specified ID, not the term
             * identifier.
             */

            final BigdataBNode bnode = valueFactory.createBNode(tid.bnodeId());

            // set the term identifier on the object.
            bnode.setIV(tid);

            return bnode;

        }

        // test the term cache,  passing IV from caller as the cache key.
        return termCache.get(tid);

    }
    
    /**
     * Note: {@link BNode}s are not stored in the reverse lexicon and are
     * recognized using {@link AbstractTripleStore#isBNode(long)}.
     * <p>
     * Note: Statement identifiers (when enabled) are not stored in the reverse
     * lexicon and are recognized using
     * {@link AbstractTripleStore#isStatement(IV)}. If the term identifier is
     * recognized as being, in fact, a statement identifier, then it is
     * externalized as a {@link BNode}. This fits rather well with the notion
     * in a quad store that the context position may be either a {@link URI} or
     * a {@link BNode} and the fact that you can use {@link BNode}s to "stamp"
     * statement identifiers.
     * <p>
     * Note: Handles both unisolatable and isolatable indices.
     * <P>
     * Note: Sets {@link BigdataValue#getIV()} as a side-effect.
     * <p>
     * Note: this always mints a new {@link BNode} instance when the term
     * identifier identifies a {@link BNode} or a {@link Statement}.
     * 
     * @return The {@link BigdataValue} -or- <code>null</code> iff there is no
     *         {@link BigdataValue} for that term identifier in the lexicon.
     */
    @SuppressWarnings("unchecked")
    final public BigdataValue getTerm(final IV iv) {
    	
    	return getTerm(iv, true);
    	
    }

    /**
     * When readFromIndex=false, only handles inline, NULL, bnodes, SIDs, and
     * the termCache - does not attempt to read from disk.
     * 
     * @param iv
     *            The {@link IV}.
     * @param readFromIndex
     *            When <code>true</code> an attempt will be made to resolve the
     *            {@link IV} against the TERMS index iff none of the fast paths
     *            succeed.
     */
    @SuppressWarnings("unchecked")
    final private BigdataValue getTerm(final IV iv, final boolean readFromIndex) {

        // if (false) { // alternative forces the standard code path.
        // final Collection<IV> ivs = new LinkedList<IV>();
        // ivs.add(iv);
        // final Map<IV, BigdataValue> values = getTerms(ivs);
        // return values.get(iv);
        // }

        if (iv.isInline())
            return iv.asValue(this);

        final TermId tid = (TermId) iv;

        // handle NULL, bnodes, statement identifiers, and the termCache.
        BigdataValue value = _getTermId(tid);
        
        if (value != null || !readFromIndex)
            return value;
        
        final IIndex ndx = getTermsIndex();

        final TermsTupleSerializer tupleSer = (TermsTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.serializeKey(tid);

        final byte[] data = ndx.lookup(key);

        if (data == null)
            return null;

        // This also sets the value factory.
        value = valueFactory.getValueSerializer().deserialize(data);
        
        // This sets the term identifier.
        value.setIV(iv);

        // Note: passing the IV object as the key.
        final BigdataValue tmp = termCache.putIfAbsent(iv, value);

        if (tmp != null) {

            value = tmp;

        }
        
// Note: This assert could be tripped by a data race on the cache, which is not an error.
//        assert value.getIV() == iv : "expecting iv=" + iv + ", but found "
//                + value.getIV();
//        //        value.setTermId( id );

        return value;

    }

    /**
     * <strong>WARNING DO NOT USE OUTSIDE OF THE UNIT TESTS: </strong> This
     * method is extremely inefficient for scale-out as it does one RMI per
     * request!
     * <p>
     * Note: If {@link BigdataValue#getIV()} is set, then returns that value
     * immediately. Next, try to get an inline internal value for the value.
     * Otherwise looks up the termId in the index and
     * {@link BigdataValue#setIV(IV) sets the term identifier} as a side-effect.
     * 
     * @deprecated Not even the unit tests should be doing this.
     */
    @SuppressWarnings("unchecked")
    final public IV getIV(final Value value) {

        if (value == null)
            return null;

        // see if it already has a value
        if (value instanceof BigdataValue) {
            final IV iv = ((BigdataValue) value).getIV();
            if (iv != null)
                return iv;
        }

        // see if it can be assigned an inline value
        IV iv = getInlineIV(value);
        if (iv != null)
            return iv;
        
        // go to the index
        iv = getTermId(value);
        
        return iv;
        
    }
        
    /**
     * Attempt to convert the value to an inline internal value.
     * 
     * @param value
     *          the value to convert
     * @return
     *          the inline internal value, or <code>null</code> if it cannot
     *          be converted
     */
    @SuppressWarnings("unchecked")
    final public IV getInlineIV(final Value value) {
        
        return getLexiconConfiguration().createInlineIV(value);

    }
    
    /**
     * This method assumes we've already exhausted all other possibilities
     * and need to go to the index for the {@link IV}.
     * 
     * @param value
     *          the value to lookup
     * @return
     *          the term identifier for the value
     */
    @SuppressWarnings("unchecked")
    private TermId getTermId(final Value value) {
        
        final IKeyBuilder keyBuilder = h.newKeyBuilder();
        
        final BigdataValue asValue = valueFactory.asValue(value);
        
        final byte[] baseKey = h.makePrefixKey(keyBuilder.reset(), asValue);

        final byte[] val = valueFactory.getValueSerializer().serialize(asValue);

		final int counter = h
				.resolveOrAddValue(getTermsIndex(), true/* readOnly */,
						keyBuilder, baseKey, val, null/* bucketSize */);

		if (counter == TermsIndexHelper.NOT_FOUND) {

            // Not found.
            return null;
            
        }
        
//        final TermId tid = (TermId) IVUtility.decode(key);
		final TermId tid = new TermId<BigdataValue>(VTE.valueOf(asValue),
				asValue.hashCode(), (byte) counter);

        if(value instanceof BigdataValue) {

			final BigdataValue impl = (BigdataValue) value;
            
            // set as side-effect.
            impl.setIV(tid);

            /*
             * Note that we have the termId and the term, we stick the value
             * into in the term cache IFF it has the correct value factory, but
             * do not replace the entry if there is one already there.
             */

            if (impl.getValueFactory() == valueFactory) {

                if (storeBlankNodes || !tid.isBNode()) {

                    // if (termCache.get(id) == null) {
                    //
                    // termCache.put(id, value, false/* dirty */);
                    //
                    // }

                    termCache.putIfAbsent(tid, impl);

                }
                
            }

        }

        return tid;

    }

    /**
     * Iterator visits all term identifiers in order by the <em>term</em> key
     * (efficient index scan).
     */
    @SuppressWarnings("unchecked")
    public Iterator<TermId> termsIndexScan() {

        final IIndex ndx = getTermsIndex();

        return new Striterator(ndx.rangeIterator(null, null, 0/* capacity */,
                IRangeQuery.VALS, null/* filter */)).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * Deserialize the term identifier.
             * 
             * @param val
             *            The serialized term identifier.
             */
            protected Object resolve(final Object val) {

                final ITuple tuple = (ITuple) val;

                if (tuple.isNull()) {
                 
                    return TermId.NullIV;
                    
                }
                
                return IVUtility.decode(tuple.getValue());
                
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
        final Iterator<TermId> itr = termsIndexScan();

        // resolve term identifiers to terms.
        return new Striterator(itr).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the term identifier.
             */
            protected Object resolve(final Object val) {

                if (((TermId) val).isNullIV())
                    return null;

                // resolve against the id:term index (random lookup).
                return getTerm((TermId) val);

            }

        });

    }

//    /**
//     * Dumps the lexicon in a variety of ways (test suites only).
//     */
//    public StringBuilder dumpTerms() {
//
//        final StringBuilder sb = new StringBuilder(Bytes.kilobyte32 * 4);
//
//        /**
//         * Dumps the terms in term order.
//         */
//        sb.append("---- terms in term order ----\n");
//        for( Iterator<Value> itr = termIterator(); itr.hasNext(); ) {
//            
//            final Value val = itr.next();
//            
//            if (val == null) {
//                sb.append("NullIV");
//            } else {
//                sb.append(val.toString());
//            }
//            
//            sb.append("\n");
//            
//        }
//        
//        return sb;
//        
//    }

    /**
     * See {@link ILexiconConfiguration#isInline(DTE)}.  Delegates to the
     * {@link #lexiconConfiguration} instance.
    public boolean isInline(DTE dte) {
        return lexiconConfiguration.isInline(dte);
    }
     */

    /**
     * See {@link ILexiconConfiguration#isLegacyEncoding()}.  Delegates to the
     * {@link #lexiconConfiguration} instance.
    public boolean isLegacyEncoding() {
        return lexiconConfiguration.isLegacyEncoding();
    }
     */
    
    /**
     * Return the {@link #lexiconConfiguration} instance.  Used to determine
     * how to encode and decode terms in the key space.
     */
    public ILexiconConfiguration getLexiconConfiguration() {

        return lexiconConfiguration;
        
    }

    /**
     * {@link LexiconKeyOrder#TERMS} is always returned.
     */
    public IKeyOrder<BigdataValue> getKeyOrder(final IPredicate<BigdataValue> p) {

        return LexiconKeyOrder.TERMS;

    }

    /**
     * Necessary for lexicon joins, which are injected into query plans as
     * necessary by the query planner. You can use a {@link LexPredicate} to
     * perform either a forward ({@link BigdataValue} to {@link IV}) or reverse
     * ( {@link IV} to {@link BigdataValue}) lookup. Either lookup will cache
     * the {@link BigdataValue} on the {@link IV} as a side effect.
     * <p>
     * Note: If you query with {@link IV} or {@link BigdataValue} which is
     * already cached (either on one another or in the termsCache) then the
     * cached value will be returned (fast path).
     * <p>
     * Note: Blank nodes will not unify with themselves unless you are using
     * told blank node semantics.
     * <p>
     * Note: <strong> This has the side effect of caching materialized
     * {@link BigdataValue}s on {@link IV}s using
     * {@link IV#setValue(BigdataValue)} for use in downstream operators that
     * need materialized values to evaluate properly. The query planner is
     * responsible for managing when we materialize and cache values. This keeps
     * us from wiring {@link BigdataValue} onto {@link IV}s all the
     * time.</strong>
     * <p>
     * The lexicon has a single TERMS index. The keys are {@link TermId}s formed
     * from the {@link VTE} of the {@link BigdataValue},
     * {@link BigdataValue#hashCode()}, and a collision counter. The value is
     * the {@link BigdataValue} as serialized by the
     * {@link BigdataValueSerializer}.
     * <p>
     * There are four possible ways to query this index using the
     * {@link LexPredicate}.
     * <dl>
     * <dt>lex(-BigdataValue,+IV)</dt>
     * <dd>The {@link IV} is given and its {@link BigdataValue} will be sought.</dd>
     * <dt>lex(+BigdataValue,-IV)</dt>
     * <dd>The {@link BigdataValue}is given and its {@link IV} will be sought.
     * This case requires a key-range scan with a filter. It has to scan the
     * collision bucket and filter for the specified Value. We get the collision
     * bucket by creating a prefix key for the Value (using its VTE and
     * hashCode). This will either return the IV for that Value or nothing.</dd>
     * <dt>lex(+BigdataValue,+IV)</dt>
     * <dd>The predicate is fully bound. In this case we can immediately verify
     * that the Value is consistent with the IV (same VTE and hashCode) and then
     * do a point lookup on the IV.</dd>
     * </dl>
     * 
     * @see LexAccessPatternEnum
     * @see LexPredicate
     * @see LexiconKeyOrder
     */
    @SuppressWarnings("unchecked")
    @Override
    public IAccessPath<BigdataValue> newAccessPath(
            final IIndexManager localIndexManager, 
            final IPredicate<BigdataValue> predicate, 
            final IKeyOrder<BigdataValue> keyOrder 
            ) {

        /*
         * Figure out which access pattern is being used.
         */
        final LexAccessPatternEnum accessPattern = LexAccessPatternEnum
                .valueOf(predicate);

        switch (accessPattern) {
        case FullyBound: {
            /*
             * Special case first verifies that the IV and Value are consistent
             * and then falls through to IVBound, which is a point lookup
             * against the TERMS index.
             */

            final BigdataValue val = (BigdataValue) predicate.get(
                    LexiconKeyOrder.SLOT_TERM).get();

            final IV iv = (IV) predicate.get(LexiconKeyOrder.SLOT_ID).get();

            if (VTE.valueOf(val) != iv.getVTE()) {

                /*
                 * The VTE is not consistent so the access path is proveably
                 * empty.
                 */
                
                return new EmptyAccessPath<BigdataValue>();
                
            }
            
            if(val.hashCode()!=iv.hashCode()) {

                /*
                 * The hashCode is not consistent so the access path is
                 * proveably empty.
                 */
                
                return new EmptyAccessPath<BigdataValue>();
                
            }

            /*
             * Fall through.
             */
            
        }
        case IVBound: {

            final IV iv = (IV) predicate.get(LexiconKeyOrder.SLOT_ID).get();
            
//            if (log.isDebugEnabled())
//                log.debug("materializing: " + iv);

            // Attempt to resolve the IV directly to a Value (no IO).
            final BigdataValue val = getTerm(iv, false/* readIndex */);

            if (val != null) {

//                if (log.isDebugEnabled())
//                    log.debug("found term in the term cache: " + val);

                // cache the IV on the value
                val.setIV(iv);

                // cache the value on the IV
                iv.setValue(val);

                return new ArrayAccessPath<BigdataValue>(
                        new BigdataValue[] { val }, predicate, keyOrder);

            }

//            if (log.isDebugEnabled())
//                log.debug("did not find term in the term cache: " + iv);

            if (!storeBlankNodes && iv.isBNode()) {

                /*
                 * Blank nodes do not unify with themselves unless you are using 
                 * told blank nodes semantics.
                 */
                
                return new EmptyAccessPath<BigdataValue>();
                
            }

            final CacheValueFilter filter = CacheValueFilter.newInstance();

            final IPredicate<BigdataValue> tmp = (IPredicate<BigdataValue>) predicate
                    .setProperty(Predicate.Annotations.ACCESS_PATH_FILTER,
                            filter);

            final AccessPath<BigdataValue> ap = new AccessPath<BigdataValue>(
                    this, localIndexManager, tmp, keyOrder).init();

            return ap;

        }
        case ValueBound: {

            final BigdataValue val = (BigdataValue) predicate.get(
                    LexiconKeyOrder.SLOT_TERM).get();

            // See if it already has an IV or can be assigned an inline IV
            IV iv = val.getIV();
            
            if (iv == null) {
            
                iv = getInlineIV(val);
                
            }

            if (iv != null) {

                // cache the IV on the value
                val.setIV(iv);

                // cache the value on the IV
                iv.setValue(val);

                return new ArrayAccessPath<BigdataValue>(
                        new BigdataValue[] { val }, predicate, keyOrder);

            }

            final CacheValueFilter filter = CacheValueFilter.newInstance();

            final IPredicate<BigdataValue> tmp = (IPredicate<BigdataValue>) predicate
                    .setProperty(Predicate.Annotations.ACCESS_PATH_FILTER,
                            filter);

            final AccessPath<BigdataValue> ap = new AccessPath<BigdataValue>(this,
                    localIndexManager, tmp, keyOrder);
            
            return ap;
        }
        case NoneBound: {
            /*
             * TODO Could be supported. This is a full index scan. We would want
             * to filter out the NullIVs during the scan.
             */
        }
        default:
            throw new UnsupportedOperationException("" + accessPattern);
        }

    }

}
