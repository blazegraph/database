
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.omg.CORBA.portable.ValueFactory;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexTypeEnum;
import com.bigdata.btree.filter.PrefixFilter;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.cache.ConcurrentWeakValueCacheWithBatchedUpdates;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtensionFactory;
import com.bigdata.rdf.internal.IInlineURIFactory;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.LexiconConfiguration;
import com.bigdata.rdf.internal.NoExtensionFactory;
import com.bigdata.rdf.internal.NoInlineURIFactory;
import com.bigdata.rdf.internal.NoSuchVocabularyItem;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.extensions.XSDStringExtension;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.BigdataSailHelper;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.RelationSchema;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.ArrayAccessPath;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.locator.ILocatableResource;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.search.FullTextIndex;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.geospatial.GeoSpatialConfig;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.Bytes;
import com.bigdata.util.NT;
import com.bigdata.util.concurrent.CanonicalFactory;

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

    private final static Logger log = Logger.getLogger(LexiconRelation.class);

    private final Set<String> indexNames;

    private final List<IKeyOrder<BigdataValue>> keyOrders;
    
    private final AtomicReference<IValueCentricTextIndexer<?>> viewRef = new AtomicReference<IValueCentricTextIndexer<?>>();

    /**
     * A new one for the subject-centric full text index.
     */
    private final AtomicReference<ISubjectCentricTextIndexer<?>> viewRef2 = new AtomicReference<ISubjectCentricTextIndexer<?>>();

    /**
     * Note: This is a stateless class.
     */
    private final BlobsIndexHelper h = new BlobsIndexHelper();

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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Class<IValueCentricTextIndexer> determineTextIndexerClass() {

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

        if (!IValueCentricTextIndexer.class.isAssignableFrom(cls)) {
            throw new RuntimeException(
                    AbstractTripleStore.Options.TEXT_INDEXER_CLASS
                            + ": Must implement: "
                            + IValueCentricTextIndexer.class.getName());
        }

        return (Class<IValueCentricTextIndexer>) cls;

    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Class<ISubjectCentricTextIndexer> determineSubjectCentricTextIndexerClass() {

        final String className = getProperty(
                AbstractTripleStore.Options.SUBJECT_CENTRIC_TEXT_INDEXER_CLASS,
                AbstractTripleStore.Options.DEFAULT_SUBJECT_CENTRIC_TEXT_INDEXER_CLASS);
        
        final Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + AbstractTripleStore.Options.SUBJECT_CENTRIC_TEXT_INDEXER_CLASS, e);
        }

        if (!ISubjectCentricTextIndexer.class.isAssignableFrom(cls)) {
            throw new RuntimeException(
                    AbstractTripleStore.Options.SUBJECT_CENTRIC_TEXT_INDEXER_CLASS
                            + ": Must implement: "
                            + ISubjectCentricTextIndexer.class.getName());
        }

        return (Class<ISubjectCentricTextIndexer>) cls;

    }
    
    @SuppressWarnings("unchecked")
    protected Class<IExtensionFactory> determineExtensionFactoryClass() {

        final String defaultClassName;
        if (vocab == null || vocab.getClass() == NoVocabulary.class) {
            /*
             * If there is no vocabulary then you can not use the default
             * extension class (or probably any extension class for that matter
             * since the vocbulary is required in order to be able to resolve
             * the URIs for the extension).
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/456
             */
            defaultClassName = NoExtensionFactory.class.getName();
        } else {
            defaultClassName = AbstractTripleStore.Options.DEFAULT_EXTENSION_FACTORY_CLASS;
        }

        final String className = getProperty(
                AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS,
                defaultClassName);
        
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

    @SuppressWarnings("unchecked")
    protected Class<IInlineURIFactory> determineInlineURIFactoryClass() {

        final String defaultClassName;
        if (vocab == null || vocab.get(XSD.IPV4) == null) {
            /*
             * If there is no vocabulary then you can not use an inline URI
             * factory because the namespaces must be in the vocabulary. If the
             * XSD.IPV4 uri is not present in the vocabulary then either you are
             * using NoVocabulary.class or an older version of the vocabulary
             * that does not have that URI in it. Newer journals should be using
             * DefaultBigdataVocabulary.
             */
            defaultClassName = NoInlineURIFactory.class.getName();
        } else {
            defaultClassName = AbstractTripleStore.Options.DEFAULT_INLINE_URI_FACTORY_CLASS;
        }

        final String className = getProperty(
                AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS,
                defaultClassName);
        
        final Class<?> cls;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS, e);
        }

        if (!IInlineURIFactory.class.isAssignableFrom(cls)) {
            throw new RuntimeException(
                    AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS
                            + ": Must implement: "
                            + IInlineURIFactory.class.getName());
        }

        return (Class<IInlineURIFactory>) cls;

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

        this(null/* container */, indexManager, namespace, timestamp,
                properties);

    }

    public LexiconRelation(final AbstractTripleStore container,
            final IIndexManager indexManager, final String namespace,
            final Long timestamp, final Properties properties) {

        super(container, indexManager, namespace, timestamp, properties);

        {
            
            this.textIndex = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.TEXT_INDEX,
                    AbstractTripleStore.Options.DEFAULT_TEXT_INDEX));
         
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
            
            // just for now while I am testing, don't feel like rebuilding
            // the entire journal
            this.subjectCentricTextIndex = textIndex;
//            this.subjectCentricTextIndex = Boolean.parseBoolean(getProperty(
//                    AbstractTripleStore.Options.SUBJECT_CENTRIC_TEXT_INDEX,
//                    AbstractTripleStore.Options.DEFAULT_SUBJECT_CENTRIC_TEXT_INDEXER_CLASS));
         
        }
        
        this.storeBlankNodes = Boolean.parseBoolean(getProperty(
                AbstractTripleStore.Options.STORE_BLANK_NODES,
                AbstractTripleStore.Options.DEFAULT_STORE_BLANK_NODES));
        
        final int blobsThreshold;
        {

            blobsThreshold = Integer.parseInt(getProperty(
                    AbstractTripleStore.Options.BLOBS_THRESHOLD,
                    AbstractTripleStore.Options.DEFAULT_BLOBS_THRESHOLD));

			/**
			 * Note: Integer.MAX_VALUE disables the BLOBS index.
			 * 
			 * @see <a href="https://github.com/SYSTAP/bigdata-gpu/issues/25">
			 *      Disable BLOBS indexing completely for GPU </a>
			 */
			if (blobsThreshold < 0 || blobsThreshold > 4 * Bytes.kilobyte && blobsThreshold != Integer.MAX_VALUE) {

                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.BLOBS_THRESHOLD + "="
                                + blobsThreshold);

            }

        }

        {

            if (indexManager instanceof IBigdataFederation<?>
                    && ((IBigdataFederation<?>) indexManager).isScaleOut()) {

                final String defaultValue = AbstractTripleStore.Options.DEFAULT_TERMID_BITS_TO_REVERSE;

                termIdBitsToReverse = Integer.parseInt(getProperty(
                        AbstractTripleStore.Options.TERMID_BITS_TO_REVERSE,
                        defaultValue));

                if (termIdBitsToReverse < 0 || termIdBitsToReverse > 31) {

                    throw new IllegalArgumentException(
                            AbstractTripleStore.Options.TERMID_BITS_TO_REVERSE
                                    + "=" + termIdBitsToReverse);

                }

            } else {

                // Note: Not used in standalone.
                termIdBitsToReverse = 0;

            }

        }

        {

            final Set<String> set = new HashSet<String>();

            set.add(getFQN(LexiconKeyOrder.TERM2ID));
            set.add(getFQN(LexiconKeyOrder.ID2TERM));
            set.add(getFQN(LexiconKeyOrder.BLOBS));

            if(textIndex) {
                
                set.add(getNamespace() + "." + FullTextIndex.NAME_SEARCH);

            }
            
            // @todo add names as registered to base class? but then how to
            // discover?  could be in the global row store.
            this.indexNames = Collections.unmodifiableSet(set);

            this.keyOrders = Arrays
                    .asList((IKeyOrder<BigdataValue>[]) new IKeyOrder[] { //
                            LexiconKeyOrder.TERM2ID,//
                            LexiconKeyOrder.ID2TERM,//
                            LexiconKeyOrder.BLOBS //
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
                termCache = new TermCache<IV<?,?>, BigdataValue>(//
                        new ConcurrentWeakValueCacheWithBatchedUpdates<IV<?,?>, BigdataValue>(//
                        termCacheCapacity, // queueCapacity
                        .75f, // loadFactor (.75 is the default)
                        16 // concurrency level (16 is the default)
                ));

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
            
            enableRawRecordsSupport = Boolean.parseBoolean(getProperty(
            		AbstractTripleStore.Options.ENABLE_RAW_RECORDS_SUPPORT,
            		AbstractTripleStore.Options.DEFAULT_ENABLE_RAW_RECORDS_SUPPORT));

            // Resolve the vocabulary.
            vocab = getContainer().getVocabulary();

            
            // Resolve the geospatial configuration, if geospatial is enabled
            final Boolean geoSpatial = Boolean.parseBoolean(getProperty(
                    AbstractTripleStore.Options.GEO_SPATIAL,
                    AbstractTripleStore.Options.DEFAULT_GEO_SPATIAL));
            
            final GeoSpatialConfig geoSpatialConfig = 
                geoSpatial!=null && geoSpatial ?  getContainer().getGeoSpatialConfig() : null;


            final IExtensionFactory xFactory;
            try {
                
            	/*
            	 * Setup the extension factory.
            	 */
                final Class<IExtensionFactory> xfc = 
                    determineExtensionFactoryClass();

                xFactory = xfc.newInstance();

            } catch (InstantiationException e) {
                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, e);
            }

            final IInlineURIFactory uriFactory;
            try {
                
                /*
                 * Setup the inline URI factory.
                 */
                final Class<IInlineURIFactory> urifc = 
                    determineInlineURIFactoryClass();

                uriFactory = urifc.newInstance();
                uriFactory.init(vocab);

            } catch (InstantiationException e) {
                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS, e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(
                        AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS, e);
            }

            /*
             * Setup the lexicon configuration.
             */
            
            lexiconConfiguration = new LexiconConfiguration<BigdataValue>(
                    blobsThreshold,
                    inlineLiterals, inlineTextLiterals,
                    maxInlineTextLength, inlineBNodes, inlineDateTimes,
                    inlineDateTimesTimeZone,
                    rejectInvalidXSDValues, enableRawRecordsSupport, xFactory, 
                    vocab, valueFactory, uriFactory, geoSpatial, geoSpatialConfig);

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
				 * unreasonable configuration due to the data duplication in the
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

            indexManager
                    .registerIndex(getTerm2IdIndexMetadata(getFQN(LexiconKeyOrder.TERM2ID)));

            indexManager
                    .registerIndex(getId2TermIndexMetadata(getFQN(LexiconKeyOrder.ID2TERM)));

			if (getLexiconConfiguration().getBlobsThreshold() != Integer.MAX_VALUE) {

				// Do not create the BLOBS index if BLOBS support has been disabled.
				indexManager.registerIndex(getBlobsIndexMetadata(getFQN(LexiconKeyOrder.BLOBS)));
				
			}

            if (textIndex) {

                // Create the full text index

				final IValueCentricTextIndexer<?> tmp = getSearchEngine();

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

    @Override
    public void destroy() {

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            final IIndexManager indexManager = getIndexManager();

            indexManager.dropIndex(getFQN(LexiconKeyOrder.TERM2ID));
            indexManager.dropIndex(getFQN(LexiconKeyOrder.ID2TERM));
			if (getLexiconConfiguration().getBlobsThreshold() != Integer.MAX_VALUE) {
				// Destroy BLOBS index IFF it exists.
				indexManager.dropIndex(getFQN(LexiconKeyOrder.BLOBS));
			}

            term2id = null;
            id2term = null;
            blobs = null;

            if (textIndex) {

                getSearchEngine().destroy();

                viewRef.set(null);

            }

            // discard the value factory for the lexicon's namespace.
            valueFactory.remove(/*getNamespace()*/);

            termCache.clear();
            
            super.destroy();

        } finally {

            unlock(resourceLock);

        }

    }
    
    /** The reference to the TERM2ID index. */
    volatile private IIndex term2id;

    /** The reference to the ID2TERM index. */
    volatile private IIndex id2term;

    /** The reference to the TERMS index. */
    volatile private IIndex blobs;
    
    /**
     * When <code>true</code> a full text index is maintained.
     * 
     * @see AbstractTripleStore.Options#TEXT_INDEX
     */
    private boolean textIndex;
    
    /**
	 * When <code>true</code> a secondary subject-centric full text index is
	 * maintained.
	 * 
	 * @see AbstractTripleStore.Options#SUBJECT_CENTRIC_TEXT_INDEX
	 * @deprecated Feature was never completed due to scalability issues. See
	 *             BZLG-1548, BLZG-563.
	 */
    @Deprecated
    private final boolean subjectCentricTextIndex;
    
    /**
     * When <code>true</code> the kb is using told blank nodes semantics.
     * 
     * @see AbstractTripleStore.Options#STORE_BLANK_NODES
     */
    private final boolean storeBlankNodes;
    
//    /**
//     * The maximum character length of an RDF {@link Value} before it will be
//     * inserted into the {@link LexiconKeyOrder#BLOBS} index rather than the
//     * {@link LexiconKeyOrder#TERM2ID} and {@link LexiconKeyOrder#ID2TERM}
//     * indices.
//     * 
//     * @see AbstractTripleStore.Options#BLOBS_THRESHOLD
//     */
//    private final int blobsThreshold;

    /**
     * @see AbstractTripleStore.Options#TERMID_BITS_TO_REVERSE
     */
    private final int termIdBitsToReverse;
    
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
     * Are xsd:dateTime literals being inlined into the statement indices.
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
     * When <code>false</code>, raw records will not be used to store 
     * the lexical forms of the RDF Values
     * {@link AbstractTripleStore.Options#ENABLE_RAW_RECORDS_SUPPORT}.
     */
    final private boolean enableRawRecordsSupport;
    
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
     * <code>true</code> iff the (value centric) full text index is enabled.
     * 
     * @see AbstractTripleStore.Options#TEXT_INDEX
     */
    final public boolean isTextIndex() {
        
        return textIndex;
        
    }

    /**
	 * <code>true</code> iff the subject-centric full text index is enabled.
	 * 
	 * @see AbstractTripleStore.Options#SUBJECT_CENTRIC_TEXT_INDEX
	 * 
	 * @deprecated Feature was never completed due to scalability issues. See
	 *             BZLG-1548, BLZG-563.
	 */
    @Deprecated
    final public boolean isSubjectCentricTextIndex() {
        
        return subjectCentricTextIndex;
        
    }

	/**
	 * Overridden to use local cache of the index reference.
	 */
    @Override
    public IIndex getIndex(final IKeyOrder<? extends BigdataValue> keyOrder) {

        if (keyOrder == LexiconKeyOrder.ID2TERM) {

            return getId2TermIndex();

        } else if (keyOrder == LexiconKeyOrder.TERM2ID) {

            return getTerm2IdIndex();

        } else if (keyOrder == LexiconKeyOrder.BLOBS) {

            return getBlobsIndex();

        } else {

            throw new AssertionError("keyOrder=" + keyOrder);

        }

    }

    final public IIndex getTerm2IdIndex() {

        if (term2id == null) {

            synchronized (this) {

                if (term2id == null) {

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
                        term2id = AbstractRelation
                                .getIndex(getIndexManager(),
                                        getFQN(LexiconKeyOrder.TERM2ID),
                                        ITx.UNISOLATED);
                    } else {
                        term2id = super.getIndex(LexiconKeyOrder.TERM2ID);
                    }

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
                        id2term = AbstractRelation
                                .getIndex(getIndexManager(),
                                        getFQN(LexiconKeyOrder.ID2TERM),
                                        ITx.UNISOLATED);
                    } else {
                        id2term = super.getIndex(LexiconKeyOrder.ID2TERM);
                    }
                
                    if (id2term == null)
                        throw new IllegalStateException();

                }

            }

        }

        return id2term;

    }

    final public IIndex getBlobsIndex() {

        if (blobs == null) {

            synchronized (this) {

                if (blobs == null) {

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
                        blobs = AbstractRelation
                                .getIndex(getIndexManager(),
                                        getFQN(LexiconKeyOrder.BLOBS),
                                        ITx.UNISOLATED);
                    } else {
                        blobs = super.getIndex(LexiconKeyOrder.BLOBS);
                    }

                    if (blobs == null)
                        throw new IllegalStateException();

                }

            }
            
        }

        return blobs;

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
    public IValueCentricTextIndexer<?> getSearchEngine() {

        if (!textIndex)
            return null;

        /*
         * Note: Double-checked locking pattern requires [volatile] variable or
         * AtomicReference. This uses the AtomicReference since that gives us a
         * lock object which is specific to this request.
         */
        if (viewRef.get() == null) {

            synchronized (viewRef) {// NB: Ignore find bugs complaint per above.

                if (viewRef.get() == null) {

                    final IValueCentricTextIndexer<?> tmp;
                    try {
                        final Class<?> vfc = determineTextIndexerClass();
                        final Method gi = vfc.getMethod("getInstance",
                                IIndexManager.class, String.class, Long.class,
                                Properties.class);
                        tmp = (IValueCentricTextIndexer<?>) gi.invoke(null/* object */,
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
	 * A factory returning the softly held singleton for the
	 * {@link FullTextIndex} representing the subject-centric full text index.
	 * 
	 * @see AbstractTripleStore.Options#TEXT_INDEX
	 * @deprecated Feature was never completed due to scalability issues. See
	 *             BZLG-1548, BLZG-563.
	 */
    @Deprecated
    public ISubjectCentricTextIndexer<?> getSubjectCentricSearchEngine() {

    	if (!subjectCentricTextIndex)
            return null;

        /*
         * Note: Double-checked locking pattern requires [volatile] variable or
         * AtomicReference. This uses the AtomicReference since that gives us a
         * lock object which is specific to this request.
         */
        if (viewRef2.get() == null) {

            synchronized (viewRef2) {// NB: Ignore find bugs complaint per above.

                if (viewRef2.get() == null) {

                    final ISubjectCentricTextIndexer<?> tmp;
                    try {
                        final Class<?> vfc = determineSubjectCentricTextIndexerClass();
                        final Method gi = vfc.getMethod("getInstance",
                                IIndexManager.class, String.class, Long.class,
                                Properties.class);
                        tmp = (ISubjectCentricTextIndexer<?>) gi.invoke(null/* object */,
                                getIndexManager(), getNamespace(),
                                getTimestamp(), getProperties());
                        if(tmp instanceof ILocatableResource<?>) {
                        	((ILocatableResource<?>)tmp).init();
                        }
                        viewRef2.set(tmp);
                    } catch (Throwable e) {
                        throw new IllegalArgumentException(
                                AbstractTripleStore.Options.SUBJECT_CENTRIC_TEXT_INDEXER_CLASS,
                                e);
                    }

                }

            }

        }

        return viewRef2.get();

    }

    /**
     * Return the {@link IndexMetadata} for the TERM2ID index.
     * 
     * @param name
     *            The name of the index.
     *            
     * @return The {@link IndexMetadata}.
     */
    protected IndexMetadata getTerm2IdIndexMetadata(final String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new Term2IdTupleSerializer(getProperties()));
        
        return metadata;

    }

    /**
     * Return the {@link IndexMetadata} for the ID2TERM index.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The {@link IndexMetadata}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/506">
     *      Load, closure and query performance in 1.1.x versus 1.0.x </a>
     */
    protected IndexMetadata getId2TermIndexMetadata(final String name) {

        final IndexMetadata metadata = newIndexMetadata(name);

        metadata.setTupleSerializer(new Id2TermTupleSerializer(
                getNamespace(), getValueFactory()));

        /*
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/506 (Load,
         * closure and query performance in 1.1.x versus 1.0.x)
         */
        if (enableRawRecordsSupport) {

            // enable raw record support.
            metadata.setRawRecords(true);

            /*
             * Very small RDF values can be inlined into the index, but after
             * that threshold we want to have the values out of line on the
             * backing store.
             * 
             * Note: I have tried it at 16 and 24 on LUBM U50. Raising it to 24
             * increases the data on the disk and might have a small negative
             * effect on the load and query rates. No difference on closure
             * rates was observed. It might all be in the noise, but it does
             * seem that less data on the disk is better.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/506 (Load,
             * closure and query performance in 1.1.x versus 1.0.x)
             */
            metadata.setMaxRecLen(16);
        
        }
        
        return metadata;

    }

	/**
	 * Return the {@link IndexMetadata} for the TERMS index.
	 * 
	 * @param name
	 *            The name of the index.
	 *            
	 * @return The {@link IndexMetadata}.
	 */
    protected IndexMetadata getBlobsIndexMetadata(final String name) {

		final IndexMetadata metadata = new IndexMetadata(getIndexManager(),
				getProperties(), name, UUID.randomUUID(), IndexTypeEnum.BTree);

        metadata.setTupleSerializer(new BlobsTupleSerializer(getNamespace(),
                valueFactory));

		// enable raw record support.
		metadata.setRawRecords(true);

        /*
         * The presumption is that we are storing large literals (blobs) in this
         * index so we always want to write them on raw records rather than have
         * them be inline in the leaves of the index.
         */
        metadata.setMaxRecLen(0);

        if ((getIndexManager() instanceof IBigdataFederation<?>)
                && ((IBigdataFederation<?>) getIndexManager()).isScaleOut()) {

            /*
             * Apply a constraint such that all entries within the same
             * collision bucket lie in the same shard.
             */
            
            metadata.setSplitHandler(new BlobsIndexSplitHandler());

        }

        return metadata;

    }

    public Set<String> getIndexNames() {

        return indexNames;

    }

    public Iterator<IKeyOrder<BigdataValue>> getKeyOrders() {
        
        return keyOrders.iterator();
        
    }

    public LexiconKeyOrder getPrimaryKeyOrder() {
        
    	return LexiconKeyOrder.BLOBS;
        
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
     * 
     *         TODO Prefix scan only visits the TERM2ID index (blobs and inline
     *         literals will not be observed). This should be mapped onto a free
     *         text index query instead. In order to have the same semantics we
     *         must also verify that (a) the prefix match is at the start of the
     *         literal; and (b) the match is contiguous.
     */
    @SuppressWarnings("rawtypes")
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
     *         TODO Prefix scan only visits the TERM2ID index (blobs and inline
     *         literals will not be observed). This should be mapped onto a free
     *         text index query instead. In order to have the same semantics we
     *         must also verify that (a) the prefix match is at the start of the
     *         literal; and (b) the match is contiguous.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<IV> prefixScan(final Literal[] lits) {

        if (lits == null || lits.length == 0)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled()) {

            log.info("#lits=" + lits.length);

        }

        /**
         * The KeyBuilder used to form the prefix keys.
         * 
         * Note: The prefix keys are formed with PRIMARY strength. This is
         * necessary in order to match all keys in the index since it causes the
         * secondary characteristics to NOT be included in the prefix key even
         * if they are present in the keys in the index.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/974" >
         *      Name2Addr.indexNameScan(prefix) uses scan + filter </a>
         */
        final LexiconKeyBuilder keyBuilder = ((Term2IdTupleSerializer) getTerm2IdIndex()
                .getIndexMetadata().getTupleSerializer())
                .getLexiconPrimaryKeyBuilder();
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

        final Iterator<IV> termIdIterator = new Striterator(
                ndx
                        .rangeIterator(
                                null/* fromKey */,
                                null/* toKey */,
                                0/* capacity */,
                                IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
                                // prefix filter.
                                new PrefixFilter<BigdataValue>(keys)))
                .addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    /**
                     * Decode the value, which is the term identifier.
                     */
                    @Override
                    protected Object resolve(final Object arg0) {

                        final byte[] bytes = ((ITuple) arg0).getValue(); 
                        
                        return IVUtility.decode(bytes);

                    }
                });

        return termIdIterator;
            
    }

    /**
     * {@inheritDoc}
     * 
     * @see IDatatypeURIResolver
     */
    public BigdataURI resolve(final URI uri) {

        if(uri == null)
            throw new IllegalArgumentException();

        // Turn the caller's argument into a BigdataURI.
        final BigdataURI value = valueFactory.asValue(uri);

        // Lookup against the Vocabulary.
        final IV<?, ?> iv = vocab.get(value);

        if (iv == null) {

            /*
             * The request URI is not part of the pre-declared vocabulary.
             */
            
            throw new NoSuchVocabularyItem("uri=" + uri + ", vocab=" + vocab);

        }
        
        // Cache the IV on the BigdataValue.
        value.setIV(iv);

        return value;
        
//        final BigdataURI buri = valueFactory.asValue(uri);
//        
//        if (buri.getIV() == null) {
//            
//            // Will set IV as a side effect
//            final IV<?, ?> iv = getTermId(buri);
//
//            if (iv == null) {
//
//                // Will set IV as a side effect
//                addTerms(new BigdataValue[] { buri }, 1, false);
//
//            }
//
//        }
//        
//        return buri.getIV() != null ? buri : null;
        
    }

    /**
     * Return <code>true</code> iff this {@link Value} would be stored in the
     * {@link LexiconKeyOrder#BLOBS} index.
     * 
     * @param v
     *            The value.
     * 
     * @return <code>true</code> if it is a "large value" according to the
     *         configuration of the lexicon.
     * 
     * @see AbstractTripleStore.Options#BLOBS_THRESHOLD
     */
    public boolean isBlob(final Value v) {

        final int blobsThreshold = lexiconConfiguration.getBlobsThreshold();

        if (blobsThreshold == 0)
            return true;
        
        final long strlen = BigdataValueSerializer.getStringLength(v);
        
		if (strlen >= blobsThreshold) {

			if (lexiconConfiguration.isBlobsDisabled()) {

				throw new IllegalArgumentException("Large literal but BLOBS index is disabled: strlen=" + strlen);

			}

        	return true;
        }
        
        return false;

    }

//    /**
//     * Return the threshold at which a literal would be stored in the
//     * {@link LexiconKeyOrder#BLOBS} index.
//     * 
//     * @see AbstractTripleStore.Options#BLOBS_THRESHOLD
//     */
//    public int getBlobsThreshold() {
//        
//        return blobsThreshold;
//        
//    }
    
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
     * @return The #of distinct terms lacking a pre-assigned term identifier. If
     *         writes were permitted, then this is also the #of terms written
     *         onto the index.
     * 
     *         TODO If we refactor the search index shortly to use a
     *         [token,S,P,O,(C)] key then search will become co-threaded with
     *         the assertion and retraction of statements (writes on the
     *         statement indices) rather than with ID2TERM writes.
     */
    public long addTerms(final BigdataValue[] values, final int numTerms,
            final boolean readOnly) {

        if (log.isDebugEnabled())
            log.debug("numTerms=" + numTerms + ", readOnly=" + readOnly);

        /*
         * Ensure that BigdataValue objects belong to the correct ValueFactory
         * for this LexiconRelation.
         * 
         * @see BLZG-1593 (LexiconRelation.addTerms() does not reject
         * BigdataValue objects from another namespace nor call asValue() on
         * them to put them in the correct namespace)
         */
        {
            final BigdataValueFactory vf = getValueFactory();
            for (int i = 0; i < numTerms; i++) {
                final BigdataValue tmp = vf.asValue(values[i]);
                if (tmp != values[i]) {
                    /*
                     * Note: When the BigdataValue does not belong to this
                     * namespace the IV can not be set on the BigdataValue as a
                     * side-effect.
                     */
                    throw new RuntimeException("Value does not belong to this namespace: value=" + values[i]);
                }
                values[i] = tmp;
            }
        }
        
        /*
         * Filter out inline terms from the supplied terms array and create a
         * collections of Values which will be resolved against the
         * TERM2ID/ID2TERM index and a collection of Values which will be
         * resolved against the BLOBS index. Duplicates are filtered out and
         * post-processed once the distinct BigdataValues have been resolved.
         */

        // Will be resolved against TERM2ID/ID2TERM.
        final LinkedHashMap<BigdataValue, BigdataValue> terms = new LinkedHashMap<BigdataValue, BigdataValue>(
                numTerms);

        // Will be resolved against BLOBS.
        final LinkedHashMap<BigdataValue, BigdataValue> blobs = new LinkedHashMap<BigdataValue, BigdataValue>(/* default */);

        // Either same reference -or- distinct reference but equals().
        final List<BigdataValue> dups = new LinkedList<BigdataValue>();
        
        // Inline literals that should still make it into the text index.
        final LinkedHashSet<BigdataValue> textIndex = new LinkedHashSet<BigdataValue>(/* default */);

        int nunknown = 0, nblobs = 0, nterms = 0;

        for (int i = 0; i < numTerms; i++) {

            final BigdataValue v = values[i];

            /*
             * Try to get an inline IV for the BigdataValue (sets the IV as a
             * side effect if not null).
             */
            
            if (getInlineIV(v) == null) {

                /*
                 * Value can not be inlined. We need to figure out which index
                 * we need to use for this Value.
                 * 
                 * Note: This also identifies duplicates (whether they are the
                 * same reference or distinct references which are equals()).
                 * Duplicates are put onto a List. That List is scanned after we
                 * have resolved Values against the indices so we can set the
                 * IVs for the duplicates as well.
                 */
                
                if (isBlob(v)) {
                
                    if (blobs.get(v) != null)
                        dups.add(v);
                    else {
                        if (blobs.put(v, v) != null)
                            throw new AssertionError();
                        nblobs++;
                    }

                } else {

                    if (terms.get(v) != null)
                        dups.add(v);
                    else {
                        if (terms.put(v, v) != null)
                            throw new AssertionError();
                        nterms++;
                    }

                }
                
                nunknown++;
                
            } else if (!readOnly && this.textIndex && v instanceof BigdataLiteral) {
                
                /*
                 * Some inline IVs will be text indexed per the 
                 * LexiconConfiguration.
                 */
                final URI dt = ((BigdataLiteral) v).getDatatype();
                if (dt == null || dt.equals(XSD.STRING) || dt.equals(RDF.LANGSTRING)) {
                    // always text index strings, even inline ones, datatyped and langtagged
                    textIndex.add(v);
                } else if (lexiconConfiguration.isInlineDatatypeToTextIndex(dt)) {
                    textIndex.add(v);
                }
                
            }
            
        }

        /*
         * Because we sometimes text index inline literals, we cannot assume
         * we're done just because nunknown == 0.
         */
        if (nunknown == 0 && textIndex.isEmpty()) {

            return 0;

        }

        /*
         * Batch insert/lookup of Values against the indices. No duplicates. No
         * inline values.
         * 
         * FIXME Co-thread the writes on the BLOBS and TERM2ID indices.
         */

        final WriteTaskStats stats = new WriteTaskStats();

        if (nblobs > 0) {
            
            final BigdataValue[] a = blobs.keySet().toArray(
                    new BigdataValue[nblobs]);

            addBlobs(a, a.length, readOnly, stats);
            
        }
        
        if (nterms > 0) {

            final BigdataValue[] a = terms.keySet().toArray(
                    new BigdataValue[nterms]);

            addTerms(a, a.length, readOnly, stats);
        
        }
        
        if (this.textIndex && textIndex.size() > 0) {
			/*
			 * There were some inline literals that need to make it into the
			 * text index. That is handled here.
			 * 
			 * See BLZG-1525
			 */
            try {
                
                stats.fullTextIndexTime
                    .addAndGet(new FullTextIndexWriterTask(
                            getSearchEngine(), textIndex.size()/* capacity */, 
                            textIndex.iterator())
                            .call());

            } catch (Exception ex) {
                
                throw new RuntimeException(ex);
                
            }
            
        }

        if(!dups.isEmpty()) {

            /*
             * There was at least one BigdataValue which was a duplicate (either
             * the same reference or a distinct reference which is equals()).
             * Now that we have resolved the IVs against the indices, we run
             * through the List of duplicates and resolve the IVs against the
             * TermIV and BlobIV maps. A duplicate will wind up with a resolved
             * IV if it was found/written on the appropriate index.
             */
            
            for(BigdataValue dup : dups) {

                BigdataValue resolved = blobs.get(dup);

                if (resolved == null)
                    resolved = terms.get(dup);

                if (resolved != null) {

                    final IV<?, ?> iv = resolved.getIV();

                    if (iv != null) {

                        dup.setIV(iv);

                    }

                }
                
            }
            
        }

        if (log.isInfoEnabled() && readOnly && stats.nunknown.get() > 0) {

            log.info("There are " + stats.nunknown + " unknown terms out of "
                    + numTerms + " given");

        }
        
        return stats.ndistinct.get();
        
    }
    
    // BLOBS+SEARCH
    private void addBlobs(final BigdataValue[] terms, final int numTerms,
            final boolean readOnly, final WriteTaskStats stats) {

        final KVO<BigdataValue>[] a;
        try {
            // write on the BLOBS index (rync sharded RPC in scale-out)
            a = new BlobsWriteTask(getBlobsIndex(), valueFactory, readOnly,
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
                 * Note: a[] is in BLOBS index order at this point and can
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
                @SuppressWarnings({ "unchecked", "rawtypes" })
                final Iterator<BigdataValue> itr = new Striterator(
                        new ChunkedArrayIterator(ndistinct, a, null/* keyOrder */))
                        .addFilter(new Resolver() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            protected Object resolve(final Object obj) {

                                return ((KVO<BigdataValue>) obj).obj;

                            }

                        });

                stats.fullTextIndexTime
                        .addAndGet(new FullTextIndexWriterTask(
                                getSearchEngine(), ndistinct/* capacity */, itr)
                                .call());

            } catch (Exception e) {

                throw new RuntimeException(e);

            }

            stats.indexTime.addAndGet(System.currentTimeMillis() - _begin);

        }

    }

    // TERM2ID/ID2TERM+SEARCH
    private void addTerms(final BigdataValue[] terms, final int numTerms,
            final boolean readOnly, final WriteTaskStats stats) {

        final KVO<BigdataValue>[] a;
        try {
            // write on the forward index (sync RPC)
            a = new Term2IdWriteTask(getTerm2IdIndex(), readOnly,
                    storeBlankNodes, termIdBitsToReverse, numTerms, terms,
                    stats).call();
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
                
                stats.keySortTime.add(System.currentTimeMillis() - _begin);
    
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

                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    final Iterator<BigdataValue> itr = new Striterator(
                            new ChunkedArrayIterator(ndistinct, a, null/* keyOrder */))
                            .addFilter(new Resolver() {

                        private static final long serialVersionUID = 1L;

                        @Override
                        protected Object resolve(final Object obj) {
                        
                            return ((KVO<BigdataValue>) obj).obj;
                            
                        }
                        
                    });

                    tasks.add(new FullTextIndexWriterTask(getSearchEngine(),
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
                        stats.fullTextIndexTime.addAndGet(futures.get(1).get());
//                    else 
//                        stats.fullTextIndexTime = 0L;

                } catch (Throwable t) {

                    throw new RuntimeException(t);

                }

                stats.indexTime.addAndGet(System.currentTimeMillis() - _begin);

            }
            
        }

    }
    
    /**
     * Utility method to (re-)build the full text index. This is a high latency
     * operation for a database of any significant size. You must be using the
     * unisolated view of the {@link AbstractTripleStore} for this operation.
     * {@link AbstractTripleStore.Options#TEXT_INDEX} must be enabled. This
     * operation is only supported when the {@link IValueCentricTextIndexer} uses the
     * {@link FullTextIndex} class.
     * 
     * @param forceCreate
     *            When <code>true</code> a new text index will be created
     *            for a namespace that had no it before.
     */
    @SuppressWarnings("unchecked")
    public void rebuildTextIndex(final boolean forceCreate) {

        if (getTimestamp() != ITx.UNISOLATED)
            throw new UnsupportedOperationException("Unisolated connection required to rebuild full text index");
        
        final IValueCentricTextIndexer<?> textIndexer;
        
        if (textIndex) {
        	
        	IValueCentricTextIndexer<?> oldTextIndexer = getSearchEngine();

        	// destroy the existing text index.
        	oldTextIndexer.destroy();

        	// clear reference to the old FTS
        	viewRef.set(null);

        	// get a new instance of FTS
            textIndexer = getSearchEngine();

        } else if (forceCreate) {
        	
        	textIndex = true;
        	
        	textIndexer = getSearchEngine();
        	
        	SparseRowStore global = indexManager.getGlobalRowStore();
        	
        	// Update "namespace" properties
            updateTextIndexConfiguration(global, getContainerNamespace());

            // Update "namespace.lex" properties
            updateTextIndexConfiguration(global, getNamespace());
        	
            // Warning: only container and lexicon properties are updated
            // with new text index configuration, other indexes may require update as well 
        } else {
        	
        	throw new UnsupportedOperationException("Could not rebuild full text index, because it is not enabled");
        	
        }

        // create a new index.
        textIndexer.create();

        // TermIVs
        {
            // The index to scan for the RDF Literals.
            final IIndex terms = getId2TermIndex();

            // used to decode the
            @SuppressWarnings("rawtypes")
            final ITupleSerializer tupSer = terms.getIndexMetadata()
                    .getTupleSerializer();

            /*
             * Visit all plain, language code, and datatype literals in the
             * lexicon.
             * 
             * Note: This uses a filter on the ITupleIterator in order to filter
             * out non-literal terms before they are shipped from a remote index
             * shard.
             */
            final Iterator<BigdataValue> itr = new Striterator(
                    terms.rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.DEFAULT,
                            new TupleFilter<BigdataValue>() {
                                private static final long serialVersionUID = 1L;

                                protected boolean isValid(
                                        final ITuple<BigdataValue> obj) {
                                    @SuppressWarnings("rawtypes")
                                    final IV iv = (IV) tupSer
                                            .deserializeKey(obj);
                                    if (iv != null && iv.isLiteral()) {
                                        return true;
                                    }
                                    return false;
                                }
                            })).addFilter(new Resolver() {
                private static final long serialVersionUID = 1L;

                protected Object resolve(final Object obj) {
                    final BigdataLiteral lit = (BigdataLiteral) tupSer
                            .deserialize((ITuple<?>) obj);
                    // System.err.println("lit: "+lit);
                    return lit;
                }
            });

            final int capacity = 10000;

            textIndexer.index(capacity, itr);

        }

        // BlobIVs
        {
            // the index to scan for the RDF Literals.
            final IIndex terms = getBlobsIndex();

            // used to decode the
            @SuppressWarnings("rawtypes")
            final ITupleSerializer tupSer = terms.getIndexMetadata()
                    .getTupleSerializer();

            /*
             * Visit all plain, language code, and datatype literals in the
             * lexicon.
             * 
             * Note: This uses a filter on the ITupleIterator in order to filter
             * out non-literal terms before they are shipped from a remote index
             * shard.
             */
            final Iterator<BigdataValue> itr = new Striterator(
                    terms.rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.DEFAULT,
                            new TupleFilter<BigdataValue>() {
                                private static final long serialVersionUID = 1L;

                                protected boolean isValid(
                                        final ITuple<BigdataValue> obj) {
                                    @SuppressWarnings("rawtypes")
                                    final IV iv = (IV) tupSer
                                            .deserializeKey(obj);
                                    if (iv != null && iv.isLiteral()) {
                                        return true;
                                    }
                                    return false;
                                }
                            })).addFilter(new Resolver() {
                private static final long serialVersionUID = 1L;

                protected Object resolve(final Object obj) {
                    final BigdataLiteral lit = (BigdataLiteral) tupSer
                            .deserialize((ITuple<?>) obj);
                    // System.err.println("lit: "+lit);
                    return lit;
                }
            });

            final int capacity = 10000;

            while (itr.hasNext()) {

                textIndexer.index(capacity, itr);

            }

        }

        // We need to finally commit the changes to apply them
        // See https://jira.blazegraph.com/browse/BLZG-1893 (Problems with Fulltext Index)
        if (indexManager instanceof IJournal) {

            // make the changes restart safe (not required for federation).
            ((IJournal) indexManager).commit();

        }
    	
    }

    private void updateTextIndexConfiguration(final SparseRowStore global, final String namespace) {

        Map<String, Object> map = global.read(
             RelationSchema.INSTANCE, namespace);

        map.put(AbstractTripleStore.Options.TEXT_INDEX, "true");

        map.put(FullTextIndex.Options.FIELDS_ENABLED, "false");
        
        if (getNamespace().equals(namespace)) {
            map.put(FullTextIndex.Options.OVERWRITE, "false");

        }

        global.write(RelationSchema.INSTANCE, map);

    }
    
    /**
     * Batch resolution of internal values to {@link BigdataValue}s.
     * 
     * @param ivs
     *            An collection of internal values. This may be an unmodifiable collection.
     * 
     * @return A map from internal value to the {@link BigdataValue}. If an
     *         internal value was not resolved then the map will not contain an
     *         entry for that internal value.
     */
    final public Map<IV<?, ?>, BigdataValue> getTerms(
            final Collection<IV<?, ?>> ivs) {

        /*
         * TODO The values below represent constants which were historically
         * hard coded into BatchResolveTermIVs and BatchResolveBlobIVs. This
         * value was not terribly sensitive when it was originally set, probably
         * because nearly all chunks are smaller than 4000 so basically all RDF
         * Value resolution was happening on the "one-chunk" code path.
         * 
         * See ASTEvalHelper which can override these values.
         */

        return getTerms(ivs, 4000/* termsChunkSize */, 4000/* blobsChunkSize */);
        
    }

	/**
	 * Utility method to (re-)build the subject-based full text index. This is a
	 * high latency operation for a database of any significant size. You must
	 * be using the unisolated view of the {@link AbstractTripleStore} for this
	 * operation. {@link AbstractTripleStore.Options#TEXT_INDEX} must be
	 * enabled. This operation is only supported when the {@link ITextIndexer}
	 * uses the {@link FullTextIndex} class.
	 * <p>
	 * The subject-based full text index is one that rolls up normal
	 * object-based full text index into a similarly structured index that
	 * captures relevancy across subjects. Instead of
	 * 
	 * (t,s) => s.len, termWeight
	 * 
	 * Where s is the subject's IV. The term weight has the same interpretation,
	 * but it is across all literals which are linked to that subject and which
	 * contain the given token. This index basically pre-computes the (?s ?p ?o)
	 * join that sometimes follows the (?o bd:search "xyz") request.
	 * <p>
	 * Truth Maintenance
	 * <p>
	 * We will need to perform truth maintenance on the subject-centric text
	 * index, that is - the index will need to be updated as statements are
	 * added and removed (to the extent that those statements involving a
	 * literal in the object position). Adding a statement is the easier case
	 * because we will never need to remove entries from the index, we can
	 * simply write over them with new relevance values. All that is involved
	 * with truth maintenance for adding a statement is taking a post- commit
	 * snapshot of the subject in the statement and running it through the
	 * indexer (a "subject-refresh").
	 * <p>
	 * The same "subject-refresh" will be necessary for truth maintenance for
	 * removal, but an additional step will be necessary beforehand - the index
	 * entries associated with the deleted subject/object (tokens+subject) will
	 * need to be removed in case the token appears only in the removed literal.
	 * After this pruning step the subject can be refreshed in the index exactly
	 * the same as for truth maintenance on add.
	 * <p>
	 * It looks like the right place to hook in truth maintenance for add is
	 * {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, com.bigdata.relation.accesspath.IElementFilter)}
	 * after the ISPOs are added to the SPORelation. Likewise, the place to hook
	 * in truth maintenance for delete is
	 * {@link AbstractTripleStore#removeStatements(IChunkedOrderedIterator, boolean)}
	 * after the ISPOs are removed from the SPORelation.
	 * 
	 * @deprecated Feature was never completed due to scalability issues. See
	 *             BZLG-1548, BLZG-563.
	 */
    @Deprecated
    @SuppressWarnings("unchecked")
    public void buildSubjectCentricTextIndex() {

        if (getTimestamp() != ITx.UNISOLATED)
            throw new UnsupportedOperationException();

        if (!subjectCentricTextIndex)
            throw new UnsupportedOperationException();

        final ISubjectCentricTextIndexer<?> textIndexer = getSubjectCentricSearchEngine();

        try {
        
	        // destroy the existing text index.
	        textIndexer.destroy();
	        
        } catch (NoSuchIndexException ex) {
        	
        	if (log.isInfoEnabled())
        		log.info("could not destroy subject-centric full text index, does not currently exist");
        	
        }

        // create a new index.
        textIndexer.create();

        // TermIVs
        {
            // The index to scan for the individual subjects and their literal
        	// values.
            final IIndex spoNdx = getContainer().getSPORelation().getPrimaryIndex();
            
            /*
             * For each S in SPO, collect up O values and pass this information
             * to the subject-centric text indexer for indexing.
             */
            
            // used to decode the
            @SuppressWarnings("rawtypes")
            final ITupleSerializer tupSer = spoNdx.getIndexMetadata()
                    .getTupleSerializer();

            /*
             * Visit all plain, language code, and datatype literals in the
             * object position of the primary statement index.
             * 
             * Note: This uses a filter on the ITupleIterator in order to filter
             * out non-literal terms before they are shipped from a remote index
             * shard.
             */
            final Iterator<ISPO> itr = new Striterator(
            		spoNdx.rangeIterator(null/* fromKey */, null/* toKey */,
                            0/* capacity */, IRangeQuery.DEFAULT,
                            new TupleFilter<ISPO>() {
                                private static final long serialVersionUID = 1L;

                                protected boolean isValid(
                                        final ITuple<ISPO> obj) {
                                    final ISPO spo = (ISPO) tupSer
                                            .deserializeKey(obj);
                                    if (spo.o().isLiteral()) {
                                        return true;
                                    }
                                    return false;
                                }
                            })).addFilter(new Resolver() {
                private static final long serialVersionUID = 1L;

                protected Object resolve(final Object obj) {
                    final ISPO spo = (ISPO) tupSer
                    	.deserializeKey((ITuple<?>) obj);
                    return spo;
                }
            });
            
            /*
             * Keep track of the current subject being indexed.
             */
            IV<?,?> s = null;
            
            /*
             * Keep a collection of literals to be indexed for that subject.
             */
            final Collection<IV<?,?>> literals = new LinkedList<IV<?,?>>();
            
            long subjectCount = 0;
            long statementCount = 0;
            
            final boolean l = log.isInfoEnabled();
            
            while (itr.hasNext()) {
            	
            	final ISPO spo = itr.next();
            	
            	if (!spo.s().equals(s)) {
            	
            		// flush the old s to the text index if != null
            		
            		if (s != null) {
            			
            			textIndexer.index(s, getTerms(literals).values().iterator());
            			
                        subjectCount++;
                        statementCount += literals.size();
                        
                        if (l && subjectCount % 1000 == 0) {
                        	log.info("indexed " + subjectCount + " subjects, " + statementCount + " statements");
                        }
                        
            		}
            		
            		// set the current s and clear the literals
            		
            		s = spo.s();
            		
            		literals.clear();
            		
            	}
            	
            	literals.add(spo.o());
            	
            }
            
            if (s != null) {

            	// flush the last subject
            	textIndexer.index(s, getTerms(literals).values().iterator());
            	
                subjectCount++;
                statementCount += literals.size();
                
            	if (log.isInfoEnabled()) {
            		log.info("indexed " + subjectCount + " subjects, " + statementCount + " statements");
            	}
            	
            }
            
        }

    }
    
//    @SuppressWarnings("unchecked")
//    public void refreshSubjectCentricTextIndex(final Set<IV<?,?>> subjects) {
//
//        if (getTimestamp() != ITx.UNISOLATED)
//            throw new UnsupportedOperationException();
//
//        if (!subjectCentricTextIndex)
//            throw new UnsupportedOperationException();
//
//        final ISubjectCentricTextIndexer<?> textIndexer = getSubjectCentricSearchEngine();
//
//        final AbstractTripleStore db = getContainer();
//        
//        /*
//         * Keep a collection of literals to be indexed for each subject.
//         */
//        final Collection<IV<?,?>> literals = new LinkedList<IV<?,?>>();
//        
//        for (IV<?,?> s : subjects) {
//        	
//        	literals.clear();
//        	
//            /*
//             * Visit all plain, language code, and datatype literals in the
//             * object position of the primary statement index.
//             * 
//             * Note: This uses a filter on the ITupleIterator in order to filter
//             * out non-literal terms before they are shipped from a remote index
//             * shard.
//             */
//            final Iterator<ISPO> itr = db.getAccessPath(s, null, null, new SPOFilter<ISPO>() {
//	                private static final long serialVersionUID = 1L;
//					@Override
//					public boolean isValid(Object e) {
//						return ((ISPO)e).o().isLiteral();
//					}
//				}).iterator(); 
//            	
//            while (itr.hasNext()) {
//            	
//            	final ISPO spo = itr.next();
//            	
//            	literals.add(spo.o());
//            	
//            }
//            
//        	// flush the last subject
//        	textIndexer.index(s, getTerms(literals).values().iterator());
//            
//        }
//
//    }
//    
//    @SuppressWarnings("unchecked")
//    public void refreshSubjectCentricTextIndex(final Set<ISPO> removed) {
//
//        if (getTimestamp() != ITx.UNISOLATED)
//            throw new UnsupportedOperationException();
//
//        if (!subjectCentricTextIndex)
//            throw new UnsupportedOperationException();
//
//        final ISubjectCentricTextIndexer<?> textIndexer = getSubjectCentricSearchEngine();
//
//        final AbstractTripleStore db = getContainer();
//        
//        /*
//         * Keep a collection of literals to be indexed for each subject.
//         */
//        final Collection<IV<?,?>> literals = new LinkedList<IV<?,?>>();
//        
//        for (ISPO spo : removed) {
//        	
//        	literals.clear();
//        	
//            /*
//             * Visit all plain, language code, and datatype literals in the
//             * object position of the primary statement index.
//             * 
//             * Note: This uses a filter on the ITupleIterator in order to filter
//             * out non-literal terms before they are shipped from a remote index
//             * shard.
//             */
//            final Iterator<ISPO> itr = db.getAccessPath(s, null, null, new SPOFilter<ISPO>() {
//	                private static final long serialVersionUID = 1L;
//					@Override
//					public boolean isValid(Object e) {
//						return ((ISPO)e).o().isLiteral();
//					}
//				}).iterator(); 
//            	
//            while (itr.hasNext()) {
//            	
//            	final ISPO spo = itr.next();
//            	
//            	literals.add(spo.o());
//            	
//            }
//            
//        	// flush the last subject
////        	textIndexer.index(s, getTerms(literals).values().iterator());
//        	
//        }
//
//    }
    
    /**
     * Batch resolution of internal values to {@link BigdataValue}s.
     * 
     * @param ivsUnmodifiable
     *            An collection of internal values
     * 
     * @return A map from internal value to the {@link BigdataValue}. If an
     *         internal value was not resolved then the map will not contain an
     *         entry for that internal value.
     * 
     * @see #getTerms(Collection)
     */
    final public Map<IV<?, ?>, BigdataValue> getTerms(
            final Collection<IV<?, ?>> ivsUnmodifiable, final int termsChunksSize,
            final int blobsChunkSize) {

        if (ivsUnmodifiable == null)
            throw new IllegalArgumentException();

        // Maximum #of IVs (assuming all are distinct).
        final int n = ivsUnmodifiable.size();
        
        if (n == 0) {

            return Collections.emptyMap();
            
        }
        
        /**
        * BLZG-1989: few lines below, we're collecting SIDs and adding them to the collection.
        * What we pass in, however, might actually be an unmodifiable set (e.g. obtained by a
        * call to a hash maps keySet() method). We therefore create a mutable copy of the map
        * for internal processing.
        */
        final Collection<IV<?, ?>> ivs = new ArrayList<IV<?, ?>>(ivsUnmodifiable);

        final long begin = System.currentTimeMillis();

        /*
         * Note: A concurrent hash map is used since the request may be split
         * across shards, in which case updates on the map may be concurrent.
         * 
         * Note: The also needs to be concurrent since the request can be split
         * across the ID2TERM and BLOBS indices.
         */
        final ConcurrentHashMap<IV<?,?>/* iv */, BigdataValue/* term */> ret = 
        		new ConcurrentHashMap<IV<?,?>, BigdataValue>(n/* initialCapacity */);

        // TermIVs which must be resolved against an index.
        final Collection<TermId<?>> termIVs = new LinkedList<TermId<?>>();
        
        // BlobIVs which must be resolved against an index.
        final Collection<BlobIV<?>> blobIVs = new LinkedList<BlobIV<?>>();
        
        final Set<IV<?, ?>> unrequestedSidTerms = new LinkedHashSet<IV<?, ?>>();
        
        /*
         * We need to materialize terms inside of SIDs so that the SIDs
         * can be materialized properly.
         */
        for (IV<?,?> iv : ivs) {
            
            if (iv instanceof SidIV) {

            	handleSid((SidIV) iv, ivs, unrequestedSidTerms);
            	
            }
            
        }
        
        /*
         * Add the SID terms to the IVs to materialize.
         */
        for (IV<?, ?> iv : unrequestedSidTerms) {
        	
        	ivs.add(iv);
        	
        }

        /*
         * Filter out the inline values first and those that have already
         * been materialized and cached.
         */
        int numNotFound = 0;

        final boolean isDebugEnabled = log.isDebugEnabled();
        
        for (IV<?,?> iv : ivs) {
            
            if (iv == null)
                throw new AssertionError();

            if (iv.hasValue()) {

                if (isDebugEnabled)
                    log.debug("already materialized: " + iv.getValue());

                // already materialized
                ret.put(iv, iv.getValue());

            } else if (iv instanceof SidIV) {

                // defer until the end
                continue;

            } else if (iv.isInline()) {

                // translate it into a value directly
                ret.put(iv, iv.asValue(this));

            } else {

                final BigdataValue value = _getTermId(iv);

                if (value != null) {

                    assert value.getValueFactory() == valueFactory;

                    // resolved.
                    ret.put(iv, value);// valueFactory.asValue(value));

                    continue;

                }

                // We will need to read on an index.
                numNotFound++;

                if (iv instanceof TermId<?>) {

                    termIVs.add((TermId<?>) iv);

                } else if (iv instanceof BlobIV<?>) {

                    blobIVs.add((BlobIV<?>) iv);

                } else {

                    throw new AssertionError("class=" + iv.getClass().getName());

                }

            }

        }

//        if (numNotFound == 0) {
//
//            // Done.
//            return ret;
//
//        }

        if (numNotFound > 0) {

        	// go to the indices
        	
	        /*
	         * Setup and run task(s) to resolve IV(s).
	         */
	
	        final ExecutorService service = getExecutorService();
	        
	        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
	
	        if (!termIVs.isEmpty()) {
	
	            tasks.add(new BatchResolveTermIVsTask(service, getId2TermIndex(),
	                    termIVs, ret, termCache, valueFactory, termsChunksSize));
	
	        }
	
	        if (!blobIVs.isEmpty()) {
	
	            tasks.add(new BatchResolveBlobIVsTask(service, getBlobsIndex(),
	                    blobIVs, ret, termCache, valueFactory, blobsChunkSize));
	
	        }
	
	        if (log.isInfoEnabled())
	            log.info("nterms=" + n + ", numNotFound=" + numNotFound
	                    + ", cacheSize=" + termCache.size());
	
	        try {
	
	            if (tasks.size() == 1) {
	             
	                tasks.get(0).call();
	                
	            } else {
	
	                // Co-thread tasks.
	                final List<Future<Void>> futures = getExecutorService()
	                        .invokeAll(tasks);
	
	                // Verify no errors.
	                for (Future<Void> f : futures)
	                    f.get();
	            
	            }
	
	        } catch (Exception ex) {
	
	            throw new RuntimeException(ex);
	
	        }

        }
        
        /*
         * SidIVs require special handling.
         */
        for (IV<?,?> iv : ivs) {
            
            if (iv instanceof SidIV) {

            	cacheTerms((SidIV<?>) iv, ret);
            	
                // translate it into a value directly
                ret.put(iv, iv.asValue(this));
                
            }
            
        }

        /*
         * Remove any IVs that were not explicitly requested in the method 
         * call but that got pulled into materialization because of a SID.
         */
        for (IV<?,?> iv : unrequestedSidTerms) {

        	//Removed in 2.1.5 per BLZG-9000
        	//See https://github.com/blazegraph/database/issues/67
        	//ivs.remove(iv);
        	
        	ret.remove(iv);
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled())
            log.info("resolved " + numNotFound + " terms: #TermIVs="
                    + termIVs.size() + ", #BlobIVs=" + blobIVs.size() + " in "
                    + elapsed + "ms");

        return ret;

    }
    
    /**
     * Add the terms inside a SID to the collection of IVs to materialize if
     * they are not already there.
     */
	@SuppressWarnings("rawtypes")
	final private void handleSid(final SidIV sid, 
			final Collection<IV<?, ?>> ivs, 
			final Set<IV<?, ?>> unrequested) {
    	
    	final ISPO spo = sid.getInlineValue();

    	handleTerm(spo.s(), ivs, unrequested);
		
    	handleTerm(spo.p(), ivs, unrequested);
		
    	handleTerm(spo.o(), ivs, unrequested);
		
    	if (spo.c() != null) {
    	
    		handleTerm(spo.c(), ivs, unrequested);
    		
    	}
		
    }
    
    /**
     * Add the terms inside a SID to the collection of IVs to materialize if
     * they are not already there.
     */
	@SuppressWarnings("rawtypes")
	final private void handleTerm(final IV<?, ?> iv, 
			final Collection<IV<?, ?>> ivs, 
			final Set<IV<?, ?>> unrequested) {

		if (iv instanceof SidIV) {
			
			handleSid((SidIV) iv, ivs, unrequested);
			
		} else {
			
			if (!ivs.contains(iv)) {
				
//				ivs.add(iv);
				
				unrequested.add(iv);
				
			}
			
		}
		
    }
    
    /**
     * We need to cache the BigdataValues on the IV components within the
     * SidIV so that the SidIV can materialize itself into a BigdataBNode
     * properly.
     */
    @SuppressWarnings("rawtypes")
	final private void cacheTerms(final SidIV sid, 
    		final Map<IV<?, ?>, BigdataValue> terms) {
    	
    	final ISPO spo = sid.getInlineValue();
    	
    	cacheTerm(spo.s(), terms);
    	
    	cacheTerm(spo.p(), terms);
    	
    	cacheTerm(spo.o(), terms);
    	
    	if (spo.c() != null) {
    		
    		cacheTerm(spo.c(), terms);
    		
    	}
    	
    }
    
    /**
     * We need to cache the BigdataValues on the IV components within the
     * SidIV so that the SidIV can materialize itself into a BigdataBNode
     * properly.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	final private void cacheTerm(final IV iv, 
			final Map<IV<?, ?>, BigdataValue> terms) {
    	
    	if (iv instanceof SidIV) {
    		
    		cacheTerms((SidIV<?>) iv, terms);
    		
    	} else {
    		
    		iv.setValue(terms.get(iv));
    		
    	}
    	
    }
    
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
//    final private ConcurrentWeakValueCacheWithBatchedUpdates<IV<?,?>, BigdataValue> termCache;
    final private ITermCache<IV<?,?>,BigdataValue> termCache;
    
    /**
     * Factory used for {@link #termCache} for read-only views of the lexicon.
     */
    static private CanonicalFactory<NT/* key */, ITermCache<IV<?,?>, BigdataValue>, Integer/* state */> termCacheFactory = new CanonicalFactory<NT, ITermCache<IV<?,?>, BigdataValue>, Integer>(
            1/* queueCapacity */) {
        @Override
        protected ITermCache<IV<?,?>, BigdataValue> newInstance(
                NT key, Integer termCacheCapacity) {
            return new TermCache<IV<?,?>,BigdataValue>(//
                    new ConcurrentWeakValueCacheWithBatchedUpdates<IV<?,?>, BigdataValue>(//
                    termCacheCapacity.intValue(),// backing hard reference LRU queue capacity.
                    .75f, // loadFactor (.75 is the default)
                    16 // concurrency level (16 is the default)
            ));
        }
    };
    
    /**
     * Clear all term caches for the supplied namespace.
     */
    @SuppressWarnings("rawtypes")
    static public void clearTermCacheFactory(final String namespace) {
        
        final Iterator it = termCacheFactory.entryIterator();
        while (it.hasNext()) {
            final NT nt = (NT) ((Entry) it.next()).getKey();
            if (nt.getName().equals(namespace)) {
                it.remove();
            }
        }
        
    }
    
    /**
     * The {@link Vocabulary} implementation class.
     */
    private final Vocabulary vocab;
    
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
     * Handles non-inline {@link IV}s by synthesizing a {@link BigdataBNode}
     * using {@link IV#bnodeId()} (iff told bnodes support is enabled and the
     * {@link IV} represents a blank node) and testing the {@link #termCache
     * term cache} otherwise.
     * 
     * @param iv
     *            A non-inline {@link IV}.
     * 
     * @return The corresponding {@link BigdataValue} if the {@link IV}
     *         represents a blank node or is found in the {@link #termCache},
     *         and <code>null</code> otherwise.
     * 
     * @throws IllegalArgumentException
     *             if <i>iv</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if {@link IV#isNullIV()}
     * @throws IllegalArgumentException
     *             if the {@link IV} is {@link IV#isInline()}.
     */
    private BigdataValue _getTermId(final IV<?,?> iv) {

        if (iv == null)
            throw new IllegalArgumentException();

        if (iv.isNullIV())
            throw new IllegalArgumentException();

        if (iv.isInline()) // only for non-inline IVs.
            throw new IllegalArgumentException();
        
        if (!storeBlankNodes && iv.isBNode()) {

            /*
             * Except when the "told bnodes" mode is enabled, blank nodes are
             * not stored in the reverse lexicon (or the cache).
             * 
             * Note: In a told bnodes mode, we need to store the blank nodes in
             * the lexicon and enter them into the term cache since their
             * lexical form will include the specified ID, not the term
             * identifier.
             */

            final String id = 't' + ((BNode) iv).getID();

            final BigdataBNode bnode = valueFactory.createBNode(id);

            // set the term identifier on the object.
            bnode.setIV(iv);

            return bnode;

        }

        // Test the term cache.
        return termCache.get(iv);

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
    @SuppressWarnings("rawtypes")
    final public BigdataValue getTerm(final IV iv) {
    	
    	return getValue(iv, true);
    	
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
    @SuppressWarnings("rawtypes")
    final private BigdataValue getValue(final IV iv, final boolean readFromIndex) {

        // if (false) { // alternative forces the standard code path.
        // final Collection<IV> ivs = new LinkedList<IV>();
        // ivs.add(iv);
        // final Map<IV, BigdataValue> values = getTerms(ivs);
        // return values.get(iv);
        // }

        if (iv.isInline())
            return iv.asValue(this);

        // handle bnodes, the termCache.
        BigdataValue value = _getTermId(iv);
        
        if (value != null || !readFromIndex)
            return value;

        if(iv instanceof BlobIV) {
            
            return __getBlob((BlobIV<?>) iv);
            
        }
        
        return __getTerm((TermId<?>) iv);

    }
    
    private BigdataValue __getTerm(final TermId<?> iv) {
        
        final IIndex ndx = getId2TermIndex();

        final Id2TermTupleSerializer tupleSer = (Id2TermTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.id2key(iv);

        final byte[] data = ndx.lookup(key);

        if (data == null)
            return null;

        // This also sets the value factory.
        BigdataValue value = valueFactory.getValueSerializer().deserialize(data);
        
        // This sets the term identifier.
        value.setIV(iv);

        // Note: passing the IV object as the key.
        final BigdataValue tmp = termCache.putIfAbsent(iv, value);

        if (tmp != null) {

            value = tmp;

        }

//        assert value.getIV() == iv : "expecting iv=" + iv + ", but found "
//                + value.getIV();
        //        value.setTermId( id );

        return value;

    }
    
    private BigdataValue __getBlob(final BlobIV<?> iv) {
    
        final IIndex ndx = getBlobsIndex();

        final BlobsTupleSerializer tupleSer = (BlobsTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.serializeKey(iv);

        final byte[] data = ndx.lookup(key);

        if (data == null)
            return null;

        // This also sets the value factory.
        BigdataValue value = valueFactory.getValueSerializer().deserialize(data);
        
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
     * 
     * @see #getTerms(Collection), Use this method to resolve {@link Value} to
     *      their {@link IV}s efficiently.
     */
    @SuppressWarnings("rawtypes")
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
     * Attempt to convert the value to an inline internal value. If the caller
     * provides a {@link BigdataValue} and this method is successful, then the
     * {@link IV} will be set as a side-effect on the {@link BigdataValue}.
     * 
     * @param value
     *            The value to convert
     * 
     * @return The inline internal value, or <code>null</code> if it cannot be
     *         converted
     * 
     * @see ILexiconConfiguration#createInlineIV(Value)
     */
    @SuppressWarnings("rawtypes")
    final public IV getInlineIV(final Value value) {
        if (value instanceof BigdataValue) {
            BigdataValue bv = (BigdataValue)value;
            if(bv.isRealIV()) {
                return bv.getIV();
            }
        }
        return getLexiconConfiguration().createInlineIV(value);

    }

    /**
     * This method assumes we've already exhausted all other possibilities and
     * need to go to the index for the {@link IV}. It is "optimized" for the
     * lookup of a single {@link Value}. Note, however, that single value lookup
     * is NOT efficient. {@link #getTerms(Collection)} SHOULD be used for
     * efficient batch resolution of {@link Value}s to {@link IV}s.
     * <p>
     * <strong>WARNING DO NOT USE OUTSIDE OF THE UNIT TESTS OR CAREFULLY VETTED
     * CODE: </strong> This method is extremely inefficient for scale-out as it
     * does one RMI per request!
     * 
     * @param value
     *            the value to lookup
     * 
     * @return The {@link IV} for the value
     */
    private IV<?,?> getTermId(final Value value) {

        if(isBlob(value)) {
            
            return getBlobIV(value);
            
        }

        return getTermIV(value);
        
    }
    
    /**
     * 
     * <strong>WARNING DO NOT USE OUTSIDE OF THE UNIT TESTS OR CAREFULLY VETTED
     * CODE: </strong> This method is extremely inefficient for scale-out as it
     * does one RMI per request!
     * 
     * @param value
     * @return
     */
    private TermId<?> getTermIV(final Value value) {

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

        if (tmp == null)
            return null;

        final TermId<?> iv = (TermId<?>) IVUtility.decode(tmp);

        if(value instanceof BigdataValue) {

            final BigdataValue impl = (BigdataValue) value;
            
            // set as side-effect.
            impl.setIV(iv);

            /*
             * Note that we have the termId and the term, we stick the value
             * into in the term cache IFF it has the correct value factory, but
             * do not replace the entry if there is one already there.
             */

            if (impl.getValueFactory() == valueFactory) {

                if (storeBlankNodes || !iv.isBNode()) {

                    // if (termCache.get(id) == null) {
                    //
                    // termCache.put(id, value, false/* dirty */);
                    //
                    // }

                    termCache.putIfAbsent(iv, impl);

                }
                
            }

        }

        return iv;

    }
    
    /**
     * <strong>WARNING DO NOT USE OUTSIDE OF THE UNIT TESTS OR CAREFULLY VETTED
     * CODE: </strong> This method is extremely inefficient for scale-out as it
     * does one RMI per request!
     */
    private BlobIV<?> getBlobIV(final Value value) {
        
        final IKeyBuilder keyBuilder = h.newKeyBuilder();
        
        final BigdataValue asValue = valueFactory.asValue(value);
        
        final byte[] baseKey = h.makePrefixKey(keyBuilder.reset(), asValue);

        final byte[] val = valueFactory.getValueSerializer().serialize(asValue);
        
        final int counter = h.resolveOrAddValue(getBlobsIndex(),
                true/* readOnly */, keyBuilder, baseKey, val, null/* tmp */,
                null/* bucketSize */);

		if (counter == BlobsIndexHelper.NOT_FOUND) {

            // Not found.
            return null;
            
        }
        
		final BlobIV<?> iv = new BlobIV<BigdataValue>(VTE.valueOf(asValue),
				asValue.hashCode(), (short) counter);

        if(value instanceof BigdataValue) {

			final BigdataValue impl = (BigdataValue) value;
            
            // set as side-effect.
            impl.setIV(iv);

            /*
             * Note that we have the termId and the term, we stick the value
             * into in the term cache IFF it has the correct value factory, but
             * do not replace the entry if there is one already there.
             */

            if (impl.getValueFactory() == valueFactory) {

                if (storeBlankNodes || !iv.isBNode()) {

                    // if (termCache.get(id) == null) {
                    //
                    // termCache.put(id, value, false/* dirty */);
                    //
                    // }

                    termCache.putIfAbsent(iv, impl);

                }
                
            }

        }

        return iv;

    }

    /**
     * Visits all RDF {@link Value}s in the {@link LexiconKeyOrder#BLOBS} index
     * in {@link BlobIV} order (efficient index scan).
     */
    @SuppressWarnings("unchecked")
    public Iterator<Value> blobsIterator() {

        final IIndex ndx = getBlobsIndex();

        return new Striterator(ndx.rangeIterator(null, null, 0/* capacity */,
                IRangeQuery.VALS, null/* filter */)).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            protected Object resolve(final Object val) {

                return ((ITuple<?>) val).getObject();

            }

        });

    }

    /**
     * Return the {@link #lexiconConfiguration} instance.  Used to determine
     * how to encode and decode terms in the key space.
     */
    public ILexiconConfiguration<BigdataValue> getLexiconConfiguration() {

        return lexiconConfiguration;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation examines the predicate, looking at the
     * {@link LexiconKeyOrder#SLOT_IV} and {@link LexiconKeyOrder#SLOT_TERM}
     * slots and chooses the appropriate index based on the {@link IV} and/or
     * {@link Value} which it founds bound. When both slots are bound it prefers
     * the index for the {@link IV} => {@link Value} mapping as that index will
     * be faster (ID2TERM has a shorter key and higher fan-out than TERM2ID).
     */
    public IKeyOrder<BigdataValue> getKeyOrder(final IPredicate<BigdataValue> p) {

        /*
         * Examine the IV slot first. Reverse lookup (IV => Value). This is
         * always our fastest and most common access path.
         */        
        {

            @SuppressWarnings("unchecked")
            final IVariableOrConstant<IV<?, ?>> t = (IVariableOrConstant<IV<?, ?>>) p
                    .get(LexiconKeyOrder.SLOT_IV);

            if (t != null) {

                final IV<?, ?> iv = t.get();

                if (iv instanceof TermId<?>)
                    return LexiconKeyOrder.ID2TERM;

                if (iv instanceof BlobIV<?>)
                    return LexiconKeyOrder.BLOBS;

                throw new UnsupportedOperationException(p.toString());

            }
        }

        /*
         * Examine the Value slot next. This is used for forward lookup (Value
         * => IV).
         */
        {

            @SuppressWarnings("unchecked")
            final IVariableOrConstant<BigdataValue> v = (IVariableOrConstant<BigdataValue>) p
                    .get(LexiconKeyOrder.SLOT_TERM);

            if (v != null) {

                final BigdataValue value = v.get();

                if (isBlob(value)) {

                    return LexiconKeyOrder.BLOBS;

                }

                return LexiconKeyOrder.TERM2ID;

            }

        }

        throw new UnsupportedOperationException(p.toString());

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
     * The lexicon has a single TERMS index. The keys are {@link BlobIV}s formed
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
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

            final IV iv = (IV) predicate.get(LexiconKeyOrder.SLOT_IV).get();

            if (VTE.valueOf(val) != iv.getVTE()) {

                /*
                 * The VTE is not consistent so the access path is provably
                 * empty.
                 */
                
                return new EmptyAccessPath<BigdataValue>();
                
            }

            if (val.hashCode() != iv.hashCode()) {

                /*
                 * The hashCode is not consistent so the access path is
                 * provably empty.
                 */
                
                return new EmptyAccessPath<BigdataValue>();
                
            }

            /*
             * Fall through.
             */
            
        }
        case IVBound: {

            final IV iv = (IV) predicate.get(LexiconKeyOrder.SLOT_IV).get();
            
//            if (log.isDebugEnabled())
//                log.debug("materializing: " + iv);

            // Attempt to resolve the IV directly to a Value (no IO).
            final BigdataValue val = getValue(iv, false/* readIndex */);

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
             * TODO Could be supported. This is a full index scan on both the
             * IV2TERM and BLOBS indices.
             */
        }
        default:
            throw new UnsupportedOperationException("" + accessPattern);
        }

    }

}
