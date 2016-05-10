/*

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
 * Created on Jan 23, 2008
 */

package com.bigdata.search;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexTypeEnum;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.btree.raba.codec.EmptyRabaValueCoder;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.lexicon.ITextIndexer.FullTextQuery;
import com.bigdata.relation.AbstractRelation;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.concurrent.ExecutionHelper;

/**
 * Full text indexing and search support.
 * <p>
 * The basic data model consists of documents, fields in documents, and tokens
 * extracted by an analyzer from those fields.
 * <p>
 * The frequency distributions may be normalized to account for a variety of
 * effects producing "term weights". For example, normalizing for document
 * length or relative frequency of a term in the overall collection. Therefore
 * the logical model is:
 * 
 * <pre>
 * 
 *             token : {docId, freq?, weight?}+
 * 
 * </pre>
 * 
 * (For RDF, docId is the term identifier as assigned by the term:id index.)
 * <p>
 * The freq and weight are optional values that are representative of the kinds
 * of statistical data that are kept on a per-token-document basis. The freq is
 * the token frequency (the frequency of occurrence of the token in the
 * document). The weight is generally a normalized token frequency weight for
 * the token in that document in the context of the overall collection.
 * <p>
 * In fact, we actually represent the data as follows:
 * 
 * <pre>
 * 
 *             {sortKey(token), weight, docId, fldId} : {freq?, sorted(pos)+}
 * 
 * </pre>
 * 
 * That is, there is a distinct entry in the full text B+Tree for each field in
 * each document in which a given token was recognized. The text of the token is
 * not stored in the key, just the Unicode sort key generated from the token
 * text. The value associated with the B+Tree entry is optional - it is simply
 * not used unless we are storing statistics for the token-document pair. The
 * advantages of this approach are: (a) it reuses the existing B+Tree data
 * structures efficiently; (b) we are never faced with the possibility overflow
 * when a token is used in a large number of documents. The entries for the
 * token will simply be spread across several leaves in the B+Tree; (c) leading
 * key compression makes the resulting B+Tree very efficient; and (d) in a
 * scale-out range partitioned index we can load balance the resulting index
 * partitions by choosing the partition based on an even token boundary.
 * <p>
 * A field is any pre-identified text container within a document. Field
 * identifiers are integers, so there are <code>32^2</code> distinct possible
 * field identifiers. It is possible to manage the field identifiers through a
 * secondary index, but that has no direct bearing on the structure of the full
 * text index itself. Field identifies appear after the token in the key so that
 * queries may be expressed that will be matched against any field in the
 * document. Likewise, field identifiers occur before the document identifier in
 * the key since we always search across documents (in a search key, the
 * document identifier is always {@link Long#MIN_VALUE} and the field identifier
 * is always {@link Integer#MIN_VALUE}). There are many applications for fields:
 * for example, distinct fields may be used for the title, abstract, and full
 * text of a document or for the CDATA section of each distinct element in
 * documents corresponding to some DTD. The application is responsible for
 * recognizing the fields in the document and producing the appropriate token
 * stream, each of which must be tagged by the field.
 * <p>
 * A query is tokenized, producing a (possibly normalized) token-frequency
 * vector. The relevance of documents to the query is generally taken as the
 * cosine between the query's and each document's (possibly normalized)
 * token-frequency vectors. The main effort of search is assembling a token
 * frequency vector for just those documents with which there is an overlap with
 * the query. This is done using a key range scan for each token in the query
 * against the full text index.
 * 
 * <pre>
 *             fromKey := token, Long.MIN_VALUE
 *             toKey   := successor(token), Long.MIN_VALUE
 * </pre>
 * 
 * and extracting the appropriate token frequency, normalized token weight, or
 * other statistic. When no value is associated with the entry we follow the
 * convention of assuming a token frequency of ONE (1) for each document in
 * which the token appears.
 * <p>
 * Tokenization is informed by the language code (when declared) and by the
 * configured {@link Locale} for the database otherwise. An appropriate
 * {@link Analyzer} is chosen based on the language code or {@link Locale} and
 * the "document" is broken into a token-frequency distribution (alternatively a
 * set of tokens). The same process is used to tokenize queries, and the API
 * allows the caller to specify the language code used to select the
 * {@link Analyzer} to tokenize the query.
 * <p>
 * Once the tokens are formed the language code / {@link Locale} used to produce
 * the token is discarded (it is not represented in the index). The reason for
 * this is that we never utilize the total ordering of the full text index,
 * merely the manner in which it groups tokens that map onto the same Unicode
 * sort key together. Further, we use only a single Unicode collator
 * configuration regardless of the language family in which the token was
 * originally expressed. Unlike the collator used by the terms index (which
 * often is set at IDENTICAL strength), the collector used by the full text
 * index should be chosen such that it makes relatively few distinctions in
 * order to increase recall (e.g., set at PRIMARY strength). Since a total order
 * over the full text index is not critical from the perspective of its IR
 * application, the {@link Locale} for the collator is likewise not critical and
 * PRIMARY strength will produce significantly shorter Unicode sort keys.
 * <p>
 * The term frequency within that literal is an optional property associated
 * with each term identifier, as is the computed weight for the token in the
 * term.
 * <p>
 * Note: Documents should be tokenized using an {@link Analyzer} appropriate for
 * their declared language code (if any). However, once tokenized, the language
 * code is discarded and we perform search purely on the Unicode sort keys
 * resulting from the extracted tokens.
 * <h2>Scale-out</h2>
 * <p>
 * Because the first component in the key is the token, both updates (when
 * indexing document) and queries (reading against different tokens) will be
 * scattered across shards. Therefore it is not necessary to register a split
 * handler for the full text index.
 * 
 * @todo The key for the terms index is {term,docId,fieldId}. Since the data are
 *       not pre-aggregated by {docId,fieldId} we can not easily remove only
 *       those tuples corresponding to some document (or some field of some
 *       document).
 *       <p>
 *       In order to removal of the fields for a document we need to know either
 *       which fields were indexed for the document and the tokens found in
 *       those fields and then scatter the removal request (additional space
 *       requirements) or we need to flood a delete procedure across the terms
 *       index (expensive).
 * 
 * @todo provide M/R alternatives for indexing or computing/updating global
 *       weights.
 * 
 * @todo Consider model in which fields are declared and then a "Document" is
 *       indexed. This lets us encapsulate the "driver" for indexing. The
 *       "field" can be a String or a Reader, etc.
 *       <p>
 *       Note that lucene handles declaration of the data that will be stored
 *       for a field on a per Document basis {none, character offsets, character
 *       offsets + token positions}. There is also an option to store the term
 *       vector itself. Finally, there are options to store, compress+store, or
 *       not store the field value. You can also choose {None, IndexTokenized,
 *       IndexUntokenized} and an option dealing with norms.
 * 
 * @todo lucene {@link Analyzer}s may be problematic. For example, it is
 *       difficult to tokenize numbers. consider replacing the lucene
 *       analyzer/tokenizer with our own stuff. this might help with
 *       tokenization of numbers, etc. and with tokenization of native html or
 *       xml with intact offsets.
 * 
 * @todo lucene analyzers will strip stopwords by default. There should be a
 *       configuration option to strip out stopwords and another to enable
 *       stemming. how we do that should depend on the language family.
 *       Likewise, there should be support for language family specific stopword
 *       lists and language family specific exclusions.
 * 
 * @todo support more term weighting schemes and make them easy to configure.
 * 
 * @param <V>
 *            The generic type of the document identifier.
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FullTextIndex<V extends Comparable<V>> extends AbstractRelation {

    final private static transient Logger log = Logger
            .getLogger(FullTextIndex.class);

    /**
     * The backing index.
     */
    volatile private IIndex ndx;

    /**
     * The index used to associate term identifiers with tokens parsed from
     * documents.
     */
    public IIndex getIndex() {
        
        if(ndx == null) {

            synchronized (this) {

                ndx = getIndex(getNamespace() + "." + NAME_SEARCH);

                if (ndx == null)
                    throw new IllegalStateException();
                
            }
            
        }
        
        return ndx;
        
    }
    
    /**
     * Options understood by the {@link FullTextIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Options {
       
        /**
         * <code>indexer.overwrite</code> - boolean option (default
         * <code>true</code>) controls the behavior when a write is requested
         * on the index and the {term,doc,field} tuple which forms the key is
         * already present in the index. When <code>true</code>, the new
         * value will be written on the index. When <code>false</code>, the
         * existing value will be retained. This option is an optimization which
         * makes sense when the corpus (a) only grows; and (b) the content of
         * the documents in the corpus never changes. For example, this is true
         * for an RDF database since the set of terms only grows and each term
         * is immutable.
         */
        String OVERWRITE = FullTextIndex.class.getName() + ".overwrite";

        String DEFAULT_OVERWRITE = "true";

        /**
         * Specify the collator {@link StrengthEnum strength} for the full-text
         * index (default {@value StrengthEnum#Primary}).
         * <p>
         * Note: {@link StrengthEnum#Primary} is generally what you want for a
         * full text index as search will consider tokens which differ in case
         * and other subtle features to be the same token (a 'match').
         * 
         * @see KeyBuilder.Options#STRENGTH
         */
        String INDEXER_COLLATOR_STRENGTH = FullTextIndex.class.getName()
                + ".collator.strength";

        String DEFAULT_INDEXER_COLLATOR_STRENGTH = StrengthEnum.Primary.toString();

        /**
         * The maximum time in milliseconds that the search engine will await
         * completion of the tasks reading on each of the query terms (default
         * {@value #DEFAULT_INDEXER_TIMEOUT}). A value of ZERO (0) means NO
         * timeout and is equivalent to a value of {@link Long#MAX_VALUE}. If
         * the timeout expires before all tasks complete then the search results
         * will only reflect partial information.
         */
        String INDEXER_TIMEOUT = FullTextIndex.class.getName() + ".timeout";

        String DEFAULT_INDEXER_TIMEOUT = "0";

        /**
         * When <code>true</code>, the <code>fieldId</code> is stored as part of
         * the key (default {@value #DEFAULT_FIELDS_ENABLED}). When
         * <code>false</code>, each key will be four bytes shorter. Applications
         * which do not use <code>fieldId</code> are should disable it when
         * creating the {@link FullTextIndex}.
         */
        String FIELDS_ENABLED = FullTextIndex.class.getName()
                + ".fieldsEnabled";

        String DEFAULT_FIELDS_ENABLED = "false";

//        /**
//         * When <code>true</code>, the <code>localTermWeight</code> is stored
//         * using double-precision. When <code>false</code>, it is stored using
//         * single-precision.
//         */
//        String DOUBLE_PRECISION = FullTextIndex.class.getName()
//                + ".doublePrecision";
//
//        String DEFAULT_DOUBLE_PRECISION = "false";
        
        /**
         * The name of the {@link IAnalyzerFactory} class which will be used to
         * obtain analyzers when tokenizing documents and queries (default
         * {@value #DEFAULT_ANALYZER_FACTORY_CLASS}).  The specified class MUST
         * implement {@link IAnalyzerFactory} and MUST have a constructor with
         * the following signature:
         * <pre>
         * public MyAnalyzerFactory(FullTextIndexer indexer)
         * </pre>
         */
        String ANALYZER_FACTORY_CLASS = FullTextIndex.class.getName()
                + ".analyzerFactoryClass";

        String DEFAULT_ANALYZER_FACTORY_CLASS = DefaultAnalyzerFactory.class.getName();
        
        /**
        * We keep a small hit cache based on search parameters: search string +
        * prefixMatch + matchAllTerms.  This defines the size of that cache.
        * The value should remain small.
        */
        String HIT_CACHE_SIZE = FullTextIndex.class.getName()
        		+ ".hitCacheSize";
        
        String DEFAULT_HIT_CACHE_SIZE = "10";
        
       /**
        * We keep a small hit cache based on search parameters: search string +
        * prefixMatch + matchAllTerms. This defines the timeout for values in
        * that cache (in milliseconds). The value should remain small.
        */
        String HIT_CACHE_TIMEOUT_MILLIS = FullTextIndex.class.getName()
                + ".hitCacheTimeoutMillis";
        
        /**
         * Default is 1 minute.
         */
        String DEFAULT_HIT_CACHE_TIMEOUT_MILLIS =
               String.valueOf(TimeUnit.MINUTES.toMillis(1));
        
    }
    
    /**
     * @see Options#OVERWRITE
     */
    private final boolean overwrite;
    
    /**
     * Return the value configured by the {@link Options#OVERWRITE} property.
     */
    public boolean isOverwrite() {
        
        return overwrite;
        
    }
    
    /**
     * @see Options#INDEXER_TIMEOUT
     */
    private final long timeout;
    
//    /**
//     * @see Options#FIELDS_ENABLED
//     */
//    private final boolean fieldsEnabled;
//
//    /**
//     * @see Options#DOUBLE_PRECISION
//     */
//    private final boolean doublePrecision;
//
//    /**
//     * Return the value configured by the {@link Options#FIELDS_ENABLED}
//     * property.
//     */
//    public boolean isFieldsEnabled() {
//        
//        return fieldsEnabled;
//        
//    }

    /**
     * @see Options#ANALYZER_FACTORY_CLASS
     */
    private final IAnalyzerFactory analyzerFactory;
    
    /**
     * See {@link Options#HIT_CACHE_SIZE}.
     */
    private final int hitCacheSize;

    /**
     * See {@link Options#HIT_CACHE_TIMEOUT_MILLIS}.
     */
    private final long hitCacheTimeoutMillis;

    /**
     * See {@link Options#HIT_CACHE_SIZE}.
     */
    private final ConcurrentWeakValueCacheWithTimeout<FullTextQuery, Hit<V>[]> cache;

//    /**
//     * @see Options#DOCID_FACTORY_CLASS
//     */
//    private final IKeyBuilderExtension<V> docIdFactory;
    
//    /**
//     * The concrete {@link IRecordBuilder} instance.
//     */
//    private final IRecordBuilder<V> recordBuilder;
//    
//    /**
//     * Return the object responsible for encoding and decoding the tuples
//     * in the full text index.
//     */
//    public final IRecordBuilder<V> getRecordBuilder() {
//        
//        return recordBuilder;
//        
//    }
    
    /**
     * The basename of the search index.
     */
    public static final transient String NAME_SEARCH = "search";
    
    /**
     * <code>true</code> unless {{@link #getTimestamp()} is {@link ITx#UNISOLATED}.
     */
    final public boolean isReadOnly() {
        
        return TimestampUtility.isReadOnly(getTimestamp());
        
    }

//    protected void assertWritable() {
//        
//        if(isReadOnly()) {
//            
//            throw new IllegalStateException("READ_ONLY");
//            
//        }
//        
//    }

    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @param client
     *            The client. Configuration information is obtained from the
     *            client. See {@link Options}.
     * 
     * @see Options
     */
    public FullTextIndex(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);
        
        {
            
            overwrite = Boolean.parseBoolean(properties.getProperty(
                    Options.OVERWRITE, Options.DEFAULT_OVERWRITE));

            if (log.isInfoEnabled())
                log.info(Options.OVERWRITE + "=" + overwrite);

        }

        {

            timeout = Long.parseLong(properties.getProperty(
                    Options.INDEXER_TIMEOUT, Options.DEFAULT_INDEXER_TIMEOUT));

            if (log.isInfoEnabled())
                log.info(Options.INDEXER_TIMEOUT + "=" + timeout);

        }

//        {
//
//            fieldsEnabled = Boolean.parseBoolean(properties.getProperty(
//                    Options.FIELDS_ENABLED, Options.DEFAULT_FIELDS_ENABLED));
//
//            if (log.isInfoEnabled())
//                log.info(Options.FIELDS_ENABLED + "=" + fieldsEnabled);
//
//        }
//
//        {
//
//            doublePrecision = Boolean
//                    .parseBoolean(properties.getProperty(
//                            Options.DOUBLE_PRECISION,
//                            Options.DEFAULT_DOUBLE_PRECISION));
//
//            if (log.isInfoEnabled())
//                log.info(Options.DOUBLE_PRECISION + "=" + doublePrecision);
//
//        }

        {

            hitCacheSize = Integer.parseInt(properties.getProperty(
                    Options.HIT_CACHE_SIZE, Options.DEFAULT_HIT_CACHE_SIZE));

            if (log.isInfoEnabled())
                log.info(Options.HIT_CACHE_SIZE + "=" + hitCacheSize);

        }

        {

            hitCacheTimeoutMillis = Long.parseLong(properties.getProperty(
                    Options.HIT_CACHE_TIMEOUT_MILLIS,
                    Options.DEFAULT_HIT_CACHE_TIMEOUT_MILLIS));

            if (log.isInfoEnabled())
                log.info(Options.HIT_CACHE_TIMEOUT_MILLIS + "=" + hitCacheTimeoutMillis);

        }

        this.cache =
               new ConcurrentWeakValueCacheWithTimeout<FullTextQuery, Hit<V>[]>(
                               hitCacheSize, hitCacheTimeoutMillis);

        {

            final String className = getProperty(
                    Options.ANALYZER_FACTORY_CLASS,
                    Options.DEFAULT_ANALYZER_FACTORY_CLASS);

            if (log.isInfoEnabled())
                log.info(Options.ANALYZER_FACTORY_CLASS + "=" + className);

            final Class<IAnalyzerFactory> cls;
            try {
                cls = (Class<IAnalyzerFactory>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Bad option: "
                        + Options.ANALYZER_FACTORY_CLASS, e);
            }

            if (!IAnalyzerFactory.class.isAssignableFrom(cls)) {
                throw new RuntimeException(Options.ANALYZER_FACTORY_CLASS
                        + ": Must extend: " + IAnalyzerFactory.class.getName());
            }

            try {

                final Constructor<? extends IAnalyzerFactory> ctor = cls
                        .getConstructor(new Class[] { FullTextIndex.class });

                // save reference.
                analyzerFactory = ctor.newInstance(new Object[] { this });

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }

        /*
         * Note: defer resolution of the index.
         */
//        // resolve index (might not exist, in which case this will be null).
//        ndx = getIndex(getNamespace()+"."+NAME_SEARCH);
        
    }

    /**
     * Conditionally registers the necessary index(s).
     * 
     * @throws IllegalStateException
     *             if the client does not have write access.
     * 
     * @todo this is not using {@link #acquireExclusiveLock()} since I generally
     *       allocate the text index inside of another relation and
     *       {@link #acquireExclusiveLock()} is not reentrant for zookeeper.
     */
    /*
     * Note: BigdataRDFFullTextIndex overrides this method to setup IV support.
     */
    @Override
    public void create() {

        assertWritable();
        
        final String name = getNamespace() + "."+NAME_SEARCH;

        final IIndexManager indexManager = getIndexManager();

//        final IResourceLock resourceLock = acquireExclusiveLock();
//
//        try {

            /*
             * Register a tuple serializer that knows how to unpack the values and
             * how to extract the bytes corresponding to the encoded text (they can
             * not be decoded) from key and how to extract the document and field
             * identifiers from the key.
             */
            final Properties p = getProperties();
            
            final IndexMetadata indexMetadata = new IndexMetadata(indexManager,
                    p, name, UUID.randomUUID(), IndexTypeEnum.BTree);

            /*
             * Override the collator strength property to use the configured
             * value or the default for the text indexer rather than the
             * standard default. This is done because you typically want to
             * recognize only Primary differences for text search while you
             * often want to recognize more differences when generating keys for
             * a B+Tree.
             * 
             * Note: The choice of the language and country for the collator
             * should not matter much for this purpose since the total ordering
             * is not used except to scan all entries for a given term, so the
             * relative ordering between terms does not matter.
             */
            final IKeyBuilderFactory keyBuilderFactory;
            {
            
                final Properties tmp = new Properties(p);

                tmp.setProperty(KeyBuilder.Options.STRENGTH, p.getProperty(
                    Options.INDEXER_COLLATOR_STRENGTH,
                    Options.DEFAULT_INDEXER_COLLATOR_STRENGTH));

                keyBuilderFactory = new DefaultKeyBuilderFactory(tmp);

            }
    
            final boolean fieldsEnabled = Boolean.parseBoolean(p
                    .getProperty(Options.FIELDS_ENABLED,
                            Options.DEFAULT_FIELDS_ENABLED));
    
            if (log.isInfoEnabled())
                log.info(Options.FIELDS_ENABLED + "=" + fieldsEnabled);
    
//            final boolean doublePrecision = Boolean.parseBoolean(p
//                    .getProperty(Options.DOUBLE_PRECISION,
//                            Options.DEFAULT_DOUBLE_PRECISION));
//    
//            if (log.isInfoEnabled())
//                log.info(Options.DOUBLE_PRECISION + "=" + doublePrecision);

            indexMetadata.setTupleSerializer(new FullTextIndexTupleSerializer<V>(
                    keyBuilderFactory,//
                    DefaultTupleSerializer.getDefaultLeafKeysCoder(),//
                    EmptyRabaValueCoder.INSTANCE,//
                    fieldsEnabled//
            ));
            
            indexManager.registerIndex(indexMetadata);

            if (log.isInfoEnabled())
                log.info("Registered new text index: name=" + name);

            /*
             * Note: defer resolution of the index.
             */
//            ndx = getIndex(name);

//        } finally {
//
//            unlock(resourceLock);
//
//        }
        
    }

    public void destroy() {

        if (log.isInfoEnabled())
            log.info("");

        assertWritable();

        final IIndexManager indexManager = getIndexManager();

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            indexManager.dropIndex(getNamespace() +"."+ NAME_SEARCH);

        } finally {

            unlock(resourceLock);

        }
        
    }

    /**
     * Return the token analyzer to be used for the given language code.
     * 
     * @param languageCode
     *            The language code or <code>null</code> to use the default
     *            {@link Locale}.
     * 
     * @return The token analyzer best suited to the indicated language family.
     */
    protected Analyzer getAnalyzer(final String languageCode, final boolean filterStopwords) {

        return analyzerFactory.getAnalyzer(languageCode, filterStopwords);
        
    }

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured to
     * support full text indexing and search.
     * 
     * @see Options#INDEXER_COLLATOR_STRENGTH
     */
    protected final IKeyBuilder getKeyBuilder() {

        return getIndex().getIndexMetadata().getKeyBuilder();
            
    }

    /**
     * See {@link #index(TokenBuffer, long, int, String, Reader, boolean)}.
     * <p>
     * Uses a default filterStopwords value of <code>true</code>.
     */
    public void index(final TokenBuffer<V> buffer, final V docId,
            final int fieldId, final String languageCode, final Reader r) {
    	
    	index(buffer, docId, fieldId, languageCode, r, true/* filterStopwords */);
    	
    }
    
    /**
     * Index a field in a document.
     * <p>
     * Note: This method does NOT force a write on the indices. If the <i>buffer</i>
     * overflows, then there will be an index write. Once the caller is done
     * indexing, they MUST invoke {@link TokenBuffer#flush()} to force any data
     * remaining in their <i>buffer</i> to the indices.
     * <p>
     * Note: If a document is pre-existing, then the existing data for that
     * document MUST be removed unless you know that the fields to be found in
     * the will not have changed (they may have different contents, but the same
     * fields exist in the old and new versions of the document).
     * 
     * @param buffer
     *            Used to buffer writes onto the text index.
     * @param docId
     *            The document identifier.
     * @param fieldId
     *            The field identifier.
     * @param languageCode
     *            The language code -or- <code>null</code> to use the default
     *            {@link Locale}.
     * @param r
     *            A reader on the text to be indexed.
     * @param filterStopwords
     * 			  if true, filter stopwords from the token stream            
     * 
     * @see TokenBuffer#flush()
     */
    public void index(final TokenBuffer<V> buffer, final V docId,
            final int fieldId, final String languageCode, final Reader r,
            final boolean filterStopwords) {

        /*
         * Note: You can invoke this on a read-only index. It is only overflow
         * of the TokenBuffer that requires a writable index. Overflow itself
         * will only occur on {document,field} tuple boundaries, so it will
         * never overflow when indexing a search query.
         */
//        assertWritable();
        
        int n = 0;
        
        // tokenize (note: docId,fieldId are not on the tokenStream, but the field could be).
        final TokenStream tokenStream = getTokenStream(languageCode, r,
                filterStopwords);

        
        try {

            tokenStream.reset();   

            while (tokenStream.incrementToken()) {
                
                final CharTermAttribute term = tokenStream
                        .getAttribute(CharTermAttribute.class);
                
                buffer.add(docId, fieldId, term.toString());

                n++;

            }
            
            tokenStream.end();
            
            tokenStream.close();

        } catch (IOException ioe) {
            
            throw new RuntimeException(ioe);
            
        }

        if (log.isInfoEnabled())
            log.info("Indexed " + n + " tokens: docId=" + docId + ", fieldId="
                    + fieldId);

    }

    /**
     * Tokenize text using an {@link Analyzer} that is appropriate to the
     * specified language family.
     * 
     * @param languageCode
     *            The language code -or- <code>null</code> to use the default
     *            {@link Locale}).
     * 
     * @param r
     *            A reader on the text to be indexed.
     *            
     * @param filterStopwords
     * 			  if true, filter stopwords from the token stream            
     * 
     * @return The extracted token stream.
     */
    protected TokenStream getTokenStream(final String languageCode, 
    		final Reader r, final boolean filterStopwords) {

        /*
         * Note: This is stripping out stopwords by default.
         * 
         * @todo is it using a language family specific stopword list?
         */
        final Analyzer a = getAnalyzer(languageCode, filterStopwords);
        
        TokenStream tokenStream;
		tokenStream = a.tokenStream(null/* @todo field? */, r);

		// force to lower case.
		tokenStream = new LowerCaseFilter(tokenStream);

		return tokenStream;
    }

    /**
     * Performs a full text search against indexed documents returning a hit
     * list.
     * <p>
     * The basic algorithm computes cosine between the term-frequency vector of
     * the query and the indexed "documents". The cosine may be directly
     * interpreted as the "relevance" of a "document" to the query. The query
     * and document term-frequency vectors are normalized, so the cosine values
     * are bounded in [0.0:1.0]. The higher the cosine the more relevant the
     * document is to the query. A cosine of less than .4 is rarely of any
     * interest.
     * <p>
     * The implementation creates and runs a set parallel tasks, one for each
     * distinct token found in the query, and waits for those tasks to complete
     * or for a timeout to occur. Each task uses a key-range scan on the terms
     * index, collecting metadata for the matching "documents" and aggregating
     * it on a "hit" for that document. Since the tasks run concurrently, there
     * are concurrent writers on the "hits". On a timeout, the remaining tasks
     * are interrupted.
     * <p>
     * The collection of hits is scored and hits that fail a threshold are
     * discarded. The remaining hits are placed into a total order and the
     * caller is returned an iterator which can read from that order. If the
     * operation is interrupted, then only those {@link IHit}s that have already
     * been computed will be returned.
     * 
     * @param query
     *            The query (it will be parsed into tokens).
     * @param languageCode
     *            The language code that should be used when tokenizing the
     *            query -or- <code>null</code> to use the default {@link Locale}
     *            ).
     * @param minCosine
     *            The minimum cosine that will be returned.
     * @param maxCosine
     *            The maximum cosine that will be returned.
	 * @param minRank
	 *            The min rank of the search result.
	 * @param maxRank
	 *            The max rank of the search result.
     * @param prefixMatch
     *            When <code>true</code>, the matches will be on tokens which
     *            include the query tokens as a prefix. This includes exact
     *            matches as a special case when the prefix is the entire token,
     *            but it also allows longer matches. For example,
     *            <code>free</code> will be an exact match on <code>free</code>
     *            but a partial match on <code>freedom</code>. When
     *            <code>false</code>, only exact matches will be made.
     * @param matchAllTerms
     * 			  if true, return only hits that match all search terms
     * @param timeout
     *            The timeout -or- ZERO (0) for NO timeout (this is equivalent
     *            to using {@link Long#MAX_VALUE}).
     * @param unit
     *            The unit in which the timeout is expressed.
     * 
     * @return The hit list.
     * 
     * @todo Allow search within field(s). This will be a filter on the range
     *       iterator that is sent to the data service such that the search
     *       terms are visited only when they occur in the matching field(s).
     */
    public Hiterator<Hit<V>> search(final FullTextQuery query) {
        
		final Hit<V>[] a = _search(query);
    	
        return new Hiterator<Hit<V>>(a);

    }
    
    /**
     * Perform a range count on a full text query.
     */
    public int count(final FullTextQuery query) {
    	
    	if (cache.containsKey(query)) {
    		
        	if (log.isInfoEnabled())
        		log.info("found hits in cache");
        	
    		return cache.get(query).length;
    		
        } else {
        	
        	if (log.isInfoEnabled())
        		log.info("did not find hits in cache");
        	
        }
    	
        // tokenize the query.
        final TermFrequencyData<V> qdata = tokenize(query);
        
        // No terms after stopword extraction
        if (qdata == null) {
        	
        	cache.put(query, new Hit[] {});
        	
        	return 0;
        			
        }
        
        /*
         * We can run an optimized version of this (just a quick range count)
         * but only if the caller does not care about exact match and has
         * not specified a regex.
         */
        if (qdata.distinctTermCount() == 1 &&
        		!query.isMatchExact() && query.getMatchRegex() == null) {
        	
        	final boolean prefixMatch = query.isPrefixMatch();
        	
        	final Map.Entry<String, ITermMetadata> e = qdata.getSingletonEntry();
        	
            final String termText = e.getKey();
        	
            final ITermMetadata md = e.getValue();

            final CountIndexTask<V> task1 = new CountIndexTask<V>(termText, 0, 1, 
            		prefixMatch, md.getLocalTermWeight(), this);
            
            return (int) task1.getRangeCount();
        	
        } else {
	        
			final Hit<V>[] a = _search(query);
	    	
			return a.length;
			
        }
		
    }
    
    protected TermFrequencyData<V> tokenize(final FullTextQuery query) {
    	
		final String q = query.getQuery();
		final String languageCode = query.getLanguageCode(); 
		final boolean prefixMatch = query.isPrefixMatch();

        // tokenize the query.
        final TermFrequencyData<V> qdata;
        {
            
    		final TokenBuffer<V> buffer = new TokenBuffer<V>(1, this);

            /*
             * If we are using prefix match ('*' operator) then we don't want to
             * filter stopwords from the search query.
             */
            final boolean filterStopwords = !prefixMatch;

            index(buffer, //
                    null, // docId // was Long.MIN_VALUE
                    Integer.MIN_VALUE, // fieldId
                    languageCode,//
                    new StringReader(q), //
                    filterStopwords//
            );

            if (buffer.size() == 0) {

                /*
                 * There were no terms after stopword extration.
                 */

                log.warn("No terms after stopword extraction: query=" + query);

                return null; 

            }
            
            qdata = buffer.get(0);
            
            qdata.normalize();
            
        }
        
        return qdata;

    }
    
    public Hit<V>[] _search(final FullTextQuery query) {
    	
		final String queryStr = query.getQuery();
		final String languageCode = query.getLanguageCode(); 
		final boolean prefixMatch = query.isPrefixMatch();
        final double minCosine = query.getMinCosine();
        final double maxCosine = query.getMaxCosine();
        final int minRank = query.getMinRank();
        final int maxRank = query.getMaxRank(); 
        final boolean matchAllTerms = query.isMatchAllTerms();
        final boolean matchExact = query.isMatchExact();
        final String regex = query.getMatchRegex();
        long timeout = query.getTimeout();
        final TimeUnit unit = query.getTimeUnit(); 

        final long begin = System.currentTimeMillis();
        
//        if (languageCode == null)
//            throw new IllegalArgumentException();

        if (queryStr == null)
            throw new IllegalArgumentException();
        
        if (minCosine < 0d || minCosine > 1d)
            throw new IllegalArgumentException();

        if (minRank <= 0 || maxRank <= 0)
            throw new IllegalArgumentException();

        if (minRank > maxRank)
            throw new IllegalArgumentException();

        if (timeout < 0L)
            throw new IllegalArgumentException();
        
        if (unit == null)
            throw new IllegalArgumentException();
        
        if (log.isInfoEnabled())
            log.info("languageCode=[" + languageCode + "], text=[" + queryStr
                    + "], minCosine=" + minCosine 
                    + ", maxCosine=" + maxCosine
                    + ", minRank=" + minRank
                    + ", maxRank=" + maxRank
                    + ", matchAllTerms=" + matchAllTerms
                    + ", prefixMatch=" + prefixMatch
                    + ", timeout=" + timeout + ", unit=" + unit);

        if (timeout == 0L) {

            // treat ZERO as equivalent to MAX_LONG.
            timeout = Long.MAX_VALUE;
            
        }
        
        final FullTextQuery cacheKey = query;
        
        Hit<V>[] a;
        
        if (cache.containsKey(cacheKey)) {
        	
        	if (log.isInfoEnabled())
        		log.info("found hits in cache");
        	
        	a = cache.get(cacheKey);
        	
        } else {
        	
        	if (log.isInfoEnabled())
        		log.info("did not find hits in cache");
        	
            // tokenize the query.
            final TermFrequencyData<V> qdata = tokenize(query);
            
            // No terms after stopword extraction
            if (qdata == null) {
            	
            	cache.put(cacheKey, a = new Hit[] {});
            	
            	return a;
            			
            }
            
            a = executeQuery(qdata, prefixMatch, timeout, unit);
            
	        if (a.length == 0) {
	        	
	            log.info("No hits: languageCode=[" + languageCode + "], query=["
	                    + queryStr + "]");
	            
	            cache.put(cacheKey, a);
	            
	            return a; 
	            
	        }
	
	        /*
	         * If match all is specified, remove any hits with a term count less
	         * than the number of search tokens.  It's also an optimization to
	         * run the pruning if we're going to do matchExact.
	         */
	        if ((matchAllTerms || matchExact) && qdata.distinctTermCount() > 1) {
	        	
		        final int nterms = qdata.terms.size();
		        
		        if (log.isInfoEnabled()) {
		        	log.info("matchAll=true, nterms=" + nterms);
		        	log.info("size before: " + a.length);
		        }
		        
	        	final Hit<V>[] tmp = new Hit[a.length];
	        	
	        	int i = 0;
	        	for (Hit<V> hit : a) {
	        		
	        		if (hit.getTermCount() == nterms) {
	        			tmp[i++] = hit;
	        		}
	        		
	        	}
	        	
	        	if (log.isDebugEnabled()) {
	        		log.debug(i);
	        	}
	        	
	        	if (i < a.length) {
	        		
	        		a = new Hit[i];
	        		System.arraycopy(tmp, 0, a, 0, i);
	        		
	        	}
	        	
	        }
	        
	        /*
	         * Delegate match exact to subclasses.
	         */
	        if (matchExact) {
	        	
	        	a = matchExact(a, queryStr);
	        	
	        }
	        
	        if (a.length == 0) {
	        	
	            log.warn("No hits after matchAllTerms pruning: languageCode=[" + languageCode + "], query=["
	                    + queryStr + "]");
	            
	            cache.put(cacheKey, a);
	            
	            return a; 
	            
	        }
	        
	        /*
	         * Do regex matching.
	         */
	        if (regex != null) {
	        	
	        	final Pattern pattern = Pattern.compile(regex);//, Pattern.CASE_INSENSITIVE);
	        	
	        	if (log.isDebugEnabled()) {
	        		log.debug("hits before regex: " + a.length);
	        	}
	        	
	        	a = applyRegex(a, pattern);
	        	
	        	if (log.isDebugEnabled()) {
	        		log.debug("hits after regex: " + a.length);
	        	}
	        	
	        }
	        
	        if (a.length == 0) {
	        	
	            log.warn("No hits after regex pruning: languageCode=[" + languageCode + "], query=["
	                    + queryStr + "], regex=[" + regex + "]");
	            
	            cache.put(cacheKey, a);
	            
	            return a; 
	            
	        }
	
	        /*
	         * Rank order the hits by relevance.
	         * 
	         * @todo consider moving documents through a succession of N pools where
	         * N is the #of distinct terms in the query. The read tasks would halt
	         * if the size of the pool for N terms reached maxRank. This might (or
	         * might not) help with triage since we could process hits by pool and
	         * only compute the cosines for one pool at a time until we had enough
	         * hits.
	         */
	        
	        if (log.isInfoEnabled())
	            log.info("Rank ordering "+a.length+" hits by relevance");
	        
	        final long start = System.currentTimeMillis();
	        
	        Arrays.sort(a);
	        
	        if (log.isInfoEnabled()) {
	        	final long sortTime = System.currentTimeMillis() - start;
	        	log.info("sort time: " + sortTime);
	        }
	        
	        for (int i = 0; i < a.length; i++) {
	        	a[i].setRank(i+1);
	        }
	        
	        cache.put(cacheKey, a);
        
        }
        
        /*
         * Take a slice of the hits based on min/max cosine and min/max rank.
         */
        a = slice(query, a);
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        if (log.isInfoEnabled())
            log.info("Done: " + a.length + " hits in " + elapsed + "ms");

        return a;
        
    }
    
    protected Hit<V>[] slice(final FullTextQuery query, Hit<V>[] a) {
    	
        final double minCosine = query.getMinCosine();
        final double maxCosine = query.getMaxCosine();
        final int minRank = query.getMinRank();
        final int maxRank = query.getMaxRank();
        
//        if (log.isDebugEnabled()) {
//        	log.debug("before min/max cosine/rank pruning:");
//        	for (Hit<V> h : a)
//        		log.debug(h);
//        }

        /*
         * If maxCosine is specified, prune the hits that are above the max
         */
        if (maxCosine < 1.0d) {

        	// find the first occurrence of a hit that is <= maxCosine
        	int i = 0;
        	for (Hit<V> h : a) {
        		if (h.getCosine() <= maxCosine)
        			break;
        		i++;
        	}
        	
        	// no hits with relevance less than maxCosine
        	if (i == a.length) {
        		
        		return new Hit[] {};
        		
        	} else {
        	
	        	// copy the hits from that first occurrence to the end
	        	final Hit<V>[] tmp = new Hit[a.length - i];
	        	System.arraycopy(a, i, tmp, 0, tmp.length);
	        	
	        	a = tmp;
	        	
        	}
        	
        }

        /*
         * If minCosine is specified, prune the hits that are below the min
         */
        if (minCosine > 0.0d) {

        	// find the first occurrence of a hit that is < minCosine
        	int i = 0;
        	for (Hit<V> h : a) {
        		if (h.getCosine() < minCosine)
        			break;
        		i++;
        	}
        	
        	// no hits with relevance greater than minCosine
        	if (i == 0) {
        		
        		return new Hit[] {};
        		
        	} else if (i < a.length) {
        	
	        	// copy the hits from 0 up to that first occurrence
	        	final Hit<V>[] tmp = new Hit[i];
	        	System.arraycopy(a, 0, tmp, 0, tmp.length);
	        	
	        	a = tmp;
	        	
        	}
        	
        }

        // exactly one hit
        if (minRank > 0 && minRank == maxRank) {
        	
        	if (minRank > a.length) {
        		
            	// out of range
        		return new Hit[] {};
        		
        	} else {

        		// in range
        		return new Hit[] { a[minRank-1] };
	        	
        	}
        	
        }
        
        /*
         * If minRank is specified, prune the hits that rank higher than the min
         */
        if (minRank > 1) {

        	// no hits above minRank
        	if (minRank > a.length) {
        		
        		return new Hit[] {};
        		
        	} else {
        	
	        	// copy the hits from the minRank to the end
	        	final Hit<V>[] tmp = new Hit[a.length - (minRank-1)];
	        	System.arraycopy(a, minRank-1, tmp, 0, tmp.length);
	        	
	        	a = tmp;
	        	
        	}
        	
        }

        final int newMax = maxRank-minRank+1;
        
        if (log.isDebugEnabled())
        	log.debug("new max rank: " + newMax);
        
        /*
         * If maxRank is specified, prune the hits that rank lower than the max
         */
        if (newMax < a.length) {

        	// copy the hits from the minRank to the end
        	final Hit<V>[] tmp = new Hit[newMax];
        	System.arraycopy(a, 0, tmp, 0, tmp.length);
        	
        	a = tmp;
        	
        }

        return a;
        
    }
    
    protected Hit<V>[] executeQuery(final TermFrequencyData<V> qdata,
    		final boolean prefixMatch, final long timeout, final TimeUnit unit) {
    	
        final IHitCollector<V> hits;
        
        if (qdata.distinctTermCount() == 1) {
        	
        	final Map.Entry<String, ITermMetadata> e = qdata.getSingletonEntry();
        	
            final String termText = e.getKey();
        	
            final ITermMetadata md = e.getValue();

            final CountIndexTask<V> task1 = new CountIndexTask<V>(termText, 0, 1,
            		prefixMatch, md.getLocalTermWeight(), this);
            
            hits = new SingleTokenHitCollector<V>(task1);
        	
        } else {
        	
            final List<CountIndexTask<V>> tasks = new ArrayList<CountIndexTask<V>>(
                    qdata.distinctTermCount());

            int i = 0;
            for (Map.Entry<String, ITermMetadata> e : qdata.terms.entrySet()) {

                final String termText = e.getKey();

                final ITermMetadata md = e.getValue();

                tasks.add(new CountIndexTask<V>(termText, i++, qdata.terms.size(), 
                		prefixMatch, md.getLocalTermWeight(), this));

            }
            
            hits = new MultiTokenHitCollector<V>(tasks);
        	
        }
        
        // run the queries.
        {

            final List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(
                    qdata.distinctTermCount());

            int i = 0;
            for (Map.Entry<String, ITermMetadata> e : qdata.terms.entrySet()) {

                final String termText = e.getKey();

                final ITermMetadata md = e.getValue();

                tasks.add(new ReadIndexTask<V>(termText, i++, qdata.terms.size(),
                		prefixMatch, md.getLocalTermWeight(), this, hits));

            }

            final ExecutionHelper<Object> executionHelper = new ExecutionHelper<Object>(
                    getExecutorService(), timeout, unit);

            try {

    	        final long start = System.currentTimeMillis();
    	        
                executionHelper.submitTasks(tasks);
                
                if (log.isInfoEnabled()) {
	                final long readTime = System.currentTimeMillis() - start;
	                log.info("read time: " + readTime);
                }
                
            } catch (InterruptedException ex) {

            	if (log.isInfoEnabled()) {
	                // TODO Should we wrap and toss this interrupt instead?
	                log.info("Interrupted - only partial results will be returned.");
            	}
                
            	/*
            	 * Yes, let's toss it.  We were getting into a situation
            	 * where the ExecutionHelper above received an interrupt
            	 * but we still went through the heavy-weight filtering
            	 * operations below (matchExact or matchRegex).
            	 */
            	throw new RuntimeException(ex);

            } catch (ExecutionException ex) {

                throw new RuntimeException(ex);
                
            }

        }
        
        return hits.getHits();

    }
    
    /**
     * Subclasses can override this method to do exact match processing.  This
     * involves materializing the hits into their original text values and
     * checking for the query string in the materialized value.  Not possible
     * from the base class.  The value-centric RDF version can use the
     * lexicon to materialize the hits and check them for exact match.
     */
    protected Hit<V>[] matchExact(final Hit<V>[] hits, final String query) {
    	
    	throw new UnsupportedOperationException();
    	
    }

	/**
	 * Subclasses can override this method to do regex post-processing. This
	 * involves materializing the hits into their original text values and
	 * checking against the regex string in the materialized value. Not possible
	 * from the base class. The value-centric RDF version can use the lexicon to
	 * materialize the hits and check them against the regex.
	 */
    protected Hit<V>[] applyRegex(final Hit<V>[] hits, final Pattern regex) {
    	
    	throw new UnsupportedOperationException();
    	
    }
    
    /*
     * @todo implement the relevant methods.
     */

    @SuppressWarnings("unchecked")
    public long delete(IChunkedOrderedIterator itr) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public long insert(IChunkedOrderedIterator itr) {
        throw new UnsupportedOperationException();
    }

    public Set<String> getIndexNames() {
        throw new UnsupportedOperationException();
    }

    public Iterator getKeyOrders() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public Object newElement(List a,
            IBindingSet bindingSet) {
        throw new UnsupportedOperationException();
    }
    
    public Class<?> getElementClass() {
        throw new UnsupportedOperationException();
    }

    public IKeyOrder getPrimaryKeyOrder() {
        throw new UnsupportedOperationException();
    }

    public IKeyOrder getKeyOrder(IPredicate p) {
        throw new UnsupportedOperationException();
    }

}
