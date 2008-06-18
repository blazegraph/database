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
 * Created on Jan 23, 2008
 */

package com.bigdata.search;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cn.ChineseAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.KeyBuilder.StrengthEnum;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;

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
 *             {sortKey(token), docId, fldId} : {freq?, weight?, sorted(pos)+}
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
 * field identifiers. It is possible to manage the field identifers through a
 * secondary index, but that has no direct bearing on the structure of the full
 * text index itself. Field identifies appear after the token in the key so that
 * queries may be expressed that will be matched against any field in the
 * document. Likewise, field identifiers occur before the document identifier in
 * the key since we always search across documents (the document identifier is
 * always 0L in the search keys). There are many applications for fields: for
 * example, distinct fields may be used for the title, abstract, and full text
 * of a document or for the CDATA section of each distinct element in documents
 * corresponding to some DTD. The application is responsible for recognizing the
 * fields in the document and producing the appropriate token stream, each of
 * which must be tagged by the field.
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
 *             fromKey := token, 0L
 *             toKey   := successor(token), 0L
 * </pre>
 * 
 * and extracting the appropriate token frequency, normalized token weight, or
 * other statistic. When no value is associated with the entry we follow the
 * convention of assuming a token frequency of ONE (1) for each document in
 * which the token appears.
 * <p>
 * Tokenization is informed by the language code for a {@link Literal} (when
 * declared) and by the configured {@link Locale} for the database otherwise. An
 * appropriate {@link Analyzer} is choosen based on the language code or
 * {@link Locale} and the "document" is broken into a token-frequency
 * distribution (alternatively a set of tokens). The same process is used to
 * tokenize queries, and the API allows the caller to specify the language code
 * used to select the {@link Analyzer} to tokenize the query.
 * <p>
 * Once the tokens are formed the language code / {@link Locale} used to produce
 * the token is discarded (it is not represented in the index). The reason for
 * this is that we never utilize the total ordering of the full text index,
 * merely the manner in which it groups tokens that map onto the same Unicode
 * sort key together. Further, we use only a single Unicode collator
 * configuration regardless of the language family in which the token was
 * originally expressed. Unlike the collator used by the terms index (which
 * often is set at IDENTICAL strength), the collector used by the full text
 * index should be choosen such that it makes relatively few distinctions in
 * order to increase recall (e.g., set at PRIMARY strength). Since a total order
 * over the full text index is not critical from the perspective of its IR
 * application, the {@link Locale} for the collator is likewise not critical and
 * PRIMARY strength will produce significantly shorter Unicode sort keys.
 * <p>
 * A map from tokens extracted from {@link Literal}s to the term identifiers of
 * the literals from which those tokens were extracted.
 * 
 * The term frequency within that literal is an optional property associated
 * with each term identifier, as is the computed weight for the token in the
 * term.
 * <p>
 * Note: Documents should be tokenized using an {@link Analyzer} appropriate for
 * their declared language code (if any). However, once tokenized, the language
 * code is discarded and we perform search purely on the Unicode sort keys
 * resulting from the extracted tokens.
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
 * @todo key compression: prefix compression, possibly with hamming codes for
 *       the docId and fieldId.
 * 
 * @todo support more term weighting schemes and make them easy to configure.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FullTextIndex {

    final public Logger log = Logger.getLogger(FullTextIndex.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

//    private final IBigdataFederation fed;
    
    private final long timestamp;
    
    /** The namespace for the indices maintained by this class (from the ctor). */
    private final String namespace;
    
    /**
     * The backing index.
     */
    private IIndex ndx;

    /**
     * The index used to associate term identifiers with tokens parsed from
     * documents.
     */
    public IIndex getIndex() {
        
        return ndx;
        
    }
    
    private final Properties properties;
    
    /**
     * Return an object wrapping the properties used to configured this
     * instance.
     */
    public Properties getProperties() {
        
        return new Properties( properties );
        
    }

    final private IIndexManager indexManager;
    
    final private ExecutorService threadPool;
    
    /**
     * The {@link ExecutorService} that will be used to run queries.
     */
    protected ExecutorService getThreadPool() {
        
        return threadPool;
        
    }

    /**
     * Options understood by the {@link FullTextIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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
        String OVERWRITE = "indexer.overwrite";

        String DEFAULT_OVERWRITE = "true";

        /**
         * Specify the collator {@link StrengthEnum strength} for the full-text
         * index (default {@value StrengthEnum#Primary}).
         * 
         * @see KeyBuilder.Options#STRENGTH
         * 
         * @todo the configuration should probably come from a configuration
         *       properties stored for the full text indexer in the
         *       {@link IBigdataFederation#getGlobalRowStore()}. The main issue
         *       is how you want to encode unicode strings for search, which can
         *       be different than encoding for other purposes.
         * 
         * @todo consider modifying the default system so that defaults can be
         *       made on a per-index, per-application, or per-namespace basis.
         */
        String INDEXER_COLLATOR_STRENGTH = "indexer.collator.strength";

        String DEFAULT_INDEXER_COLLATOR_STRENGTH = StrengthEnum.Primary.toString();

        /**
         * The maximum time in milliseconds that the search engine will await
         * completion of the tasks reading on each of the query terms (default
         * {@value #DEFAULT_INDEXER_TIMEOUT}).
         */
        String INDEXER_TIMEOUT = "indexer.timeout";
        
        String DEFAULT_INDEXER_TIMEOUT = "1000";
        
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
    
    /**
     * <code>true</code> unless {{@link #getTimestamp()} is {@link ITx#UNISOLATED}.
     */
    final public boolean isReadOnly() {
        
        return timestamp != ITx.UNISOLATED;
        
    }

    protected void assertWritable() {
        
        if(isReadOnly()) {
            
            throw new IllegalStateException("READ_ONLY");
            
        }
        
    }
    
    /**
     * @param client
     *            The client. Configuration information is obtained from the
     *            client. See {@link Options}.
     * 
     * @see Options
     * 
     * @todo Customize a {@link ISplitHandler} such that we never split a
     *       {term,doc} tuple?
     * 
     * @todo Make sure that the defaults for the split points (in terms of the
     *       #of entries) are reasonable. The #of entries per split could be
     *       smaller if we know that we are storing more data in the values.
     */
    public FullTextIndex(IBigdataClient client, String namespace) {

        this(client.getProperties(), namespace, client.getFederation(), client
                .getFederation().getThreadPool());

    }
    
    public FullTextIndex(Properties properties, String namespace,
            IIndexManager indexManager, ExecutorService threadPool) {
        
        if (properties == null)
            throw new IllegalArgumentException();
        
        if (namespace == null)
            throw new IllegalArgumentException();
        
        if (indexManager == null)
            throw new IllegalArgumentException();
        
        if (threadPool == null)
            throw new IllegalArgumentException();
        
        this.properties = (Properties) properties.clone();
        
        this.namespace = namespace;
        
        this.indexManager = indexManager;
        
        this.threadPool = threadPool;

        this.timestamp = ITx.UNISOLATED;
        
        // indexer.overwrite
        {
            
            overwrite = Boolean.parseBoolean(properties.getProperty(
                    Options.OVERWRITE, Options.DEFAULT_OVERWRITE));

            log.info(Options.OVERWRITE + "=" + overwrite);

        }

        // indexer.timeout
        {
            
            timeout = Long.parseLong(properties.getProperty(
                    Options.INDEXER_TIMEOUT, Options.DEFAULT_INDEXER_TIMEOUT));

            log.info(Options.INDEXER_TIMEOUT+ "=" + timeout);

        }
        
        setupIndices();
        
    }

    /**
     * Conditionally registers the necessary index(s).
     * 
     * @throws IllegalStateException
     *             if the client does not have write access.
     */
    private void setupIndices() {

        final String name = namespace + "search";

        IIndex ndx = indexManager.getIndex(name, timestamp);

        if (ndx == null) {

            log.warn("No such index: name="+name);
            
            assertWritable();
            
            IndexMetadata indexMetadata = new IndexMetadata(name, UUID
                    .randomUUID());

            /*
             * FIXME register a tuple serializer that knows how to unpack the
             * values and how to extract the bytes corresponding to the encoded
             * text (they can not be decoded) from key and how to extract the
             * document and field identifiers from the key. It should also
             * encapsulate the use of PRIMARY strength for the key builder.
             */
            
            indexManager.registerIndex(indexMetadata);
            
            log.warn("Registered new text index: name="+name);

            ndx = indexManager.getIndex(name, timestamp);
            
        } else {

            log.warn("Re-opened text index: name="+name);

        }

        assert ndx != null;
        
        this.ndx = ndx;

    }

    /**
     * Clears all indexed data.
     * 
     * @todo While this method is synchronized, that only serializes concurent
     *       invocations of this method. When multiple clients have write access
     *       to the {@link FullTextIndex} the caller MUST ensure that no
     *       concurrent client has write access.
     */
    synchronized public void clear() {

        log.info("");

        assertWritable();

        if (ndx instanceof BTree && !ndx.getIndexMetadata().getDeleteMarkers()) {

            ((BTree) ndx).removeAll();

        } else {

            final String name = namespace + "search";

            indexManager.dropIndex(name);

            setupIndices();

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
    protected Analyzer getAnalyzer(String languageCode) {

        Map<String, AnalyzerConstructor> map = getAnalyzers();
        
        AnalyzerConstructor ctor = null;
        
        if(languageCode==null) {
        
            final IKeyBuilder keyBuilder = getKeyBuilder();

            // The configured local for the database.
            final Locale locale = ((KeyBuilder) keyBuilder).getSortKeyGenerator()
                    .getLocale();

            // The analyzer for that locale.
            Analyzer a = getAnalyzer(locale.getLanguage());
            
            if(a != null) return a;
            
            // fall through
            
        } else {
            
            /*
             * Check the declared analyzers. We first check the three letter
             * language code. If we do not have a match there then we check the
             * 2 letter language code.
             */
            
            String code = languageCode;

            if (code.length() > 3) {

                code = code.substring(0, 2);

                ctor = map.get(languageCode);

            }

            if (ctor == null && code.length() > 2) {

                code = code.substring(0, 1);

                ctor = map.get(languageCode);
                
            }
            
        }
        
        if (ctor == null) {

            // request the default analyzer.
            
            ctor = map.get("");
            
            if (ctor == null) {

                throw new IllegalStateException("No entry for empty string?");
                
            }
            
        }

        Analyzer a = ctor.newInstance();
        
        return a;
        
    }
    
    abstract private static class AnalyzerConstructor {
        
        abstract public Analyzer newInstance();
        
    }

    /**
     * A map containing instances of the various kinds of analyzers that we know
     * about.
     * <p>
     * Note: There MUST be an entry under the empty string (""). This entry will
     * be requested when there is no entry for the specified language code.
     */
    private Map<String,AnalyzerConstructor> analyzers;
    
    /**
     * Initializes the various kinds of analyzers that we know about.
     * <p>
     * Note: Each {@link Analyzer} is registered under both the 3 letter and the
     * 2 letter language codes. See <a
     * href="http://www.loc.gov/standards/iso639-2/php/code_list.php">ISO 639-2</a>.
     * 
     * @todo get some informed advice on which {@link Analyzer}s map onto which
     *       language codes.
     * 
     * @todo thread safety? Analyzers produce token processors so maybe there is
     *       no problem here once things are initialized. If so, maybe this
     *       could be static.
     * 
     * @todo configuration. Could be configured by a file containing a class
     *       name and a list of codes that are handled by that class.
     * 
     * @todo strip language code down to 2/3 characters during lookup.
     * 
     * @todo There are a lot of pidgins based on french, english, and other
     *       languages that are not being assigned here.
     */
    synchronized private Map<String,AnalyzerConstructor> getAnalyzers() {
        
        if (analyzers != null) {

            return analyzers;
            
        }

        analyzers = new HashMap<String, AnalyzerConstructor>();

        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new BrazilianAnalyzer();
                }
            };
            analyzers.put("por", a);
            analyzers.put("pt", a);
        }

        /*
         * Claims to handle Chinese. Does single character extraction. Claims to
         * produce smaller indices as a result.
         * 
         * Note: you can not tokenize with the Chinese analyzer and the do
         * search using the CJK analyzer and visa versa.
         * 
         * Note: I have no idea whether this would work for Japanese and Korean
         * as well. I expect so, but no real clue.
         */
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new ChineseAnalyzer();
                }
            };
            analyzers.put("zho", a);
            analyzers.put("chi", a);
            analyzers.put("zh", a);
        }
        
        /*
         * Claims to handle Chinese, Japanese, Korean. Does double character
         * extraction with overlap.
         */
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new CJKAnalyzer();
                }
            };
//            analyzers.put("zho", a);
//            analyzers.put("chi", a);
//            analyzers.put("zh", a);
            analyzers.put("jpn", a);
            analyzers.put("ja", a);
            analyzers.put("jpn", a);
            analyzers.put("kor",a);
            analyzers.put("ko",a);
        }

        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new CzechAnalyzer();
                }
            };
            analyzers.put("ces",a);
            analyzers.put("cze",a);
            analyzers.put("cs",a);
        }

        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new DutchAnalyzer();
                }
            };
            analyzers.put("dut",a);
            analyzers.put("nld",a);
            analyzers.put("nl",a);
        }
        
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new FrenchAnalyzer();
                }
            };
            analyzers.put("fra",a); 
            analyzers.put("fre",a); 
            analyzers.put("fr",a);
        }

        /*
         * Note: There are a lot of language codes for German variants that
         * might be useful here.
         */
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new GermanAnalyzer();
                }
            };
            analyzers.put("deu",a); 
            analyzers.put("ger",a); 
            analyzers.put("de",a);
        }
        
        // Note: ancient greek has a different code (grc).
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new GreekAnalyzer();
                }
            };
            analyzers.put("gre",a); 
            analyzers.put("ell",a); 
            analyzers.put("el",a);
        }        

        // @todo what about other Cyrillic scripts?
        {  
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new RussianAnalyzer();
                }
            };
            analyzers.put("rus",a); 
            analyzers.put("ru",a); 
        }        
        
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new ThaiAnalyzer();
                }
            };
            analyzers.put("tha",a); 
            analyzers.put("th",a); 
        }

        // English
        {
            AnalyzerConstructor a = new AnalyzerConstructor() {
                public Analyzer newInstance() {
                    return new StandardAnalyzer();
                }
            };
            analyzers.put("eng", a);
            analyzers.put("en", a);
            /*
             * Note: There MUST be an entry under the empty string (""). This
             * entry will be requested when there is no entry for the specified
             * language code.
             */
            analyzers.put("", a);
        }

        return analyzers;
        
    }
    
    
    /*
     * thread-local key builder. 
     */

    /**
     * A {@link ThreadLocal} variable providing access to thread-specific
     * instances of a configured {@link IKeyBuilder}.
     * <p>
     * Note: this {@link ThreadLocal} is not static since we need configuration
     * properties from the constructor - those properties can be different for
     * different {@link IBigdataClient}s on the same machine.
     */
    private ThreadLocal<IKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<IKeyBuilder>() {

        protected synchronized IKeyBuilder initialValue() {

            /*
             * Override the collator strength property to use the configured
             * value or the default for the text indexer rather than the
             * standard default. This is done because you typically want to
             * recognize only Primary differences for text search while you
             * often want to recognize more differences when generating keys for
             * a B+Tree.
             */

            Properties properties = getProperties();

            properties.setProperty(KeyBuilder.Options.STRENGTH, properties
                    .getProperty(Options.INDEXER_COLLATOR_STRENGTH,
                            Options.DEFAULT_INDEXER_COLLATOR_STRENGTH));

            /*
             * Note: The choice of the language and country for the collator
             * should not matter much for this purpose since the total ordering
             * is not used except to scan all entries for a given term, so the
             * relative ordering between terms does not matter.
             */
        
            return KeyBuilder.newUnicodeInstance(properties);

        }

    };

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured to
     * support full text indexing and search.
     * 
     * @see Options#INDEXER_COLLATOR_STRENGTH
     */
    protected final IKeyBuilder getKeyBuilder() {

        return threadLocalKeyBuilder.get();
            
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
     * 
     * @see TokenBuffer#flush()
     */
    public void index(TokenBuffer buffer, long docId, int fieldId,
            String languageCode, Reader r) {

        assertWritable();
        
        int n = 0;
        
        // tokenize (note: docId,fieldId are not on the tokenStream, but the field could be).
        final TokenStream tokenStream = getTokenStream(languageCode, r);

        while (true) {
            
            final Token token;
            try {

                token = tokenStream.next(/* token */);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            if (token == null) {

                break;
                
            }

            buffer.add(docId, fieldId, token);
            
            n++;

        }
        
        if (INFO)
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
     * @return The extracted token stream.
     */
    protected TokenStream getTokenStream(String languageCode, Reader r) {

        /*
         * Note: This stripping out stopwords by default.
         * 
         * @todo is it using a language family specific stopword list?
         */
        final Analyzer a = getAnalyzer(languageCode);
        
        TokenStream tokenStream = a.tokenStream(null/* @todo field? */, r);
        
        // force to lower case.
        tokenStream = new LowerCaseFilter(tokenStream);
        
        return tokenStream;
        
    }
    
    /**
     * Create a key for a term.
     * 
     * @param keyBuilder
     *            Used to construct the key.
     * @param token
     *            The token whose key will be formed.
     * @param successor
     *            When <code>true</code> the successor of the token's text
     *            will be encoded into the key. This is useful when forming the
     *            <i>toKey</i> in a search.
     * @param docId
     *            The document identifier - use <code>0L</code> when forming a
     *            search key.
     * @param fieldId
     *            The field identifier - use <code>0</code> when forming a
     *            search key.
     * 
     * @return The key.
     */
    protected byte[] getTokenKey(IKeyBuilder keyBuilder, String termText,
            boolean successor, long docId, int fieldId) {
        
        keyBuilder.reset();

        // the token text (or its successor as desired).
        keyBuilder.appendText(termText, true/* unicode */, successor);
        
        keyBuilder.append(docId);

        keyBuilder.append(fieldId);
        
        final byte[] key = keyBuilder.getKey();

        if (DEBUG) {

            log.debug("{" + termText + "," + docId + "," + fieldId
                    + "}, successor=" + (successor?"true ":"false") + ", key="
                    + BytesUtil.toString(key));

        }

        return key;

    }

    /**
     * Return the byte[] that is the encoded value for per-{token,docId,fieldId}
     * entry in the index.
     * 
     * @param buf
     *            Used to encode the value.
     * @param metadata
     *            Metadata about the term.
     * 
     * @return The encoded value.
     * 
     * @todo optionally record the token position metadata (sequence of token
     *       positions in the source) and the token offset (character offsets
     *       for the inclusive start and exclusive end of each token).
     * 
     * @todo value compression: 
     * 
     * @todo value compression: code "position" as delta from last position in
     *       the same field or from 0 if the first token of a new document; code
     *       "offsets" as deltas - the maximum offset between tokens will be
     *       quite small (it depends on the #of stopwords) so use a nibble
     *       format for this.
     */
    protected byte[] getTokenValue(ByteArrayBuffer buf, TermMetadata metadata) {

        final int termFreq = metadata.termFreq();
        
        final double localTermWeight = metadata.localTermWeight;
        
        if (DEBUG) {

            log.debug("termText=" + metadata.termText() + ", termFreq="
                            + termFreq);
            
        }
        
        // reset for new value.
        buf.reset();
        
        /*
         * the term frequency
         * 
         * @todo rather the packing as a short, write an IDataSerializer to
         * compress the whole thing onto a bit stream.
         */
        buf.putShort(termFreq > Short.MAX_VALUE ? Short.MAX_VALUE
                : (short) termFreq);
        
        /*
         *  
         */
        buf.putDouble(localTermWeight);
        
        return buf.toByteArray();
        
    }
    
    /**
     * Performs a full text search against indexed documents returning a hit
     * list.
     * 
     * @param query
     *            The query (it will be parsed into tokens).
     * @param languageCode
     *            The language code that should be used when tokenizing the
     *            query (an empty string will be interpreted as the default
     *            {@link Locale}).
     * 
     * @return A {@link Iterator} which may be used to traverse the search
     *         results in order of decreasing relevance to the query.
     *         
     * @throws InterruptedException 
     */
    public Hiterator search(String query, String languageCode) throws InterruptedException {

        return search( //
                query,//
                languageCode,//
                .4, // minCosine
                10000 // maxRank
                );
        
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
     * caller is returned an iterator which can read from that order.
     * 
     * @param query
     *            The query (it will be parsed into tokens).
     * @param languageCode
     *            The language code that should be used when tokenizing the
     *            query -or- <code>null</code> to use the default
     *            {@link Locale}).
     * @param minCosine
     *            The minimum cosine that will be returned.
     * @param maxRank
     *            The upper bound on the #of hits in the result set.
     * 
     * @return The hit list.
     * 
     * @todo there is a upper bound on [maxRank] of 10000 results that is
     *       currently hard-coded. this could be raised, but at some point the
     *       evaluate needs to be pruned further.
     * 
     * @todo manage the life of the result sets and perhaps serialize them onto
     *       an index backed by a {@link TemporaryStore}. The fromIndex/toIndex
     *       might be with respect to that short-term result set. Reclaim result
     *       sets after N seconds.
     * 
     * @todo consider other kinds of queries that we might write here. For
     *       example, full text search should support AND OR NOT operators for
     *       tokens.
     * 
     * @todo allow search within field(s). This will be a filter on the range
     *       iterator that is sent to the data service such that the search
     *       terms are visited only when they occur in the matching field(s).
     * 
     * @throws InterruptedException
     *             If the operation is interrupted. Since the search runs on the
     *             {@link IBigdataFederation#getThreadPool()}, a disconnect
     *             from the federation will automatically interrupt the search.
     */
    public Hiterator search(String query, String languageCode,
            double minCosine, int maxRank) throws InterruptedException {

        final long begin = System.currentTimeMillis();
        
//        if (languageCode == null)
//            throw new IllegalArgumentException();

        if (query == null)
            throw new IllegalArgumentException();
        
        if (minCosine < 0d || minCosine > 1d)
            throw new IllegalArgumentException();

        if (maxRank <= 0)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
            log.info("languageCode=[" + languageCode + "], text=[" + query + "]");

        // tokenize the query.
        final TermFrequencyData qdata;
        {
            
            TokenBuffer buffer = new TokenBuffer(1,this);
            
            index(buffer, 0L/*docId*/, 0/*fieldId*/, languageCode, new StringReader(query));
            
            if (buffer.size() == 0) {
                
                /*
                 * There were no terms after stopword extration.
                 */
                
                log.warn("No terms after stopword extraction: query="+query);
                
                final long elapsed = System.currentTimeMillis() - begin;
                
                return new Hiterator<Hit>(Arrays.asList(new Hit[]{}), elapsed, minCosine, maxRank);
                
            }
            
            qdata = buffer.get(0);
            
            qdata.normalize();
            
        }
        
        final ConcurrentHashMap<Long/*docId*/,Hit> hits;
        {
       
            // @todo use size of collection as upper bound.
            final int initialCapacity = Math.min(maxRank,10000);
            
            hits = new ConcurrentHashMap<Long, Hit>(initialCapacity);
            
        }

        final List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(qdata.distinctTermCount());
        
        for(TermMetadata md : qdata.terms.values()) {
            
            tasks.add(new ReadIndexTask(md.termText(), md.localTermWeight, this,
                            hits));
            
        }

        final ExecutorService threadPool = getThreadPool();

        // run on the client's thread pool.
        final List<Future<Object>> futures = threadPool.invokeAll(tasks, timeout, TimeUnit.MILLISECONDS);
        
        for(Future f : futures) {
            
            if(!f.isDone()) {
                
            }
            
        }
        
        // #of hits.
        final int nhits = hits.size();
        
        if (nhits == 0) {

            log.warn("No hits: languageCode=[" + languageCode + "], query=["
                    + query + "]");
            
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
        
        log.info("Rank ordering "+nhits+" hits by relevance");
        
        Hit[] a = hits.values().toArray(new Hit[0]);
        
        Arrays.sort(a);

        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: "+nhits+" hits in "+elapsed+"ms");

        /*
         * Note: The caller will only see those documents which satisify both
         * results below a threshold will be pruned. Any relevant results
         * exceeding the maxRank will be pruned.
         */
        return new Hiterator<Hit>(Arrays.asList(a), elapsed, minCosine, maxRank);

    }
        
}
