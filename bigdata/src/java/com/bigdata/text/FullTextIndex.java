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

package com.bigdata.text;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

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

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;

/**
 * Full text indexing and search support.
 * <p>
 * The basic data model is consists of documents, fields in documents, and
 * tokens extracted by an analyzer from those fields.
 * <p>
 * The frequency distributions may be normalized to account for a varient of
 * effects producing "term weights". For example, normalizing for document
 * length or relative frequency of a term in the overall collection. Therefore
 * the logical model is:
 * 
 * <pre>
 *                                                    
 *   token : {docId, freq?, weight?}+
 *                                                    
 * </pre>
 * 
 * where docId happens to be the term identifier as assigned by the terms index.
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
 *                {sortKey(token), fldId, docId} : {freq?, weight?, sorted(pos)+}
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
 * vector identifical. The relevance of documents to the query is generally
 * taken as the cosine between the query's and each document's (possibly
 * normalized) token-frequency vectors. The main effort of search is assembling
 * a token frequency vector for just those documents with which there is an
 * overlap with the query. This is done using a key range scan for each token in
 * the query against the full text index.
 * 
 * <pre>
 *                    fromKey := token, 0L
 *                    toKey   := successor(token), 0L
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
 * @todo verify that PRIMARY US English is an acceptable choice for the full
 *       text index collator regardless of the language family in which the
 *       terms were expressed and in which search is performed. I.e., that the
 *       specific implementations retain some distinction among characters
 *       regardless of their code points
 *       (http://unicode.org/reports/tr10/#Legal_Code_Points indicates that they
 *       SHOULD).
 * 
 * @todo we will get relatively little compression on the fldId or docId
 *       component in the key using just leading key compression. A Hu-Tucker
 *       encoding of those components would be much more compact. Note that the
 *       encoding must be reversable since we need to be able to read the docId
 *       out of the key in order to retrieve the document.
 * 
 * @todo refactor the full text index into a general purpose bigdata service
 * 
 * @todo support normalization passes over the index in which the weights are
 *       updated based on aggregated statistics.
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
    
    /**
     * 
     */
    public FullTextIndex(IIndex ndx) {
        
        if(ndx==null) throw new IllegalArgumentException();
        
        this.ndx = ndx;
        
    }

    /**
     * Return the token analyzer to be used for the given language code.
     * 
     * @param languageCode
     *            The language code from a {@link Literal}.
     *            <p>
     *            Note: When the language code is not explicitly stated on a
     *            literal the configured {@link Locale} for the database should
     *            be used.
     * 
     * @return The token analyzer best suited to the indicated language family.
     */
    protected Analyzer getAnalyzer(IKeyBuilder keyBuilder,String languageCode) {
        
        Map<String,Analyzer> map = getAnalyzers();
        
        Analyzer a = null;
        
        if(languageCode==null) {
            
            // The configured local for the database.
            Locale locale = ((KeyBuilder)keyBuilder).getSortKeyGenerator().getLocale();
            
            // The analyzer for that locale.
            a = getAnalyzer(keyBuilder,locale.getLanguage());
            
        } else {
            
            /*
             * Check the declared analyzers. We first check the three letter
             * language code. If we do not have a match there then we check the
             * 2 letter language code.
             */
            
            String code = languageCode;

            if (code.length() > 3) {

                code = code.substring(0, 2);

                a = map.get(languageCode);

            }

            if (a == null && code.length() > 2) {

                code = code.substring(0, 1);

                a = map.get(languageCode);
                
            }
            
        }
        
        if (a == null) {

            // request the default analyzer.
            
            a = map.get("");
            
            if (a == null) {

                throw new IllegalStateException("No entry for empty string?");
                
            }
            
        }

        return a;
        
    }
    
    /**
     * A map containing instances of the various kinds of analyzers that we know
     * about.
     * <p>
     * Note: There MUST be an entry under the empty string (""). This entry will
     * be requested when there is no entry for the specified language code.
     */
    private Map<String,Analyzer> analyzers;
    
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
    protected Map<String,Analyzer> getAnalyzers() {
        
        if (analyzers != null) {

            return analyzers;
            
        }

        analyzers = new HashMap<String, Analyzer>();

        {
            Analyzer a = new BrazilianAnalyzer();
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
            Analyzer a = new ChineseAnalyzer();
            analyzers.put("zho", a);
            analyzers.put("chi", a);
            analyzers.put("zh", a);
        }
        
        /*
         * Claims to handle Chinese, Japanese, Korean. Does double character
         * extraction with overlap.
         */
        {
            Analyzer a = new CJKAnalyzer();
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
            Analyzer a = new CzechAnalyzer();
            analyzers.put("ces",a);
            analyzers.put("cze",a);
            analyzers.put("cs",a);
        }

        {
            Analyzer a = new DutchAnalyzer();
            analyzers.put("dut",a);
            analyzers.put("nld",a);
            analyzers.put("nl",a);
        }
        
        {  
            Analyzer a = new FrenchAnalyzer();
            analyzers.put("fra",a); 
            analyzers.put("fre",a); 
            analyzers.put("fr",a);
        }

        /*
         * Note: There are a lot of language codes for German variants that
         * might be useful here.
         */
        {  
            Analyzer a = new GermanAnalyzer();
            analyzers.put("deu",a); 
            analyzers.put("ger",a); 
            analyzers.put("de",a);
        }
        
        // Note: ancient greek has a different code (grc).
        {  
            Analyzer a = new GreekAnalyzer();
            analyzers.put("gre",a); 
            analyzers.put("ell",a); 
            analyzers.put("el",a);
        }        

        // @todo what about other Cyrillic scripts?
        {  
            Analyzer a = new RussianAnalyzer();
            analyzers.put("rus",a); 
            analyzers.put("ru",a); 
        }        
        
        {
            Analyzer a = new ThaiAnalyzer();
            analyzers.put("tha",a); 
            analyzers.put("th",a); 
        }

        // English
        {
            Analyzer a = new StandardAnalyzer();
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
    
    /**
     * Models a document frequency vector (the set of document identifiers
     * having some token in common). In this context a document is something in
     * the terms index. Each document is broken down into a series of tokens by
     * an {@link Analyzer}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @deprecated not used yet.
     */
    protected static final class DFV {
        
        private final String token;
        
        /*
         * @todo could just manage a long[] directly using an insertion sort.
         * the frequency data could be tracked in a co-ordered array.
         */
        private final ArrayList<Long> data = new ArrayList<Long>();

        public String getToken() {
            
            return token;
            
        }
        
        public DFV(String token) {
        
            if(token==null) throw new IllegalArgumentException();
            
            this.token = token;
        
        }
        
        // @todo track the frequency count.
        public void add(long docId) {

            data.add(docId);
            
        }
        
        public long[] toArray() {
            
            long[] a = new long[data.size()];
            
            for(int i=0; i<a.length; i++) {
                
                a[i] = data.get(i);
                
            }
            
            return a;
            
        }
        
    }
    
    /**
     * Index a document.
     * 
     * @param keyBuilder
     * @param docId
     * @param languageCode
     * @param text
     * 
     * @todo handle multiple fields.
     * 
     * @todo delete old data when updating the document.
     */
    public void index(IKeyBuilder keyBuilder, Object docId,
            String languageCode, String text) {

        // tokenize.
        final TokenStream tokenStream = getTokenStream(keyBuilder,
                languageCode, text);

        Token token = null;
        
        /*
         * @todo modify to accept the current token for reuse (only in the
         * current nightly build). Also, termText() is deprecated in the
         * nightly build.
         * 
         * @todo this would be a good example for a mark/reset feature on
         * the KeyBuilder.
         */
        try {

            while ((token = tokenStream.next(/* token */)) != null) {

                final byte[] key = getTokenKey(token, false/* successor */,
                        docId);

                if (!ndx.contains(key)) {

                    ndx.insert(key, null/* no value */);
                    
                }

            }
     
        } catch (IOException ex) {
            
            throw new RuntimeException("Tokenization problem", ex);
            
        }

    }
    
    /**
     * Tokenize text using an {@link Analyzer} that is appropriate to the
     * specified language family.
     * 
     * @param languageCode
     *            The language code (an empty string will be interpreted as
     *            the default {@link Locale}).
     * 
     * @param text
     *            The text to be tokenized.
     * 
     * @return The extracted token stream.
     * 
     * @todo there should be a configuration option to strip out stopwords and
     *       another to enable stemming. how we do that should depend on the
     *       language family. Likewise, there should be support for language
     *       family specific stopword lists and language family specific
     *       exclusions.
     */
    protected TokenStream getTokenStream(IKeyBuilder keyBuilder,
            String languageCode, String text) {

        /*
         * @todo is this stripping out stopwords by default regardless of
         * the language family and yet in a language family specific manner?
         */
        final Analyzer a = getAnalyzer(keyBuilder,languageCode);
        
        TokenStream tokenStream = a.tokenStream(null/*field*/, new StringReader(text));
        
        // force to lower case.
        tokenStream = new LowerCaseFilter(tokenStream);
        
        return tokenStream;
        
    }
    
    /**
     * Create a key for the {@link #getFullTextIndex()} from a token extracted
     * from some text.
     * 
     * @param token
     *            The token whose key will be formed.
     * @param successor
     *            When <code>true</code> the successor of the token's text
     *            will be encoded into the key. This is useful when forming the
     *            <i>toKey</i> in a search.
     * @param docId
     *            The document identifier - use <code>null</code> when forming
     *            a search key.
     * 
     * @return The key.
     */
    protected byte[] getTokenKey(Token token, boolean successor, Object docId) {
        
        IKeyBuilder keyBuilder = getFullTextKeyBuilder();
        
        final String tokenText = token.termText();
        
        keyBuilder.reset();

        // the token text (or its successor as desired).
        keyBuilder
                .appendText(tokenText, true/* unicode */, successor);
        
        // the document identifier.
        if(docId!=null) {

            keyBuilder.append(docId);
            
        }
        
        final byte[] key = keyBuilder.getKey();

        if (INFO) {

            log.info("[" + tokenText + "][" + docId + "]");
            
        }

        return key;

    }
    
    /**
     * The {@link IKeyBuilder} used to form the keys for the
     * {@link #getFullTextIndex()}.
     * 
     * @todo thread-safety for the returned object (as well as its allocation).
     */
    protected final IKeyBuilder getFullTextKeyBuilder() {
        
        if(fullTextKeyBuilder==null) {
        
            Properties properties = new Properties();
            
            /*
             * Use primary strength only to increase retrieval with little
             * impact on precision.
             */
            
            properties.setProperty(KeyBuilder.Options.STRENGTH,
                    KeyBuilder.StrengthEnum.Primary.toString());

            /*
             * Note: The choice of the language and country for the collator
             * should not matter much for this purpose.
             * 
             * @todo consider explicit configuration of this in any case so that
             * the configuration may be stable rather than relying on the default
             * locale?  Of course, you can just explicitly specify the locale
             * on the command line!
             */
            
            fullTextKeyBuilder = KeyBuilder.newUnicodeInstance(properties);
            
        }
        
        return fullTextKeyBuilder;
        
    }
    private IKeyBuilder fullTextKeyBuilder;
    
    /**
     * Performs a full text search against indexed documents returning a hit
     * list.
     * 
     * @param languageCode
     *            The language code that should be used when tokenizing the
     *            query (an empty string will be interpreted as the default
     *            {@link Locale}).
     * @param text
     *            The query (it will be parsed into tokens).
     * 
     * @return An iterator that visits each term in the lexicon in which one or
     *         more of the extracted tokens has been found.
     * 
     * @todo introduce basic normalization for free text indexing and search
     *       (e.g., of term frequencies in the collection, document length,
     *       etc).
     * 
     * @todo consider other kinds of queries that we might write here. For
     *       example, full text search should support AND OR NOT operators for
     *       tokens.
     * 
     * @todo support fields in documents.
     */
    public Iterator<Long> textSearch(IKeyBuilder keyBuilder,
            String languageCode, String text) {

        if (languageCode == null)
            throw new IllegalArgumentException();

        if (text == null)
            throw new IllegalArgumentException();
        
        log.info("languageCode=["+languageCode+"], text=["+text+"]");
        
        final IIndex ndx = getIndex(); 
        
        final TokenStream tokenStream = getTokenStream(keyBuilder,languageCode, text);
        
        /*
         * @todo modify to accept the current token for reuse (only in the
         * current nightly build). Also, termText() is deprecated in the nightly
         * build.
         * 
         * @todo this would be a good example for a mark/reset feature on the
         * KeyBuilder.
         *
         * @todo thread-safety for the keybuilder.
         */
        
        final Vector<Vector<Long>> termVectors = new Vector<Vector<Long>>();
        
        try {

            Token token = null;
            
            while ((token = tokenStream.next(/* token */)) != null) {

                final byte[] fromKey = getTokenKey(token,
                        false/* successor */, null);

                final byte[] toKey = getTokenKey(token, true/* successor */,
                        null);

                // @todo capacity and growth policy? native long[]?
                final Vector<Long> v = new Vector<Long>();

                /*
                 * Extract the term identifier for each entry in the key range
                 * for the current token.
                 */
                
                final IEntryIterator itr = ndx.rangeIterator(fromKey, toKey);
                
                while(itr.hasNext()) {
                    
                    // next entry (value is ignored).
                    itr.next();
                    
                    // the key contains the term identifier.
                    final byte[] key = itr.getKey();
                    
                    // decode the term identifier (aka docId).
                    final long termId = KeyBuilder.decodeLong(key, key.length - Bytes.SIZEOF_LONG);

                    // add to this term vector.
                    v.add( termId );
                    
                }

                if(!v.isEmpty()) {

                    log.info("token: ["+token.termText()+"]("+languageCode+") : termIds="+v);
                    
                    termVectors.add( v );
                    
                } else {
                    
                    log.info("Not found: token=["+token.termText()+"]("+languageCode+")");
                    
                }
                
            }

            if(termVectors.isEmpty()) {
                
                log.warn("No tokens found in index: languageCode=["
                        + languageCode + "], text=[" + text + "]");
                
            }
            
            // rank order the terms based on the idss.
            log.error("Rank order and return term ids");
            
            return null;
            
        } catch (IOException ex) {

            throw new RuntimeException("Tokenization problem: languageCode=["
                    + languageCode + "], text=[" + text + "]", ex);

        }

    }

}
