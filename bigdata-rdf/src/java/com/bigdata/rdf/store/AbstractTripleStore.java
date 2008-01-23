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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.sail.SailException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IIndexProcedureConstructor;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.IndexProcedure;
import com.bigdata.btree.IntegerCounterAggregator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.cache.LRUCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.IJustificationIterator;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.JustificationIterator;
import com.bigdata.rdf.inf.Rule;
import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.inf.SPOJustificationIterator;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.IChunkedIterator;
import com.bigdata.rdf.spo.ISPOBuffer;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOIterator;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.service.EmbeddedDataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ResultSet;
import com.bigdata.service.Split;
import com.bigdata.text.FullTextIndex;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store.
 * 
 * @todo performance improvements:
 *       <p>
 *       Tune serialization for {@link ResultSet}, {@link IndexProcedure}, and
 *       batch operations (reduce network utilization for distributed stores).
 *       <p>
 *       Tune btree record structure using the same mechanisms (reduce disk
 *       utilization).
 *       <p>
 *       Tune inference - when computing closure on a store that supports
 *       concurrent read/write, for each SPO[] chunk from the 1st triple
 *       pattern, create an array of binding[]s and process each binding in
 *       parallel.
 * 
 * @todo Add 3+1, 4, and 4+1 stores (+1 indicates statement identifiers and 4
 *       indicates a quad store).
 * 
 * @todo finish the full text indexing support.
 * 
 * @todo sesame 2.x TCL (technology compatibility kit).
 * 
 * @todo retest on 1B+ triples (single host single threaded and concurrent data
 *       load and scale-out architecture with concurrent data load and policy
 *       for deciding which host loads which source files).
 * 
 * @todo maven 2.x builds.
 * 
 * @todo Write and benchmark a distributed bulk RDF data loader (there is
 *       already a concurrent bulk loader, but it only runs multiple threads on
 *       a single host against N data services rather than run the load from M
 *       hosts against N data services). Work through the search for equal
 *       "size" partitions for the statement indices. Note that the actual size
 *       of the statement indices will vary by access path since leading key
 *       compression will have different results for each access path. Ideally
 *       the generated index partitions can be index segments, in which case we
 *       of course need a metadata index that can be used to query them
 *       coherently. It is easy to generate that MDI.
 *       <p>
 *       The terms indices will be scale-out indices, so initially they should
 *       be pre-partitioned.
 *       <p>
 *       As an alternative to indexing the locally loaded data, we could just
 *       fill {@link StatementBuffer}s, convert to {@link ISPOBuffer}s (using
 *       the distributed terms indices), and then write out the long[3] data
 *       into a raw file. Once the local data have been converted to long[]s we
 *       can sort them into total SPO order (by chunks if necessary) and build
 *       the scale-out SPO index. The same process could then be done for each
 *       of the other access paths (OSP, POS).
 * 
 * @todo bnodes do not need to be stored in the terms or ids indices if we
 *       presume that an unknown identifier is a bnode. however, we still need
 *       to ensure that bnode identifiers are distinct or the same when and
 *       where appropriate, so we need to assign identifiers to bnodes in a
 *       restart-safe manner even if we "forget" the term-id mapping. (The
 *       possibility of an incomplete ids index during data load for the
 *       scale-out solution means that we must either read from a historical
 *       known consistent timestamp or record bnodes in the terms index.)
 * 
 * @todo the only added cost for a quad store is the additional statement
 *       indices. There are only three more statement indices in a quad store.
 *       Since statement indices are so cheap, it is probably worth implementing
 *       them now, even if only as a configuration option. (There may be reasons
 *       to maintain both versions.)
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions.
 * 
 * @todo explore possible uses of bitmap indices
 * 
 * @todo examine role for semi joins for a Sesame 2.x integration (quad store
 *       with real query operators). semi-joins (join indices) can be declared
 *       for various predicate combinations and then maintained. The
 *       declarations can be part of the scale-out index metadata. The logic
 *       that handles batch data load can also maintain the join indices. While
 *       triggers could be used for this purpose, there would need to be a means
 *       to aggregate and order the triggered events and then redistribute them
 *       against the partitions of the join indices. If the logic is in the
 *       client, then we need to make sure that newly declared join indices are
 *       fully populated (e.g., clients are notified to start building the join
 *       index and then we start the index build from existing data to remove
 *       any chance that the join index would be incomplete - the index would be
 *       ready as soon as the index build completes and client operations would
 *       be in a maintenance role).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTripleStore implements ITripleStore, IRawTripleStore {

    /**
     * This is used to conditionally enable the logic to retract justifications
     * when the corresponding statements is retracted.
     */
    final protected boolean justify;

    /**
     * This is used to conditionally disable the lexicon support, principally in
     * conjunction with a {@link TempTripleStore}.
     */
    final protected boolean lexicon;
    
    /**
     * This is used to conditionally disable the free text index
     * for literals.
     */
    final protected boolean textIndex;

    /**
     * This is used to conditionally disable all but a single statement index
     * (aka access path).
     */
    final protected boolean oneAccessPath;

    /**
     * When <code>true</code> the database will support statement identifiers.
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     */
    final protected boolean statementIdentifiers;
    
    /**
     * A copy of properties used to configure the {@link ITripleStore}.
     */
    final protected Properties properties;
    
    /**
     * The properties used to configure the configure the database wrapped up by
     * a new {@link Properties} object to prevent accidental modification by the
     * caller.
     */
    final public Properties getProperties() {
        
        /*
         * wrap them up so that people can not easily mess with the initial
         * properties.
         */

        return new Properties(properties);
        
    }
    
    /**
     * Configuration options.
     * 
     * @todo {@link AbstractTripleStore#readService} capacity.
     * 
     * @todo add property for the prefix so that you can have more than on
     *       triple/quad store in the same database.
     * 
     * @todo add property for triple vs quad variants. Statement identifiers are
     *       always possible by adding the statements into the lexicon.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends InferenceEngine.Options,
            com.bigdata.journal.Options, KeyBuilder.Options, DataLoader.Options {

        /**
         * Boolean option (default <code>true</code>) enables support for the
         * lexicon (the forward and backward term indices). When
         * <code>false</code>, this option disables Unicode support for the
         * {@link RdfKeyBuilder} and causes the lexicon indices to not be
         * registered. This can be safely turned off for the
         * {@link TempTripleStore} when only the statement indices are to be
         * used.
         */
        public static final String LEXICON = "lexicon"; 

        public static final String DEFAULT_LEXICON = "true"; 

        /**
         * Boolean option (default <code>false</code>) disables all but a
         * single statement index (aka access path).
         * <p>
         * Note: The main purpose of the option is to make it possible to turn
         * off the other access paths for special bulk load purposes. The use of
         * this option is NOT compatible with either the application of the
         * {@link InferenceEngine} or high-level query.
         */
        public static final String ONE_ACCESS_PATH = "oneAccessPath";

        public static final String DEFAULT_ONE_ACCESS_PATH = "false";
        
        /**
         * Boolean option (default <code>false</code>) enables support for a
         * full text index that may be used to lookup literals by tokens found
         * in the text of those literals.
         */
        public static final String TEXT_INDEX = "textIndex";

        public static final String DEFAULT_TEXT_INDEX = "false";

        /**
         * Boolean option (default <code>false</code>) enables support for
         * statement identifiers. A statement identifier is unique to a
         * <em>triple</em> (regardless of the graph in which that triple may
         * be found). Statement identifiers may be used to make statements about
         * statements without using RDF style reification.
         * 
         * FIXME This is a work in progress. There are two basic ways in which
         * we are considering extending the platform to make statement
         * identifiers useful.
         * <ol>
         * <li>Transparently convert ontology and data that uses RDF style
         * reification such that it uses statement identifiers instead. There
         * are a few drawbacks with this approach. One is that you still have to
         * write high level queries in terms of RDF style reification, which
         * means that we have not reduced the burden on the user significantly.
         * There is also the possibility that the change would effect the
         * semantics of RDF containers, or at least the relationship between the
         * semantics of RDF containers and RDF style reification. The plus side
         * of this approach is that the change could be completely transparent,
         * but I am not sure that keeping RDF reification in any form - and
         * especially in the queries - is a good idea.</li>
         * <li>The other approach is to provide an extension of RDF/XML in
         * which statements may be made and a high-level query language
         * extension in which statement identifiers may be exploited to
         * efficiently recover statements about statements. This approach has
         * the advantage of being completely decoupled from the RDF reification
         * semantics - it is a pure superset of the RDF data model and query
         * language. Also, in this approach we can guarentee that the statement
         * identifiers are purely an internal convenience of the database - much
         * like blank nodes. In fact, statement identifiers can be created
         * lazily - when there is a need to actually make a statement about a
         * specific statement. </li>
         * </ol>
         */
        public static final String STATEMENT_IDENTIFIERS = "statementIdentifiers";

        public static final String DEFAULT_STATEMENT_IDENTIFIERS = "false";
        
    }
    
    /**
     * 
     * @param properties
     * 
     * @see Options
     */    
    protected AbstractTripleStore(Properties properties) {
        
        // Copy the properties object.
        this.properties = (Properties)properties.clone();

        /*
         * Reads off the property for the inference engine that tells us whether
         * or not the justification index is being used. This is used to
         * conditionally enable the logic to retract justifications when the
         * corresponding statements is retracted.
         */

        this.justify = Boolean.parseBoolean(properties.getProperty(
                Options.JUSTIFY,
                Options.DEFAULT_JUSTIFY));

        log.info(Options.JUSTIFY+"="+justify);

        this.lexicon = Boolean.parseBoolean(properties.getProperty(
                Options.LEXICON,
                Options.DEFAULT_LEXICON));

        log.info(Options.LEXICON+"="+lexicon);

        // Note: the full text index is allowed iff the lexicon is enabled.
        if(lexicon) {
            
            this.textIndex = Boolean.parseBoolean(properties.getProperty(
                    Options.TEXT_INDEX, Options.DEFAULT_TEXT_INDEX));
            
        } else {

            this.textIndex = false;
            
        }

        log.info(Options.TEXT_INDEX+"="+textIndex);

        this.oneAccessPath = Boolean.parseBoolean(properties.getProperty(
                Options.ONE_ACCESS_PATH,
                Options.DEFAULT_ONE_ACCESS_PATH));

        log.info(Options.ONE_ACCESS_PATH+"="+oneAccessPath);

        this.statementIdentifiers = Boolean.parseBoolean(properties.getProperty(
                Options.STATEMENT_IDENTIFIERS,
                Options.DEFAULT_STATEMENT_IDENTIFIERS));

        log.info(Options.STATEMENT_IDENTIFIERS+"="+statementIdentifiers);

        // setup namespace mapping for serialization utility methods.
        addNamespace(RDF.NAMESPACE, "rdf");
        addNamespace(RDFS.NAMESPACE, "rdfs");
        addNamespace(OWL.NAMESPACE, "owl");
        addNamespace(XMLSchema.NAMESPACE, "xsd");
        
    }

    /**
     * Return <code>true</code> iff the store is safe for concurrent readers
     * and writers. This property depends on primarily on the concurrency
     * control mechanisms (if any) that are used to prevent concurrent access to
     * an unisolated index while a thread is writing on that index. Stores based
     * on the {@link IBigdataFederation} or an {@link EmbeddedDataService}
     * automatically inherent the appropriate concurrency controls as would a
     * store whose index access was intermediated by the executor service of the
     * {@link ConcurrentJournal}.
     */
    abstract public boolean isConcurrent();
    
    /**
     * Close the client. If the client uses an embedded database, then close and
     * delete the embedded database as well. If the client is connected to a
     * remote database then only the connection is closed.
     * <p>
     * Note: This is mainly used by the test suites.
     * <p>
     * Note: The default implementation merely terminates some thread pools.
     */
    public void closeAndDelete() {

        shutdown();

    }

    /**
     * Note: The default implementation merely terminates some thread pools.
     */
    public void close() {

        shutdown();
        
    }

    /**
     * Terminates the {@link #readService} and the {@link #writeService}.
     */
    protected void shutdown() {
        
        writeService.shutdown();
        
        try {

            writeService.awaitTermination(2, TimeUnit.SECONDS);
            
        } catch(InterruptedException ex) {
            
            log.warn("Write service did not terminate within timeout.");
            
        }
        
        readService.shutdown();
        
        try {

            readService.awaitTermination(2, TimeUnit.SECONDS);
            
        } catch(InterruptedException ex) {
            
            log.warn("Read service did not terminate within timeout.");
            
        }
        
    }
    
    /**
     * True iff the backing store is stable (exists on disk somewhere and may be
     * closed and re-opened).
     * <p>
     * Note: This is mainly used by the test suites.
     */
    abstract public boolean isStable();

    /**
     * Return a newly allocated {@link RdfKeyBuilder} instance for this client.
     * The object will be compatible with the Unicode preferences that are in
     * effect for the {@link ITripleStore}.
     * <p>
     * Note: This object is NOT thread-safe.
     * 
     * @todo Consider making this a {@link ThreadLocal} to avoid hassles with
     *       access by multiple threads. Note however that only the term:id
     *       index requires Unicode support.
     */
    final public RdfKeyBuilder getKeyBuilder() {
     
        RdfKeyBuilder keyBuilder = (RdfKeyBuilder)threadLocalKeyBuilder.get();
        
        if(keyBuilder==null) {
            
            throw new AssertionError();
            
        }
        
        return keyBuilder;

    }
    
    // Note: not static since we need configuration properties.
    private ThreadLocal threadLocalKeyBuilder = new ThreadLocal() {

        protected synchronized Object initialValue() {

            if (lexicon) {
                // unicode enabled.
                return new RdfKeyBuilder(KeyBuilder
                        .newUnicodeInstance(properties));
            } else {
                // no unicode support
                return new RdfKeyBuilder(KeyBuilder.newInstance());
            }

        }

    };

    /**
     * Create and return a new collator object responsible for encoding unicode
     * strings into sort keys.
     * 
     * @return A new collator object.
     * 
     * @todo define the means to configure the key builder for desired unicode
     *       support. the configuration should be restart-safe and must be
     *       shared by all clients for the same triple store. (Actually, I think
     *       that the {@link KeyBuilder} needs to be able to use different
     *       collation sequences for different keys - the main example here is
     *       of course a language code literal where the key contains the
     *       langauge code in order to partition literals using different
     *       language families, and possible different collation sequences, into
     *       different parts of the key space).
     */
    final protected RuleBasedCollator createCollator() {
        
        // choose a collator for the default locale.
        RuleBasedCollator collator = (RuleBasedCollator) Collator
                .getInstance(Locale.getDefault());

        /*
         * Primary uses case folding and produces smaller sort strings.
         * 
         * Secondary does not fold case.
         * 
         * Tertiary is the default.
         * 
         * Identical is also allowed.
         * 
         * @todo handle case folding - currently the indices complain, e.g., for
         * wordnet that a term already exists with a given id "Yellow Pages" vs
         * "yellow pages". Clearly the logic to fold case needs to extend
         * further if it is to work.
         */
//        collator.setStrength(Collator.PRIMARY);
//        collator.setStrength(Collator.SECONDARY);

        return collator;
        
    }
    
    /**
     * Executor service for read parallelism.
     * 
     * @deprecated We do not need the read vs write distinction for this service -
     *             that is enforced by the concurrency control mechanisms when
     *             the store supports such.
     * 
     * @todo use for parallel execution of map of new vs old+new over the terms
     *       of a rule.
     * 
     * @todo use for parallel execution of sub-queries.
     * 
     * @todo {@link SPOAssertionBuffer} must be thread-safe. {@link Rule}
     *       bindings must be per-thread.
     * 
     * @todo Note that rules that write entailments on the database statement
     *       MUST coordinate to avoid concurrent modification during traversal
     *       of the statement indices. The chunked iterators go a long way to
     *       addressing this.
     */
    final public ExecutorService readService = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

    /**
     * A service used to run index write operations in parallel.
     * 
     * @todo we do not really need different services for reads and writes.
     * 
     * @todo configure capacity.
     */
    public ExecutorService writeService = Executors.newFixedThreadPool(20,
            DaemonThreadFactory.defaultThreadFactory());

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
     * @see KeyBuilder
     */
    final public void generateSortKeys(RdfKeyBuilder keyBuilder,
            _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }

    final public IIndex getStatementIndex(KeyOrder keyOrder) {

        switch (keyOrder) {
        case SPO:
            return getSPOIndex();
        case POS:
            return getPOSIndex();
        case OSP:
            return getOSPIndex();
        default:
            throw new IllegalArgumentException("Unknown: " + keyOrder);
        }

    }

    final public int getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    final public int getJustificationCount() {
        
        if(justify) {
            
            return getJustificationIndex().rangeCount(null, null);
            
        }
        
        return 0;
        
    }
    
    final public int getTermCount() {
        
        return getTermIdIndex().rangeCount(null,null);
        
    }
    
    final public int getURICount() {
        
        byte[] fromKey = new byte[] { KeyBuilder.encodeByte(RdfKeyBuilder.TERM_CODE_URI) };

        byte[] toKey = new byte[] { KeyBuilder.encodeByte((byte)(RdfKeyBuilder.TERM_CODE_URI+1))};
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public int getLiteralCount() {
        
        // Note: the first of the kinds of literals (plain).
        byte[] fromKey = new byte[] { KeyBuilder.encodeByte(RdfKeyBuilder.TERM_CODE_LIT) };

        // Note: spans the last of the kinds of literals.
        byte[] toKey = new byte[] { KeyBuilder.encodeByte((byte)(RdfKeyBuilder.TERM_CODE_DTL+1))};
                
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public int getBNodeCount() {
        
        byte[] fromKey = new byte[] { KeyBuilder.encodeByte(RdfKeyBuilder.TERM_CODE_BND) };

        byte[] toKey = new byte[] { KeyBuilder.encodeByte((byte)(RdfKeyBuilder.TERM_CODE_BND+1))};
                
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }

    /*
     * term index
     */

    /**
     * Delegates to the batch API.
     */
    public long addTerm(Value value) {

        final _Value[] terms = new _Value[] {//
                
             OptimizedValueFactory.INSTANCE.toNativeValue(value) //

        };
    
        addTerms(getKeyBuilder(), terms, 1);
            
        return terms[0].termId;
            
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
    protected LRUCache<Long, _Value> termCache = new LRUCache<Long, _Value>(10000);
       
    /**
     * Note: This specializes the return to {@link _Value}. This keeps the
     * {@link ITripleStore} interface cleaner while imposes the actual semantics
     * on all implementation of this class.
     * <p>
     * Note: Handles both unisolatable and isolatable indices.
     * <P>
     * Note: Sets {@link _Value#termId} and {@link _Value#known} as
     * side-effects.
     */
    final public _Value getTerm(long id) {

        _Value value = termCache.get(id);
        
        if (value != null) {

            return value;
            
        }
        
        final IIndex ndx = getIdTermIndex();
        
        final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
        
        // Note: shortcut for keyBuilder.id2key(id)
        final byte[] key = keyBuilder.reset().append(id).getKey();
        
        final Object data = ndx.lookup( key );

        if (data == null) {

            return null;
            
        }

        value = _Value.deserialize((byte[]) data);
        
        termCache.put(id, value, false/*dirty*/);
        
        // @todo modify unit test to verify that these fields are being set.

        value.termId = id;
        
        value.known = true;
        
        return value;

    }
    
    /**
     * Note: Handles both unisolatable and isolatable indices.
     * <p>
     * Note: If {@link _Value#key} is set, then that key is used. Otherwise the
     * key is computed and set as a side effect.
     * <p>
     * Note: If {@link _Value#termId} is set, then returns that value
     * immediately. Otherwise looks up the termId in the index and sets
     * {@link _Value#termId} as a side-effect.
     */
    final public long getTermId(Value value) {

        if (value == null) {

            return IRawTripleStore.NULL;
            
        }
        
        final _Value val = (_Value) OptimizedValueFactory.INSTANCE
                .toNativeValue(value);
        
        if (val.termId != IRawTripleStore.NULL) {

            return val.termId;
            
        }

        final IIndex ndx = getTermIdIndex();
        
        if (val.key == null) {

            // generate key iff not on hand.
            val.key = getKeyBuilder().value2Key(val);
            
        }
        
        // lookup in the forward index.
        final Object tmp = ndx.lookup(val.key);
        
        if (tmp == null) {

            return IRawTripleStore.NULL;
            
        }
        
        try {

            val.termId = new DataInputBuffer((byte[])tmp).unpackLong();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
            
        // was found in the forward mapping.
        val.known = true;
        
        return val.termId;

    }

    /**
     * This implementation is designed to use unisolated batch writes on the
     * terms and ids index that guarentee consistency.
     * 
     * @todo "known" terms should be filtered out of this operation.
     *       <p>
     *       A term is marked as "known" within a client if it was successfully
     *       asserted against both the terms and ids index (or if it was
     *       discovered on lookup against the ids index).
     *       <p>
     *       We should also check the {@link AbstractTripleStore#termCache} for
     *       known terms.
     */
    public void addTerms(RdfKeyBuilder keyBuilder,final _Value[] terms, final int numTerms) {
        
        if (numTerms == 0)
            return;

        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        {

            /*
             * First make sure that each term has an assigned sort key.
             */
            {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /*
             * Sort terms by their assigned sort key. This places them into the
             * natural order for the term:id index.
             */
            {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            /*
             * For each term that does not have a pre-assigned term identifier,
             * execute a remote unisolated batch operation that assigns the term
             * identifier.
             */
            {
                
                final long _begin = System.currentTimeMillis();

                final IIndex termIdIndex = getTermIdIndex();

                /*
                 * Create a key buffer holding the sort keys. This does not
                 * allocate new storage for the sort keys, but rather aligns the
                 * data structures for the call to splitKeys().
                 */
                final byte[][] keys = new byte[numTerms][];
                {
                    
                    for(int i=0; i<numTerms; i++) {
                        
                        keys[i] = terms[i].key;
                        
                    }
                    
                }

                // run the procedure.
                termIdIndex.submit(numTerms, keys, null/* vals */,

                new IIndexProcedureConstructor() {

                    public IIndexProcedure newInstance(int n, int offset,
                            byte[][] keys, byte[][] vals) {

                        return new AddTerms(n, offset, keys);

                    }

                }, new IResultHandler<AddTerms.Result, Void>() {

                    /**
                     * Copy the assigned/discovered term identifiers onto the
                     * corresponding elements of the terms[].
                     */
                    public void aggregate(AddTerms.Result result, Split split) {

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
             * This is required in order to assure that the reverse index
             * remains complete and consistent. Otherwise a client that writes
             * on the terms index and fails before writing on the ids index
             * would cause those terms to remain undefined in the reverse index.
             */
            
            final long _begin = System.currentTimeMillis();
            
            final IIndex idTermIndex = getIdTermIndex();

            /*
             * Create a key buffer to hold the keys generated from the term
             * identifers and then generate those keys. The terms are already in
             * sorted order by their term identifiers from the previous step.
             */
            final byte[][] keys = new byte[numTerms][];
            final byte[][] vals = new byte[numTerms][];
            
            {

                // Private key builder removes single-threaded constraint.
                final KeyBuilder tmp = new KeyBuilder(Bytes.SIZEOF_LONG); 

                // buffer is reused for each serialized term.
                final DataOutputBuffer out = new DataOutputBuffer();
                
                for(int i=0; i<numTerms; i++) {
                    
                    keys[i] = tmp.reset().append(terms[i].termId).getKey();
                    
                    // Serialize the term.
                    vals[i] = terms[i].serialize(out.reset());                    

                }
                
            }

            // run the procedure on the index.
            idTermIndex.submit(numTerms, keys, vals,

            new IIndexProcedureConstructor() {

                public IIndexProcedure newInstance(int n, int offset,
                        byte[][] keys, byte[][] vals) {

                    return new AddIds(n, offset, keys, vals);

                }

            }, new IResultHandler<Void, Void>() {

                /**
                 * Since the unisolated write succeeded the client knows that
                 * the term is now in both the forward and reverse indices. We
                 * codify that knowledge by setting the [known] flag on the
                 * term.
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
            
            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        if (numTerms > 1000 || elapsed > 3000) {

            log.info("Wrote " + numTerms + " in " + elapsed + "ms; keygen="
                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
                    + insertTime + "ms");
            
        }
        
    }

    /*
     * singletons.
     */
    
    private WeakReference<InferenceEngine> inferenceEngineRef = null;
    
    final public InferenceEngine getInferenceEngine() {
    
        synchronized(this) {
        
            InferenceEngine inf = inferenceEngineRef == null ? null
                    : inferenceEngineRef.get();
            
            if (inf == null) {
                
                inf = new InferenceEngine(this);
            
                inferenceEngineRef = new WeakReference<InferenceEngine>(inf);
                
            }
            
            return inf;
            
        }
        
    }
    
    private WeakReference<DataLoader> dataLoaderRef = null;

    final public DataLoader getDataLoader() {
        
        synchronized(this) {
        
            DataLoader dataLoader = dataLoaderRef == null ? null
                    : dataLoaderRef.get();
            
            if (dataLoader == null) {
                
                dataLoader = new DataLoader(this);
            
                dataLoaderRef = new WeakReference<DataLoader>(dataLoader);
                
            }
            
            return dataLoader;
            
        }
        
    }
    
    /*
     * Sesame integration.
     */

    final public void addStatement(Resource s, URI p, Value o) {

        /*
         * Note: This uses the batch API.
         */
        
        IStatementBuffer buffer = new StatementBuffer(this, 1);
        
        buffer.add(s, p, o);
        
        buffer.flush();
        
    }

    final public SPO getStatement(long s, long p, long o) {
        
        if (s == NULL || p == NULL || o == NULL) {

            throw new IllegalArgumentException();
            
        }

        // @todo thread-safety for the key builder.
        byte[] key = getKeyBuilder().statement2Key(s, p, o);

        byte[] val = (byte[]) getStatementIndex(KeyOrder.SPO).lookup(key);

        if (val == null) {

            return null;

        }

        // The statement is known to the database.

        StatementEnum type = StatementEnum.deserialize(val);

        return new SPO(s, p, o, type);

    }
    
    /**
     * Return true if the triple pattern matches any statement(s) in the store
     * (non-batch API).
     * <p>
     * Note: This method does not verify whether or not the statement is
     * explicit.
     * 
     * @param s
     * @param p
     * @param o
     */
    final public boolean hasStatement(long s, long p, long o) {

        return ! getAccessPath(s,p,o).isEmpty();
        
    }

    final public boolean hasStatement(Resource s, URI p, Value o) {

        IAccessPath accessPath = getAccessPath(s,p,o);
        
        if(accessPath instanceof EmptyAccessPath) {
            
            return false;
            
        }

        return ! accessPath.isEmpty();
        
    }
    
    final public int removeStatements(Resource s, URI p, Value o) {
        
        return getAccessPath(s,p,o).removeAll();
        
    }

    /**
     * Return the statement from the database matching the fully bound query.
     * 
     * @param s
     * @param p
     * @param o
     * @return
     */
    public StatementWithType getStatement(Resource s, URI p, Value o) throws SailException {

        if(s == null || p == null || o == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        final StatementIterator itr = getStatements(s, p, o);
        
        try {

            if (!itr.hasNext()) {

                return null;

            }

            return (StatementWithType) itr.next();

        } finally {

            itr.close();

        }

    }
    
    public StatementIterator getStatements(Resource s, URI p, Value o) {

        return asStatementIterator( getAccessPath(s, p, o).iterator() );
        
    }

    /**
     * Converts an internal {@link _Value} to a Sesame {@link Value} object.
     * 
     * @param value
     *            Either an internal {@link _Value}, a Sesame {@link Value}
     *            object, or <code>null</code>.
     * 
     * @return A corresponding Sesame {@link Value} object -or-
     *         <code>null</code> iff <i>value</i> is <code>null</code>.
     */
    final public Value asValue(Value value) {
        
        return OptimizedValueFactory.INSTANCE.toSesameObject(value);
        
    }
    
    public Statement asStatement(SPO spo) {

        return new StatementWithType( //
                (Resource) OptimizedValueFactory.INSTANCE
                        .toSesameObject(getTerm(spo.s)),//
                (URI) OptimizedValueFactory.INSTANCE
                        .toSesameObject(getTerm(spo.p)), //
                (Value) OptimizedValueFactory.INSTANCE
                        .toSesameObject(getTerm(spo.o)), //
                spo.type//
        );
        
    }
    
    public StatementIterator asStatementIterator(ISPOIterator src) {
        
        return new SesameStatementIterator(this,src);
        
    }
    
    public IAccessPath getAccessPath(Resource s, URI p, Value o) {
        
        /*
         * convert other Value object types to our object types.
         */

        s = (Resource) OptimizedValueFactory.INSTANCE.toNativeValue(s);
        
        p = (URI) OptimizedValueFactory.INSTANCE.toNativeValue(p);
        
        o = OptimizedValueFactory.INSTANCE.toNativeValue(o);
        
        /*
         * Convert our object types to internal identifiers.
         * 
         * Note: If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        final long _s = getTermId(s);

        if (_s == NULL && s != null) return new EmptyAccessPath();

        final long _p = getTermId(p);

        if (_p == NULL && p != null) return new EmptyAccessPath();

        final long _o = getTermId(o);

        if (_o == NULL && o != null) return new EmptyAccessPath();
        
        /*
         * Return the access path.
         */
        
        return getAccessPath(_s, _p, _o);

    }
    
    final public IAccessPath getAccessPath(long s, long p, long o) {
        
        return new AccessPath(KeyOrder.get(s,p,o),s,p,o);
        
    }
    
    final public IAccessPath getAccessPath(KeyOrder keyOrder) {
        
        return new AccessPath(keyOrder,NULL,NULL,NULL);
        
    }
    
    /**
     * An access path that is known to be empty. There is a single instance of
     * this class. Various methods will return that object if you request an
     * access path using the Sesame {@link Value} objects and one of the
     * {@link Value}s is not known to the database. In such cases we know that
     * nothing can be read from the database for the given triple pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class EmptyAccessPath implements IAccessPath {
        
        /**
         * @throws UnsupportedOperationException
         */
        public long[] getTriplePattern() {
            
            throw new UnsupportedOperationException();
            
        }

//        public IIndex getStatementIndex() {
//            
//            return AbstractTripleStore.this.getStatementIndex(getKeyOrder());
//            
//        }

        public KeyOrder getKeyOrder() {
            
            // arbitrary.
            return KeyOrder.SPO;
            
        }

        /**
         * Always returns <code>true</code>.
         */
        public boolean isEmpty() {
            
            return true;
            
        }

        /**
         * Always returns ZERO(0).
         */
        public int rangeCount() {
            
            return 0;
            
        }

        /**
         * @throws UnsupportedOperationException
         */
        public IEntryIterator rangeQuery() {

            throw new UnsupportedOperationException();
            
        }

        public ISPOIterator iterator() {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(ISPOFilter filter) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(int limit, int capacity) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

            return new SPOArrayIterator(new SPO[]{},0);
            
        }

        public Iterator<Long> distinctTermScan() {
            
            return Arrays.asList(new Long[]{}).iterator();
            
        }

        public int removeAll() {

            return 0;
            
        }
        
        public int removeAll(ISPOFilter filter) {

            return 0;
            
        }
        
    }
    
    /**
     * Basic implementation resolves indices dynamically against the outer
     * class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class AccessPath implements IAccessPath {

        /** The triple pattern. */
        final long s, p, o;
        
        final KeyOrder keyOrder;

        /**
         * A private key builder for thread-safety whose initial capacity is
         * sufficient for the statement index keys.
         */
        final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(new KeyBuilder(N
                * Bytes.SIZEOF_LONG));
        
        final byte[] fromKey;
        
        final byte[] toKey;

        /** true iff the triple pattern is fully bound. */
        final boolean allBound;
             
        public long[] getTriplePattern() {
            
            return new long[]{s,p,o};
            
        }
        
        private IIndex getStatementIndex() {
            
            return AbstractTripleStore.this.getStatementIndex( keyOrder );
            
        }

        public KeyOrder getKeyOrder() {

            return keyOrder;
            
        }

        public boolean isEmpty() {

            /*
             * Note: empty iff the iterator can not visit anything.
             */
            
            return !iterator(1/* limit */, 1/* capacity */).hasNext();
            
        }
        
        public int rangeCount() {
            
            return getStatementIndex().rangeCount(fromKey,toKey);
            
        }

        public IEntryIterator rangeQuery() {
            
            return getStatementIndex().rangeIterator(fromKey, toKey);
            
        }

        public ISPOIterator iterator() {
            
            return iterator(null/*filter*/);
            
        }

        /**
         * FIXME This currently sucks everything into an array. Get traversal
         * with concurrent modification working for {@link IEntryIterator} and
         * then modify this to use {@link SPOIterator}, which uses asynchronous
         * reads and buffers a reasonable number of statements in memory.
         * <p>
         * Note: the {@link Rule}s basically assume that they can traverse the
         * statement indices with concurrent modification of those indices (the
         * {@link SPOAssertionBuffer} incrementally flushes the results to the database,
         * which means that it is writing on the statement indices at the same
         * time as we are reading from those indices).
         * <p>
         * However, the underlying {@link BTree} does NOT support traversal with
         * concurrent modification.
         * <p>
         * There are several ways to accomplish this ranging from: (a) the data
         * service, which issues a series of range queries in order to
         * incrementally cover a range and which would support interleaving of
         * range query and update operations; (b) a specialization akin to
         * isolation in which writes are absorbed onto an isolated index while
         * reads traverse the primary index (or writes are absorbed onto the
         * live index while reads traverse a historical version, which does not
         * cause double buffering but means that your writes are not visible to
         * the reader until you commit); and (c) modifying the {@link BTree} to
         * support traversal with concurrent modification, which could be as
         * simple as restarting the iterator from the successor of the last
         * visited key if the tree has been modified.
         * <p>
         * Updated position: There are two uses to support. One is remove() on
         * IEntryIterator with a single-thread accessing the BTree. This case is
         * easy enough since we only need to support "delete behind." It can be
         * handled with restarting the iterator if necessary to accomodate a
         * delete which causes a structural modification (leaves to merge).
         * <p>
         * The second use case is logically concurrent tasks that read and write
         * on the same unisolated index(s). This use case is already supported
         * by the concurrency control mechanisms in the concurrent journal.
         * However, the refactor should wait on a change to the concurrency
         * control to: (a) check point the indices at the each of each write so
         * that we can abort individual tasks without throwing away the work of
         * other tasks; and (b) add an "awaitCommit" property to the task so
         * that tasks may execute without either forcing a commit or awaiting a
         * commit. When false, "awaitCommit" does NOT mean that the application
         * can rely on the data to NOT be durable, it simply means that the
         * application does not require an immediate commit (ie, is willing to
         * discover that the data were not durable on restart). This would be
         * useful in an embedded application such as the RDF database which
         * performs a lot of write tasks on the unisolated indices before it has
         * completed an atomic operation from the application perspective. Note
         * that "awaitCommit" probably is only viable in an environment without
         * application level concurrency since partial writes would be visible
         * immediately and COULD be restart safe. However, an application such
         * as the {@link ScaleOutTripleStore} already manages concurrency at the
         * application level using a "consistent" data approach (but this issue
         * has not been solved for computing closure or truth maintenance on
         * statement removal.)
         */
        public ISPOIterator iterator(ISPOFilter filter) {

            if (allBound) {

                // Optimization for point test.

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        1/* limit */, filter);

            }

            if (isConcurrent()) {

                /*
                 * This is an async incremental iterator that buffers some but not
                 * necessarily all statements.
                 */
                
                return iterator(0/* limit */, 0/* capacity */, filter);

            } else {
                
                /*
                 * This is a synchronous read that buffers all statements.
                 * 
                 * Note: This limits the capacity of index scans to the
                 * available memory. This is primarily a problem during
                 * inference, where it imposes a clear upper bound on the size
                 * of the store whose closure can be computed. It is also a
                 * problem in high level query if any access path is relatively
                 * unselective, e.g., 1-bound.
                 */

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        0/* no limit */, filter);

            }

        }

        /**
         * Note: Return an iterator that will use transparent read-ahead when no
         * limit is specified (limit is zero) or the limit is "small".
         * 
         * @see SPOIterator
         */
        public ISPOIterator iterator(int limit, int capacity) {

            return iterator(limit, capacity, null/*filter*/);
            
        }

        /**
         * Note: Return an iterator that will use transparent read-ahead when no
         * limit is specified (limit is zero) or the limit is "small".
         * 
         * @see SPOIterator
         */
        public ISPOIterator iterator(int limit, int capacity, ISPOFilter filter) {

            if (allBound) {

                // Optimization for point test.

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        1/* limit */, filter);

            }

            if (limit > 0 && limit < 100) {

                /*
                 * Use a light-weight synchronous fully buffered variant when
                 * the limit is small, especially when all that you are doing is
                 * an existence test (limit := 1).
                 */

                return new SPOArrayIterator(AbstractTripleStore.this, this,
                        limit, filter);

            }

            boolean async = true;

            return new SPOIterator(this, limit, capacity, async, filter);

        }
        
        /**
         * Chooses the best access path for the given triple pattern.
         * 
         * @param s
         *            The term identifier for the subject -or-
         *            {@link IRawTripleStore#NULL}.
         * @param p
         *            The term identifier for the predicate -or-
         *            {@link IRawTripleStore#NULL}.
         * @param o
         *            The term identifier for the object -or-
         *            {@link IRawTripleStore#NULL}.
         */
        AccessPath(final KeyOrder keyOrder, long s, long p, long o) {

            if (keyOrder == null)
                throw new IllegalArgumentException();
            
            this.keyOrder = keyOrder;
            
            this.s = s;
            
            this.p = p;
            
            this.o = o;
            
            this.allBound = (s != NULL && p != NULL & o != NULL);
            
            if (s != NULL && p != NULL && o != NULL) {
        
                assert keyOrder == KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, p, o);

                toKey = keyBuilder.statement2Key(s, p, o + 1);

            } else if (s != NULL && p != NULL) {

                assert keyOrder == KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, p, NULL);

                toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            } else if (s != NULL && o != NULL) {

                assert keyOrder == KeyOrder.OSP;
                
                fromKey = keyBuilder.statement2Key(o, s, NULL);

                toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            } else if (p != NULL && o != NULL) {

                assert keyOrder == KeyOrder.POS;
                
                fromKey = keyBuilder.statement2Key(p, o, NULL);

                toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            } else if (s != NULL) {

                assert keyOrder == KeyOrder.SPO;
                
                fromKey = keyBuilder.statement2Key(s, NULL, NULL);

                toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            } else if (p != NULL) {

                assert keyOrder == KeyOrder.POS;
                
                fromKey = keyBuilder.statement2Key(p, NULL, NULL);

                toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            } else if (o != NULL) {

                assert keyOrder == KeyOrder.OSP;
                
                fromKey = keyBuilder.statement2Key(o, NULL, NULL);

                toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            } else {

                /*
                 * Note: The KeyOrder does not matter when you are fully
                 * unbound.
                 */
                
                fromKey = toKey = null;

            }

        }

        /**
         * Representation of the state for the access path (key order, triple
         * pattern, and from/to keys).
         */
        public String toString() {
            
            return super.toString() + ": " + keyOrder + ", {" + s + "," + p
                    + "," + o + "}, fromKey=" + (fromKey==null?"n/a":Arrays.toString(fromKey))
                    + ", toKey=" + (toKey==null?"n/a":Arrays.toString(toKey));
            
        }
        
        /**
         * This materializes a set of {@link SPO}s at a time and then submits
         * tasks to parallel threads to remove those statements from each of the
         * statement indices. This continues until all statements selected by
         * the triple pattern have been removed.
         */
        public int removeAll() {
            
            return removeAll(null/*filter*/);
            
        }
        
        /**
         * This materializes a set of {@link SPO}s at a time and then submits
         * tasks to parallel threads to remove those statements from each of the
         * statement indices. This continues until all statements selected by
         * the triple pattern have been removed.
         */
        public int removeAll(ISPOFilter filter) {

            // @todo try with an asynchronous read-ahead iterator.
//            ISPOIterator itr = iterator(0,0);
            
            // synchronous fully buffered iterator.
            ISPOIterator itr = iterator(filter);
            
            int nremoved = 0;
            
            try {

                while(itr.hasNext()) {
                    
                    final SPO[] stmts = itr.nextChunk();
                    
                    // The #of statements that will be removed.
                    final int numStmts = stmts.length;
                    
                    final long begin = System.currentTimeMillis();

                    // The time to sort the data.
                    final AtomicLong sortTime = new AtomicLong(0);
                    
                    // The time to delete the statements from the indices.
                    final AtomicLong writeTime = new AtomicLong(0);
                    
                    /**
                     * Class writes on a statement index, removing the specified
                     * statements.
                     * 
                     * @author <a
                     *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                     *         Thompson</a>
                     * @version $Id$
                     */
                    class IndexWriter implements Callable<Long> {

                        final KeyOrder keyOrder;
                        final SPO[] a;

                        /*
                         * Private key builder for the SPO, POS, or OSP keys (one instance
                         * per thread).
                         */
                        final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(
                                new KeyBuilder(N * Bytes.SIZEOF_LONG));

                        IndexWriter(KeyOrder keyOrder, boolean clone) {
                            
                            this.keyOrder = keyOrder;

                            if(clone) {
                                
                                a = new SPO[numStmts];
                                
                                System.arraycopy(stmts, 0, a, 0, numStmts);
                                
                            } else {
                                
                                this.a = stmts;
                                
                            }
                            
                        }
                        
                        public Long call() throws Exception {

                            final long begin = System.currentTimeMillis();
                            
                            IIndex ndx = AbstractTripleStore.this.getStatementIndex(keyOrder);

                            // Place statements in index order.
                            Arrays.sort(a, 0, numStmts, keyOrder.getComparator());

                            final long beginWrite = System.currentTimeMillis();
                            
                            sortTime.addAndGet(beginWrite - begin);
                            
                            // remove statements from the index.
                            for (int i = 0; i < numStmts; i++) {

                                SPO spo = a[i];

                                if(DEBUG) {
                                    
                                    /*
                                     * Note: the externalized terms will be NOT
                                     * FOUND when removing a statement from a
                                     * temp store since the term identifiers for
                                     * the temp store are generally only stored
                                     * in the database.
                                     */
                                    log.debug("Removing "
                                                    + spo.toString(AbstractTripleStore.this)
                                                    + " from " + keyOrder);
                                    
                                }
                                
                                byte[] key = keyBuilder.statement2Key(keyOrder, spo);

                                if(ndx.remove( key )==null) {
                                    
                                    throw new AssertionError(
                                            "Missing statement: keyOrder="
                                                    + keyOrder + ", spo=" + spo
                                                    + ", key=" + Arrays.toString(key));
                                    
                                }

                            }

                            final long endWrite = System.currentTimeMillis();
                            
                            writeTime.addAndGet(endWrite - beginWrite);
                            
                            return endWrite - begin;
                            
                        }
                        
                    }

                    /**
                     * Class writes on the justification index, removing all
                     * justifications for each statement that is being removed.
                     * <p>
                     * Note: There is only one index for justifications. The
                     * keys all use the SPO of the entailed statement as their
                     * prefix, so given a statement it is trivial to do a range
                     * scan for its justifications.
                     * 
                     * @todo if we supported remove() on the IEntryIterator then
                     *       we would not have to buffer the justifications that
                     *       we want to delete.
                     * 
                     * @author <a
                     *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                     *         Thompson</a>
                     * @version $Id$
                     */
                    class JustificationWriter implements Callable<Long> {

                        final SPO[] a;

//                        /*
//                         * Private key builder.
//                         * 
//                         * Note: This capacity estimate is based on N longs per SPO, one head,
//                         * and 2-3 SPOs in the tail. The capacity will be extended automatically
//                         * if necessary.
//                         */
//                        KeyBuilder keyBuilder = new KeyBuilder(N * (1 + 3) * Bytes.SIZEOF_LONG);

                        JustificationWriter(boolean clone) {
                            
                            if(clone) {
                                
                                a = new SPO[numStmts];
                                
                                System.arraycopy(stmts, 0, a, 0, numStmts);
                                
                            } else {
                                
                                this.a = stmts;
                                
                            }
                            
                        }

                        public Long call() throws Exception {
                            
                            final long begin = System.currentTimeMillis();
                            
                            IIndex ndx = AbstractTripleStore.this.getJustificationIndex();

                            /*
                             * Place statements in index order (SPO since all
                             * justifications begin with the SPO of the entailed
                             * statement.
                             */
                            Arrays.sort(a, 0, numStmts, KeyOrder.SPO.getComparator());

                            final long beginWrite = System.currentTimeMillis();
                            
                            sortTime.addAndGet(beginWrite - begin);

                            // remove statements from the index.
                            for (int i = 0; i < numStmts; i++) {

                                SPO spo = a[i];

                                // will visit justifications for that statement.
                                // FIXME use chunks.
                                SPOJustificationIterator itr = new SPOJustificationIterator(
                                        AbstractTripleStore.this, spo);
                                
                                if(DEBUG) {
                                    
                                    log.debug("Removing "
                                                    + ndx.rangeCount(fromKey,toKey)
                                                    + " justifications for "
                                                    + spo.toString(AbstractTripleStore.this));
                                    
                                }

                                while(itr.hasNext()) {
                                    
                                    itr.next();
                                    
                                    itr.remove();
                                    
                                }

                            }

                            final long endWrite = System.currentTimeMillis();
                            
                            writeTime.addAndGet(endWrite - beginWrite);
                            
                            return endWrite - begin;

                        }
                        
                    }
                    
                    List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

                    tasks.add(new IndexWriter(KeyOrder.SPO, false/* clone */));
                    
                    if(!oneAccessPath) {

                        tasks.add(new IndexWriter(KeyOrder.POS, true/* clone */));
                        
                        tasks.add(new IndexWriter(KeyOrder.OSP, true/* clone */));
                        
                    }
                    
                    if(justify) {

                        /*
                         * Also retract the justifications for the statements.
                         */
                        
                        tasks.add(new JustificationWriter(true/* clone */));
                        
                    }

                    final List<Future<Long>> futures;
                    final long elapsed_SPO;
                    final long elapsed_POS;
                    final long elapsed_OSP;
                    final long elapsed_JST;

                    try {

                        futures = writeService.invokeAll(tasks);

                        elapsed_SPO = futures.get(0).get();
                        
                        if(!oneAccessPath) {
                        
                            elapsed_POS = futures.get(1).get();
                            
                            elapsed_OSP = futures.get(2).get();
                            
                        } else {
                            
                            elapsed_POS = 0;
                            
                            elapsed_OSP = 0;
                            
                        }
                        
                        if(justify) {
                        
                            elapsed_JST = futures.get(3).get();
                            
                        } else {
                            
                            elapsed_JST = 0;
                            
                        }

                    } catch (InterruptedException ex) {

                        throw new RuntimeException(ex);

                    } catch (ExecutionException ex) {

                        throw new RuntimeException(ex);

                    }

                    long elapsed = System.currentTimeMillis() - begin;

                    if(numStmts>1000) {

                        log.info("Removed "+numStmts+" in " + elapsed + "ms; sort=" + sortTime
                            + "ms, keyGen+delete=" + writeTime + "ms; spo="
                            + elapsed_SPO + "ms, pos=" + elapsed_POS + "ms, osp="
                            + elapsed_OSP + "ms, jst="+elapsed_JST);
                        
                    }

                    // removed all statements in this chunk.
                    nremoved += numStmts;
                    
                }
                
            } finally {
                
                itr.close();
                
            }
            
            return nremoved;

        }

        /**
         * The implementation uses a key scan to find the first term identifer
         * for the given index. It then forms a fromKey that starts at the next
         * possible term identifier and does another scan, thereby obtaining the
         * 2nd distinct term identifier for that position on that index. This
         * process is repeated iteratively until the key scan no longer
         * identifies a match. This approach skips quickly over regions of the
         * index which have many statements for the same term and makes N+1
         * queries to identify N distinct terms. Note that there is no way to
         * pre-compute the #of distinct terms that will be identified short of
         * running the queries.
         * 
         * @todo This will need to be modified to return a chunked iterator that
         *       encapsulates the logic so that the distinct term scan may be
         *       applied when very large #s of terms would be visited.
         *       <p>
         *       If the indices are range partitioned and the iterator only
         *       guarentee "distinct" (and not also ordered) then those steps be
         *       parallelized. The only possibility for conflict is when the
         *       last distinct term identifier is read from one index before the
         *       right sibling index partition has reported its first distinct
         *       term identifier.  We could withhold the first result from each
         *       partition until the partition that proceeds it in the metadata
         *       index has completed, which would give nearly full parallelism.
         *       <p>
         *       If the indices are range partitioned and distinct + ordered is
         *       required, then the operation can not be parallelized, or if it
         *       is parallelized then a merge sort must be done before returning
         *       the first result.
         *       <p>
         *       Likewise, if the indices are hash partitioned, then we can do
         *       parallel index scans and a merge sort but the caller will have
         *       to wait for the merge sort to complete before obtaining the 1st
         *       result.
         */
        public Iterator<Long> distinctTermScan() {

            ArrayList<Long> ids = new ArrayList<Long>(1000);
            
            byte[] fromKey = null;
            
            final byte[] toKey = null;
            
            IIndex ndx = getStatementIndex();
            
            IEntryIterator itr = ndx.rangeIterator(fromKey, toKey);
            
//            long[] tmp = new long[IRawTripleStore.N];
            
            while(itr.hasNext()) {
                
                itr.next();
                
                // clone of the key.
//                final byte[] key = itr.getKey();
                
                // copy of the key in a reused buffer.
                final byte[] key = itr.getTuple().getKeyBuffer().array();
                
                // extract the term ids from the key. 
//                RdfKeyBuilder.key2Statement( key , tmp);
//                
//                final long id = tmp[0];
                
                final long id = KeyBuilder.decodeLong( key, 0);
                
                // append tmp[0] to the output list.
                ids.add(id);

//                log.debug(ids.size() + " : " + id + " : "+ toString(id));
                
                // restart scan at the next possible term id.

                final long nextId = id + 1;
                
                fromKey = keyBuilder.statement2Key(nextId, NULL, NULL);
                
                // new iterator.
                itr = ndx.rangeIterator(fromKey, toKey);
                
            }
            
//            log.debug("Distinct key scan: KeyOrder=" + keyOrder + ", #terms=" + ids.size());
            
            return ids.iterator();
            
        }
        
    }
    
    /*
     * statement externalization serialization stuff.
     */
    
    // namespace to prefix
    private final Map<String, String> uriToPrefix = new HashMap<String, String>();
    
    /**
     * Defines a transient mapping from a URI to a namespace prefix that will be
     * used for that URI by {@link #toString()}.
     * 
     * @param namespace
     * 
     * @param prefix
     */
    final public void addNamespace(String namespace, String prefix) {
    
        uriToPrefix.put(namespace, prefix);

    }
    
    /**
     * Return an unmodifiable view of the mapping from namespaces to namespace
     * prefixes.
     * <p>
     * Note: this is NOT a persistent map. It is used by {@link #toString(long)}
     * when externalizing URIs.
     */
    final public Map<String,String> getNamespaces() {
        
        return Collections.unmodifiableMap(uriToPrefix);
        
    }
    
    /**
     * Return the namespace for the given prefix.
     * 
     * @param prefix
     *            The prefix.
     *            
     * @return The associated namespace -or- <code>null</code> if no namespace
     *         was mapped to that prefix.
     */
    final public String getNamespace(String prefix) {

        // Note: this is not an efficient operation.
        Iterator<Map.Entry<String/*namespace*/,String/*prefix*/>> itr = uriToPrefix.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String/*namespace*/,String/*prefix*/> entry = itr.next();
            
            if(entry.getValue().equals(prefix)) {
                
                return entry.getKey();
                
            }
            
        }
        
        return null;

    }
    
    /**
     * Removes the namespace associated with the prefix.
     * 
     * @param prefix
     *            The prefix.
     * @return The namespace associated with that prefic (if any) and
     *         <code>null</code> otherwise.
     */
    final public String removeNamespace(String prefix) {
        
        Iterator<Map.Entry<String/*namespace*/,String/*prefix*/>> itr = uriToPrefix.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String/*namespace*/,String/*prefix*/> entry = itr.next();
            
            if(entry.getValue().equals(prefix)) {
                
                itr.remove();
                
                return entry.getKey();
                
            }
            
        }
        
        return null;
        
    }

    /**
     * Clears the namespace map.
     */
    final public void clearNamespaces() {
        
        uriToPrefix.clear();
        
    }
    
    final public String toString( long s, long p, long o ) {
        
        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o) +" >");
        
    }

    /**
     * Return true iff the term identifier is associated with a RDF Literal in
     * the database.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a literal.
     * <p>
     * Note: Some entailments require the ability to filter based on whether or
     * not a term is a literal. For example, literals may not be entailed into
     * the subject position. This method makes it possible to determine whether
     * or not a term is a literal without materializing the term, thereby
     * allowing the entailments to be computed purely within the term identifier
     * space.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is an RDF literal.
     */
    final public boolean isLiteral( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_LITERAL;
        
    }

    final public boolean isBNode( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_BNODE;
        
    }

    final public boolean isURI( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_URI;
        
    }

    /**
     * Return true iff the term identifier identifies a statement (this feature
     * is enabled with {@link Options#STATEMENT_IDENTIFIERS}).
     * 
     * @param termId
     *            The term identifier.
     *            
     * @return <code>true</code> iff the term identifier identifies a statement.
     */
    final public boolean isStatement( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_STATEMENT;
        
    }

    final public String toString( long termId ) {

        if (termId == 0)
            return "0";

        _Value v = (_Value)getTerm(termId);

        if (v == null)
            return "<NOT_FOUND#" + termId + ">";

        String s = (v instanceof URI ? abbrev((URI) v) : v.toString());
        
        return s + ("("+termId+")");
        
    }
//    private final String TERM_NOT_FOUND = "<NOT_FOUND>";
    
    /**
     * Substitutes in well know namespaces (rdf, rdfs, etc).
     */
    final private String abbrev( URI uri ) {
        
        String uriString = uri.toString();
        
//        final int index = uriString.lastIndexOf('#');
//        
//        if(index==-1) return uriString;
//
//        final String namespace = uriString.substring(0, index);
        
        final String namespace = uri.getNamespace();
        
        final String prefix = uriToPrefix.get(namespace);
        
        if(prefix != null) {
            
            return prefix+":"+uri.getLocalName();
            
        } else return uriString;
        
    }

    final public void predicateUsage() {
        
        predicateUsage(this);
        
    }

    /**
     * Dumps the #of statements using each predicate in the kb on
     * {@link System#err} (tab delimited, unordered).
     * 
     * @param resolveTerms
     *            Used to resolve term identifiers to terms (you can use this to
     *            dump a {@link TempTripleStore} that is using the term
     *            dictionary of the main database).
     */
    final public void predicateUsage(AbstractTripleStore resolveTerms) {

        // visit distinct term identifiers for the predicate position.
        Iterator<Long> itr = getAccessPath(KeyOrder.POS).distinctTermScan();
        
        while(itr.hasNext()) {
            
            long p = itr.next();
            
            int n = getAccessPath(NULL, p, NULL).rangeCount();
            
            System.err.println(n+"\t"+resolveTerms.toString(p));
            
        }
        
    }
    
    /**
     * Utility method dumps the statements in the store onto {@link System#err}
     * using the SPO index (subject order).
     */
    final public void dumpStore() {
    
        dumpStore(true, true, true);
        
    }

    final public void dumpStore(boolean explicit, boolean inferred, boolean axioms) {

        dumpStore(this,explicit,inferred,axioms);
        
    }

    final public void dumpStore(AbstractTripleStore resolveTerms,
            boolean explicit, boolean inferred, boolean axioms) {
        
        dumpStore(resolveTerms,explicit,inferred,axioms,false);
        
    }

    /**
     * Dumps the store in a human readable format (not suitable for
     * interchange).
     * 
     * @param resolveTerms
     *            Used to resolve term identifiers to terms (you can use this to
     *            dump a {@link TempTripleStore} that is using the term
     *            dictionary of the main database).
     * @param explicit
     *            Show statements marked as explicit.
     * @param inferred
     *            Show statements marked inferred.
     * @param axioms
     *            Show statements marked as axioms.
     * @param justifications
     *            Dump the justifications index also.
     */
    final public void dumpStore(AbstractTripleStore resolveTerms,
            boolean explicit, boolean inferred, boolean axioms,
            boolean justifications) {

        final int nstmts = getStatementCount();

        int nexplicit = 0;
        int ninferred = 0;
        int naxioms = 0;

        {

            IEntryIterator itr = getSPOIndex().rangeIterator(null, null);

            int i = 0;

            while (itr.hasNext()) {

                final SPO spo = new SPO(KeyOrder.SPO, itr);

                switch (spo.type) {

                case Explicit:
                    nexplicit++;
                    if (!explicit)
                        continue;
                    else
                        break;

                case Inferred:
                    ninferred++;
                    if (!inferred)
                        continue;
                    else
                        break;

                case Axiom:
                    naxioms++;
                    if (!axioms)
                        continue;
                    else
                        break;

                default:
                    throw new AssertionError();

                }

                System.err.println("#" + (i + 1) + "\t"
                        + spo.toString(resolveTerms));

                i++;

            }
            
        }
        
        int njust = 0;
        
        if(justifications) {
            
            IIndex ndx = getJustificationIndex();
            
            IEntryIterator itrj = ndx.rangeIterator(null, null);
            
            while(itrj.hasNext()) {
                
                itrj.next();
                
                Justification jst = new Justification(itrj.getKey());
                
                System.err.println("#" + (njust + 1) + "\t"
                        + jst.toString(resolveTerms));
                
                njust++;
                
            }
            
        }
        
        System.err.println("dumpStore: #statements=" + nstmts + ", #explicit="
                + nexplicit + ", #inferred=" + ninferred + ", #axioms="
                + naxioms+(justifications?", #just="+njust:""));

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
    public Iterator<Value> idTermIndexScan() {

        final IIndex ndx = getIdTermIndex();

        return new Striterator(ndx.rangeIterator(null, null))
                .addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the serialized term.
             */
            protected Object resolve(Object val) {
                
                _Value term = _Value.deserialize((byte[]) val);

                return term;
                
            }
            
        });

    }

    /**
     * Iterator visits all term identifiers in order by the <em>term</em> key
     * (efficient index scan).
     */
    public Iterator<Long> termIdIndexScan() {

        final IIndex ndx = getTermIdIndex();

        return new Striterator(ndx.rangeIterator(null, null))
                .addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * Deserialize the term identifier (packed long integer).
             * 
             * @param val The serialized term identifier.
             */
            protected Object resolve(Object val) {

                final long id;
                
                try {

                    id = new DataInputBuffer((byte[]) val).unpackLong();

                } catch (IOException ex) {

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
    
    /**
     * Returns some usage information for the database.
     */
    public String usage() {

        return "usage summary: class="
                + getClass().getSimpleName()
                + "\n"
                + "\nsummary by index::\n"
                + (lexicon //
                        ? "\n" + usage(name_termId, getTermIdIndex())
                        + "\n" + usage(name_idTerm, getIdTermIndex()) //
                        : "")
                //
                + (oneAccessPath //
                        ?  "\n" + usage(name_spo, getSPOIndex())
                        :( "\n" + usage(name_spo, getSPOIndex()) //
                         + "\n" + usage(name_pos, getPOSIndex()) //
                         + "\n" + usage(name_osp, getOSPIndex())//
                         ))//
                + (justify?"\n"+ usage(name_just, getJustificationIndex()):"")
        ;
        
    }

    /**
     * Writes out some usage information on the named index. More information is
     * available for local indices. Information for scale-out indices is both
     * less detailed and more approximate.
     * 
     * @param name
     *            The index name.
     * 
     * @param ndx
     *            The index.
     */
    final public String usage(String name,IIndex ndx) {
        
        if (ndx == null) {
            
            return name+" : not used";
            
        }
        
        return name + " : "+ndx.getStatistics();
        
//        if (ndx instanceof BTree) {
//
//            BTree btree = (BTree) ndx;
//
//            return name+" : "+btree.getStatistics();
//            
//        } else {
//
//            // Note: this is only an estimate if the index is a view.
//            final int nentries = ndx.rangeCount(null, null);
//
//            return (name+": #entries(est)="+nentries);
//            
//        }
        
    }

    /*
     * IRawTripleStore
     */

    /**
     * Copies the statements from <i>this</i> store into the specified store
     * using the <strong>same</strong> term identifiers (the lexicon is neither
     * copied to nor asserted on the target).
     * <p>
     * Note: This method MUST NOT be used unless it is known in advance that the
     * statements in <i>this</i> store use term identifiers that are consistent
     * with (term for term identical to) those in the destination store.
     * <p>
     * Note: The statements in <i>this</i> store are NOT removed.
     * 
     * @param dst
     *            The persistent database (destination).
     * @param filter
     *            An optional filter to be applied. Statements in <i>this</i>
     *            matching the filter will NOT be copied.
     * @param copyJustifications
     *            When true, the justifications will be copied as well.
     * 
     * @return The #of statements inserted into <i>dst</i> (the count only
     *         reports those statements that were not already in the main
     *         store).
     */
    public int copyStatements(AbstractTripleStore dst, ISPOFilter filter,
            boolean copyJustifications) {

        if (dst == this)
            throw new IllegalArgumentException();

        // obtain a chunked iterator reading from any access path.
        ISPOIterator itr = getAccessPath(KeyOrder.SPO).iterator(filter);
        
        if (!copyJustifications) {
            
            // add statements to the target store.
            return dst.addStatements(itr, null/*filter*/);

        } else {
            
            /*
             * Use a thread pool to write out the statement and the
             * justifications concurrently. This drammatically reduces the
             * latency when also writing justifications.
             */

            List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(2);
            
            /*
             * Note: we reject using the filter before stmts or
             * justifications make it into the buffer so we do not need to
             * apply the filter again here.
             */
           
            // set as a side-effect.
            AtomicInteger nwritten = new AtomicInteger();

            // task will write SPOs on the statement indices.
            tasks.add(new StatementWriter(dst, itr, nwritten));
            
            // task will write justifications on the justifications index.
            AtomicInteger nwrittenj = new AtomicInteger();
            
            if(justify) {

                IJustificationIterator jitr = new JustificationIterator(
                        getJustificationIndex(), 0/* capacity */, true/* async */);
                
                tasks.add(new JustificationWriter(dst, jitr, nwrittenj ));
                
            }
            
            final List<Future<Long>> futures;
            final long elapsed_SPO;
            final long elapsed_JST;
            
            try {

                futures = writeService.invokeAll( tasks );

                elapsed_SPO = futures.get(0).get();
                
                if(justify) {
                
                    elapsed_JST = futures.get(1).get();
                    
                } else {

                    elapsed_JST = 0;
                    
                }

            } catch(InterruptedException ex) {
                
                throw new RuntimeException(ex);
                
            } catch(ExecutionException ex) {
            
                throw new RuntimeException(ex);
            
            }

            log.info("Copied "
                    + nwritten
                    + " statements in "
                    + elapsed_SPO
                    + "ms"
                    + (justify ? (" and " + nwrittenj + " justifications in "
                            + elapsed_JST + "ms") : ""));
            
            return nwritten.get();
            
        }

    }
    
    /**
     * Writes statements on the statement indices.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StatementWriter implements Callable<Long>{

        private final AbstractTripleStore dst;
        private final ISPOIterator itr;
        
        /**
         * Incremented by the #of statements written on the statements indices.
         */
        public final AtomicInteger nwritten;

        /**
         * 
         * @param dst
         *            The database on which the statements will be written.
         * @param itr
         *            The source iterator for the {@link SPO}s to be written.
         * @param nwritten
         *            Incremented by the #of statements written on the statement
         *            indices as a side-effect.
         */
        public StatementWriter(AbstractTripleStore dst, ISPOIterator itr,
                AtomicInteger nwritten) {
        
            this.dst = dst;
            
            this.itr = itr;
            
            this.nwritten = nwritten;

        }
        
        /**
         * Writes on the statement indices (parallel, batch api).
         * 
         * @return The elapsed time for the operation.
         */
        public Long call() throws Exception {
            
            final long begin = System.currentTimeMillis();
            
            nwritten.addAndGet(dst.addStatements(itr,null/*filter*/));
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            return elapsed;

        }
        
    }
    
    /**
     * Writes {@link Justification}s on the justification index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JustificationWriter implements Callable<Long>{

        /**
         * The database on which to write the justifications.
         */
        private final AbstractTripleStore dst;

        /**
         * The source iterator.
         */
        private final IChunkedIterator<Justification> src;

        /**
         * The #of justifications that were written on the justifications index.
         */
        private final AtomicInteger nwritten;
        
        /**
         * 
         * @param dst
         *            The database on which the statements will be written.
         * @param src
         *            The source iterator.
         * @param nwritten
         *            Incremented as a side-effect for each justification
         *            actually written on the justification index.
         */
        public JustificationWriter(AbstractTripleStore dst, IChunkedIterator<Justification> src, AtomicInteger nwritten) {
        
            this.dst = dst;
            
            this.src = src;
            
            this.nwritten = nwritten;
            
        }
        
        /**
         * Write justifications on the justifications index.
         * 
         * @return The elapsed time.
         */
        public Long call() throws Exception {
            
            final long begin = System.currentTimeMillis();
            
            nwritten.addAndGet(dst.addJustifications(src));
            
            final long elapsed = System.currentTimeMillis() - begin;
            
            return elapsed;

        }
        
    }
    
    public int addStatements(SPO[] stmts, int numStmts ) {
       
        if( numStmts == 0 ) return 0;

        return addStatements( new SPOArrayIterator(stmts,numStmts), null /*filter*/);

    }
    
    public int addStatements(SPO[] stmts, int numStmts, ISPOFilter filter ) {
    
        if( numStmts == 0 ) return 0;

        return addStatements( new SPOArrayIterator(stmts,numStmts), filter);
        
    }
    
    public int addStatements(ISPOIterator itr, final ISPOFilter filter) {

        try {
        
            if(!itr.hasNext()) return 0;
        
            final AtomicLong numWritten = new AtomicLong(0);

            /*
             * Note: We process the iterator a "chunk" at a time. If the
             * iterator is backed by an SPO[] then it will all be processed in
             * one "chunk".
             */

            while (itr.hasNext()) {

                final SPO[] a = itr.nextChunk();

                final int numStmts = a.length;

                /*
                 * Note: The statements are inserted into each index in
                 * parallel. We clone the statement[] and sort and bulk load
                 * each index in parallel using a thread pool.
                 */

                long begin = System.currentTimeMillis();

                // time to sort the statements.
                final AtomicLong sortTime = new AtomicLong(0);

                // time to generate the keys and load the statements into the
                // indices.
                final AtomicLong insertTime = new AtomicLong(0);

                /**
                 * Writes an {@link SPO}[] on one of the statement indices.
                 * 
                 * @author <a
                 *         href="mailto:thompsonbry@users.sourceforge.net">Bryan
                 *         Thompson</a>
                 * @version $Id$
                 */
                class IndexWriter implements Callable<Long> {

                    private final SPO[] stmts;

                    private final Comparator<SPO> comparator;

                    private final IIndex ndx;

                    private final KeyOrder keyOrder;

                    /**
                     * Private key builder for the SPO, POS, or OSP keys (one
                     * instance per thread).
                     */
                    private final RdfKeyBuilder keyBuilder = new RdfKeyBuilder(
                            new KeyBuilder(N * Bytes.SIZEOF_LONG));

                    /**
                     * Writes statements on a statement index (batch api).
                     * 
                     * @param clone
                     *            When true the statements are cloned.
                     *            <p>
                     *            Note:One of the {@link IndexWriter}s gets to
                     *            use the caller's array. The others MUST be
                     *            instructed to clone the caller's array so that
                     *            they can impose their distinct sort orders.
                     * @param keyOrder
                     *            Identifies the statement index on which to
                     *            write.
                     * @param filter
                     *            An optional filter.
                     */
                    IndexWriter(boolean clone, KeyOrder keyOrder,
                            ISPOFilter filter) {

                        if (clone) {

                            // copy the caller's data.
                            
                            this.stmts = new SPO[numStmts];

                            System.arraycopy(a, 0, this.stmts, 0, numStmts);

                        } else {

                            // use the callers reference.
                            
                            this.stmts = a;

                        }

                        this.comparator = keyOrder.getComparator();

                        this.ndx = getStatementIndex(keyOrder);

//                        this.keys = new byte[numStmts][];

                        // this.vals = new byte[numStmts][];

                        this.keyOrder = keyOrder;

                    }

                    /**
                     * Write the statements on the appropriate statement index.
                     * <p>
                     * Note: This method is designed to NOT write on the index
                     * unless either the statement is new or the value
                     * associated with the statement has been changed. This
                     * helps to keep down the IO costs associated with index
                     * writes when the data are already in the index.
                     * 
                     * @return The elapsed time for the operation.
                     */
                    public Long call() throws Exception {

                        final long beginIndex = System.currentTimeMillis();

                        { // sort

                            final long _begin = System.currentTimeMillis();

                            Arrays.sort(stmts, 0, numStmts, comparator);

                            sortTime.addAndGet(System.currentTimeMillis()
                                    - _begin);

                        }

                        /*
                         * Generate keys for the statements to be added.
                         * 
                         * Note: This also filters out duplicate statements
                         * (since the data are sorted duplicates will be grouped
                         * together) and, if a filter has been specified, that
                         * filter is used to filter out any matching statements.
                         * 
                         * The outcome is that both keys[] and vals[] are dense
                         * and encode only the statements to be written on the
                         * index. Only the 1st [numToAdd] entries in those
                         * arrays contain valid data.
                         * 
                         * @todo write a unit test in which we verify: (a) the
                         * correct elimination of duplicate statements; (b) the
                         * correct filtering of statements; and (c) the correct
                         * application of the override flag.
                         */

                        int numToAdd = 0;

                        SPO last = null;

                        final byte[][] keys = new byte[numStmts][];

                        final byte[][] vals = new byte[numStmts][];

                        for (int i = 0; i < numStmts; i++) {

                            final SPO spo = stmts[i];

                            // skip statements that match the filter.
                            if (filter != null && filter.isMatch(spo))
                                continue;

                            // skip duplicate records.
                            if (last != null && last.equals(spo))
                                continue;

                            // generate key for the index.
                            keys[numToAdd] = keyBuilder.statement2Key(keyOrder, spo);
                            
                            // generate value for the index.
                            vals[numToAdd] = spo.type.serialize();
                            
                            if(spo.override) {
                                
                                // set the override bit on the value.
                                vals[numToAdd][0] |= StatementEnum.MASK_OVERRIDE;
                                
                            }

                            last = spo;

                            numToAdd++;

                        }
                        
                        /*
                         * Run the batch insert/update logic as a procedure.
                         * 
                         * @todo use efficient compression on the serialized
                         * form when the index is remote (bit coded longs based
                         * on frequency, e.g., hamming or hu-tucker).
                         */
                        final long _begin = System.currentTimeMillis();
                        
                        final IntegerCounterAggregator aggregator = new IntegerCounterAggregator();
                        
                        ndx.submit(numToAdd, keys, vals,
                                new IIndexProcedureConstructor() {

                                    public IIndexProcedure newInstance(int n,
                                            int offset, byte[][] keys,
                                            byte[][] vals) {

                                        return new IndexWriteProc(n, offset,
                                                keys, vals);

                                    }

                                },

                                aggregator

                        );
                        
                        final int writeCount = aggregator.getResult();

                        insertTime.addAndGet(System.currentTimeMillis()
                                - _begin);
                        
                        if (keyOrder == KeyOrder.SPO) {

                            /*
                             * One task takes responsibility for reporting
                             * the #of statements that were written on the
                             * indices.
                             */

                            numWritten.addAndGet(writeCount);

                        }

                        long elapsed = System.currentTimeMillis() - beginIndex;

                        return elapsed;

                    }

                } // class IndexWriter
                
                List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

                tasks.add(new IndexWriter(false/* clone */, KeyOrder.SPO,
                        filter));

                tasks.add(new IndexWriter(true/* clone */, KeyOrder.POS,
                        filter));

                tasks.add(new IndexWriter(true/* clone */, KeyOrder.OSP,
                        filter));

//                if(numStmts>1000) {
//
//                    log.info("Writing " + numStmts + " statements...");
//                    
//                }

                final List<Future<Long>> futures;
                final long elapsed_SPO;
                final long elapsed_POS;
                final long elapsed_OSP;

                try {

                    futures = writeService.invokeAll(tasks);

                    elapsed_SPO = futures.get(0).get();
                    elapsed_POS = futures.get(1).get();
                    elapsed_OSP = futures.get(2).get();

                } catch (InterruptedException ex) {

                    throw new RuntimeException(ex);

                } catch (ExecutionException ex) {

                    throw new RuntimeException(ex);

                }

                long elapsed = System.currentTimeMillis() - begin;

                if (numStmts > 1000) {
                
                    log.info("Wrote " + numStmts + " statements in " + elapsed
                            + "ms; sort=" + sortTime + "ms, keyGen+insert="
                            + insertTime + "ms; spo=" + elapsed_SPO
                            + "ms, pos=" + elapsed_POS + "ms, osp="
                            + elapsed_OSP + "ms");
                    
                }

            }

            return (int) numWritten.get();

        } finally {

            itr.close();

        }

    }
    
    /**
     * Procedure for batch index on a single statement index (or index
     * partition).
     * <p>
     * The key for each statement encodes the {s:p:o} of the statement in the
     * order that is appropriate for the index (SPO, POS, OSP, etc).
     * <p>
     * The value for each statement is a single byte that encodes the
     * {@link StatementEnum} and also encodes whether or not the "override" flag
     * is set.  See {@link SPO#override}.
     * <p>
     * Note: This needs to be a custom batch operation using a conditional
     * insert so that we do not write on the index when the data would not be
     * changed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class IndexWriteProc extends IndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 3969394126242598370L;

        protected AbstractCompression getCompression() {
            
            return FastRDFCompression.INSTANCE;
            
        }
        
        /**
         * De-serialization constructor.
         */
        public IndexWriteProc() {
            
        }
        
        public IndexWriteProc(int n, int offset, byte[][] keys, byte[][] vals) {
            
            super( n, offset, keys, vals );
            
            assert vals != null;
            
        }

        /**
         * 
         * @return The #of statements actually written on the index as an
         *         {@link Integer}.
         */
        public Object apply(IIndex ndx) {

            // #of statements actually written on the index partition.
            int writeCount = 0;

            final int n = getKeyCount();
            
            for (int i = 0; i < n; i++) {

                // the key encodes the {s:p:o} of the statement.
                final byte[] key = getKey(i);
                assert key != null;

                // the value encodes the statement type.
                final byte[] val = getValue(i);
                assert val != null;
                assert val.length == 1;

                // figure out if the override bit is set.
                final boolean override = StatementEnum.isOverride(val[0]);

                /*
                 * Decode the new (proposed) statement type (override bit is
                 * masked off).
                 */
                final StatementEnum newType = StatementEnum.decode(val[0]);

                /*
                 * The current statement type in this index partition (iff the
                 * stmt is defined.
                 */
                final byte[] oldval = (byte[]) ndx.lookup(key);

                if (oldval == null) {

                    /*
                     * Statement is NOT pre-existing.
                     */

                    ndx.insert(key, newType.serialize());

                    writeCount++;

                } else {

                    /*
                     * Statement is pre-existing.
                     */

                    // old statement type.
                    final StatementEnum oldType = StatementEnum
                            .deserialize(oldval);

                    if (override) {

                        if (oldType != newType) {

                            /*
                             * We are downgrading a statement from explicit to
                             * inferred during TM
                             */

                            ndx.insert(key, newType.serialize());

                            writeCount++;

                        }

                    } else {

                        // choose the max of the old and the proposed type.
                        final StatementEnum maxType = StatementEnum.max(
                                oldType, newType);

                        if (oldType != maxType) {

                            /*
                             * write on the index iff the type was actually
                             * changed.
                             */

                            ndx.insert(key, maxType.serialize());

                            writeCount++;

                        }

                    }

                }

            }

            return Integer.valueOf(writeCount);

        }
        
    } // class IndexWriteProcedure

    /**
     * Adds justifications to the store.
     * 
     * @param itr
     *            The iterator from which we will read the {@link Justification}s
     *            to be added. The iterator is closed by this operation.
     * 
     * @return The #of {@link Justification}s written on the justifications
     *         index.
     * 
     * @todo a lot of the cost of loading data is writing the justifications.
     *       SLD/magic sets will relieve us of the need to write the
     *       justifications since we can efficiently prove whether or not the
     *       statements being removed can be entailed from the remaining
     *       statements. Any statement which can still be proven is converted to
     *       an inference. Since writing the justification chains is such a
     *       source of latency, SLD/magic sets will translate into an immediate
     *       performance boost for data load.
     */
    public int addJustifications(IChunkedIterator<Justification> itr) {

        try {

            if (!itr.hasNext())
                return 0;

            final long begin = System.currentTimeMillis();

            /*
             * Note: This capacity estimate is based on N longs per SPO, one
             * head, and 2-3 SPOs in the tail. The capacity will be extended
             * automatically if necessary.
             */

            KeyBuilder keyBuilder = new KeyBuilder(N * (1 + 3)
                    * Bytes.SIZEOF_LONG);

            int nwritten = 0;

            while (itr.hasNext()) {

                final Justification[] a = itr.nextChunk();

                final int n = a.length;

                // sort into their natural order.
                Arrays.sort(a);
                
                final byte[][] keys = new byte[n][];
                
                for (int i=0; i<n; i++) {

                    final Justification jst = a[i];
                    
                    keys[i] = jst.getKey(keyBuilder);

                }

                /*
                 * sort into their natural order.
                 * 
                 * @todo is it faster to sort the Justification[] or the keys[]?
                 * See above for the alternative.
                 */
//                Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);

                final IIndex ndx = getJustificationIndex();

                final IntegerCounterAggregator aggregator = new IntegerCounterAggregator();
                
                ndx.submit(n, keys, null/*vals*/, new IIndexProcedureConstructor(){

                    public IIndexProcedure newInstance(int n,
                                    int offset, byte[][] keys, byte[][] vals) {
                                
                        return new WriteJustificationsProc(n, offset, keys);
                        
                            }
                        }, aggregator);
                
                    nwritten += aggregator.getResult();
                    
            }

            final long elapsed = System.currentTimeMillis() - begin;

            log.info("Wrote " + nwritten + " justifications in " + elapsed
                    + " ms");

            return nwritten;
            
        } finally {

            itr.close();

        }
        
    }

    /**
     * Procedure for writing {@link Justification}s on an index or index
     * partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class WriteJustificationsProc extends IndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = -7469842097766417950L;

        /**
         * De-serialization constructor.
         *
         */
        public WriteJustificationsProc() {
            
            super();
            
        }
        
        public WriteJustificationsProc(int n, int offset, byte[][] keys) {
            
            super(n, offset, keys, null/* vals */);
            
        }
        
        /**
         * @return The #of justifications actually written on the index.
         */
        public Object apply(IIndex ndx) {

            int nwritten = 0;
            
            int n = getKeyCount();
            
            for (int i=0; i<n; i++) {

                byte[] key = getKey( i );

                if (!ndx.contains(key)) {

                    ndx.insert(key, null/* no value */);

                    nwritten++;

                }

            }
            
            return Integer.valueOf(nwritten);
            
        }
        
    }
    
    /**
     * Return true iff there is a grounded justification chain in the database
     * for the {@link SPO}.
     * <p>
     * Note: A grounded justification chain demonstrates the existence of a
     * proof for the {@link SPO}. During truth maintenance, if there is a
     * request to remove an explicit statement from the store AND a grounded
     * justification chain can be identified, then the statement will be
     * converted to an inference rather than being deleted from the store.
     * <p>
     * If justifications are being saved, then each entailed {@link SPO} will
     * have one or more justifications in the {@link #getJustificationIndex()}.
     * An SPO has a grounded justification chain IFF there exists a chain of
     * justifications that grounds out in explicit {@link SPO}s NOT including
     * itself.
     * 
     * @param spo
     *            An {@link StatementEnum#Explicit} SPO.
     * 
     * @return true if there is a grounded justification chain for that
     *         {@link SPO}.
     * 
     * @todo if an explicit statement being removed is an Axiom (test against
     *       the set of RDFS axioms) then it is always converted to an Axiom
     *       rather than searching for a grounded justification chain.
     * 
     * @todo if at all possible make this a set at a time operation.
     * 
     * @todo if we can prove that only grounded justifications are generated by
     *       the "fast" closure method then we do not need to chase the chain.
     *       In this case a statement is grounded if it there are any
     *       justification(s) in the index.
     */
    public boolean getGroundedJustification(SPO spo) {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * The optional index on which {@link Justification}s are stored.
     */
    abstract public IIndex getJustificationIndex();

    /**
     * @todo It might be worth doing a more efficient method for bulk statement
     *       removal. This will wind up doing M * N operations. The N are
     *       parallelized, but the M are not.
     */
    public int removeStatements(ISPOIterator itr) {

        final long begin = System.currentTimeMillis();

        int n = 0;

        try {

            while (itr.hasNext()) {

                SPO[] chunk = itr.nextChunk();

                for (SPO spo : chunk) {

                    n += getAccessPath(spo.s, spo.p, spo.o).removeAll();

                }

            }

        } finally {

            itr.close();
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        log.info("Retracted " + n + " statements in " + elapsed + " ms");

        return n;

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
     * @see #getFullTextIndex()
     * 
     * FIXME Optimize the insert order (sort by the key before insert) and batch
     * indexing.
     * 
     * @todo isolate method to return the 1st 2/3 characters of the language
     *       code (without any embedded whitespace).
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     */
    protected void indexTermText(_Value[] terms, int numTerms) {

        final FullTextIndex ndx = new FullTextIndex(getFullTextIndex());

        final IKeyBuilder keyBuilder = getKeyBuilder().keyBuilder;
        
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

            ndx.index(keyBuilder, val.termId, languageCode, text);

        }

    }

    /**
     * Full text information retrieval for RDF essentially treats the lexical
     * terms in the RDF database (plain and language code {@link Literal}s and
     * possibly {@link URI}s) as "documents." You can understand the items in
     * the terms index as "documents" that are broken down into "token"s to
     * obtain a "token frequency distribution" for that document.  The full
     * text index contains the indexed token data.
     */
    abstract public IIndex getFullTextIndex();

    /**
     * Performs a full text search against literals returning the term
     * identifiers of literals containing tokens parsed from the query. Those
     * term identifiers may be used to join against the statement indices in
     * order to bring back appropriate results.
     * <p>
     * Note: If you want to discover a data typed value, then form the
     * appropriate data typed {@link Literal} and use
     * {@link IRawTripleStore#getTermId(Value)}. Likewise, that method is also
     * more appropriate when you want to lookup a specific {@link URI}.
     * </p>
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
     * @todo the returned iterator should be a chunked iterator. The same
     *       requirement exists for the iterator returned by
     *       {@link AccessPath#distinctTermScan()}.
     * 
     * @todo introduce basic normalization for free text indexing and search
     *       (e.g., of term frequencies in the collection, document length,
     *       etc).
     * 
     * @todo consider other kinds of queries that we might write here. For
     *       example, full text search should support AND OR NOT operators for
     *       tokens. Filtering by {@link Value} type, language code, data type
     *       attributes, or role played in {@link Statement}s is done once you
     *       have the search results.
     */
    public Iterator<Long> textSearch(String languageCode,String text) {
        
        return new FullTextIndex(getFullTextIndex()).textSearch(
                getKeyBuilder().keyBuilder, languageCode, text);
        
    }
    
}
