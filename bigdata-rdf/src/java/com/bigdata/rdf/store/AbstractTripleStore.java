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
import java.io.StringReader;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.BNode;
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
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.LongAggregator;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.BatchContains.BatchContainsConstructor;
import com.bigdata.btree.IDataSerializer.NoDataSerializer;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.IJustificationIterator;
import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.JustificationIterator;
import com.bigdata.rdf.inf.Rule;
import com.bigdata.rdf.inf.SPOAssertionBuffer;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._BNode;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Resource;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.EmptySPOIterator;
import com.bigdata.rdf.spo.IChunkedIterator;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOArrayIterator;
import com.bigdata.rdf.spo.SPOComparator;
import com.bigdata.rdf.store.AddIds.AddIdsConstructor;
import com.bigdata.rdf.store.AddTerms.AddTermsConstructor;
import com.bigdata.rdf.store.IndexWriteProc.FastRDFKeyCompression;
import com.bigdata.rdf.store.IndexWriteProc.FastRDFValueCompression;
import com.bigdata.rdf.store.WriteJustificationsProc.WriteJustificationsProcConstructor;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.search.TokenBuffer;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedDataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store. The
 * {@link ScaleOutTripleStore} supports concurrency and can be used with either
 * local or distributed {@link IBigdataFederation}s. There is also a
 * {@link LocalTripleStore} and a {@link TempTripleStore}, neither of which
 * supports concurrent operations. Also, full text indexing is only available
 * with the {@link ScaleOutTripleStore} since it uses the {@link IBigdataClient}
 * API.
 * 
 * FIXME term prefix scan. write prefix scan procedure (aka distinct term scan).
 * possible use for both the {@link SparseRowStore} and the triple store? do
 * efficent merge join of two such iterators to support backchaining. do term
 * prefix scan in key range so that it can be restricted to just the URIs for
 * backchaining. See {@link AccessPath}.
 * 
 * FIXME Custom joins for the {@link LocalDataServiceFederation} deployment
 * scenario.
 * <p>
 * Tune inference - when computing closure on a store that supports concurrent
 * read/write, for each SPO[] chunk from the 1st triple pattern, create an array
 * of binding[]s and process each binding in parallel.
 * 
 * @todo The only reason to enter {@link BNode} into the foward (term->id) index
 *       is during data load. We can do without bnodes in the forward index also
 *       as long as we (a) keep a hash or btree whose scope is the data load (a
 *       btree would be required for very large files); and (b) we hand the
 *       {@link BNode}s to {@link AddTerms}s but it only assigns a one up term
 *       identifier and does NOT enter the bnode into the index. The local (file
 *       load scope only) bnode resolution is the only context in which it is
 *       possible for two {@link BNode} {@link BNode#getID() ID}s to be
 *       interpreted as the same ID and therefore assigned the same term
 *       identifier. In all other cases we will assign a new term identifier.
 *       The assignment of the term identifier for a BNode ID can be from ANY
 *       key-range partition of the term:id index.
 * 
 * @todo Do the quad store. Make some of the indices optional such that we still
 *       have good access patterns but store less data when the quad is used as
 *       a statement identifier (by always using a distinct {@link BNode} as its
 *       value). There is no need for a pure triple store and the "bnode" trick
 *       can be used to model eagerly assigned statement identifiers. One
 *       question is whether the quad position can be left open (0L aka NULL).
 *       It seems that SPARQL might allow this, in which case we can save a lot
 *       of term identifiers that would otherwise be gobbled up eagerly. The
 *       quad position could of course be bound to some constant, in which case
 *       it would not be a statement identifier - more the context model.
 * 
 * @todo sesame 2.x TCK (technology compatibility kit).
 * 
 * @todo retest on 1B+ triples (single host single threaded and concurrent data
 *       load and scale-out architecture with concurrent data load and policy
 *       for deciding which host loads which source files).
 * 
 * @todo test on 10, 20, 40, ... nodes using dynamic partitioning and scale-out
 *       indices.
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions and index access.
 * 
 * @todo examine role for semi joins (join indices) - these can be declared for
 *       various predicate combinations and then maintained, effectively by
 *       rules using the existing TM mechanisms.
 * 
 * @todo There are two basic ways in which we are considering extending the
 *       platform to make statement identifiers useful.
 *       <ol>
 *       <li>Transparently convert ontology and data that uses RDF style
 *       reification such that it uses statement identifiers instead. There are
 *       a few drawbacks with this approach. One is that you still have to write
 *       high level queries in terms of RDF style reification, which means that
 *       we have not reduced the burden on the user significantly. There is also
 *       the possibility that the change would effect the semantics of RDF
 *       containers, or at least the relationship between the semantics of RDF
 *       containers and RDF style reification. The plus side of this approach is
 *       that the change could be completely transparent, but I am not sure that
 *       keeping RDF reification in any form - and especially in the queries -
 *       is a good idea.</li>
 *       <li>The other approach is to provide an extension of RDF/XML in which
 *       statements may be made and a high-level query language extension in
 *       which statement identifiers may be exploited to efficiently recover
 *       statements about statements. This approach has the advantage of being
 *       completely decoupled from the RDF reification semantics - it is a pure
 *       superset of the RDF data model and query language. Also, in this
 *       approach we can guarentee that the statement identifiers are purely an
 *       internal convenience of the database - much like blank nodes. In fact,
 *       statement identifiers can be created lazily - when there is a need to
 *       actually make a statement about a specific statement. </li>
 *       </ol>
 *       The second is where we are putting our effort right now. We are looking
 *       at doing this as a "database mode" which understands that there is a
 *       1:1 correspondence between a triple and a BNode used to identify the
 *       source in a named graph interchange syntax and that the default graph
 *       exposed via SPARQL is the RDF merge of the named graphs, and is
 *       leanified under that 1:1 correspondence assumption. All of which is to
 *       say that graph identifiers must be bnodes and have the semantics of
 *       statement identifiers in that database mode. You can then write a
 *       SPARQL query which uses a graph variable binding to recover statement
 *       metadata with resort to RDF style reification (aka statement models).
 *       <p>
 *       Statement identifiers are assigned when you add statements to the
 *       database, whether the statements are read from some RDF interchange
 *       syntax or added directly using
 *       {@link #addStatements(ISPOIterator, ISPOFilter)} and friends.  
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
     * The branching factor for indices registered by this class.
     * 
     * @see com.bigdata.journal.Options#BRANCHING_FACTOR
     */
    final private int branchingFactor;
    
    /**
     * The #of term identifiers in the key for a statement index (3 is a triple
     * store, 4 is a quad store).
     * 
     * @see #statementIdentifiers
     */
    final public int N = IRawTripleStore.N;
    
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
     * @todo add property for the prefix so that you can have more than on
     *       triple/quad store in the same database (the {@link ScaleOutTripleStore} 
     *       ctor already accepts a namespace parameter.)
     * 
     * @todo add property for triple vs quad variants.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends InferenceEngine.Options,
            com.bigdata.journal.Options, KeyBuilder.Options,
            DataLoader.Options, FullTextIndex.Options {

        /**
         * Boolean option (default <code>true</code>) enables support for the
         * lexicon (the forward and backward term indices). When
         * <code>false</code>, this option disables Unicode support for the
         * {@link RdfKeyBuilder} and causes the lexicon indices to not be
         * registered. This can be safely turned off for the
         * {@link TempTripleStore} when only the statement indices are to be
         * used.
         */
        String LEXICON = "lexicon"; 

        String DEFAULT_LEXICON = "true"; 

        /**
         * Boolean option (default <code>false</code>) disables all but a
         * single statement index (aka access path).
         * <p>
         * Note: The main purpose of the option is to make it possible to turn
         * off the other access paths for special bulk load purposes. The use of
         * this option is NOT compatible with either the application of the
         * {@link InferenceEngine} or high-level query.
         */
        String ONE_ACCESS_PATH = "oneAccessPath";

        String DEFAULT_ONE_ACCESS_PATH = "false";
        
        /**
         * Boolean option (default <code>true</code>) enables support for a
         * full text index that may be used to lookup literals by tokens found
         * in the text of those literals.
         */
        String TEXT_INDEX = "textIndex";

        String DEFAULT_TEXT_INDEX = "true";

        /**
         * Boolean option (default <code>false</code>) enables support for
         * statement identifiers. A statement identifier is unique to a
         * <em>triple</em> (regardless of the graph in which that triple may
         * be found). Statement identifiers may be used to make statements about
         * statements without using RDF style reification.
         * <p>
         * Statement identifers are assigned consistently when {@link Statement}s
         * are mapped into the database. This is done using an extension of the
         * <code>term:id</code> index to map the statement as if it were a
         * term onto a unique statement identifier. That statement identifier is
         * in fact a term identifier, and the identified statement may be
         * resolved against the <code>id:term</code> index. While the
         * statement identifier is assigned canonically by the
         * <code>term:id</code> index, it is stored redundently in the value
         * position for each of the statement indices.
         * <p>
         * Statement identifiers add latency when loading data since it
         * increases the size of the writes on the terms index (and also its
         * space requirements since all statements are also replicated in the
         * terms index). However, if you are doing concurrent data load then the
         * added latency is nicely offset by the parallelism.
         * 
         * @see ITermIndexCodes#TERM_CODE_STMT
         * 
         * @todo A design alternative is to use the SPO index to assign
         *       statement identifiers and to NOT store them in the value
         *       positions on the other indices (POS, OSP). The advantage of
         *       this approach is less redundency (the statement is not present
         *       in the forward and reverse indices for the lexicon is not
         *       present in the other statement indices) - the drawback is not
         *       having the statement identifier "on hand" regardless of the
         *       index on which you are reading.
         *       <p>
         *       Regardless of whether the statement identifier is assigned by
         *       the lexicon or the SPO index, the presence of source markers in
         *       RDF/XML necessitates a two stage process to (a) resolve the
         *       source Resource {URI or BNode} to the statement identifier; and
         *       (b) utilize that statement identifier as the resolved "term
         *       identifier" for other statements in the RDF/XML document with a
         *       reference to the same Resource.
         *       <p>
         *       For example:
         * 
         * <pre>
         *            (joe, loves, mary):_a.
         *            (_a, reportedBy, tom).
         * </pre>
         * 
         * where _a is a BNode identifying the source for (joe, loves, mary).
         * The second statement uses _a to add provenance for the assertion
         * (joe, loves, mary) - namely that the assertion was reported by tom.
         */
        String STATEMENT_IDENTIFIERS = "statementIdentifiers";

        String DEFAULT_STATEMENT_IDENTIFIERS = "true";
        
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

        // Explicitly disable overwrite for the lexicon.
        this.properties.setProperty(Options.OVERWRITE,"false");
        
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

        // Note: the full text index is not allowed unless the lexicon is enabled.
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

        /*
         * branchingFactor.
         */
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
     * store whose index access was intermediated by the executor service of an
     * {@link IConcurrencyManager}.
     */
    abstract public boolean isConcurrent();
    
    /**
     * Disconnect from and drop the {@link ITripleStore}.
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
     * Default is a NOP - invoked by {@link #close()} and {@link #closeAndDelete()}
     */
    protected void shutdown() {
        
    }
    
    /**
     * True iff the backing store is stable (exists on disk somewhere and may be
     * closed and re-opened).
     * <p>
     * Note: This is mainly used by the test suites.
     */
    abstract public boolean isStable();

    /**
     * Return a thread-local {@link RdfKeyBuilder} The object will be compatible
     * with the Unicode preferences that are in effect for the
     * {@link ITripleStore}.
     */
    final public RdfKeyBuilder getKeyBuilder() {
     
        return threadLocalKeyBuilder.get();
        
    }
    
    // Note: not static since we need configuration properties.
    private ThreadLocal<RdfKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<RdfKeyBuilder>() {

        protected synchronized RdfKeyBuilder initialValue() {

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
     * A service used to run operations in parallel.
     * 
     * @todo use for parallel execution of map of new vs old+new over the terms
     *       of a rule.
     * 
     * @todo use for parallel execution of sub-queries.
     * 
     * @todo {@link SPOAssertionBuffer} must be thread-safe (or writes must
     *       occur after threads join at a barrier). {@link Rule} bindings must
     *       be per-thread (e.g., up to a chunk at a time processing with a
     *       thread per SPO in the chunk).
     * 
     * @todo Note that rules that write entailments on the database MUST
     *       coordinate to avoid concurrent modification during traversal of the
     *       statement indices. The chunked iterators go a long way to
     *       addressing this. The issue is completely resolved by the
     *       {@link ConcurrencyManager} bundled in the {@link Journal} or
     *       {@link DataService} (any federation), but you have to submit index
     *       procedures or tasks for the {@link ConcurrencyManager} to work
     *       rather than directly accessing the unisolated indices on the
     *       {@link Journal}.
     */
    abstract public ExecutorService getThreadPool();
    
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

    /**
     * Shared {@link IndexMetadata} configuration.
     * 
     * @param name
     *            The index name.
     *            
     * @return A new {@link IndexMetadata} object for that index.
     */
    protected IndexMetadata getIndexMetadata(String name) {
        
        IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
        
        metadata.setBranchingFactor(branchingFactor);
        
        return metadata;
            
    }

    /**
     * Overrides for the {@link IRawTripleStore#getTermIdIndex()}.
     */
    protected IndexMetadata getTermIdIndexMetadata(String name) {

        final IndexMetadata metadata = getIndexMetadata(name);

        return metadata;

    }
    
    /**
     * Overrides for the {@link IRawTripleStore#getIdTermIndex()}.
     */
    protected IndexMetadata getIdTermIndexMetadata(String name) {
            
        final IndexMetadata metadata = getIndexMetadata(name);

        return metadata;
        
    }
    
    /**
     * Overrides for the statement indices.
     */
    protected IndexMetadata getStatementIndexMetadata(String name) {

        final IndexMetadata metadata = getIndexMetadata(name);

        metadata.setLeafKeySerializer(FastRDFKeyCompression.N3);

        if(!statementIdentifiers) {

            /*
             * @todo this value serializer does not know about statement
             * identifiers. Therefore it is turned off if statement identifiers
             * are enabled. Examine some options for value compression for the
             * statement indices when statement identifiers are enabled.
             */
            
            metadata.setValueSerializer(new FastRDFValueCompression());
            
        }

        return metadata;
        
    }

    /**
     * Overrides for the {@link IRawTripleStore#getJustificationIndex()}.
     */
    protected IndexMetadata getJustIndexMetadata(String name) {
            
        final IndexMetadata metadata = getIndexMetadata(name);
        
        metadata.setValueSerializer(NoDataSerializer.INSTANCE);

        return metadata;
        
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

    final public long getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    final public long getStatementCount(boolean exact) {
        
        final IIndex ndx = getSPOIndex();
        
        if( exact && ndx.getIndexMetadata().getDeleteMarkers() ) {
            
            /*
             * The use of delete markers means that index entries are not
             * removed immediately but rather a delete flag is set. This is
             * always true for the scale-out indices because delete markers are
             * used to support index partition views. It is also true for
             * indices that can support transactions regardless or whether or
             * not the database is using scale-out indices. In any case, if you
             * want an exact range count when delete markers are in use then you
             * need to actually visit every tuple in the index, which is what
             * this code does. Note that the [flags] are 0 since we do not need
             * either the KEYS or VALS. We are just interested in the #of tuples
             * that the iterator is willing to visit.
             */
            
            long n = 0L;
            
            final Iterator itr = ndx.rangeIterator(null/* fromKey */, null/* toKey */,
                    0/* capacity */, 0/* flags */, null/* filter */);
            
            while(itr.hasNext()) {
                
                n++;
                
            }
            
            return n;
            
        }
        
        /*
         * Either an exact count is not required or delete markers are not in
         * use and therefore rangeCount() will report the exact count.
         */
        
        return ndx.rangeCount(null, null);
        
    }
    
    final public long getJustificationCount() {
        
        if(justify) {
            
            return getJustificationIndex().rangeCount(null, null);
            
        }
        
        return 0;
        
    }
    
    final public long getTermCount() {

        byte[] fromKey = new byte[] { KeyBuilder.encodeByte(RdfKeyBuilder.TERM_CODE_URI) };

        /*
         * Note: the term count deliberately excludes the statement identifiers
         * which follow the bnodes in the lexicon.
         */
        byte[] toKey = new byte[] { KeyBuilder.encodeByte((byte)(RdfKeyBuilder.TERM_CODE_BND+1)) };

        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public long getURICount() {
        
        byte[] fromKey = new byte[] { KeyBuilder.encodeByte(RdfKeyBuilder.TERM_CODE_URI) };

        byte[] toKey = new byte[] { KeyBuilder.encodeByte((byte)(RdfKeyBuilder.TERM_CODE_URI+1))};
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    final public long getLiteralCount() {
        
        // Note: the first of the kinds of literals (plain).
        byte[] fromKey = new byte[] { KeyBuilder.encodeByte(RdfKeyBuilder.TERM_CODE_LIT) };

        // Note: spans the last of the kinds of literals.
        byte[] toKey = new byte[] { KeyBuilder.encodeByte((byte)(RdfKeyBuilder.TERM_CODE_DTL+1))};
                
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    // @todo also statement identifer count.
    final public long getBNodeCount() {
        
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
    
        addTerms(terms, 1);
            
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
     * @todo add a batch variant accepting long[] termIds or an IChunkedIterator
     *       whose generic type is Long.
     */
    final public _Value getTerm(long id) {

        if (id == NULL)
            throw new IllegalArgumentException("NULL");

        if (isStatement(id)) {

            /*
             * Statement identifiers are not stored in the reverse lexicon (or
             * the cache).
             * 
             * A statement identifier is externalized as a BNode. The "_S"
             * prefix is a syntactic marker for those in the know to indicate
             * that the BNode corresponds to a statement identifier.
             */

            return (_BNode) OptimizedValueFactory.INSTANCE
                    .createBNode("_S"+ id);

        }

        if (isBNode(id)) {

            /*
             * BNodes are not stored in the reverse lexicon (or the cache).
             */

            return (_BNode) OptimizedValueFactory.INSTANCE
                    .createBNode("_" + id);
            
        }
        
        _Value value = termCache.get(id);
        
        if (value != null) {

            return value;
            
        }
        
        final IIndex ndx = getIdTermIndex();
        
        final IKeyBuilder keyBuilder = getKeyBuilder().keyBuilder;//new KeyBuilder(Bytes.SIZEOF_LONG);
        
        // Note: shortcut for keyBuilder.id2key(id)
        final byte[] key = keyBuilder.reset().append(id).getKey();
        
        final byte[] data = ndx.lookup( key );

        if (data == null) {

            return null;
            
        }

        value = _Value.deserialize(data);
        
        synchronized(termCache) {

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
        final byte[] tmp = ndx.lookup(val.key);
        
        if (tmp == null) {

            return IRawTripleStore.NULL;
            
        }
        
        try {

            val.termId = new DataInputBuffer(tmp).unpackLong();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
            
        // was found in the forward mapping (@todo if known means in both mappings then DO NOT set it here)
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
    public void addTerms(final _Value[] terms, final int numTerms) {
        
        if (numTerms == 0)
            return;

        final long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
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
                
                // per-thread key builder.
                final RdfKeyBuilder keyBuilder = getKeyBuilder();

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
                termIdIndex.submit(0/* fromIndex */, numTerms/* toIndex */, keys,
                        null/* vals */, AddTermsConstructor.INSTANCE,
                        new IResultHandler<AddTerms.Result, Void>() {

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
             * This is required in order to guarentee that the reverse index
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
             * 
             * Note: We DO NOT write BNodes on the reverse index.
             */
            final byte[][] keys = new byte[numTerms][];
            final byte[][] vals = new byte[numTerms][];
            int nonBNodeCount = 0; // #of non-bnodes.
            {

                // thread-local key builder removes single-threaded constraint.
                final IKeyBuilder tmp = getKeyBuilder().keyBuilder; //new KeyBuilder(Bytes.SIZEOF_LONG); 

                // buffer is reused for each serialized term.
                final DataOutputBuffer out = new DataOutputBuffer();
                
                for(int i=0; i<numTerms; i++) {
                    
                    if(terms[i] instanceof BNode) {
                        
                        terms[i].known = true;
                        
                        continue;
                        
                    }
                    
                    keys[nonBNodeCount] = tmp.reset().append(terms[i].termId).getKey();
                    
                    // Serialize the term.
                    vals[nonBNodeCount] = terms[i].serialize(out.reset());

                    nonBNodeCount++;
                    
                }
                
            }

            // run the procedure on the index.
            if (nonBNodeCount > 0) {
                
                idTermIndex.submit(0/* fromIndex */,
                        nonBNodeCount/* toIndex */, keys, vals,
                        AddIdsConstructor.INSTANCE,
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
         * Index the terms for keyword search.
         */
        if(textIndex && getSearchEngine() != null) {
            
            final long _begin = System.currentTimeMillis();

            indexTermText(terms, numTerms);

            indexerTime = System.currentTimeMillis() - _begin;
            
        }
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        if (numTerms > 1000 || elapsed > 3000) {

            log.info("Wrote " + numTerms + " in " + elapsed + "ms; keygen="
                    + keyGenTime + "ms, sort=" + sortTime + "ms, insert="
                    + insertTime + "ms"+", indexerTime="+indexerTime+"ms");
            
        }
        
    }

    /**
     * Assign unique statement identifiers to triples.
     * <p>
     * Each distinct {s,p,o} is assigned a unique statement identifier using the
     * {@link IRawTripleStore#getTermIdIndex()}. The assignment of statement
     * identifiers is <i>consistent</i> using an unisolated atomic write
     * operation similar to {@link #addTerms(_Value[], int)}.
     * <p>
     * Note: Statement identifiers are NOT inserted into the reverse (id:term)
     * index. Instead, they are written into the values associated with the
     * {s,p,o} in each of the statement indices. That is handled by
     * {@link #addStatements(AbstractTripleStore, ISPOIterator, ISPOFilter)},
     * which is also responsible for invoking this method in order to have the
     * statement identifiers on hand before it writes on the statement indices.
     * <p>
     * Note: The caller's {@link SPO}[] is sorted into SPO order as a
     * side-effect.
     * <p>
     * Note: The statement identifiers are assigned to the {@link SPO}s as a
     * side-effect.
     * 
     * @throws UnsupportedOperationException
     *             if {@link Options#STATEMENT_IDENTIFIERS} was not specified.
     * 
     * @todo use #termCache to shortcut recently used statements?
     */
    protected void addStatementIdentifiers(final SPO[] a, final int n) {

        if (!statementIdentifiers)
            throw new UnsupportedOperationException();

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
         * Note: the advantage of sorting before we generate the keys is that
         * the keys and the SPO[] remain in a correlated order. This makes it
         * trivial to assign the statement identifier to the correct SPO below.
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
         */
        final byte[][] keys = new byte[n][];
            
        /*
         * Generate the sort keys for the term:id index.
         */
        {

            final long _begin = System.currentTimeMillis();

            // per-thread key builder.
            final RdfKeyBuilder keyBuilder = getKeyBuilder();

            for (int i = 0; i < n; i++) {

                SPO spo = a[i];

                keys[i] = keyBuilder.keyBuilder.reset() //
                        .append(ITermIndexCodes.TERM_CODE_STMT)//
                        .append(spo.s).append(spo.p).append(spo.o)//
                        .getKey()//
                        ;
                
            }

            keyGenTime = System.currentTimeMillis() - _begin;

        }

//            /*
//             * Sort by the assigned sort keys. This places the data into the
//             * natural order for the term:id index.
//             */
//            {
//
//                long _begin = System.currentTimeMillis();
//
//                Arrays.sort(keys, 0, numTerms,
//                        UnsignedByteArrayComparator.INSTANCE);
//
//                sortTime += System.currentTimeMillis() - _begin;
//
//            }

        /*
         * Execute a remote unisolated batch operation that assigns the
         * statement identifier.
         */
        {

            final long _begin = System.currentTimeMillis();

            final IIndex termIdIndex = getTermIdIndex();

            // run the procedure.
            termIdIndex.submit(0/* fromIndex */, n/* toIndex */, keys,
                    null/* vals */, AddTermsConstructor.INSTANCE,
                    new IResultHandler<AddTerms.Result, Void>() {

                        /**
                         * Copy the assigned/discovered statement identifiers
                         * onto the corresponding elements of the SPO[].
                         */
                        public void aggregate(AddTerms.Result result,
                                Split split) {

                            for (int i = split.fromIndex, j = 0; i < split.toIndex; i++, j++) {

                                a[i].setStatementIdentifier(result.ids[j]);

                            }

                        }

                        public Void getResult() {

                            return null;

                        }

                    });

            insertTime = System.currentTimeMillis() - _begin;

        }

        final long elapsed = System.currentTimeMillis() - begin;
        
        if (n > 1000 || elapsed > 3000) {

            log.info("Wrote " + n + " in " + elapsed + "ms; keygen="
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

        final byte[] key = getKeyBuilder().statement2Key(s, p, o);

        final byte[] val = getStatementIndex(KeyOrder.SPO).lookup(key);

        if (val == null) {

            return null;

        }

        // The statement is known to the database.

        final StatementEnum type = StatementEnum.deserialize(val);

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

    // @todo modify to use a batch variant.
    public Statement asStatement(SPO spo) {

        final _Resource s = (_Resource) getTerm(spo.s);
        final _URI      p = (_URI)      getTerm(spo.p);
        final _Value    o = (_Value)    getTerm(spo.o);
        
        return new StatementWithType( //
                (Resource) OptimizedValueFactory.INSTANCE.toSesameObject(s),//
                (URI) OptimizedValueFactory.INSTANCE.toSesameObject(p), //
                (Value) OptimizedValueFactory.INSTANCE.toSesameObject(o), //
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
        
        return new AccessPath(this,KeyOrder.get(s,p,o),s,p,o);
        
    }
    
    final public IAccessPath getAccessPath(KeyOrder keyOrder) {
        
        return new AccessPath(this,keyOrder,NULL,NULL,NULL);
        
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
     * Return true iff the term identifier is marked as a RDF {@link Literal}.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a {@link Literal}.
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
     * @return <code>true</code> iff the term identifier is marked as an RDF {@link Literal}.
     */
    static final public boolean isLiteral( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_LITERAL;
        
    }

    /**
     * Return true iff the term identifier is marked as a RDF {@link BNode}.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a {@link BNode}.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is marked as an RDF
     *         {@link BNode}.
     */
    static final public boolean isBNode( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_BNODE;
        
    }

    /**
     * Return true iff the term identifier is marked as a RDF {@link URI}.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a {@link URI}.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is marked as an RDF
     *         {@link URI}.
     */
    static final public boolean isURI( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_URI;
        
    }

    /**
     * Return true iff the term identifier identifies a statement (this feature
     * is enabled with {@link Options#STATEMENT_IDENTIFIERS}).
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is actually a statement
     * identifier.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier identifies a
     *         statement.
     */
    static final public boolean isStatement( long termId ) {
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_STATEMENT;
        
    }

    final public String toString( long termId ) {

        if (termId == NULL)
            return "NULL";

        final _Value v = getTerm(termId);

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
            
            long n = getAccessPath(NULL, p, NULL).rangeCount();
            
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

        final long nstmts = getStatementCount();

        long nexplicit = 0;
        long ninferred = 0;
        long naxioms = 0;

        {

            ITupleIterator itr = getSPOIndex().rangeIterator(null, null);

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
        
        if(justifications && justify) {
            
            IIndex ndx = getJustificationIndex();
            
            ITupleIterator itrj = ndx.rangeIterator(null, null);
            
            while(itrj.hasNext()) {
                
                Justification jst = new Justification(itrj);
                
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
    @SuppressWarnings("unchecked")
    public Iterator<Value> idTermIndexScan() {

        final IIndex ndx = getIdTermIndex();

        return new Striterator(ndx.rangeIterator(null, null, 0/* capacity */,
                IRangeQuery.VALS, null/* filter */)).addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * @param val
             *            the serialized term.
             */
            protected Object resolve(Object val) {
                
                ITuple tuple = (ITuple)val;
                
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

        final IIndex ndx = getTermIdIndex();

        return new Striterator(ndx.rangeIterator(null, null, 0/* capacity */,
                IRangeQuery.VALS, null/* filter */))
                .addFilter(new Resolver() {

            private static final long serialVersionUID = 1L;

            /**
             * Deserialize the term identifier (packed long integer).
             * 
             * @param val The serialized term identifier.
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
    final public String usage(String name, IIndex ndx) {
        
        if (ndx == null) {
            
            return name+" : not used";
            
        }
        
        return name + " : "+ndx.getStatistics();
        
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
     * with (term for term identical to) those in the destination store.  If
     * statement identifiers are enabled, then they MUST be enabled for both
     * stores (statement identifiers are stored in the foward lexicon).
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
        final ISPOIterator itr = getAccessPath(KeyOrder.SPO).iterator(filter);
        
        if (!copyJustifications) {
            
            // add statements to the target store.
            return dst.addStatements(dst,true/*copyOnly*/,itr, null/*filter*/);

        } else {
            
            /*
             * Use a thread pool to write out the statement and the
             * justifications concurrently. This drammatically reduces the
             * latency when also writing justifications.
             */

            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(2);
            
            /*
             * Note: we reject using the filter before stmts or
             * justifications make it into the buffer so we do not need to
             * apply the filter again here.
             */
           
            // set as a side-effect.
            final AtomicInteger nwritten = new AtomicInteger();

            // task will write SPOs on the statement indices.
            tasks.add(new StatementWriter(this, dst, true/* copyOnly */, itr,
                    nwritten));
            
            // task will write justifications on the justifications index.
            final AtomicLong nwrittenj = new AtomicLong();
            
            if(justify) {

                final IJustificationIterator jitr = new JustificationIterator(
                        getJustificationIndex(), 0/* capacity */, true/* async */);
                
                tasks.add(new JustificationWriter(dst, jitr, nwrittenj ));
                
            }
            
            final List<Future<Long>> futures;
            final long elapsed_SPO;
            final long elapsed_JST;
            
            try {

                futures = getThreadPool().invokeAll( tasks );

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
     * Filter the supplied set of SPO objects for whether they are "present" or
     * "not present" in the database, depending on the value of the supplied
     * boolean variable.
     * <p>
     * Note: This is different from using an ISPOFilter, as the point tests
     * for existence in this method will be done in bulk using 
     * {@link IIndex#submit(int, int, byte[][], byte[][], com.bigdata.btree.AbstractIndexProcedureConstructor, IResultHandler)}.
     * 
     * @param stmts 
     *          the statements to test
     * @param numStmts 
     *          the number of statements to test
     * @param present 
     *          if true, filter for statements that exist in the db, otherwise
     *          filter for statements that do not exist
     * 
     * @return an iteration over the filtered set of statements
     */
    public ISPOIterator bulkFilterStatements(SPO[] stmts, int numStmts, boolean present ) {
        
        if( numStmts == 0 ) return new EmptySPOIterator(KeyOrder.SPO);

        return bulkFilterStatements(new SPOArrayIterator(stmts,numStmts), present);
        
    }

    /**
     * Filter the supplied set of SPO objects for whether they are "present" or
     * "not present" in the database, depending on the value of the supplied
     * boolean variable.
     * <p>
     * Note: This is different from using an ISPOFilter, as the point tests
     * for existence in this method will be done in bulk using 
     * {@link IIndex#submit(int, int, byte[][], byte[][], com.bigdata.btree.AbstractIndexProcedureConstructor, IResultHandler)}.
     * 
     * @param itr
     *          an iterator over the set of statements to test
     * @param present
     *          if true, filter for statements that exist in the db, otherwise
     *          filter for statements that do not exist
     * 
     * @return an iteration over the filtered set of statements
     */
    public ISPOIterator bulkFilterStatements(ISPOIterator itr, boolean present) {

        try {
        
            IIndex index = getSPOIndex();
            RdfKeyBuilder keyBuilder = new RdfKeyBuilder(new KeyBuilder());
            
            SPO[] stmts = new SPO[0];
            int numStmts = 0;
            
            /*
             * Note: We process the iterator a "chunk" at a time. If the
             * iterator is backed by an SPO[] then it will all be processed in
             * one "chunk".
             */

            while (itr.hasNext()) {
                
                // get the next chunk
                final SPO[] chunk = itr.nextChunk(KeyOrder.SPO);
                
                // create an array of keys for the chunk
                final byte[][] keys = new byte[chunk.length][];
                for(int i = 0; i < chunk.length; i++) {
                    keys[i] = keyBuilder.statement2Key(KeyOrder.SPO, chunk[i]);
                }
                
                // create an IResultHandler that can aggregate ResultBitBuffer objects
                final IResultHandler<ResultBitBuffer, ResultBitBuffer> resultHandler = 
                    new IResultHandler<ResultBitBuffer, ResultBitBuffer>() {
                    
                    private boolean[] results = new boolean[keys.length];
                    
                    public void aggregate(ResultBitBuffer result, Split split) {
                        System.arraycopy(result.getResult(), 0, results, split.fromIndex, split.ntuples);
                    }
                    
                    public ResultBitBuffer getResult() {
                        return new ResultBitBuffer(results.length, results);
                    }
                };
                
                // submit the batch contains procedure to the SPO index
                index.submit
                    (0/*fromIndex*/,keys.length/*toIndex*/, keys, null/*vals*/,
                     BatchContainsConstructor.INSTANCE, resultHandler
                     );
                
                // get the array of existence test results
                boolean[] contains = resultHandler.getResult().getResult();
                
                // filter in or out, depending on the present variable
                int chunkSize = chunk.length;
                int j = 0;
                for(int i = 0; i < chunk.length; i++) {
                    if(contains[i]==present) {
                        chunk[j] = chunk[i];
                        j++;
                    } else {
                        chunkSize--;
                    }
                }
                
                // aggegate this chunk's results to the return value
                SPO[] temp = new SPO[numStmts+chunkSize];
                System.arraycopy(stmts, 0, temp, 0, numStmts);
                System.arraycopy(chunk, 0, temp, numStmts, chunkSize);
                stmts = temp;
                numStmts += chunkSize;
                
            }

            return new SPOArrayIterator(stmts,numStmts);
            
        } finally {

            itr.close();

        }

    }

    public int addStatements(SPO[] stmts, int numStmts) {

        if (numStmts == 0)
            return 0;

        return addStatements(new SPOArrayIterator(stmts, numStmts), null /* filter */);

    }

    public int addStatements(SPO[] stmts, int numStmts, ISPOFilter filter) {

        if (numStmts == 0)
            return 0;

        return addStatements(new SPOArrayIterator(stmts, numStmts), filter);
        
    }
    
    public int addStatements(final ISPOIterator itr, final ISPOFilter filter) {
        
        return addStatements(this/* statementStore */, false/* copyOnly */, itr,
                filter);
        
    }

    /**
     * Add statements to the <i>statementStore</i>.
     * <p>
     * Note: If {@link Options#STATEMENT_IDENTIFIERS} was specified, then
     * statement identifiers are assigned using the lexicon associated with
     * <i>this</i> database. This is done in a pre-procerssing stage for each
     * "chunk" reported by the source <i>itr</i>. This step sets the statement
     * identifier on the {@link SPO} so that it is present when we write on the
     * statement indices.
     * 
     * @param statementStore
     *            Either <i>this</i> database or the focusStore (the latter
     *            option is used only during truth maintenance).
     * @param copyOnly
     *            When <code>true</code>, it is assumed that the {@link SPO}s
     *            are being copied from another store using a consistent lexicon
     *            (or onto a store that uses the same lexicon). The flag only
     *            has an effect when statement identifiers are enabled, since it
     *            is then presume that {@link SPO#getStatementIdentifier()} will
     *            return a pre-assigned statement identifier and that we do NOT
     *            need to invoke {@link #addStatementIdentifiers(SPO[], int)}.
     *            This is only an optimization - the value <code>false</code>
     *            is always safe for this flag, but it will do some extra work
     *            in the case described here. See {@link StatementWriter},
     *            which uses this flag and
     *            {@link #copyStatements(AbstractTripleStore, ISPOFilter, boolean)}
     *            which always specifies <code>true</code> for this flag.
     * @param itr
     *            The source from which the {@link SPO}s are read.
     * @param filter
     *            An optional filter.
     * 
     * @return The #of statements that were written on the indices (a statement
     *         that was previously an axiom or inferred and that is converted to
     *         an explicit statement by this method will be reported in this
     *         count as well as any statement that was not pre-existing in the
     *         database).
     */
    public int addStatements(final AbstractTripleStore statementStore,
            final boolean copyOnly, final ISPOIterator itr,
            final ISPOFilter filter) {

        if (statementStore == null)
            throw new IllegalArgumentException();
        
        if (itr == null)
            throw new IllegalArgumentException();
        
        try {

            if (!itr.hasNext())
                return 0;

            final AtomicLong numWritten = new AtomicLong(0);

            /*
             * Note: We process the iterator a "chunk" at a time. If the
             * iterator is backed by an SPO[] then it will all be processed in
             * one "chunk".
             */

            while (itr.hasNext()) {

                final SPO[] a = itr.nextChunk();

                final int numStmts = a.length;

                final long statementIdentifierTime;
                
                if(statementIdentifiers && ! copyOnly) {
                    
                    final long begin = System.currentTimeMillis();
                    
                    /*
                     * Note: the statement identifiers are always assigned by
                     * the database. During truth maintenance, the
                     * [statementStore] is NOT the database but the statement
                     * identifiers are still assigned by the database. The
                     * situation is exactly parallel to the manner in which term
                     * identifiers are always assigned by the database. This is
                     * done to ensure that they remain consistent between the
                     * focusStore using by truth maintenance and the database.
                     */
                    addStatementIdentifiers( a, numStmts );
                    
                    statementIdentifierTime = System.currentTimeMillis() - begin;
                    
                } else statementIdentifierTime = 0L;
                
                /*
                 * Note: The statements are inserted into each index in
                 * parallel. We clone the statement[] and sort and bulk load
                 * each statement index in parallel using a thread pool.
                 */

                final long begin = System.currentTimeMillis();

                // time to sort the statements.
                final AtomicLong sortTime = new AtomicLong(0);

                // time to generate the keys and load the statements into the
                // indices.
                final AtomicLong insertTime = new AtomicLong(0);

                final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(3);

                tasks.add(new SPOIndexWriter(statementStore, a, numStmts,
                        false/* clone */, KeyOrder.SPO, filter, sortTime,
                        insertTime, numWritten));

                if (!statementStore.oneAccessPath) {

                    tasks.add(new SPOIndexWriter(statementStore, a, numStmts,
                            true/* clone */, KeyOrder.POS, filter, sortTime,
                            insertTime, numWritten));

                    tasks.add(new SPOIndexWriter(statementStore, a, numStmts,
                            true/* clone */, KeyOrder.OSP, filter, sortTime,
                            insertTime, numWritten));
                    
                }

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

                    futures = getThreadPool().invokeAll(tasks);

                    elapsed_SPO = futures.get(0).get();
                    if (!statementStore.oneAccessPath) {
                        elapsed_POS = futures.get(1).get();
                        elapsed_OSP = futures.get(2).get();
                    } else {
                        elapsed_POS = 0;
                        elapsed_OSP = 0;
                    }

                } catch (InterruptedException ex) {

                    throw new RuntimeException(ex);

                } catch (ExecutionException ex) {

                    throw new RuntimeException(ex);

                }

                long elapsed = System.currentTimeMillis() - begin;

                if (numStmts > 1000) {
                
                    log.info("Wrote " + numStmts + " statements in " + elapsed + "ms" //
                            + (statementStore != this ? "; truthMaintenance":"") //
                            + (statementIdentifiers ? "; sid="+ statementIdentifierTime + "ms" : "") //
                            + "; sort=" + sortTime + "ms" //
                            + ", keyGen+insert="+ insertTime + "ms" //
                            + "; spo=" + elapsed_SPO + "ms" //
                            + ", pos=" + elapsed_POS + "ms" //
                            + ", osp=" + elapsed_OSP + "ms" //
                            );
                    
                }

            } // nextChunk

            return (int) numWritten.get();

        } finally {

            itr.close();

        }

    }
    
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
    public long addJustifications(IChunkedIterator<Justification> itr) {

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

            long nwritten = 0;

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

                final LongAggregator aggregator = new LongAggregator();
                
                ndx.submit(0/*fromIndex*/, n/*toIndex*/, keys, null/* vals */,
                                WriteJustificationsProcConstructor.INSTANCE,
                                aggregator);

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
     * 
     * @todo allow registeration of datatype specific tokenizers (we already
     *       have language family based lookup).
     */
    protected void indexTermText(_Value[] terms, int numTerms) {

        final FullTextIndex ndx = getSearchEngine();

        final TokenBuffer buffer = new TokenBuffer(numTerms,ndx);
        
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
            
            ndx.index(buffer, val.termId, 0/*fieldId*/, languageCode, new StringReader(text));
         
            n++;
            
        }
        
        // flush writes to the text index.
        buffer.flush();
        
        log.info("indexed "+n+" new terms");
        
    }

    /**
     * Full text information retrieval for RDF essentially treats the lexical
     * terms in the RDF database (plain and language code {@link Literal}s and
     * possibly {@link URI}s) as "documents." You can understand the items in
     * the terms index as "documents" that are broken down into "token"s to
     * obtain a "token frequency distribution" for that document. The full text
     * index contains the indexed token data.
     * <p>
     * Note: the {@link FullTextIndex} is implemented against the
     * {@link IBigdataFederation} API and is therefore not available for a
     * {@link TempTripleStore}.
     * 
     * @return The object managing the text search indices or <code>null</code>
     *         iff text search is not enabled.
     */
    abstract public FullTextIndex getSearchEngine();
    
    /**
     * <p>
     * Performs a full text search against literals returning an {@link IHit}
     * list visiting the term identifiers for literals containing tokens parsed
     * from the query. Those term identifiers may be used to join against the
     * statement indices in order to bring back appropriate results.
     * </p>
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
     *         more of the extracted tokens has been found. The value returned
     *         by {@link IHit#getDocId()} is in fact the <i>termId</i> and you
     *         can resolve it to the term using {@link #getTerm(long)}.
     * 
     * @throws InterruptedException
     *             if the search operation is interrupted.
     * 
     * @todo Abstract the search api so that it queries the terms index directly
     *       when a data typed literal or a URI is used (typed query).
     */
    @SuppressWarnings("unchecked")
    public Iterator<IHit> textSearch(String languageCode, String text)
            throws InterruptedException {

        return getSearchEngine().search(text, languageCode);

    }
    
}
