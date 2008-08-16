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

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
import org.openrdf.rio.rdfxml.RDFXMLParser;
import org.openrdf.sail.SailException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.filter.ITupleFilter;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.IJustificationIterator;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.JustificationIterator;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine;
import com.bigdata.rdf.rules.MatchRule;
import com.bigdata.rdf.rules.RDFJoinNexusFactory;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.spo.BulkCompleteConverter;
import com.bigdata.rdf.spo.BulkFilterConverter;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.JustificationWriter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOConvertingIterator;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOTupleSerializer;
import com.bigdata.rdf.spo.StatementWriter;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IDatabase;
import com.bigdata.relation.IMutableDatabase;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.RelationSchema;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.IHit;
import com.bigdata.service.AbstractEmbeddedDataService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedResolvingIterator;
import com.bigdata.striterator.DelegateChunkedIterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store.
 * 
 * @todo Do the full quad store (vs the provenance mode, which uses the context
 *       position for statements about statements).
 *       <p>
 *       Make some of the indices optional such that we still have good access
 *       patterns but store less data when the quad is used as a statement
 *       identifier (by always using a distinct {@link BNode} as its value).
 *       There is no need for a pure triple store and the "bnode" trick can be
 *       used to model eagerly assigned statement identifiers. One question is
 *       whether the quad position can be left open (0L aka NULL). It seems that
 *       SPARQL might allow this, in which case we can save a lot of term
 *       identifiers that would otherwise be gobbled up eagerly. The quad
 *       position could of course be bound to some constant, in which case it
 *       would not be a statement identifier - more the context model.
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
 * @todo examine role for semi joins (join indices) - these can be declared for
 *       various predicate combinations and then maintained, effectively by
 *       rules using the existing TM mechanisms.
 * 
 * @todo Support rules (IF TripleExpr THEN ...) This is basically encapsulating
 *       the rule execution layer.
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
 *       {@link #addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)}
 *       and friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTripleStore extends
        AbstractResource<IDatabase<AbstractTripleStore>> implements ITripleStore,
        IRawTripleStore, IMutableDatabase<AbstractTripleStore> {

    static protected transient final Logger log = Logger.getLogger(ITripleStore.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * This is used to conditionally enable the logic to retract justifications
     * when the corresponding statements is retracted.
     * 
     * @deprecated by {@link SPORelation#justify}?
     */
    final private boolean justify;
    
    /**
     * True iff justification chains are being recorded for entailments and used
     * to support truth maintenance
     */
    final public boolean isJustify() {
        
        return justify;
        
    }

    /**
     * This is used to conditionally disable the lexicon support, principally in
     * conjunction with a {@link TempTripleStore}.
     */
    final protected boolean lexicon;

    /**
     * This is used to conditionally disable all but a single statement index
     * (aka access path).
     * 
     * @deprecated by {@link SPORelation#oneAccessPath}
     */
    final protected boolean oneAccessPath;

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
     * 
     * @todo by {@link SPORelation#statementIdentifiers} or keep this here and
     *       resolve it from {@link SPORelation}?
     */
    final protected boolean statementIdentifiers;

    /**
     * The capacity of the buffers used when computing entailments (for insert
     * or query).
     * 
     * @see Options#BUFFER_CAPACITY
     * @see #newJoinNexusFactory(ActionEnum, int, IElementFilter)
     * 
     * FIXME Re-evaluate the effect of the buffer capacity for the chunked
     * iterators. When we are running parallel rules and asynchronous iterators
     * it could make sense to use much smaller buffers (10k vs 200k).
     */
    final public int bufferCapacity;

    /**
     * When <code>true</code> the database will support statement identifiers.
     * <p>
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     * <p>
     * Only explicit statements will have a statement identifier. Statements
     * made about statements using their statement identifiers will
     * automatically be retracted if a statement they describe is retracted (a
     * micro form of truth maintenance that is always enabled when statement
     * identifiers are enabled).
     * 
     * @todo by {@link SPORelation#statementIdentifiers} or keep this here and
     *       resolve it from {@link SPORelation}?
     */
    public boolean getStatementIdentifiers() {
        
        return statementIdentifiers;
        
    }

    /**
     * The {@link BigdataValueFactoryImpl} for namespace of the
     * {@link LexiconRelation} associated with this {@link AbstractTripleStore}.
     * 
     * @throws UnsupportedOperationException
     *             if there is no associated lexicon.
     * 
     * @todo allow a {@link TempTripleStore} to specify another db's lexicon?
     */
    final public BigdataValueFactoryImpl getValueFactory() {

        if (valueFactory != null)
            return valueFactory;

        if(!lexicon) {

            throw new UnsupportedOperationException();

        }
        
        synchronized(this) {
            
            if (valueFactory == null) {
                
                valueFactory = getLexiconRelation().getValueFactory();
                
            }
            
        }
        
        return valueFactory;
        
    }
    private volatile BigdataValueFactoryImpl valueFactory;
    
    /*
     * IDatabase, ILocatableResource
     */
    
    public Iterator<IRelation> relations() {
        
        return Collections.unmodifiableList(Arrays.asList(new IRelation[] { //
                getSPORelation(), //
                getLexiconRelation() //
                })).iterator();
        
    }
    
    /**
     * Configuration options.
     * 
     * @todo refactor options to {@link SPORelation} and {@link LexiconRelation}.
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
         * <code>false</code>, the lexicon indices are not registered. This
         * can be safely turned off for the {@link TempTripleStore} when only
         * the statement indices are to be used.
         */
        String LEXICON = "lexicon";

        String DEFAULT_LEXICON = "true";

        /**
         * Boolean option (default {@value #DEFAULT_STORE_BLANK_NODES})
         * controls whether or not we store blank nodes in the forward mapping
         * of the lexicon. When <code>false</code> blank node semantics are
         * enforced and you CAN NOT unify blank nodes based on their IDs in the
         * lexicon. When <code>true</code>, you are able to violate blank
         * node semantics and force unification of blank nodes by assigning the
         * ID from the RDF interchange syntax to the blank node. RIO has an
         * option that will allow you to do this. When this option is also
         * <code>true</code>, then you will in fact be able to resolve
         * pre-existing blank nodes using their identifiers. The tradeoff is
         * time and space : if you have a LOT of document using blank nodes then
         * you might want to disable this option in order to spend less time
         * writing the forward lexicon index (and it will also take up less
         * space).
         */
        String STORE_BLANK_NODES = "storeBlankNodes";
        
        String DEFAULT_STORE_BLANK_NODES = "false";
        
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
         * Boolean option (default {@value #DEFAULT_STATEMENT_IDENTIFIERS})
         * enables support for statement identifiers. A statement identifier is
         * unique identifier for a <em>triple</em> in the database. Statement
         * identifiers may be used to make statements about statements without
         * using RDF style reification.
         * <p>
         * Statement identifers are assigned consistently when {@link Statement}s
         * are mapped into the database. This is done using an extension of the
         * <code>term:id</code> index to map the statement as if it were a
         * term onto a unique statement identifier. While the statement
         * identifier is assigned canonically by the <code>term:id</code>
         * index, it is stored redundently in the value position for each of the
         * statement indices. While the statement identifier is, in fact, a term
         * identifier, the reverse mapping is NOT stored in the id:term index
         * and you CAN NOT translate from a statement identifier back to the
         * original statement.
         * <p>
         * bigdata supports an RDF/XML interchange extension for the interchange
         * of <em>triples</em> with statement identifiers that may be used as
         * blank nodes to make statements about statements. See {@link BNS} and
         * {@link RDFXMLParser}.
         * <p>
         * Statement identifiers add some latency when loading data since it
         * increases the size of the writes on the terms index (and also its
         * space requirements since all statements are also replicated in the
         * terms index). However, if you are doing concurrent data load then the
         * added latency is nicely offset by the parallelism.
         * <p>
         * The main benefit for statement identifiers is that they provide a
         * mechanism for statement level provenance. This is critical for some
         * applications.
         * <p>
         * An alternative approach to provenance within RDF is to use the
         * concatenation of the subject, predicate, and object (or a hash of
         * their concatenation) as the value in the context position. While this
         * approach can be used with any quad store, it is less transparent and
         * requires <em>twice</em> the amount of data on the disk since you
         * need an additional three statement indices to cover the quad access
         * paths.
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
         *                     (joe, loves, mary):_a.
         *                     (_a, reportedBy, tom).
         * </pre>
         * 
         * where _a is a BNode identifying the source for (joe, loves, mary).
         * The second statement uses _a to add provenance for the assertion
         * (joe, loves, mary) - namely that the assertion was reported by tom.
         * 
         * @todo consider renaming as "provenance" mode and allowing a
         *       "tripleStore" and "namedGraph" (or quadStore) mode as well.
         *       <p>
         *       The provenance mode supports statement metadata, including an
         *       extension to the truth maintenance to remove statement metadata
         *       when the statement about which it was asserted are removed and
         *       an extension when reading or writing RDF/XML. The provenance
         *       mode can function as if it were a quadStore as a SPARQL end
         *       point, but it only uses 3 statement indices (the statement
         *       identifiers are stored as values in the statement indices, not
         *       as part of the key), and it will insist that the "source"
         *       identifiers is a BNode and that there is a 1:1 correspondence
         *       between a "source" and an explicit triple.
         *       <p>
         *       The tripleStore mode is a plain triple store using 3 statement
         *       indices.
         *       <p>
         *       The quadStore mode is a plain quad store using 6 statement
         *       indices (not implemented yet).
         */
        String STATEMENT_IDENTIFIERS = "statementIdentifiers";

        String DEFAULT_STATEMENT_IDENTIFIERS = "true";
        
        /**
         * <p>
         * Sets the capacity of the buffers used by the chunked iterators for
         * query answering and truth maintenance. These buffers make it possible
         * to perform efficient ordered writes using the batch API (default
         * 200k).
         * </p>
         * <p>
         * Some results for comparison on a 45k triple data set:
         * </p>
         * 
         * <pre>
         *  1k    - Computed closure in 4469ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=17966
         *  10k   - Computed closure in 3250ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=24704
         *  50k   - Computed closure in 3187ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=25193
         *  100k  - Computed closure in 3359ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=23903
         *  1000k - Computed closure in 3954ms yeilding 125943 statements total, 80291 inferences, entailmentsPerSec=20306
         * </pre>
         * 
         * <p>
         * Note that the actual performance will depend on the sustained
         * behavior of the JVM, including online performance tuning, parallism
         * in the implementation, and the characteristics of the specific
         * ontology, especially how it influences the #of entailments generated
         * by each rule.
         * </p>
         * 
         * @todo add a buffer capacity property for the
         *       {@link InferenceEngine#backchainIterator(long, long, long)} or
         *       use this capacity there as well.
         */
        public static final String BUFFER_CAPACITY = "bufferCapacity";

        /**
         * @todo experiment with different values for this capacity.
         */
        public static final String DEFAULT_BUFFER_CAPACITY = ""+200*Bytes.kilobyte32;

    }

    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @see Options
     */
    protected AbstractTripleStore(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);
        
        /*
         * Reads off the property for the inference engine that tells us whether
         * or not the justification index is being used. This is used to
         * conditionally enable the logic to retract justifications when the
         * corresponding statements is retracted.
         */

        this.justify = Boolean.parseBoolean(properties.getProperty(
                Options.JUSTIFY, Options.DEFAULT_JUSTIFY));

        log.info(Options.JUSTIFY + "=" + justify);

        this.lexicon = Boolean.parseBoolean(properties.getProperty(
                Options.LEXICON, Options.DEFAULT_LEXICON));

        log.info(Options.LEXICON + "=" + lexicon);

        this.oneAccessPath = Boolean.parseBoolean(properties.getProperty(
                Options.ONE_ACCESS_PATH, Options.DEFAULT_ONE_ACCESS_PATH));

        log.info(Options.ONE_ACCESS_PATH + "=" + oneAccessPath);

        this.statementIdentifiers = Boolean.parseBoolean(properties
                .getProperty(Options.STATEMENT_IDENTIFIERS,
                        Options.DEFAULT_STATEMENT_IDENTIFIERS));

        log.info(Options.STATEMENT_IDENTIFIERS + "=" + statementIdentifiers);

        {
            bufferCapacity = Integer.parseInt(properties.getProperty(
                    Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));

            if (bufferCapacity <= 0) {
                throw new IllegalArgumentException(Options.BUFFER_CAPACITY
                        + " must be positive");
            }

            log.info(Options.BUFFER_CAPACITY + "=" + bufferCapacity);
            
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
     * on the {@link IBigdataFederation} or an
     * {@link AbstractEmbeddedDataService} automatically inherent the
     * appropriate concurrency controls as would a store whose index access was
     * intermediated by the executor service of an {@link IConcurrencyManager}.
     * <p>
     * Note: if {@link #isConcurrent()} returns <code>true</code> then the
     * database will provide concurrency control for write tasks submitted
     * against the same index. However, concurrent writers are always supported
     * on distinct indices and concurrent readers on an index are always
     * supported IF there is no concurrent writer on the index. If
     * {@link #isConcurrent()} is <code>false</code> then you need to avoid
     * submitting a write task concurrently with ANY other task for the same
     * index (concurrent reads or concurrent writes will both cause problems
     * with a write task in the absence of concurrency controls).
     * <p>
     * The main place where this is an issue is rule execution, where the
     * entailments are being written back onto the database while reads are
     * proceeding concurrently. The new rule execution layer finesses this by
     * have a read view and a write view, so an {@link IProgram} will read from
     * an historical state and write on the {@link ITx#UNISOLATED} indices. This
     * also works for closure - each round of closure updates the read-behind
     * point so that all writes from the last round become visible during the
     * next round.
     * <p>
     * Note that closure must read against a stable state of the database and
     * that concurrent writes on the database NOT related to the closure
     * operation are disallowed. With these guidelines, you can force the
     * application to use a single thread for all mutation operations on the
     * {@link AbstractTripleStore} (readers from historical states may be
     * concurrent) and concurrency problems will not arise.
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

    public boolean isOpen() {
        
        return open;
        
    }
    
    public void close() {

        shutdown();

    }

    /**
     * Default is a NOP - invoked by {@link #close()} and
     * {@link #closeAndDelete()}
     */
    protected void shutdown() {

        open = false;
        
    }
    private boolean open = true;

    /**
     * True iff the backing store is stable (exists on disk somewhere and may be
     * closed and re-opened).
     * <p>
     * Note: This is mainly used by the test suites.
     */
    abstract public boolean isStable();

    /**
     * <code>true</code> unless {{@link #getTimestamp()} is {@link ITx#UNISOLATED}.
     */
    public boolean isReadOnly() {

        return getTimestamp() != ITx.UNISOLATED;
        
    }
    
    final protected void assertWritable() {
        
        if(isReadOnly()) {
            
            throw new IllegalStateException("READ_ONLY");
            
        }
        
    }
    
    public void create() {

        assertWritable();
        
        final IResourceLock resourceLock = getIndexManager()
                .getResourceLockManager().acquireExclusiveLock(getNamespace());

        final Properties tmp = getProperties();
        
        // set property that will let the contained relations locate their container.
        tmp.setProperty(RelationSchema.CONTAINER, getNamespace());
        
        try {

            super.create();
            
            if (lexicon) {

                lexiconRelation = new LexiconRelation(getIndexManager(),
                        getNamespace() + NAME_LEXICON_RELATION, getTimestamp(),
                        tmp);

                lexiconRelation.create();
                
                valueFactory = lexiconRelation.getValueFactory();

            }

            spoRelation = new SPORelation(getIndexManager(), getNamespace()
                    + NAME_SPO_RELATION, getTimestamp(), tmp);

            spoRelation.create();

            /*
             * Note: A commit is required in order for a read-committed view to
             * have access to the registered indices.
             * 
             * @todo have the caller do this? It does not really belong here
             * since you can not make a large operation atomic if you do a
             * commit here.
             */

            commit();

        } finally {

            resourceLock.unlock();

        }

    }

    public void destroy() {

        assertWritable();

        final IResourceLock resourceLock = getIndexManager()
                .getResourceLockManager().acquireExclusiveLock(getNamespace());

        try {

            if (lexicon) {

                getLexiconRelation().destroy();

                lexiconRelation = null;
                
                valueFactory = null;
                
            }

            getSPORelation().destroy();

            spoRelation = null;
            
            super.destroy();
            
        } finally {

            resourceLock.unlock();
            
        }

    }
    
    /**
     * The {@link SPORelation} (triples and their access paths).
     */
    final synchronized public SPORelation getSPORelation() {
    
        if (spoRelation == null) {

            spoRelation = (SPORelation) getIndexManager().getResourceLocator()
                    .locate(getNamespace()
                                    + NAME_SPO_RELATION, getTimestamp());

        }

        return spoRelation;

    }
    private SPORelation spoRelation;

    /**
     * The {@link LexiconRelation} handles all things related to the indices
     * mapping RDF {@link Value}s onto internal 64-bit term identifiers.
     */
    final synchronized public LexiconRelation getLexiconRelation() {

        if (lexiconRelation == null && lexicon) {

            lexiconRelation = (LexiconRelation) getIndexManager()
                    .getResourceLocator().locate(
                            getNamespace()
                                    + NAME_LEXICON_RELATION, getTimestamp());
            
        }

        return lexiconRelation;
        
    }
    private LexiconRelation lexiconRelation;
        
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
    final public FullTextIndex getSearchEngine() {

        if (!lexicon)
            return null;

        return getLexiconRelation().getSearchEngine();
        
    }

    final public IIndex getTerm2IdIndex() {

        if (!lexicon)
            return null;

        return getLexiconRelation().getTerm2IdIndex();

    }

    final public IIndex getId2TermIndex() {

        if (!lexicon)
            return null;

        return getLexiconRelation().getId2TermIndex();

    }

    final public IIndex getSPOIndex() {

        return getSPORelation().getSPOIndex();

    }

    final public IIndex getPOSIndex() {
        
        return getSPORelation().getPOSIndex();

    }

    final public IIndex getOSPIndex() {

        return getSPORelation().getOSPIndex();

    }

    final public IIndex getStatementIndex(IKeyOrder<ISPO> keyOrder) {

        return getSPORelation().getIndex(keyOrder);
        
    }

    final public IIndex getJustificationIndex() {
        
        return getSPORelation().getJustificationIndex();

    }

    final public long getStatementCount() {

        return getSPOIndex().rangeCount(null, null);

    }

    final public long getExactStatementCount() {
        
        return getStatementCount(true/*exact*/);
        
    }

    
    /**
     * Clears hard references to any indices, relations, etc. MUST be extended
     * to discard write sets for impls with live indices.
     * 
     * @throws IllegalStateException
     *             if the view is read only.
     */
    public void abort() {

        if (isReadOnly())
            throw new IllegalStateException();

        /*
         * Relations have hard references to indices so they must be discarded.
         * Note only do we need to release our hard references to those
         * relations but we need to inform the relation locator that they should
         * be discarded from its cache as well. Otherwise the same objects will
         * be returned from the cache and buffered writes on the indices for
         * those relations (if they are local index objects) will still be
         * visible.
         */

        final DefaultResourceLocator locator = (DefaultResourceLocator) getIndexManager()
                .getResourceLocator();

        if (lexiconRelation != null) {

            locator.discard(lexiconRelation);

            lexiconRelation = null;

        }

        if (spoRelation != null) {

            locator.discard(spoRelation);

            spoRelation = null;

        }
        
    }

    /**
     * MUST be extended to perform commit for impls with live indices.
     * 
     * @throws IllegalStateException
     *             if the view is read only.
     */
    public void commit() {
        
        if (isReadOnly())
            throw new IllegalStateException();

        /*
         * Clear the reference since it was as of the last commit point.
         */
        readCommittedRef = null;

    }
    
    /**
     * A factory returning a read-committed view of the database.
     * <p>
     * Note: There is a distinct instance <i>per commit time</i>. If an
     * intervening commit has occurred, then you will get back a new instance
     * providing a read-consistent view as of the now most recent commit point.
     * 
     * FIXME The [per commit time] constraint is actually a function of the
     * {@link ITx#READ_COMMITTED} semantics as implemented by the
     * {@link IIndexManager}. If the indices are {@link IClientIndex}s then
     * the instances remain valid since all requests are delegated through the
     * {@link DataService} layer. However, if they are {@link BTree}s, then the
     * instances are NOT valid.
     * <p>
     * Perhaps the best way to deal with this is to have a ReadCommittedBTree or
     * to modify BTree to intrinsically understand read-committed semantics and
     * to reload from the most recent checkpoint after each commit. That way the
     * index references would always remain valid.
     * <p>
     * However, we have to be much more careful about read-consistent (choose a
     * timestamp corresponding to the last commit point or the last closure
     * point) vs read-committed (writes become immediately visible once they are
     * committed).
     */
    @SuppressWarnings("unchecked")
    final public AbstractTripleStore asReadCommittedView() {

        if (getTimestamp() == ITx.READ_COMMITTED) {
            
            return this;
            
        }

        synchronized(this) {
        
            AbstractTripleStore view = readCommittedRef == null ? null
                    : readCommittedRef.get();
            
            if(view == null) {
                
                view = (AbstractTripleStore) getIndexManager().getResourceLocator()
                        .locate(getNamespace(), ITx.READ_COMMITTED);
                
                readCommittedRef = new SoftReference<AbstractTripleStore>(view);
                
            }
            
            return view; 
        
        }
        
    }
    private SoftReference<AbstractTripleStore> readCommittedRef;
    
    /**
     * The #of explicit statements in the database (exact count based on
     * key-range scan).
     * <p>
     * Note: In order to get the #of explicit statements in the repository we
     * have to actually do a range scan and figure out for each statement
     * whether or not it is explicit.
     * 
     * @todo Re-write using an {@link ITupleFilter} that gets evaluated local to
     *       the statement indices and avoids sending back either the keys or
     *       the values. All we need is the count of the #of tuples that
     *       satisify an {@link ExplicitSPOFilter}. This will have to be
     *       written to the {@link IAccessPath#rangeIterator()} method either
     *       that method will have to accept a {@link IFilterConstructor} or we
     *       can generate an {@link SPOPredicate} and specify a predicate
     *       constraint that will do what we want directly.
     */
    public long getExplicitStatementCount() {

        long n = 0;

        final IChunkedOrderedIterator<ISPO> itr = getAccessPath(SPOKeyOrder.SPO,
                ExplicitSPOFilter.INSTANCE).iterator();

        try {

            while (itr.hasNext()) {

                ISPO[] chunk = itr.nextChunk();

                n += chunk.length;

            }

            return n;

        } finally {

            itr.close();
            
        }
        
    }
    
    final public long getStatementCount(boolean exact) {

        final IIndex ndx = getSPOIndex();

        if (exact) {

            return ndx.rangeCountExact(null/* fromKey */, null/* toKey */);

        } else {

            return ndx.rangeCount(null/* fromKey */, null/* toKey */);
            
        }

    }

    final public long getJustificationCount() {

        if (justify) {

            return getJustificationIndex().rangeCount(null, null);

        }

        return 0;

    }

    final public long getTermCount() {

        byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_URI) };

        /*
         * Note: the term count deliberately excludes the statement identifiers
         * which follow the bnodes in the lexicon.
         */
        byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_BND + 1)) };

        return getTerm2IdIndex().rangeCount(fromKey, toKey);

    }

    final public long getURICount() {

        byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_URI) };

        byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_URI + 1)) };

        return getTerm2IdIndex().rangeCount(fromKey, toKey);

    }

    final public long getLiteralCount() {

        // Note: the first of the kinds of literals (plain).
        byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_LIT) };

        // Note: spans the last of the kinds of literals.
        byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_DTL + 1)) };

        return getTerm2IdIndex().rangeCount(fromKey, toKey);

    }

    // @todo also statement identifer count.
    final public long getBNodeCount() {

        byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_BND) };

        byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_BND + 1)) };

        return getTerm2IdIndex().rangeCount(fromKey, toKey);

    }

    /*
     * term index
     */

    /**
     * Delegates to the batch API.
     */
    public long addTerm(Value value) {

        final BigdataValue[] terms = new BigdataValue[] {//

            getValueFactory().asValue(value) //

        };

        getLexiconRelation().addTerms(terms, 1, false/* readOnly */);

        return terms[0].getTermId();

    }

    final public BigdataValue getTerm(long id) {

        return getLexiconRelation().getTerm(id);

    }

    final public long getTermId(Value value) {

        return getLexiconRelation().getTermId(value);
        
    }

    public void addTerms(final BigdataValue[] terms) {

        getLexiconRelation().addTerms( terms, terms.length, false/*readOnly*/);
        
    }

    /*
     * singletons.
     */

    private WeakReference<InferenceEngine> inferenceEngineRef = null;

    final public InferenceEngine getInferenceEngine() {

        synchronized (this) {

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

        synchronized (this) {

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

        final IIndex ndx = getStatementIndex(SPOKeyOrder.SPO);
        
        final SPOTupleSerializer tupleSer = (SPOTupleSerializer)ndx.getIndexMetadata().getTupleSerializer();
        
        final byte[] key = tupleSer.statement2Key(s, p, o);

        final byte[] val = getStatementIndex(SPOKeyOrder.SPO).lookup(key);

        if (val == null) {

            // statement is not in the database.
            return null;

        }

        // The statement is known to the database.
        return new SPO(s, p, o, val);

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

        if (s != NULL && p != NULL && o != NULL) {

            final IIndex ndx = getSPORelation().getSPOIndex();

            final SPO spo = new SPO(s, p, o);

            final byte[] key = ndx.getIndexMetadata().getTupleSerializer()
                    .serializeKey(spo);

            final boolean found = ndx.contains(key);

            if(log.isDebugEnabled()) {
                
                log.debug(spo + " : found=" + found + ", key="
                        + BytesUtil.toString(key));
                
            }
            
            return found;

        }

        /*
         * Use a range scan over the triple pattern and see if there are any
         * matches.
         */
        
        final IAccessPath accessPath = getAccessPath(s, p, o);
        
        final boolean isEmpty = accessPath.isEmpty();

        return !isEmpty;

    }

    final public boolean hasStatement(Resource s, URI p, Value o) {

        /*
         * convert other Value object types to our object types.
         */
        final BigdataValueFactory valueFactory = getValueFactory();
        
        s = (Resource) valueFactory.asValue(s);

        p = (URI) valueFactory.asValue(p);

        o = valueFactory.asValue(o);

        /*
         * Convert our object types to internal identifiers.
         * 
         * Note: If a value was specified and it is not in the terms index then
         * the statement can not exist in the KB.
         */
        final long _s = getTermId(s);

        if (_s == NULL && s != null) {
         
            if(log.isDebugEnabled())
                log.debug("Subject not in kb: "+s);
            
            return false;
            
        }

        final long _p = getTermId(p);

        if (_p == NULL && p != null) {

            if(log.isDebugEnabled())
                log.debug("Predicate not in kb: "+s);            
            
            return false;
            
        }
        
        final long _o = getTermId(o);

        if (_o == NULL && o != null) {

            if(log.isDebugEnabled())
                log.debug("Object not in kb: "+s);
            
            return false;
            
        }

        final boolean found = hasStatement(_s, _p, _o);
        
        if(log.isDebugEnabled()) {
            
            log.debug("<" + s + "," + p + "," + o + "> : found=" + found);
            
        }
        
        return found;
        
//        final IAccessPath accessPath = getAccessPath(s, p, o);
//
//        if (accessPath instanceof EmptyAccessPath) {
//
//            return false;
//
//        }
//
//        return !accessPath.isEmpty();

    }

    final public long removeStatements(Resource s, URI p, Value o) {

        return getAccessPath(s, p, o).removeAll();

    }

    public BigdataStatement getStatement(Resource s, URI p, Value o)
            throws SailException {

        if (s == null || p == null || o == null) {

            throw new IllegalArgumentException();

        }

        final BigdataStatementIterator itr = getStatements(s, p, o);

        try {

            if (!itr.hasNext()) {

                return null;

            }

            return itr.next();

        } finally {

            itr.close();

        }

    }

    public BigdataStatementIterator getStatements(Resource s, URI p, Value o) {

        return asStatementIterator(getAccessPath(s, p, o).iterator());

    }

    final public BigdataValue asValue(Value value) {

        return getValueFactory().asValue(value);

    }

    public BigdataStatement asStatement(ISPO spo) {

        /*
         * Use batch API to resolve the term identifiers.
         */
        final List<Long> ids = new ArrayList<Long>(4);
        
        ids.add(spo.s());
        
        ids.add(spo.p());
        
        ids.add(spo.o());
        
        if (spo.hasStatementIdentifier()) {

            ids.add(spo.getStatementIdentifier());
            
        }
        
        final Map<Long,BigdataValue> terms = getLexiconRelation().getTerms(ids);
        
        /*
         * Expose as a Sesame compatible Statement object.
         */
        return getValueFactory().createStatement(//
                (BigdataResource)  terms.get(spo.s()),//
                (BigdataURI)       terms.get(spo.p()),
                (BigdataValue)     terms.get(spo.o()),
                (BigdataResource)  (spo.hasStatementIdentifier() ? terms.get(spo.getStatementIdentifier()) : null),//
                spo.getStatementType()
                );
        
    }

    public BigdataStatementIterator asStatementIterator(IChunkedOrderedIterator<ISPO> src) {

        return new BigdataStatementIteratorImpl(this, src);

    }

    public IAccessPath<ISPO> getAccessPath(Resource s, URI p, Value o) {

        return getAccessPath(s, p, o, null/* filter */);
        
    }
    
    public IAccessPath<ISPO> getAccessPath(Resource s, URI p, Value o,IElementFilter<ISPO> filter) {

        /*
         * convert other Value object types to our object types.
         */

        final BigdataValueFactory valueFactory = getValueFactory();
        
        s = (Resource) valueFactory.asValue(s);

        p = (URI) valueFactory.asValue(p);

        o = valueFactory.asValue(o);

        /*
         * Convert our object types to internal identifiers.
         * 
         * Note: If a value was specified and it is not in the terms index then
         * the statement can not exist in the KB.
         */
        final long _s = getTermId(s);

        if (_s == NULL && s != null)
            return new EmptyAccessPath<ISPO>();

        final long _p = getTermId(p);

        if (_p == NULL && p != null)
            return new EmptyAccessPath<ISPO>();

        final long _o = getTermId(o);

        if (_o == NULL && o != null)
            return new EmptyAccessPath<ISPO>();

        /*
         * Return the access path.
         */

        return getAccessPath(_s, _p, _o, filter);

    }

    final public IAccessPath<ISPO> getAccessPath(long s, long p, long o) {
    
        return getSPORelation().getAccessPath(s, p, o, null/*filter*/);
        
    }
    
    final public IAccessPath<ISPO> getAccessPath(long s, long p, long o,
            IElementFilter<ISPO> filter) {

        return getSPORelation().getAccessPath(s, p, o, filter);

    }

    final public IAccessPath<ISPO> getAccessPath(IKeyOrder<ISPO> keyOrder) {

        return getAccessPath(keyOrder, null/*filter*/);
        
    }
    
    /**
     * 
     * @param keyOrder
     * @param filter
     *            The filter will be incorporated as a constraint on the
     *            {@link IPredicate} for the {@link IAccessPath} and will be
     *            evaluated close to the data.
     * @return
     */
    final public IAccessPath<ISPO> getAccessPath(IKeyOrder<ISPO> keyOrder,
            final IElementFilter<ISPO> filter) {

        final SPORelation r = getSPORelation();
        
        final SPOPredicate p = new SPOPredicate(//
                new String[] { r.getNamespace() },//
                Var.var("s"), Var.var("p"), Var.var("o"),//
                filter//
        );

        return getSPORelation().getAccessPath(keyOrder, p);

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
    final public Map<String, String> getNamespaces() {

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
        Iterator<Map.Entry<String/* namespace */, String/* prefix */>> itr = uriToPrefix
                .entrySet().iterator();

        while (itr.hasNext()) {

            Map.Entry<String/* namespace */, String/* prefix */> entry = itr
                    .next();

            if (entry.getValue().equals(prefix)) {

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

        Iterator<Map.Entry<String/* namespace */, String/* prefix */>> itr = uriToPrefix
                .entrySet().iterator();

        while (itr.hasNext()) {

            Map.Entry<String/* namespace */, String/* prefix */> entry = itr
                    .next();

            if (entry.getValue().equals(prefix)) {

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

    final public String toString(long s, long p, long o) {

        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o) + " >");

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
     * @return <code>true</code> iff the term identifier is marked as an RDF
     *         {@link Literal}.
     */
    static final public boolean isLiteral(long termId) {

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
    static final public boolean isBNode(long termId) {

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
    static final public boolean isURI(long termId) {

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
    static final public boolean isStatement(long termId) {

        return (termId & TERMID_CODE_MASK) == TERMID_CODE_STATEMENT;

    }

    final public String toString(long termId) {

        if (termId == NULL)
            return "NULL";

        final BigdataValue v = getTerm(termId);

        if (v == null)
            return "<NOT_FOUND#" + termId + ">";

        String s = (v instanceof URI ? abbrev((URI) v) : v.toString());

        return s + ("(" + termId + ")");

    }

    // private final String TERM_NOT_FOUND = "<NOT_FOUND>";

    /**
     * Substitutes in well know namespaces (rdf, rdfs, etc).
     */
    final private String abbrev(URI uri) {

        String uriString = uri.toString();

        // final int index = uriString.lastIndexOf('#');
        //        
        // if(index==-1) return uriString;
        //
        // final String namespace = uriString.substring(0, index);

        final String namespace = uri.getNamespace();

        final String prefix = uriToPrefix.get(namespace);

        if (prefix != null) {

            return prefix + ":" + uri.getLocalName();

        } else
            return uriString;

    }

    final public StringBuilder predicateUsage() {

        return predicateUsage(this);

    }

    /**
     * Dumps the #of statements using each predicate in the kb (tab delimited,
     * unordered).
     * 
     * @param resolveTerms
     *            Used to resolve term identifiers to terms (you can use this to
     *            dump a {@link TempTripleStore} that is using the term
     *            dictionary of the main database).
     */
    final public StringBuilder predicateUsage(AbstractTripleStore resolveTerms) {

        // visit distinct term identifiers for the predicate position.
        final IChunkedIterator<Long> itr = getSPORelation().distinctTermScan(SPOKeyOrder.POS);

        // resolve term identifiers to terms efficiently during iteration.
        final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                resolveTerms, itr);
        
        try {
        
            final StringBuilder sb = new StringBuilder();

            while (itr.hasNext()) {

//                final long p = itr.next();
                final BigdataValue term = itr2.next();

                final long p = term.getTermId();
                
                final long n = getAccessPath(NULL, p, NULL)
                        .rangeCount(false/* exact */);

                sb.append(n + "\t" + resolveTerms.toString(p) + "\n");

            }
        
            return sb;
        
        } catch (SailException e) {

            throw new RuntimeException(e);

        } finally {

            try {

                itr2.close();
                
            } catch (SailException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
        }

    }

    /**
     * Utility method dumps the statements in the store using the SPO index
     * (subject order).
     */
    final public StringBuilder dumpStore() {

        return dumpStore(true, true, true);

    }

    final public StringBuilder dumpStore(boolean explicit, boolean inferred,
            boolean axioms) {

        return dumpStore(this, explicit, inferred, axioms);

    }

    final public StringBuilder dumpStore(AbstractTripleStore resolveTerms,
            boolean explicit, boolean inferred, boolean axioms) {

        return dumpStore(resolveTerms, explicit, inferred, axioms, false);

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
    final public StringBuilder dumpStore(AbstractTripleStore resolveTerms,
            boolean explicit, boolean inferred, boolean axioms,
            boolean justifications) {

        final StringBuilder sb = new StringBuilder();
        
        final long nstmts = getStatementCount();

        long nexplicit = 0;
        long ninferred = 0;
        long naxioms = 0;

        {

            final ITupleIterator itr = getSPOIndex().rangeIterator();

            int i = 0;

            while (itr.hasNext()) {

                final SPO spo = (SPO)itr.next().getObject();

                switch (spo.getStatementType()) {

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

                sb.append("#" + (i + 1) + "\t" + spo.toString(resolveTerms)
                        + "\n");

                i++;

            }

        }

        int njust = 0;

        if (justifications && justify) {

            final IIndex ndx = getJustificationIndex();

            final ITupleIterator itrj = ndx.rangeIterator();

            while (itrj.hasNext()) {

                Justification jst = (Justification)itrj.next().getObject();

                sb.append("#" + (njust + 1) //+ "\t"
                        + jst.toString(resolveTerms)+"\n");

                njust++;

            }

        }

        sb.append("dumpStore: #statements=" + nstmts + ", #explicit="
                + nexplicit + ", #inferred=" + ninferred + ", #axioms="
                + naxioms + (justifications ? ", #just=" + njust : "")+"\n");

        return sb;
        
    }

    /**
     * Dumps the access path, resolving term identifiers to terms.
     * 
     * @param accessPath
     */
    public StringBuilder dump(IAccessPath<ISPO> accessPath) {
                
        final StringBuilder sb = new StringBuilder();
        
        final BigdataStatementIterator itr = asStatementIterator(accessPath.iterator());
        
        try {
            
            while(itr.hasNext()) {
                
                sb.append("\n"+itr.next());
                
            }
            
            return sb;
            
//        } catch(SailException ex) {
//            
//            throw new RuntimeException(ex);
            
        } finally {
            
            try {

                itr.close();
                
            } catch (SailException ex) {
                
                log.error(ex, ex);
                
            }
            
        }
        
    }
    
    /**
     * Returns some usage information for the database.
     * 
     * @deprecated by the {@link CounterSet}s exposed by the database which
     *             provide live, historical and post-mortem data.
     */
    public String usage() {

        return "usage summary: class=" + getClass().getSimpleName();

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

            return name + " : not used";

        }

        return name + " : " + ndx.getCounters().asXML(null/*filter*/);

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
     * with (term for term identical to) those in the destination store. If
     * statement identifiers are enabled, then they MUST be enabled for both
     * stores (statement identifiers are assigned by, and stored in, the foward
     * lexicon and replicated into the statement indices).
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
     * 
     * @todo method signature could be changed to accept the source access path
     *       for the read and then just write on the database
     */
    public long copyStatements(AbstractTripleStore dst, IElementFilter<ISPO> filter,
            boolean copyJustifications) {

        if (dst == this)
            throw new IllegalArgumentException();

        // obtain a chunked iterator reading from any access path.
        final IChunkedOrderedIterator<ISPO> itr = getAccessPath(SPOKeyOrder.SPO,
                filter).iterator();

        try {
        
        if (!copyJustifications) {

            // add statements to the target store.
            return dst
                    .addStatements(dst, true/* copyOnly */, itr, null/* filter */);

        } else {

            /*
             * Use a thread pool to write out the statement and the
             * justifications concurrently. This drammatically reduces the
             * latency when also writing justifications.
             */

            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(2);

            /*
             * Note: we reject using the filter before stmts or justifications
             * make it into the buffer so we do not need to apply the filter
             * again here.
             */

            // set as a side-effect.
            final AtomicLong nwritten = new AtomicLong();

            // task will write SPOs on the statement indices.
            tasks.add(new StatementWriter(this, dst, true/* copyOnly */, itr,
                    nwritten));

            // task will write justifications on the justifications index.
            final AtomicLong nwrittenj = new AtomicLong();

            if (justify) {

                final IJustificationIterator jitr = new JustificationIterator(
                        getJustificationIndex(), 0/* capacity */, true/* async */);

                tasks.add(new JustificationWriter(dst, jitr, nwrittenj));

            }

            final List<Future<Long>> futures;
            final long elapsed_SPO;
            final long elapsed_JST;

            try {

                futures = getIndexManager().getExecutorService().invokeAll(tasks);

                elapsed_SPO = futures.get(0).get();

                if (justify) {

                    elapsed_JST = futures.get(1).get();

                } else {

                    elapsed_JST = 0;

                }

            } catch (InterruptedException ex) {

                throw new RuntimeException(ex);

            } catch (ExecutionException ex) {

                throw new RuntimeException(ex);

            }

            if(INFO)
            log.info("Copied "
                    + nwritten
                    + " statements in "
                    + elapsed_SPO
                    + "ms"
                    + (justify ? (" and " + nwrittenj + " justifications in "
                            + elapsed_JST + "ms") : ""));

            return nwritten.get();

        }
        
        } finally {
            
            itr.close();
            
        }

    }

    public IChunkedOrderedIterator<ISPO> bulkFilterStatements(final ISPO[] stmts,
            final int numStmts, boolean present) {
        
        if (numStmts == 0) {

            return new EmptyChunkedIterator<ISPO>(SPOKeyOrder.SPO);
            
        }
        
        return bulkFilterStatements(
                new ChunkedArrayIterator<ISPO>(numStmts,
                stmts, null/* keyOrder */), present);
        
    }

    public IChunkedOrderedIterator<ISPO> bulkFilterStatements(
            final IChunkedOrderedIterator<ISPO> itr, final boolean present) {
        
        return new SPOConvertingIterator(itr, new BulkFilterConverter(
                getSPOIndex(), present));
        
    }

    public IChunkedOrderedIterator<ISPO> bulkCompleteStatements(
            final SPO[] stmts, final int numStmts) {
        
        if (numStmts == 0) {

            return new EmptyChunkedIterator<ISPO>(SPOKeyOrder.SPO);

        }

        return bulkCompleteStatements(new ChunkedArrayIterator<ISPO>(numStmts,
                stmts, null/* keyOrder */));
        
    }

    public IChunkedOrderedIterator<ISPO> bulkCompleteStatements(final IChunkedOrderedIterator<ISPO> itr) {
        
        return new SPOConvertingIterator(itr, new BulkCompleteConverter(
                getSPOIndex()));

    }

    public long addStatements(ISPO[] stmts, int numStmts) {

        if (numStmts == 0)
            return 0;

        return addStatements(new ChunkedArrayIterator<ISPO>(numStmts, stmts,
                null/* keyOrder */), null /* filter */);

    }

    public long addStatements(ISPO[] stmts, int numStmts,
            IElementFilter<ISPO> filter) {

        if (numStmts == 0)
            return 0;

        return addStatements(new ChunkedArrayIterator<ISPO>(numStmts, stmts,
                null/* keyOrder */), filter);

    }

    public long addStatements(final IChunkedOrderedIterator<ISPO> itr,
            final IElementFilter<ISPO> filter) {

        return addStatements(this/* statementStore */, false/* copyOnly */,
                itr, filter);

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
     *            is then presumed that {@link SPO#getStatementIdentifier()}
     *            will return a pre-assigned statement identifier and that we do
     *            NOT need to invoke
     *            {@link #addStatementIdentifiers(SPO[], int)}. This is only an
     *            optimization - the value <code>false</code> is always safe
     *            for this flag, but it will do some extra work in the case
     *            described here. See {@link StatementWriter}, which uses this
     *            flag and
     *            {@link #copyStatements(AbstractTripleStore, IElementFilter, boolean)}
     *            which always specifies <code>true</code> for this flag.
     * @param itr
     *            The source from which the {@link SPO}s are read.
     * @param filter
     *            An optional filter.
     * 
     * @return The mutation count, which is the #of statements that were written
     *         on the indices. A statement that was previously an axiom or
     *         inferred and that is converted to an explicit statement by this
     *         method will be reported in this count as well as any statement
     *         that was not pre-existing in the database.
     */
    public long addStatements(final AbstractTripleStore statementStore,
            final boolean copyOnly, final IChunkedOrderedIterator<ISPO> itr,
            final IElementFilter<ISPO> filter) {

        if (statementStore == null)
            throw new IllegalArgumentException();

        if (itr == null)
            throw new IllegalArgumentException();

        try {

            final LexiconRelation lexiconRelation = getLexiconRelation();

            final SPORelation spoRelation = statementStore.getSPORelation();
            
            if (!itr.hasNext())
                return 0;

            long mutationCount = 0;

            /*
             * Note: We process the iterator a "chunk" at a time. If the
             * iterator is backed by an SPO[] then it will all be processed in
             * one "chunk".
             */

            while (itr.hasNext()) {

                final ISPO[] a = itr.nextChunk();

                final int numStmts = a.length;

                final long statementIdentifierTime;

                if (statementIdentifiers && !copyOnly) {

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
                    lexiconRelation.addStatementIdentifiers(a, numStmts);

                    statementIdentifierTime = System.currentTimeMillis()
                            - begin;

                } else {

                    statementIdentifierTime = 0L;
                    
                }

                //@todo raise the filter into the caller's iterator?
                final long numWritten = spoRelation.insert(a, numStmts, filter);

                mutationCount += numWritten;
                
                if (numStmts > 1000) {

                    if(INFO)
                    log.info("Wrote "
                            + numStmts
                            + " statements (mutationCount="
                            + numWritten
                            + ")"
                            + (statementStore != this ? "; truthMaintenance"
                                    : "") //
                            + (statementIdentifiers ? "; sid="
                                    + statementIdentifierTime + "ms" : "") //
                    );

                }

            } // nextChunk

            return mutationCount;

        } finally {

            itr.close();

        }

    }

    public long removeStatements(ISPO[] stmts, int numStmts) {

        return removeStatements(new ChunkedArrayIterator<ISPO>(numStmts, stmts,
                null/* keyOrder */), true);

    }

    public long removeStatements(IChunkedOrderedIterator<ISPO> itr) {

        return removeStatements(itr, true);
        
    }
    
    /**
     * This processes a chunk of {@link SPO}s at a time and then submits tasks
     * to parallel threads to remove those statements from each of the statement
     * indices. This continues until all statements visited by the iterator have
     * been removed.
     * <p>
     * Note: If {@link #justify justifications} are being used to support truth
     * maintenance, then all justifications for the removed statements are also
     * removed.
     * 
     * @param itr
     *            An iterator visiting {@link SPO}s to be removed from the
     *            database.
     * 
     * @param computeClosureForStatementIdentifiers
     *            When <code>false</code> the caller asserts that they have
     *            pre-computed the closure of the statements that assert
     *            metadata about statement identifiers to be deleted. When
     *            <code>true</code> this method will compute that closure on
     *            behalf of the caller with the effect that any statements made
     *            about statements to be removed are also removed. This option
     *            has no effect when {@link #statementIdentifiers} are not
     *            enabled. See {@link Options#STATEMENT_IDENTIFIERS}
     * 
     * @todo If you are using statement identifiers but you are NOT using truth
     *       maintenance then this method does NOT guarentee consistency when
     *       removing statements in the face of concurrent writers on the
     *       statement indices. The problem is that we collect the statement
     *       identifiers in one unisolated operation, then collect the
     *       statements that use those statement identifiers two other
     *       operations, and finally we remove those statements. In order to be
     *       consistent you need to obtain an exclusive lock (which is difficult
     *       to do with distributed clients) or be encompassed by a transaction.
     *       (This is the same constraint that applies when truth maintenance is
     *       enabled since you have to serialize incremental TM operations
     *       anyway.)
     */
    public long removeStatements(IChunkedOrderedIterator<ISPO> itr,
            boolean computeClosureForStatementIdentifiers) {

        if (itr == null)
            throw new IllegalArgumentException();

        long mutationCount = 0;

        if( statementIdentifiers && computeClosureForStatementIdentifiers) {
            
            itr = computeClosureForStatementIdentifiers( itr );
            
        }
        
        try {

            while (itr.hasNext()) {

                final ISPO[] stmts = itr.nextChunk();

                // The #of statements that will be removed.
                final int numStmts = stmts.length;

                mutationCount += getSPORelation().delete(stmts, numStmts);
                
            }

        } finally {

            itr.close();

        }

        return mutationCount;

    }

    /**
     * Return an iterator which will visit the closure of the statements visited
     * by the source iterator plus any statements in the database made using a
     * statement identifier found on any of the statements visited by the source
     * iterator (only explicit statements have statement identifiers and then
     * iff {@link #statementIdentifiers} are enabled).
     * <p>
     * Note: This uses a {@link TempTripleStore} which is iteratively populated
     * until a fix point is obtained. The {@link TempTripleStore} is released
     * when the returned iterator is {@link IChunkedIterator#close() closed} or
     * when it is finalized.
     * 
     * @param src
     *            The source iterator.
     */
    public IChunkedOrderedIterator<ISPO> computeClosureForStatementIdentifiers(
            IChunkedOrderedIterator<ISPO> src) {
     
        if(!statementIdentifiers) {
            
            // There will be no statement identifiers unless they were enabled.
            
            return src;
            
        }
        
        final Properties properties = getProperties();

        // do not store terms
        properties.setProperty(Options.LEXICON, "false");

        // only store the SPO index
        properties.setProperty(Options.ONE_ACCESS_PATH, "true");

        final TempTripleStore tmp = new TempTripleStore(properties,this);
        
        /*
         * buffer everything in a temp triple store.
         * 
         * Note: copyOnly is false since we need to make sure that the sids are
         * defined for explicit statements before we attempt to compute the
         * fixed point for the statements about statemetns. This will have the
         * side effect of writing on the lexicon for the database if the caller
         * supplies an explicit SPO that does not exist yet in the database.
         */
        this.addStatements(tmp, false/*copyOnly*/, src, null/*filter*/);

        fixPointStatementIdentifiers(this, tmp);
        
        /*
         * Note: The returned iterator will automatically release the backing
         * temporary store when it is closed or finalized.
         */
        return new DelegateChunkedIterator<ISPO>(tmp.getAccessPath(SPOKeyOrder.SPO).iterator()) {
            
            public void close() {
                
                super.close();
                
                tmp.closeAndDelete();
                
            }
            
            protected void finalize() throws Throwable {
                
                super.finalize();
                
                if(tmp.isOpen()) {

                    tmp.closeAndDelete();
                    
                }
                
            }
            
        };
        
    }
    
    /**
     * Computes the fixed point of those statements in the database which make
     * assertions about statement identifiers in the tmp store.
     * 
     * @param db
     *            The database.
     * @param tempStore
     *            The temporary store.
     */
    static public void fixPointStatementIdentifiers(
            final AbstractTripleStore db, final AbstractTripleStore tempStore) {
        
        /*
         * Fix point the temp triple store.
         * 
         * Note: A [sids] collection is used to filter out statement identifiers
         * for which we have already queried against the database and thereby
         * avoid a re-scan for the use of that statement identifier in the
         * database.
         * 
         * FIXME A TempTripleStore does not support concurrent readers and
         * writers because it is backed by a TemporaryStore and not a Journal
         * (with its concurrency controls). This is handled in AccessPath by
         * always using a fully buffered iterator(), so we do not really gain
         * the ability to scale-up from the TempTripleStore.
         */
        long statementCount0;
        long statementCount1;
        final HashSet<Long> sids = new HashSet<Long>();
        int nrounds = 0;
        do {

            // increment the round counter.
            nrounds++;

            // note: count will be exact.
            statementCount0 = tempStore.getStatementCount();

            // visit the explicit statements since only they can have statement identifiers.
            final IChunkedOrderedIterator<ISPO> itr = tempStore.getAccessPath(
                    SPOKeyOrder.SPO,ExplicitSPOFilter.INSTANCE).iterator();

            try {
                
                while(itr.hasNext()) {
                    
                    final long sid = itr.next().getStatementIdentifier();
                    
                    if(sids.contains(sid)) continue;
                    
                    // sid in the subject position.
                    tempStore.addStatements(tempStore, true/* copyOnly */, db
                                    .getAccessPath(sid, NULL, NULL).iterator(),
                                    null/* filter */);

                    /*
                     * sid in the predicate position.
                     * 
                     * Note: this case is not allowed by RDF but a TMGraph model
                     * might use it.
                     */
                    tempStore.addStatements(tempStore, true/* copyOnly */, db
                                    .getAccessPath(NULL, sid, NULL).iterator(),
                                    null/* filter */);

                    // sid in the object position.
                    tempStore.addStatements(tempStore, true/*copyOnly*/, db
                                    .getAccessPath(NULL, NULL, sid).iterator(),
                                    null/* filter */);

                    // finished with this sid.
                    sids.add(sid);
                    
                }
                
            } finally {
                
                itr.close();
                
            }
            
            // note: count will be exact.
            statementCount1 = tempStore.getStatementCount();

            log.warn("Finished " + nrounds + " rounds: statementBefore="
                    + statementCount0 + ", statementsAfter=" + statementCount1);
            
        } while (statementCount0 < statementCount1);

    }
    
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

    /**
     * 
     * @param solutionFlags
     *            See {@link IJoinNexus#ELEMENT} and friends.
     * @param filter
     *            Optional filter.
     * @return
     */
    public IJoinNexusFactory newJoinNexusFactory(RuleContextEnum ruleContext,
            ActionEnum action, int solutionFlags, IElementFilter filter) {
        
        return newJoinNexusFactory(ruleContext, action, solutionFlags, filter, 
                isJustify());
        
    }
    
    /**
     * 
     * @param solutionFlags
     *            See {@link IJoinNexus#ELEMENT} and friends.
     * @param filter
     *            Optional filter.
     * @return
     */
    public IJoinNexusFactory newJoinNexusFactory(RuleContextEnum ruleContext,
			ActionEnum action, int solutionFlags, IElementFilter filter,
            boolean justify) {
        
        if (ruleContext == null)
            throw new IllegalArgumentException();

        if (action == null)
            throw new IllegalArgumentException();
        
        if(action.isMutation()) {
        	
        	assertWritable();
        	
        }

        // use the timestamp for the database view.
        final long writeTimestamp = getTimestamp();
        
//        /*
//		 * The default is to read from the last committed state for the
//		 * database.
//         * 
//         * @issue negative timestamp
//		 */
//        final long readTimestamp = -getIndexManager().getLastCommitTime();
        
        /*
         * Use the timestamp for the database view.
         * 
         * Note: The choice here reflects the use of the
         * UnisolatedReadWriteIndex to allow interleaved reads and writes on the
         * unisolated indices when using a Journal or Temporary(Raw)Store. When
         * running against an IBigdataFederation, the ConcurrencyManager will be
         * interposed and unisolated writes will result in commits.
         */
        final long readTimestamp;
        if (writeTimestamp == ITx.UNISOLATED
                && getIndexManager() instanceof IBigdataFederation) {
           
            /*
             * We reading against a federation we use the READ_COMMITTED view in
             * order to permit higher concurrency (writers will obtain a lock on
             * the unisolated view, but readers will not block if they are
             * reading from the read-committed view).
             */
            
            readTimestamp = ITx.READ_COMMITTED;
            
        } else {

            /*
             * Otherwise, just use the same view as the resource container.
             */
            
            readTimestamp = getTimestamp();
            
        }

        return new RDFJoinNexusFactory(ruleContext, action, writeTimestamp,
				readTimestamp, justify, bufferCapacity, solutionFlags, filter);
        
    }
    
    /**
     * Specialized JOIN operation using the full text index to identify possible
     * completions of the given literals for which there exists a subject
     * <code>s</code> such that:
     * 
     * <pre>
     * SELECT ?s, ?t, ?lit
     *     (?lit completionOf, lits)
     * AND (?s ?p ?lit)
     * AND (?s rdf:type ?t)
     * AND (?t rdfs:subClassOf cls)
     * WHERE
     *     p IN {preds}
     * </pre>
     * 
     * Note: The JOIN runs asynchronously.
     * 
     * @param lits
     *            One or more literals. The completions of these literals will
     *            be discovered using the {@link FullTextIndex}. (A completion
     *            is any literal having one of the given literals as a prefix
     *            and includes an exact match on the litteral as a degenerate
     *            case.)
     * @param preds
     *            One or more predicates that link the subjects of interest to
     *            the completions of the given literals. Typically this array
     *            will include <code>rdf:label</code>.
     * @param cls
     *            All subjects visited by the iterator will be instances of this
     *            class.
     * 
     * @return An iterator visiting bindings sets. Each binding set will have
     *         bound values for <code>s</code>, <code>t</code>, and
     *         <code>lit</code> where those variables are defined per the
     *         pseudo-code JOIN above.
     * 
     * @throws InterruptedException
     *             if the operation is interrupted.
     */
    public Iterator<Map<String, Value>> match(Literal[] lits, URI[] preds,
            URI cls) {

        if (lits == null || lits.length == 0)
            throw new IllegalArgumentException();

        if (preds == null || preds.length == 0)
            throw new IllegalArgumentException();
        
        if (cls == null)
            throw new IllegalArgumentException();
        
        /*
         * Translate the predicates into term identifiers.
         * 
         * @todo batch translate.
         */
        final long[] _preds = new long[preds.length];
        {
            
            int nknown = 0;
            
            for(int i=0; i<preds.length; i++) {
                
                final long tid = getTermId(preds[i]);

                if (tid != NULL)
                    nknown++;
                
                _preds[i] = tid;
                
            }
            
            if (nknown == 0) {
                
                log.warn("No known predicates: preds="+preds);
                
                return EmptyIterator.DEFAULT;
                
            }
            
        }
        
        /*
         * Translate the class constraint into a term identifier.
         * 
         * @todo batch translate with the predicates (above).
         */
        final long _cls = getTermId(cls);
        
        if (_cls == NULL) {
         
            log.warn("Unknown class: class="+cls);
            
            return EmptyIterator.DEFAULT;
            
        }

        /*
         * Generate a program from the possible completions of the literals.
         */
        final IProgram program = getMatchProgram(lits, _preds, _cls);
        
        final int solutionFlags = IJoinNexus.ELEMENT;

        // @todo any filter to apply here?
		final IJoinNexus joinNexus = newJoinNexusFactory(
				RuleContextEnum.HighLevelQuery, 
				ActionEnum.Query,
				solutionFlags, null/* filter */)
				.newInstance(getIndexManager());

        /*
         * Resolve ISolutions to their SPO elements in chunks, SPO[] chunks to
         * Statements, and Statemens to a Map<String,Object> or some such.
         */
        try {
        
            return new BindingSetIterator(//
                  new BigdataStatementIteratorImpl(this,
                  new ChunkedResolvingIterator<ISPO,ISolution>(joinNexus.runQuery(program)){

                    @Override
                    protected ISPO resolve(ISolution e) {
                        
                        return (ISPO) e.get();
                        
                    }
                      
                  }));
    
        } catch(Exception ex) {
            
            // runQuery() throws Exception.
            
            throw new RuntimeException(ex);
            
        }


    }

    /**
     * Generate a program from the possible completions of the literals.
     * 
     * @param lits
     *            One or more literals. The completions of these literals will
     *            be discovered using the {@link FullTextIndex}. (A completion
     *            is any literal having one of the given literals as a prefix
     *            and includes an exact match on the litteral as a degenerate
     *            case.)
     * @param _preds
     *            One or more term identifiers for predicates that link the
     *            subjects of interest to the completions of the given literals.
     *            Typically this array will include the term identifier for
     *            <code>rdf:label</code>.
     * @param _cls
     *            All subjects visited by the iterator will be instances of the
     *            class assigned this term identifier.
     * 
     * @return A generated program. When run as a query, the program will
     *         produce {@link ISolution}s corresponding to the head of the
     *         {@link MatchRule}.
     */
    protected Program getMatchProgram(Literal[] lits, long[] _preds,
            long _cls) {
        
        final Iterator<Long> termIdIterator = getLexiconRelation().prefixScan(lits);

        // the term identifier for the completed literal.
        final IVariable<Long>lit = Var.var("lit");

        // instantiate the rule.
        final Rule r = new MatchRule(getSPORelation().getNamespace(),
                getInferenceEngine(), lit, _preds, new Constant<Long>(_cls));

        // bindings used to specialize the rule for each completed literal.
        final IBindingSet bindings = new ArrayBindingSet(r.getVariableCount());

        final Program program = new Program("match",true/*parallel*/);
        
        // specialize and apply to each completed literal.
        while (termIdIterator.hasNext()) {

            final Long tid = termIdIterator.next();

            bindings.set(lit, new Constant<Long>(tid));
            
            final IRule tmp = r.specialize(bindings, null/*constraints*/);

            program.addStep(tmp);
            
        }

        return program;

    }
    
    /**
     * Converts what appears to be a set of statements into a binding set for
     * {@link LocalTripleStore#match(Literal[], URI[], URI)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo this should be replaced by a general purpose binding set mechanism
     *       that can supports JOINs on the KB and more general JOINs on bigdata
     *       indices.
     */
    private class BindingSetIterator implements Iterator<Map<String,Value>> {

        private final BigdataStatementIterator src;

        private boolean open = true;

        public BindingSetIterator(BigdataStatementIterator src) {
            
            if (src == null)
                throw new IllegalArgumentException();
            
            this.src = src;
            
        }
        
        public boolean hasNext() {

            try {

                final boolean hasNext = src.hasNext();
                
                if(!hasNext && open) {
                    
                    open = false;
                    
                    src.close();
                    
                }
                
                return hasNext;
                
            } catch (SailException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        public Map<String, Value> next() {

            final Statement s;
//            try {

                s = src.next();
                
//            } catch (SailException e) {
//                
//                throw new RuntimeException(e);
//                
//            }
            
            final Map<String,Value> bindings = new HashMap<String,Value>();
            
            bindings.put("s",s.getSubject());

            bindings.put("t",s.getPredicate());
            
            bindings.put("lit",(Literal)s.getObject());
            
            return bindings;
            
        }

        public void remove() {

            throw new UnsupportedOperationException();

        }
        
    };
    
}
