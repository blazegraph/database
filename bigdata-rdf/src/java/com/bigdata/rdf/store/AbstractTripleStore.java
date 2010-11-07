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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
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

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.axioms.Axioms;
import com.bigdata.rdf.axioms.BaseAxioms;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.inf.IJustificationIterator;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.JustificationIterator;
import com.bigdata.rdf.internal.DefaultExtensionFactory;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.IExtensionFactory;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.BigdataRDFFullTextIndex;
import com.bigdata.rdf.lexicon.ITermIndexCodes;
import com.bigdata.rdf.lexicon.ITextIndexer;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.FastClosure;
import com.bigdata.rdf.rules.FullClosure;
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
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.spo.SPOTupleSerializer;
import com.bigdata.rdf.spo.StatementWriter;
import com.bigdata.rdf.vocab.BaseVocabulary;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IDatabase;
import com.bigdata.relation.IMutableDatabase;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationSchema;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.Var;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.DefaultRuleTaskFactory;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.search.FullTextIndex;
import com.bigdata.service.AbstractEmbeddedDataService;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ndx.IClientIndex;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedConvertingIterator;
import com.bigdata.striterator.DelegateChunkedIterator;
import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Abstract base class that implements logic for the {@link ITripleStore}
 * interface that is invariant across the choice of the backing store.
 * <p>
 * By default, this class supports RDFS inference plus optional support for
 * <code>owl:sameAs</code>, <code>owl:equivalentProperty</code>, and
 * <code>owl:equivalentClass</code>. The {@link IRule}s are declarative, and
 * it is easy to write new rules. Those {@link IRule}s can be introduced using
 * custom {@link BaseClosure} implementations. See {@link Options#CLOSURE_CLASS}.
 * 
 * @todo When using a read-historical views for a very large scale-out triple
 *       store on an {@link IBigdataFederation} with concurrent writes (or
 *       concurrent overflow of data services) can cause exceptions to be thrown
 *       if resources required for the view are simultaneously released. This is
 *       most likely to occur for operations that must traverse all tuples in a
 *       scale-out index, such as {@link #getExactStatementCount()} or
 *       {@link #getExactStatementCount()}.
 *       <p>
 *       The workaround is to use {@link ITimestampService#setEarliestTxStartTime(long)}
 *       to ensure that the data for the historical view are not released during
 *       the operation.
 *       <p>
 *       The fix will be the use of a read-historical <em>transaction</em>
 *       which will explicitly coordinate the release time for the
 *       {@link IBigdataFederation}.
 * 
 * @todo Support rules (IF TripleExpr THEN ...) This is basically encapsulating
 *       the rule execution layer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTripleStore extends
        AbstractResource<IDatabase<AbstractTripleStore>> implements ITripleStore,
        IRawTripleStore, IMutableDatabase<AbstractTripleStore> {

    final static protected Logger log = Logger.getLogger(ITripleStore.class);

    /**
     * This is used to conditionally enable the logic to retract justifications
     * when the corresponding statements is retracted.
     */
    final private boolean justify;
    
    /**
     * True iff justification chains are being recorded for entailments and used
     * to support truth maintenance.
     * <P>
     * Note: This is the same value that is reported by {@link SPORelation#justify}.
     * 
     * @see Options#JUSTIFY
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
     * The #of internal values in the key for a statement index (3 is a triple
     * store, 4 is a quad store).
     * 
     * @see Options#QUADS
     */
    final private int spoKeyArity;

    /**
     * <code>true</code> iff this is a quad store.
     */
    final private boolean quads;

    /**
     * Indicate whether this is a triple or a quad store (3 is a triple store, 4
     * is a quad store).
     * 
     * @return The #of elements in the key for the {@link SPORelation} (3 or 4).
     */
    final public int getSPOKeyArity() {
        
        return spoKeyArity;
        
    }

    /**
     * Return <code>true</code> iff this is a quad store.
     * 
     * @see Options#QUADS
     */
    final public boolean isQuads() {
        
        return quads;
        
    }
    
    /**
     * When <code>true</code> the database will support statement identifiers.
     * A statement identifier is a unique 64-bit integer taken from the same
     * space as the term identifiers and which uniquely identifiers a statement
     * in the database regardless of the graph in which that statement appears.
     * The purpose of statement identifiers is to allow statements about
     * statements without recourse to RDF style reification.
     * 
     * @see Options#STATEMENT_IDENTIFIERS
     */
    final public boolean isStatementIdentifiers() {

        return statementIdentifiers;
        
    }
    final private boolean statementIdentifiers;
    
    /**
     * The {@link Axioms} class.
     * 
     * @see com.bigdata.rdf.store.AbstractTripleStore.Options#AXIOMS_CLASS
     */
    final private Class<? extends BaseAxioms> axiomClass;
    
    /**
     * The {@link Vocabulary} class.
     * 
     * @see Options#VOCABULARY_CLASS
     */
    final private Class<? extends BaseVocabulary> vocabularyClass;
    
    /**
     * The {@link BaseClosure} class.
     * 
     * @see Options#CLOSURE_CLASS
     */
    final private Class<? extends BaseClosure> closureClass;
    
    /**
     * Return an instance of the class that is used to compute the closure of
     * the database.
     */
    public BaseClosure getClosureInstance() {

        try {

            final Constructor<? extends BaseClosure> ctor = closureClass
                    .getConstructor(new Class[] { AbstractTripleStore.class });

            return ctor.newInstance(this);

        } catch (Exception e) {

            throw new RuntimeException(e);

        }
        
    }
    
    /**
     * Return <code>true</code> iff the fully bound statement is an axiom.
     * 
     * @param s
     *            The internal value ({@link IV}) for the subject position.
     * @param p
     *            The internal value ({@link IV}) for the predicate position.
     * @param o
     *            The internal value ({@link IV}) for the object position.
     */
    public boolean isAxiom(final IV s, final IV p, final IV o) {
        
        return getAxioms().isAxiom(s, p, o);
        
    }
        
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
     */
    public boolean getStatementIdentifiers() {
        
        return statementIdentifiers;
        
    }
    
    /**
     * Returns <code>true</code> when the database is in inline terms mode. In
     * this mode, certain types of terms (numerics in particular) are inlined
     * into the statement indices rather than being mapped to and from term
     * identifiers in the lexicon.
     */
    public boolean isInlineLiterals() {
        
        return getLexiconRelation().isInlineLiterals();
        
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
    final public BigdataValueFactory getValueFactory() {

        if (valueFactory == null) {

            if (!lexicon) {

                throw new UnsupportedOperationException();

            }

            synchronized (this) {

                if (valueFactory == null) {

                    valueFactory = getLexiconRelation().getValueFactory();

                }

            }

        }
        
        return valueFactory;
        
    }
    private volatile BigdataValueFactory valueFactory;
    
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
     * @todo refactor options to/from {@link SPORelation} and {@link LexiconRelation}?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends AbstractResource.Options,
            InferenceEngine.Options, com.bigdata.journal.Options,
            KeyBuilder.Options, DataLoader.Options, FullTextIndex.Options {

        /**
         * Boolean option (default <code>true</code>) enables support for the
         * lexicon (the forward and backward term indices). When
         * <code>false</code>, the lexicon indices are not registered. This can
         * be safely turned off for the {@link TempTripleStore} when only the
         * statement indices are to be used.
         * <p>
         * You can control how the triple store will interpret the RDF URIs, and
         * literals using the {@link KeyBuilder.Options}. For example:
         * 
         * <pre>
         * // Force ASCII key comparisons.
         * properties.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
         * </pre>
         * 
         * or
         * 
         * <pre>
         * // Force identical unicode comparisons (assuming default COLLATOR setting).
         * properties.setProperty(Options.STRENGTH, StrengthEnum.IDENTICAL.toString());
         * </pre>
         * 
         * @see LexiconRelation
         * @see KeyBuilder.Options
         */
        String LEXICON = AbstractTripleStore.class.getName() + ".lexicon";

        String DEFAULT_LEXICON = "true";

        /**
         * Boolean option (default {@value #DEFAULT_STORE_BLANK_NODES}) controls
         * whether or not we store blank nodes in the forward mapping of the
         * lexicon (this is also known as the "told bnodes" mode).
         * <p>
         * When <code>false</code> blank node semantics are enforced, you CAN
         * NOT unify blank nodes based on their IDs in the lexicon, and
         * {@link AbstractTripleStore#getBNodeCount()} is disabled.
         * <p>
         * When <code>true</code>, you are able to violate blank node semantics
         * and force unification of blank nodes by assigning the ID from the RDF
         * interchange syntax to the blank node. RIO has an option that will
         * allow you to do this. When this option is also <code>true</code>,
         * then you will in fact be able to resolve pre-existing blank nodes
         * using their identifiers. The tradeoff is time and space : if you have
         * a LOT of document using blank nodes then you might want to disable
         * this option in order to spend less time writing the forward lexicon
         * index (and it will also take up less space).
         */
        String STORE_BLANK_NODES = AbstractTripleStore.class.getName() + ".storeBlankNodes";
        
        String DEFAULT_STORE_BLANK_NODES = "false";

        /**
         * Option effects how evenly distributed the assigned term identifiers
         * which has a pronounced effect on the ID2TERM and statement indices
         * for <em>scale-out deployments</em>. The default for a scale-out
         * deployment is {@value #DEFAULT_TERMID_BITS_TO_REVERSE}, but the
         * default for a scale-up deployment is ZERO(0).
         * <p>
         * For the scale-out triple store, the term identifiers are formed by
         * placing the index partition identifier in the high word and the local
         * counter for the index partition into the low word. In addition, the
         * sign bit is "stolen" from each value such that the low two bits are
         * left open for bit flags which encode the type (URI, Literal, BNode or
         * SID) of the term. The effect of this option is to cause the low N
         * bits of the local counter value to be reversed and written into the
         * high N bits of the term identifier (the other bits are shifted down
         * to make room for this). Regardless of the configured value for this
         * option, all bits (except the sign bit) of the both the partition
         * identifier and the local counter are preserved.
         * <p>
         * Normally, the low bits of a sequential counter will vary the most
         * rapidly. By reversing the localCounter and placing some of the
         * reversed bits into the high bits of the term identifier we cause the
         * term identifiers to be uniformly (but not randomly) distributed. This
         * is much like using hash function without collisions or a random
         * number generator that does not produce duplicates. When ZERO (0) no
         * bits are reversed so the high bits of the term identifiers directly
         * reflect the partition identifier and the low bits are assigned
         * sequentially by the local counter within each TERM2ID index
         * partition.
         * <p>
         * The use of a non-zero value for this option can easily cause the
         * write load on the index partitions for the ID2TERM and statement
         * indices to be perfectly balanced. However, using too many bits has
         * some negative consequences on locality of operations <em>within</em>
         * an index partition (since the distribution of the keys be
         * approximately uniform distribution, leading to poor cache
         * performance, more copy-on-write for the B+Tree, and both more IO and
         * faster growth in the journal for writes (since there will be more
         * leaves made dirty on average by each bulk write)).
         * <p>
         * The use of a non-zero value for this option also directly effects the
         * degree of scatter for bulk read or write operations. As more bits are
         * used, it becomes increasingly likely that each bulk read or write
         * operation will on average touch all index partitions. This is because
         * #of low order local counter bits reversed and rotated into the high
         * bits of the term identifier places an approximate bound on the #of
         * index partitions of the ID2TERM or a statement index that will be
         * touched by a scattered read or write. However, that number will
         * continue to grow slowly over time as new partition identifiers are
         * introduced (the partition identifiers appear next in the encoded term
         * identifier and therefore determine the degree of locality or scatter
         * once the quickly varying high bits have had their say).
         * <p>
         * The "right" value really depends on the expected scale of the
         * knowledge base. If you estimate that you will have 50 x 200M index
         * partitions for the statement indices, then SQRT(50) =~ 7 would be a
         * good choice.
         */
        String TERMID_BITS_TO_REVERSE = AbstractTripleStore.class
                .getName()
                + ".termIdBitsToReverse";

        String DEFAULT_TERMID_BITS_TO_REVERSE = "6";

        /**
         * Boolean option (default <code>true</code>) enables support for a
         * full text index that may be used to lookup literals by tokens found
         * in the text of those literals.
         */
        String TEXT_INDEX = AbstractTripleStore.class.getName() + ".textIndex";

        String DEFAULT_TEXT_INDEX = "true";

        /**
         * Boolean option (default <code>false</code>) enables support for a
         * full text index that may be used to lookup datatype literals by
         * tokens found in the text of those literals.
         */
        String TEXT_INDEX_DATATYPE_LITERALS = AbstractTripleStore.class
                .getName()
                + ".textIndex.datatypeLiterals";

        String DEFAULT_TEXT_INDEX_DATATYPE_LITERALS = "true";
        
        /**
         * Integer option whose value is the capacity of the term cache. This
         * cache provides fast lookup of frequently used RDF {@link Value}s by
         * their term identifier.
         */
        String TERM_CACHE_CAPACITY = AbstractTripleStore.class.getName()
                + ".termCache.capacity";
        
        String DEFAULT_TERM_CACHE_CAPACITY = "5000"; // was 50000
        
        /**
         * The name of the class that will establish the pre-defined
         * {@link Vocabulary} for the database (default
         * {@value #DEFAULT_VOCABULARY_CLASS}). The class MUST extend
         * {@link BaseVocabulary}. This option is ignored if the lexicon is
         * disabled.
         * <p>
         * The {@link Vocabulary} is initialized by
         * {@link AbstractTripleStore#create()} and its serialized state is
         * stored in the global row store under the
         * {@link TripleStoreSchema#VOCABULARY} property.  
         * 
         * @see NoVocabulary
         * @see RDFSVocabulary
         */
        String VOCABULARY_CLASS = AbstractTripleStore.class.getName() + ".vocabularyClass";

        String DEFAULT_VOCABULARY_CLASS = RDFSVocabulary.class.getName();
        
        /**
         * The {@link Axioms} model that will be used (default
         * {@value Options#DEFAULT_AXIOMS_CLASS}). The value is the name of the
         * class that will be instantiated by
         * {@link AbstractTripleStore#create()}. The class must extend
         * {@link BaseAxioms}. This option is ignored if the lexicon is
         * disabled.  Use {@link NoAxioms} to disable inference.
         */
        String AXIOMS_CLASS = AbstractTripleStore.class.getName() + ".axiomsClass";
        
        String DEFAULT_AXIOMS_CLASS = OwlAxioms.class.getName();

        /**
         * The name of the {@link BaseClosure} class that will be used (default
         * {@value Options#DEFAULT_CLOSURE_CLASS}). The value is the name of
         * the class that will be used to generate the {@link Program} that
         * computes the closure of the database. The class must extend
         * {@link BaseClosure}. This option is ignored if the inference is
         * disabled.
         * <p>
         * There are two pre-defined "programs" used to compute and maintain
         * closure. The {@link FullClosure} program is a simple fix point of the
         * RDFS+ entailments, except for the
         * <code> foo rdf:type rdfs:Resource</code> entailments which are
         * normally generated at query time. The {@link FastClosure} program
         * breaks nearly all cycles in the RDFS rules and runs nearly entirely
         * as a sequence of {@link IRule}s, including several custom rules.
         * <p>
         * It is far easier to modify the {@link FullClosure} program since any
         * new rules can just be dropped into place. Modifying the
         * {@link FastClosure} program requires careful consideration of the
         * entailments computed at each stage in order to determine where a new
         * rule would fit in.
         * <p>
         * Note: When support for <code>owl:sameAs</code>, etc. processing is
         * enabled, some of the entailments are computed by rules run during
         * forward closure and some of the entailments are computed by rules run
         * at query time. Both {@link FastClosure} and {@link FullClosure} are
         * aware of this and handle it correctly (e.g., as configured).
         */
        String CLOSURE_CLASS = AbstractTripleStore.class.getName() + ".closureClass";
        
        String DEFAULT_CLOSURE_CLASS = FastClosure.class.getName();

        /**
         * Boolean option (default <code>false</code>) disables all but a
         * single statement index (aka access path).
         * <p>
         * Note: The main purpose of the option is to make it possible to turn
         * off the other access paths for special bulk load purposes. The use of
         * this option is NOT compatible with either the application of the
         * {@link InferenceEngine} or high-level query.
         * <p>
         * Note: You may want to explicitly enable or disable the bloom filter
         * for this. Normally a single access path (SPO) is used for a temporary
         * store. Temporary stores tend to be smaller, so if you will also be
         * doing point tests on the temporary store then you probably want to
         * use the {@link #BLOOM_FILTER}. Otherwise it may be turned off to
         * realize some (minimal) performance gain.
         */
        String ONE_ACCESS_PATH = AbstractTripleStore.class.getName() + ".oneAccessPath";

        String DEFAULT_ONE_ACCESS_PATH = "false";

        /**
         * Optional property controls whether or not a bloom filter is
         * maintained for the SPO statement index. The bloom filter is effective
         * up to ~ 2M entries per index (partition). For scale-up, the bloom
         * filter is automatically disabled after its error rate would be too
         * large given the #of index entries. For scale-out, as the index grows
         * we keep splitting it into more and more index partitions, and those
         * index partitions are comprised of both views of one or more
         * {@link AbstractBTree}s. While the mutable {@link BTree}s might
         * occasionally grow too large to support a bloom filter, data is
         * periodically migrated onto immutable {@link IndexSegment}s which have
         * perfect fit bloom filters. This means that the bloom filter
         * scales-out, but not up.
         * <p>
         * Note: The SPO access path is used any time we have an access path
         * that corresponds to a point test. Therefore this is the only index
         * for which it makes sense to maintain a bloom filter.
         * <p>
         * If you are going to do a lot of small commits, then please DO NOT
         * enable the bloom filter for the {@link AbstractTripleStore}. The
         * bloom filter takes 1 MB each time you commit on the SPO/SPOC index.
         * The bloom filter limited value in any case for scale-up since its
         * nominal error rate will be exceeded at ~2M triples. This concern does
         * not apply for scale-out, where the bloom filter is always a good
         * idea.
         * 
         * @see IndexMetadata#getBloomFilterFactory()
         * 
         * @todo Review the various temp triple stores that are created and see
         *       which of them would benefit from the SPO bloom filter (TM,
         *       backchainers, SIDs fixed point, etc).
         */
        String BLOOM_FILTER = AbstractTripleStore.class.getName() + ".bloomFilter";
        
        String DEFAULT_BLOOM_FILTER = "true";
        
        /**
         * When <code>true</code> (default {@value Options#DEFAULT_JUSTIFY}),
         * proof chains for entailments generated by forward chaining are stored
         * in the database. This option is required for truth maintenance when
         * retracting assertion.
         * <p>
         * If you will not be retracting statements from the database then you
         * can specify <code>false</code> for a significant performance boost
         * during writes and a smaller profile on the disk.
         * <p>
         * This option does not effect query performance since the
         * justifications are maintained in a distinct index and are only used
         * when retracting assertions.
         */
        String JUSTIFY = AbstractTripleStore.class.getName() + ".justify";

        String DEFAULT_JUSTIFY = "true";

        /**
         * Boolean option (default {@value #DEFAULT_STATEMENT_IDENTIFIERS})
         * enables support for statement identifiers. A statement identifier is
         * unique identifier for a <em>triple</em> in the database. Statement
         * identifiers may be used to make statements about statements without
         * using RDF style reification.
         * <p>
         * Statement identifiers are assigned consistently when {@link Statement}
         * s are mapped into the database. This is done using an extension of
         * the <code>term:id</code> index to map the statement as if it were a
         * term onto a unique statement identifier. While the statement
         * identifier is assigned canonically by the <code>term:id</code> index,
         * it is stored redundantly in the value position for each of the
         * statement indices. While the statement identifier is, in fact, a term
         * identifier, the reverse mapping is NOT stored in the id:term index
         * and you CAN NOT translate from a statement identifier back to the
         * original statement.
         * <p>
         * bigdata supports an RDF/XML interchange extension for the interchange
         * of <em>triples</em> with statement identifiers that may be used as
         * blank nodes to make statements about statements. See {@link BD} and
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
         * requires <em>twice</em> the amount of data on the disk since you need
         * an additional three statement indices to cover the quad access paths.
         * <p>
         * The provenance mode (SIDs) IS NOT compatible with the {@link #QUADS}
         * mode. You may use either one, but not both in the same KB instance.
         * <p>
         * There are examples for using the provenance mode online.
         */
        String STATEMENT_IDENTIFIERS = AbstractTripleStore.class.getName()
                + ".statementIdentifiers";

        String DEFAULT_STATEMENT_IDENTIFIERS = "false";

        /**
         * Boolean option determines whether the KB instance will be a quad
         * store or a triple store. For a triple store only, the
         * {@link #STATEMENT_IDENTIFIERS} option determines whether or not the
         * provenance mode is enabled.
         */
        String QUADS = AbstractTripleStore.class.getName() + ".quads";
        
        String DEFAULT_QUADS = "false";
        
        /**
         * Set up database in triples mode, no provenance.  This is equivalent
         * to setting the following options:
         * <p>
         * <ul>
         * <li>{@link AbstractTripleStore.Options#QUADS} 
         *          = <code>false</code></li>
         * <li>{@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} 
         *          = <code>false</code></li>
         * </ul> 
         */
        String TRIPLES_MODE = AbstractTripleStore.class.getName()
                + ".triplesMode";
        
        String DEFAULT_TRIPLES_MODE = "false";

        /**
         * Set up database in triples mode with provenance.  This is equivalent
         * to setting the following options:
         * <p>
         * <ul>
         * <li>{@link AbstractTripleStore.Options#QUADS} 
         *          = <code>false</code></li>
         * <li>{@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} 
         *          = <code>true</code></li>
         * </ul> 
         */
        String TRIPLES_MODE_WITH_PROVENANCE = AbstractTripleStore.class.getName()
                + ".triplesModeWithProvenance";
        
        String DEFAULT_TRIPLES_MODE_WITH_PROVENANCE = "false";

        
        /**
         * Set up database in quads mode.  Quads mode means no provenance,
         * no inference.  This is equivalent to setting the following options:
         * <p>
         * <ul>
         * <li>{@link AbstractTripleStore.Options#QUADS} 
         *          = <code>true</code></li>
         * <li>{@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} 
         *          = <code>false</code></li>
         * <li>{@link AbstractTripleStore.Options#AXIOMS_CLASS} 
         *          = <code>com.bigdata.rdf.store.AbstractTripleStore.NoAxioms</code></li>
         * </ul> 
         */
        String QUADS_MODE = AbstractTripleStore.class.getName()
                + ".quadsMode";

        String DEFAULT_QUADS_MODE = "false";

        /**
         * The name of the {@link BigdataValueFactory} class. The implementation
         * MUST declare a method with the following signature which will be used
         * as a canonicalizing factory for the instances of that class.
         * 
         * <pre>
         * public static BigdataValueFactory getInstance(final String namespace)
         * </pre>
         * 
         * @see #DEFAULT_VALUE_FACTORY_CLASS
         */
        String VALUE_FACTORY_CLASS = AbstractTripleStore.class.getName()
                + ".valueFactoryClass";

        String DEFAULT_VALUE_FACTORY_CLASS = BigdataValueFactoryImpl.class
                .getName();

        /**
         * The name of the {@link ITextIndexer} class. The implementation MUST
         * declare a method with the following signature which will be used to
         * locate instances of that class.
         * 
         * <pre>
         * static public ITextIndexer getInstance(final IIndexManager indexManager,
         *             final String namespace, final Long timestamp,
         *             final Properties properties)
         * </pre>
         * 
         * @see #DEFAULT_TEXT_INDEXER_CLASS
         */
        String TEXT_INDEXER_CLASS = AbstractTripleStore.class.getName()
                + ".textIndexerClass";

        String DEFAULT_TEXT_INDEXER_CLASS = BigdataRDFFullTextIndex.class
                .getName();

        /**
         * Set up database to inline certain datatype literals directly into the 
         * statement indices rather than using the lexicon to map them to term 
         * identifiers and back.
         */
        String INLINE_LITERALS = AbstractTripleStore.class.getName()
                + ".inlineLiterals";

        String DEFAULT_INLINE_LITERALS = "true";

        /**
         * Set up database to inline bnodes directly into the statement indices 
         * rather than using the lexicon to map them to term identifiers and 
         * back.  This is only compatible with told bnodes mode.
         * <p>
         * See {@link Options#STORE_BLANK_NODES}.
         */
        String INLINE_BNODES = AbstractTripleStore.class.getName()
                + ".inlineBNodes";

        String DEFAULT_INLINE_BNODES = "false";

        /**
         * The name of the {@link IExtensionFactory} class. The implementation 
         * MUST declare a constructor that accepts an 
         * {@link IDatatypeURIResolver} as its only argument.  The 
         * {@link IExtension}s constructed by the factory need a resolver to
         * resolve datatype URIs to term identifiers in the database.
         * 
         * @see #DEFAULT_EXTENSION_FACTORY_CLASS
         */
        String EXTENSION_FACTORY_CLASS = AbstractTripleStore.class.getName()
                + ".extensionFactoryClass";

        String DEFAULT_EXTENSION_FACTORY_CLASS = DefaultExtensionFactory.class
                .getName();

    }

    protected Class determineAxiomClass() {
        
        // axiomsClass
        {

            /*
             * Note: axioms may not be defined unless the lexicon is enabled
             * since the axioms require the lexicon in order to resolve
             * their expression as Statements into their expression as SPOs.
             */

            final String className = getProperty(Options.AXIOMS_CLASS,
                    Options.DEFAULT_AXIOMS_CLASS);

            final Class cls;
            try {
                cls = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Bad option: "
                        + Options.AXIOMS_CLASS, e);
            }

            if (!BaseAxioms.class.isAssignableFrom(cls)) {
                throw new RuntimeException(Options.AXIOMS_CLASS
                        + ": Must extend: " + BaseAxioms.class.getName());
            }

            if (cls != NoAxioms.class && quads) {

                throw new UnsupportedOperationException(Options.QUADS
                        + " does not support inference ("
                        + Options.AXIOMS_CLASS + ")");

            }

            return cls;

        }
        
    }
    
    protected Class determineVocabularyClass() {
        
        // vocabularyClass
        {

            final String className = getProperty(Options.VOCABULARY_CLASS,
                    Options.DEFAULT_VOCABULARY_CLASS);

            final Class cls;
            try {
                cls = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Bad option: "
                        + Options.VOCABULARY_CLASS, e);
            }

            if (!BaseVocabulary.class.isAssignableFrom(cls)) {
                throw new RuntimeException(Options.VOCABULARY_CLASS
                        + ": Must extend: "
                        + BaseVocabulary.class.getName());
            }
            
            return cls;

        }
        
    }
    
    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @see Options
     */
    @SuppressWarnings("unchecked")
    protected AbstractTripleStore(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);
        
        /*
         * Reads off the property for the inference engine that tells us whether
         * or not the justification index is being used. This is used to
         * conditionally enable the logic to retract justifications when the
         * corresponding statements is retracted.
         */

        this.lexicon = Boolean.parseBoolean(getProperty(Options.LEXICON,
                Options.DEFAULT_LEXICON));

        DatabaseMode mode = null;
        if (Boolean.parseBoolean(getProperty(Options.TRIPLES_MODE,
                Options.DEFAULT_TRIPLES_MODE))) {
            mode = DatabaseMode.TRIPLES;
        } else if (Boolean.parseBoolean(getProperty(Options.TRIPLES_MODE_WITH_PROVENANCE,
                Options.DEFAULT_TRIPLES_MODE_WITH_PROVENANCE))) {
            if (mode != null) {
                throw new UnsupportedOperationException(
                        "please select only one of triples, provenance, or quads modes");
            }
            mode = DatabaseMode.PROVENANCE;
        } else if (Boolean.parseBoolean(getProperty(Options.QUADS_MODE,
                Options.DEFAULT_QUADS_MODE))) {
            if (mode != null) {
                throw new UnsupportedOperationException(
                        "please select only one of triples, provenance, or quads modes");
            }
            mode = DatabaseMode.QUADS;
        } 
        
        if (mode != null) {
            switch (mode) {
            case TRIPLES: {
                this.quads = false;
                this.statementIdentifiers = false;
                this.axiomClass = determineAxiomClass();
                if (lexicon) {
                    this.vocabularyClass = determineVocabularyClass();
                } else {
                    this.vocabularyClass = NoVocabulary.class;
                }
                properties.setProperty(Options.QUADS, "false");
                properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
                break;
            }
            case PROVENANCE: {
                this.quads = false;
                this.statementIdentifiers = true;
                this.axiomClass = determineAxiomClass();
                if (lexicon) {
                    this.vocabularyClass = determineVocabularyClass();
                } else {
                    this.vocabularyClass = NoVocabulary.class;
                }
                properties.setProperty(Options.QUADS, "false");
                properties.setProperty(Options.STATEMENT_IDENTIFIERS, "true");
                break;
            }
            case QUADS: {
                this.quads = true;
                this.statementIdentifiers = false;
                this.axiomClass = NoAxioms.class;
                this.vocabularyClass = NoVocabulary.class;
                properties.setProperty(Options.QUADS, "true");
                properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");
                properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
                properties.setProperty(Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
                break;
            }
            default:
                throw new AssertionError();
            }
        } else {
            
            this.quads = Boolean.valueOf(getProperty(Options.QUADS,
                    Options.DEFAULT_QUADS));

            this.statementIdentifiers = Boolean.parseBoolean(getProperty(
                    Options.STATEMENT_IDENTIFIERS,
                    Options.DEFAULT_STATEMENT_IDENTIFIERS));

            if (lexicon) {
                
                axiomClass = determineAxiomClass();
                vocabularyClass = determineVocabularyClass();
                
            } else {
                
                /*
                 * no axioms if no lexicon (the lexicon is required to write the
                 * axioms).
                 */

                axiomClass = NoAxioms.class;
                vocabularyClass = NoVocabulary.class;

            }
            
        }
        
        
        this.justify = Boolean.parseBoolean(getProperty(Options.JUSTIFY,
                Options.DEFAULT_JUSTIFY));

        this.spoKeyArity = quads ? 4 : 3;

        if (statementIdentifiers && quads) {

            throw new UnsupportedOperationException(Options.QUADS
                    + " does not support the provenance mode ("
                    + Options.STATEMENT_IDENTIFIERS + ")");
            
        }

        // closureClass
        {

            final String className = getProperty(Options.CLOSURE_CLASS,
                    Options.DEFAULT_CLOSURE_CLASS);

           final Class cls;
            try {
                cls = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Bad option: "
                        + Options.CLOSURE_CLASS, e);
            }

            if (!BaseClosure.class.isAssignableFrom(cls)) {
                throw new RuntimeException(Options.CLOSURE_CLASS
                        + ": Must extend: "
                        + BaseClosure.class.getName());
            }
            
            closureClass = cls;
            
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
     * <strong>DO NOT INVOKE FROM APPLICATION CODE</strong> - this method
     * deletes the KB instance and destroys the backing database instance. It is
     * used to help tear down unit tests.
     */
    public void __tearDownUnitTest() {

        if(isOpen())
            destroy();
        
        getIndexManager().destroy();

    }

    public boolean isOpen() {
        
        return open;
        
    }
    
    public void close() {

        shutdown();

    }

    /**
     * Default is a NOP - invoked by {@link #close()} and
     * {@link #__tearDownUnitTest()}
     */
    final protected void shutdown() {

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

    public boolean isReadOnly() {

        return TimestampUtility.isReadOnly(getTimestamp());
        
    }
    
    final protected void assertWritable() {
        
        if(isReadOnly()) {
            
            throw new IllegalStateException("READ_ONLY");
            
        }
        
    }

    @Override
    public AbstractTripleStore init() {
    
    	super.init();
    	
    	return this;
    	
    }
    
    @Override
    public void create() {

        assertWritable();
        
        final Properties tmp = getProperties();
        
        // set property that will let the contained relations locate their container.
        tmp.setProperty(RelationSchema.CONTAINER, getNamespace());
        
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            super.create();

            if (lexicon) {

                lexiconRelation = new LexiconRelation(getIndexManager(),
                        getNamespace() + "."
                                + LexiconRelation.NAME_LEXICON_RELATION,
                        getTimestamp(), tmp);

                lexiconRelation.create();//assignedSplits);
                
                valueFactory = lexiconRelation.getValueFactory();

            }

            spoRelation = new SPORelation(getIndexManager(), getNamespace()
                    + "." + SPORelation.NAME_SPO_RELATION, getTimestamp(), tmp);

            spoRelation.create();//assignedSplits);

            /*
             * The axioms and the vocabulary both require the lexicon to
             * pre-exist. The axioms also requires the SPORelation to pre-exist.
             */
            if(lexicon) {

                /*
                 * Setup the vocabulary.
                 */
                {
                    
                    assert vocab == null;

                    try {

                        final Constructor<? extends BaseVocabulary> ctor = vocabularyClass
                                .getConstructor(new Class[] { AbstractTripleStore.class });

                        // save reference.
                        vocab = ctor.newInstance(new Object[] { this });

                    } catch (Exception ex) {

                        throw new RuntimeException(ex);

                    }

                    // initialize (writes on the lexicon).
                    ((BaseVocabulary) vocab).init();

                }
                
                /*
                 * Setup the axiom model.
                 */
                {

                    assert axioms == null;

                    try {

                        final Constructor<? extends BaseAxioms> ctor = axiomClass
                                .getConstructor(new Class[] { AbstractTripleStore.class });

                        // save reference.
                        axioms = ctor.newInstance(new Object[] { this });

                    } catch (Exception ex) {

                        throw new RuntimeException(ex);

                    }

                    // initialize (writes on the lexicon and statement indices). 
                    ((BaseAxioms)axioms).init();

                }

                /*
                 * Update the global row store to set the axioms and the
                 * vocabulary objects.
                 */
                {

                    final Map<String, Object> map = new HashMap<String, Object>();

                    // primary key.
                    map.put(RelationSchema.NAMESPACE, getNamespace());

                    // axioms.
                    map.put(TripleStoreSchema.AXIOMS, axioms);

                    // vocabulary.
                    map.put(TripleStoreSchema.VOCABULARY, vocab);

                    // Write the map on the row store.
                    getIndexManager().getGlobalRowStore().write(
                            RelationSchema.INSTANCE, map);

                }

            }
        
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

            unlock(resourceLock);

        }

    }

    public void destroy() {

        assertWritable();

        // FIXME unit tests fail here during tear down if the federation has
        // already been disconnected/destroyed since they can not reach the
        // lock service.  The code should handle this better.
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            if (lexicon) {

                {
                    final LexiconRelation lex = getLexiconRelation();

                    if (lex != null)
                        lex.destroy();
                }

                lexiconRelation = null;
                
                valueFactory = null;

                axioms = null;
                
                vocab = null;
                
            }

            {
                final SPORelation spo = getSPORelation();

                if (spo != null)
                    spo.destroy();
            }

            spoRelation = null;
            
            super.destroy();
            
        } finally {

            unlock(resourceLock);
            
        }

    }
    
    /**
     * The configured axioms. This is stored in the global row store and set
     * automatically if it is found in the {@link Properties}. Otherwise it is
     * set by {@link #create()}.
     * 
     * @throws IllegalStateException
     *             if there is no lexicon.
     * 
     * @see Options#LEXICON
     * @see com.bigdata.rdf.store.AbstractTripleStore.Options#AXIOMS_CLASS
     */
    final public Axioms getAxioms() {

        if (!lexicon)
            throw new IllegalStateException();
        
        if (axioms == null) {
            
            synchronized (this) {

                if (axioms == null) {

                    /*
                     * Extract the de-serialized axiom model from the global row
                     * store.
                     */
                    
                    axioms = (Axioms) getIndexManager().getGlobalRowStore()
                            .get(RelationSchema.INSTANCE, getNamespace(),
                                    TripleStoreSchema.AXIOMS);

                    if (axioms == null)
                        throw new RuntimeException("No axioms defined? : "
                                + this);

                    if (log.isInfoEnabled())
                        log.info("read axioms: "+axioms.size());
                    
                }

            }
            
        }
        
        return axioms;
        
    }
    private volatile Axioms axioms;

    /**
     * Return the configured {@link Vocabulary}. This consists of
     * {@link BigdataValue}s of interest that have been pre-evaluated against
     * the lexicon and are associated with their correct term identifiers.
     * 
     * @return The predefined vocabulary.
     * 
     * @throws IllegalStateException
     *             if there is no lexicon.
     * 
     * @see Options#LEXICON
     * @see Options#VOCABULARY_CLASS
     */
    final public Vocabulary getVocabulary() {

        if (!lexicon)
            throw new IllegalStateException();

        if (vocab == null) {

            synchronized (this) {

                if (vocab == null) {

                    /*
                     * Extract the de-serialized vocabulary from the global row
                     * store.
                     */

                    vocab = (Vocabulary) getIndexManager().getGlobalRowStore().get(
                            RelationSchema.INSTANCE, getNamespace(),
                            TripleStoreSchema.VOCABULARY);

                    if (vocab == null)
                        throw new RuntimeException("No vocabulary defined? : "
                                + this);
                    
                    if (log.isInfoEnabled())
                        log.info("read vocabular: "+vocab.size());
                    
                }
                
            }
            
        }
        
        return vocab;
        
    }
    private volatile Vocabulary vocab;
    
    /**
     * The {@link SPORelation} (triples and their access paths).
     */
    final synchronized public SPORelation getSPORelation() {
    
        if (spoRelation == null) {

            spoRelation = (SPORelation) getIndexManager().getResourceLocator()
                    .locate(getNamespace() + "." + SPORelation.NAME_SPO_RELATION,
                            getTimestamp());

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

            long t = getTimestamp();
            
            if (TimestampUtility.isReadWriteTx(t)) {
             
                /*
                 * A read-write tx must use the unisolated view of the lexicon.
                 */
                t = ITx.UNISOLATED;
                
            }

            lexiconRelation = (LexiconRelation) getIndexManager()
                    .getResourceLocator().locate(
                            getNamespace() + "."
                                    + LexiconRelation.NAME_LEXICON_RELATION, 
                                    t);

        }

        return lexiconRelation;

    }
    private LexiconRelation lexiconRelation;

// Note: Use LexiconRelation#getSearchEngine().
//    /**
//     * Full text information retrieval for RDF essentially treats the RDF
//     * Literals as "documents." The literals are broken down into "token"s to
//     * obtain a "token frequency distribution" for that literal/document. The
//     * full text index contains the indexed token data.
//     * 
//     * @return The object managing the text search indices or <code>null</code>
//     *         iff text search is not enabled.
//     * 
//     * @see Options#TEXT_INDEX
//     * @see Options#TEXT_INDEX_DATATYPE_LITERALS
//     */
//    final public ITextIndexer getSearchEngine() {
//
//        if (!lexicon)
//            return null;
//
//        return getLexiconRelation().getSearchEngine();
//    }
    
    final public long getNamedGraphCount() {

        if(!isQuads())
            throw new UnsupportedOperationException();

        final Iterator<?> itr = getSPORelation().distinctTermScan(
                SPOKeyOrder.CSPO);

        long n = 0;

        while(itr.hasNext()) {
            
            itr.next();
            
            n++;
            
        }
        
        return n;

    }

    final public long getStatementCount() {

        return getStatementCount(null/* c */, false/* exact */);

    }

    final public long getStatementCount(final boolean exact) {

        return getStatementCount(null/*c*/, exact);

    }

    final public long getStatementCount(final Resource c) {

        return getStatementCount(c, false/* exact */);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Core implementation.
     */
    final public long getStatementCount(final Resource c, final boolean exact) {

        if (exact) {

            return getAccessPath(null/* s */, null/* p */, null/* o */, c, null/* filter */)
                    .rangeCount(exact);

        } else {

            return getAccessPath(null/* s */, null/* p */, null/* o */, c, null/* filter */)
                    .rangeCount(exact);

        }

    }

    /**
     * The #of explicit statements in the database (exact count based on
     * key-range scan).
     * <p>
     * Note: In order to get the #of explicit statements in the repository we
     * have to actually do a range scan and figure out for each statement
     * whether or not it is explicit.
     * 
     * @param c
     *            The context (optional). When not given, the count is reported
     *            across all named graphs.
     */
    public long getExplicitStatementCount(final Resource c) {

        return getAccessPath(null/* s */, null/* p */, null/* o */, c,
                ExplicitSPOFilter.INSTANCE).rangeCount(true/* exact */);

    }
    
    /**
     * Clears hard references to any indices, relations, etc. MUST be extended
     * to discard write sets for impls with live indices.
     * 
     * @throws IllegalStateException
     *             if the view is read only.
     */
    @SuppressWarnings("unchecked")
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
    public long commit() {
        
        if (isReadOnly())
            throw new IllegalStateException();

        /*
         * Clear the reference since it was as of the last commit point.
         */
        readCommittedRef = null;
        return 0l;
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
    
    final public long getJustificationCount() {

        if (justify) {

            return getSPORelation().getJustificationIndex().rangeCount(null,
                    null);

        }

        return 0;

    }

    final public long getTermCount() {

        final byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_URI) };

        /*
         * Note: the term count deliberately excludes the statement identifiers
         * which follow the bnodes in the lexicon.
         */
        final byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_BND + 1)) };

        return getLexiconRelation().getTerm2IdIndex()
                .rangeCount(fromKey, toKey);

    }

    final public long getURICount() {

        final byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_URI) };

        final byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_URI + 1)) };

        return getLexiconRelation().getTerm2IdIndex()
                .rangeCount(fromKey, toKey);

    }

    final public long getLiteralCount() {

        // Note: the first of the kinds of literals (plain).
        final byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_LIT) };

        // Note: spans the last of the kinds of literals.
        final byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_DTL + 1)) };

        return getLexiconRelation().getTerm2IdIndex()
                .rangeCount(fromKey, toKey);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Will always return zero (0) if {@value Options#STORE_BLANK_NODES}
     * is <code>false</code>.
     */
    final public long getBNodeCount() {
        
        final byte[] fromKey = new byte[] { KeyBuilder
                .encodeByte(ITermIndexCodes.TERM_CODE_BND) };

        final byte[] toKey = new byte[] { KeyBuilder
                .encodeByte((byte) (ITermIndexCodes.TERM_CODE_BND + 1)) };

        return getLexiconRelation().getTerm2IdIndex()
                .rangeCount(fromKey, toKey);

    }

    /*
     * term index
     */

    /**
     * This method delegates to the batch API, but it is extremely inefficient
     * for scale-out as it does one RMI per request!
     */
    public IV addTerm(final Value value) {

        final BigdataValue[] terms = new BigdataValue[] {//

            getValueFactory().asValue(value) //

        };

        getLexiconRelation().addTerms(terms, 1, false/* readOnly */);

        return terms[0].getIV();

    }

    /**
     * This method is extremely inefficient for scale-out as it does one RMI per
     * request!
     */
    final public BigdataValue getTerm(final IV iv) {

        return getLexiconRelation().getTerm(iv);

    }

    /**
     * This method is extremely inefficient for scale-out as it does one RMI per
     * request!
     */
    final public IV getIV(final Value value) {

        return getLexiconRelation().getIV(value);
        
    }
        
    public void addTerms(final BigdataValue[] terms) {

        getLexiconRelation().addTerms( terms, terms.length, false/*readOnly*/);
        
    }

    /*
     * singletons.
     */

    private volatile InferenceEngine inferenceEngine = null;

    final public InferenceEngine getInferenceEngine() {

        synchronized (this) {

            if(inferenceEngine == null) {

                inferenceEngine = new InferenceEngine(this);

            }

        }
        
        return inferenceEngine;

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

    final public void addStatement(final Resource s, final URI p, final Value o) {
        
        addStatement(s, p, o, null);

    }

    final public void addStatement(final Resource s, final URI p,
            final Value o, final Resource c) {

        if (quads && c == null) {

            /*
             * The context position MUST be bound for a quad store.
             */

            throw new UnsupportedOperationException();
            
        }
        
        /*
         * Note: This uses the batch API.
         */

        final IStatementBuffer<Statement> buffer = new StatementBuffer<Statement>(
                this, 1);

        buffer.add(s, p, o, c);

        buffer.flush();

    }

    final public ISPO getStatement(final IV s, final IV p, final IV o) {
        
        return getStatement(s, p, o, null/* c */);

    }
    
    final public ISPO getStatement(final IV s, final IV p, final IV o,
            final IV c) {

        if (s == null || p == null || o == null
                || (c == null && spoKeyArity == 4)) {

            throw new IllegalArgumentException();

        }

        final ISPO spo = spoKeyArity == 4 ? new SPO(s, p, o) : new SPO(s, p, o,
                c);

        final IIndex ndx = getSPORelation().getPrimaryIndex();

        final SPOTupleSerializer tupleSer = (SPOTupleSerializer) ndx
                .getIndexMetadata().getTupleSerializer();

        final byte[] key = tupleSer.serializeKey(spo);

        final byte[] val = ndx.lookup(key);

        if (val == null) {

            // The statement is not in the database.
            
            return null;

        }

        /*
         * Decode the value, set it on the SPO, and return the SPO.
         * 
         * Note: If SIDs are enabled, then this will set the statement
         * identifier on the SID if it is an explicit statement.
         */
        
        return SPO.decodeValue(spo, val);
        
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
     * 
     * @deprecated by {@link #hasStatement(IV, IV, IV, IV)}
     */
    final public boolean hasStatement(final IV s, final IV p, final IV o) {

        return hasStatement(s, p, o, null/* c */);

    }

    /**
     * Return true if the statement pattern matches any statement(s) in the store
     * (non-batch API).
     * <p>
     * Note: This method does not verify whether or not the statement is
     * explicit.
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     */
    final public boolean hasStatement(final IV s, final IV p, final IV o,
            final IV c) {

        if (s != null && p != null && o != null && (!quads || c != null)) {

            /*
             * Point test.
             */
            
            final IIndex ndx = getSPORelation().getPrimaryIndex();

            final SPO spo = new SPO(s, p, o, c);

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
         * This uses a range scan over the statement pattern to see if there are
         * any matches.
         */

        return !getSPORelation().getAccessPath(s, p, o, c).isEmpty();

    }

    /**
     * This method is extremely inefficient for scale-out as it does one RMI per
     * request!
     */
    final public boolean hasStatement(final Resource s, final URI p,
            final Value o) {

        return hasStatement(s, p, o, null/* c */);

    }
    
    /**
     * This method is extremely inefficient for scale-out as it does one RMI per
     * request!
     */
    final public boolean hasStatement(Resource s, URI p, Value o, Resource c) {

        /*
         * convert other Value object types to our object types.
         */
        final BigdataValueFactory valueFactory = getValueFactory();
        
        s = (Resource) valueFactory.asValue(s);

        p = (URI) valueFactory.asValue(p);

        o = valueFactory.asValue(o);

        c = valueFactory.asValue(c);

        /*
         * Convert our object types to internal identifiers.
         * 
         * Note: If a value was specified and it is not in the terms index then
         * the statement can not exist in the KB.
         */

        final IV _s = getIV(s);

        if (_s == null && s != null) {
            
            return false;
            
        }

        final IV _p = getIV(p);

        if (_p == null && p != null) {
            
            return false;
            
        }
        
        final IV _o = getIV(o);

        if (_o == null && o != null) {
            
            return false;
            
        }

        final IV _c = getIV(c);

        if (_c == null && c != null) {
            
            return false;
            
        }

        final boolean found = hasStatement(_s, _p, _o, _c);
        
//        if(log.isDebugEnabled()) {
//            
//            log.debug("<" + s + "," + p + "," + o + "> : found=" + found);
//            
//        }
        
        return found;

    }

    final public long removeStatements(final Resource s, final URI p,
            final Value o) {

        return removeStatements(s, p, o, null/* c */);

    }

    final public long removeStatements(final Resource s, final URI p,
            final Value o, final Resource c) {

        return getAccessPath(s, p, o, c, null/* filter */).removeAll();

    }

    final public BigdataStatement getStatement(final Resource s, final URI p,
            final Value o) {

        return getStatement(s, p, o, null/* c */);
        
    }

    final public BigdataStatement getStatement(final Resource s, final URI p,
            final Value o, final Resource c) {

        if (s == null || p == null || o == null || (quads && c == null)) {

            throw new IllegalArgumentException();

        }

        final BigdataStatementIterator itr = getStatements(s, p, o, c);

        try {

            if (!itr.hasNext()) {

                return null;

            }

            return itr.next();

        } finally {

            itr.close();

        }

    }

    final public BigdataStatementIterator getStatements(final Resource s,
            final URI p, final Value o) {

        return getStatements(s, p, o, null/* c */);

    }

    final public BigdataStatementIterator getStatements(final Resource s,
            final URI p, final Value o, final Resource c) {

        return asStatementIterator(getAccessPath(s, p, o, c).iterator());

    }

    final public BigdataValue asValue(final Value value) {

        return getValueFactory().asValue(value);

    }

    public BigdataStatement asStatement(final ISPO spo) {

        /*
         * Use batch API to resolve the term identifiers.
         */
        final List<IV> ivs = new ArrayList<IV>(4);
        
        ivs.add(spo.s());
        
        ivs.add(spo.p());
        
        ivs.add(spo.o());

        final IV c = spo.c();
        
        if (c != null) {

            ivs.add(c);

        }

        final Map<IV, BigdataValue> terms = getLexiconRelation()
                .getTerms(ivs);

        /*
         * Expose as a Sesame compatible Statement object.
         */

        return getValueFactory().createStatement(//
                (BigdataResource)  terms.get(spo.s()),//
                (BigdataURI)       terms.get(spo.p()),//
                (BigdataValue)     terms.get(spo.o()),//
                (BigdataResource)  (c != null ? terms.get(c) : null),//
                spo.getStatementType(),//
                spo.getUserFlag());
        
    }

    public BigdataStatementIterator asStatementIterator(
            final IChunkedOrderedIterator<ISPO> src) {

        return new BigdataStatementIteratorImpl(this, src)
                .start(getExecutorService());

    }

    public IAccessPath<ISPO> getAccessPath(final Resource s, final URI p,
            final Value o) {

        return getAccessPath(s, p, o, null/*c*/, null/* filter */);

    }

    public IAccessPath<ISPO> getAccessPath(final Resource s, final URI p,
            final Value o, final IElementFilter<ISPO> filter) {

        return getAccessPath(s, p, o, null/* c */, filter);
        
    }
    
    final public IAccessPath<ISPO> getAccessPath(final Resource s, final URI p,
            final Value o, Resource c) {

        return getAccessPath(s, p, o, c, null/* filter */);

    }

    final public IAccessPath<ISPO> getAccessPath(final Resource s, final URI p,
            final Value o, final Resource c, final IElementFilter<ISPO> filter) {

        /*
         * Convert other Value object types to our object types.
         * 
         * Note: the value factory is not requested unless we need to translate
         * some value. This hack allows temporary stores without a lexicon to
         * use the same entry points as those with one. Without this methods
         * such as getStatementCount(c,exact) would throw exceptions when the
         * lexicon was not associated with the store.
         */

        final BigdataValueFactory valueFactory = (s != null || p != null || o != null
                | c != null) ? getValueFactory() : null;
        
        final BigdataResource _s = valueFactory == null ? null : valueFactory
                .asValue(s);

        final BigdataURI _p = valueFactory == null ? null : valueFactory
                .asValue(p);

        final BigdataValue _o = valueFactory == null ? null : valueFactory
                .asValue(o);

        // Note: _c is null unless quads.
        final BigdataValue _c = quads ? valueFactory == null ? null
                : valueFactory.asValue(c) : null;

        /*
         * Batch resolve all non-null values to get their term identifiers.
         */
        int nnonNull = 0;
        final BigdataValue[] values = new BigdataValue[spoKeyArity];
        {

            if (s != null)
                values[nnonNull++] = _s;

            if (p != null)
                values[nnonNull++] = _p;

            if (o != null)
                values[nnonNull++] = _o;

            if (c != null && quads)
                values[nnonNull++] = _c;

            if (nnonNull > 0)
                getLexiconRelation()
                        .addTerms(values, nnonNull, true/* readOnly */);

        }

        /*
         * If any value was given but is not known to the lexicon then use an
         * empty access path since no statements can exist for the given
         * statement pattern.
         */

        if (s != null && _s.getIV() == null)
            return new EmptyAccessPath<ISPO>();

        if (p != null && _p.getIV() == null)
            return new EmptyAccessPath<ISPO>();

        if (o != null && _o.getIV() == null)
            return new EmptyAccessPath<ISPO>();

        if (quads && c != null && _c.getIV() == null)
            return new EmptyAccessPath<ISPO>();

//        /*
//         * Convert our object types to internal identifiers.
//         * 
//         * Note: If a value was specified and it is not in the terms index then
//         * the statement can not exist in the KB.
//         */
//        final long _s = getTermId(s);
//
//        if (_s == NULL && s != null)
//            return new EmptyAccessPath<ISPO>();
//
//        final long _p = getTermId(p);
//
//        if (_p == NULL && p != null)
//            return new EmptyAccessPath<ISPO>();
//
//        final long _o = getTermId(o);
//
//        if (_o == NULL && o != null)
//            return new EmptyAccessPath<ISPO>();

        /*
         * Return the access path.
         */

        return getSPORelation().getAccessPath(//
                s == null ? null : _s.getIV(), //
                p == null ? null : _p.getIV(),//
                o == null ? null : _o.getIV(),//
                (c == null || !quads) ? null : _c.getIV(),//
                filter//
        );

    }

    final public IAccessPath<ISPO> getAccessPath(final IV s, final IV p,
            final IV o) {

        return getSPORelation()
                .getAccessPath(s, p, o, null/* c */, null/* filter */);

    }

    final public IAccessPath<ISPO> getAccessPath(final IV s, final IV p,
            final IV o, final IElementFilter<ISPO> filter) {

        return getSPORelation().getAccessPath(s, p, o, null/* c */, filter);

    }
	final public IAccessPath<ISPO> getAccessPath(final IV s, final IV p,
            final IV o,final IV c) {

        return getSPORelation()
                .getAccessPath(s, p, o, c, null/* filter */);

    }

    final public IAccessPath<ISPO> getAccessPath(final IV s, final IV p,
            final IV o, final IV c, final IElementFilter<ISPO> filter) {

        return getSPORelation().getAccessPath(s, p, o, c, filter);

    }
    final public IAccessPath<ISPO> getAccessPath(final IKeyOrder<ISPO> keyOrder) {

        return getAccessPath(keyOrder, null/* filter */);

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
    @SuppressWarnings("unchecked")
    final public IAccessPath<ISPO> getAccessPath(
            final IKeyOrder<ISPO> keyOrder, final IElementFilter<ISPO> filter) {

        final SPORelation r = getSPORelation();
        
        final SPOPredicate p = new SPOPredicate(//
                new String[] { r.getNamespace() },//
                -1, // partitionId
                Var.var("s"),//
                Var.var("p"),//
                Var.var("o"),//
                quads ? Var.var("c") : null,//
                false, // optional
                filter,//
                null // expander
        );

        return r.getAccessPath(keyOrder, p);

    }

    /*
     * statement externalization serialization stuff.
     */

    // namespace to prefix
    private final Map<String, String> uriToPrefix = new LinkedHashMap<String, String>();

    /**
     * Defines a transient mapping from a URI to a namespace prefix that will be
     * used for that URI by {@link #toString()}.
     * 
     * @param namespace
     * 
     * @param prefix
     */
    final public void addNamespace(final String namespace, final String prefix) {

        uriToPrefix.put(namespace, prefix);

    }

    /**
     * Return an unmodifiable view of the mapping from namespaces to namespace
     * prefixes.
     * <p>
     * Note: this is NOT a persistent map. It is used by {@link #toString(IV)}
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
    final public String getNamespace(final String prefix) {

        // Note: this is not an efficient operation.
        final Iterator<Map.Entry<String/* namespace */, String/* prefix */>> itr = uriToPrefix
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<String/* namespace */, String/* prefix */> entry = itr
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
    final public String removeNamespace(final String prefix) {

        final Iterator<Map.Entry<String/* namespace */, String/* prefix */>> itr = uriToPrefix
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<String/* namespace */, String/* prefix */> entry = itr
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

    final public String toString(final IV s, final IV p, final IV o) {

        return toString(s, p, o, null);

    }

    final public String toString(final IV s, final IV p, final IV o,
            final IV c) {

        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o)
                + ", " + toString(c) + " >");

    }

    final public String toString(final ISPO spo) {

        return toString(spo.s(), spo.p(), spo.o(), spo.c());

    }

    final public String toString(final IV iv) {

        if (iv == null)
            return IRawTripleStore.NULLSTR;

        if(iv.isStatement()){

            // Note: SIDs are not stored in the reverse lexicon.
            return Long.toString(iv.getTermId()) + "S";
        
        }

        final BigdataValue v = getTerm(iv);

        if (v == null)
            return "<NOT_FOUND#" + iv + ">";

        final String s = (v instanceof URI ? abbrev((URI) v) : v.toString());

        return s + ("(" + iv + ")");

    }

    // private final String TERM_NOT_FOUND = "<NOT_FOUND>";

    /**
     * Substitutes in well know namespaces (rdf, rdfs, etc).
     */
    final private String abbrev(final URI uri) {

        final String uriString = uri.toString();

        // final int index = uriString.lastIndexOf('#');
        //        
        // if(index==-1) return uriString;
        //
        // final String namespace = uriString.substring(0, index);

        final String namespace = uri.getNamespace();

        final String prefix = uriToPrefix.get(namespace);

        if (prefix != null) {

            return prefix + ":" + uri.getLocalName();

        }
        
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
    final public StringBuilder predicateUsage(
            final AbstractTripleStore resolveTerms) {

        if (getSPORelation().oneAccessPath) {

            // The necessary index (POS or POCS) does not exist.
            throw new UnsupportedOperationException();
            
        }
        
        // visit distinct term identifiers for the predicate position.
        final IChunkedIterator<IV> itr = getSPORelation().distinctTermScan(
                quads ? SPOKeyOrder.POCS : SPOKeyOrder.POS);

        // resolve term identifiers to terms efficiently during iteration.
        final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                resolveTerms, itr);
        
        try {
        
            final StringBuilder sb = new StringBuilder();

            while (itr.hasNext()) {

                final BigdataValue term = itr2.next();

                final IV p = term.getIV();
                
                final long n = getSPORelation().getAccessPath(null, p, null,
                        null, null/* filter */).rangeCount(false/* exact */);

                /*
                 * FIXME do efficient term resolution for scale-out. This will
                 * require an expander pattern where we feed one iterator into
                 * another and both are chunked.
                 */
                sb.append(n + "\t" + resolveTerms.toString(p) + "\n");

            }
        
            return sb;
        
        } finally {

            itr2.close();
            
        }

    }

    /**
     * Utility method dumps the statements in the store using the SPO index
     * (subject order).
     */
    final public StringBuilder dumpStore() {

        return dumpStore(true, true, true);

    }

    final public StringBuilder dumpStore(final boolean explicit,
            final boolean inferred, final boolean axioms) {

        return dumpStore(this, explicit, inferred, axioms);

    }

    final public StringBuilder dumpStore(
            final AbstractTripleStore resolveTerms, final boolean explicit,
            final boolean inferred, final boolean axioms) {

        return dumpStore(resolveTerms, explicit, inferred, axioms, false/* justifications */);

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
    final public StringBuilder dumpStore(
            final AbstractTripleStore resolveTerms, final boolean explicit,
            final boolean inferred, final boolean axioms,
            final boolean justifications) {

        return dumpStore(resolveTerms, explicit, inferred, axioms,
                justifications, getSPORelation().getPrimaryKeyOrder());

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
     * @param keyOrder
     *            The access path to use.
     */
    public StringBuilder dumpStore(
            final AbstractTripleStore resolveTerms, final boolean explicit,
            final boolean inferred, final boolean axioms,
            final boolean justifications, final IKeyOrder<ISPO> keyOrder) {

        final StringBuilder sb = new StringBuilder();
        
        final long nstmts = getAccessPath(keyOrder).rangeCount(true/* exact */);

        long nexplicit = 0;
        long ninferred = 0;
        long naxioms = 0;

        {

            // Full SPO scan efficiently resolving SPOs to BigdataStatements.
            final BigdataStatementIterator itr = resolveTerms
                    .asStatementIterator(getAccessPath(keyOrder)
                            .iterator());

            int i = 0;
            
            try {
                
                while (itr.hasNext()) {

                    final BigdataStatement stmt = itr.next();

                    switch (stmt.getStatementType()) {

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

                    sb.append("#"
                            + (i + 1)
                            + "\t"
                            + stmt
                            + (" (" + stmt.s() + "," + stmt.p() + "," + stmt.o()
                                    + "," + stmt.c() + ")") + "\n");

                    i++;

                }
                
            } finally {
                
                itr.close();
                
            }

        }

        int njust = 0;

        if (justifications && justify) {

            final IIndex ndx = getSPORelation().getJustificationIndex();

            final ITupleIterator itrj = ndx.rangeIterator();

            while (itrj.hasNext()) {

                final Justification jst = (Justification)itrj.next().getObject();

                sb.append("#" + (njust + 1) //+ "\t"
                        + jst.toString(resolveTerms)+"\n");

                njust++;

            }

        }

        sb.append("dumpStore: #statements=" + nstmts + ", #explicit="
                + nexplicit + ", #inferred=" + ninferred + ", #axioms="
                + naxioms + (justifications ? ", #just=" + njust : ""));

        return sb;
        
    }

    /**
     * Dumps the access path, efficiently resolving term identifiers to terms.
     * 
     * @param accessPath
     */
    public StringBuilder dumpStatements(final IAccessPath<ISPO> accessPath) {
                
        final StringBuilder sb = new StringBuilder();

        final BigdataStatementIterator itr = asStatementIterator(accessPath
                .iterator());

        try {

            while(itr.hasNext()) {
                
                sb.append("\n"+itr.next());
                
            }
            
            return sb;
            
        } finally {
            
            itr.close();
            
        }
        
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
    public long copyStatements(//
            final AbstractTripleStore dst,//
            final IElementFilter<ISPO> filter,//
            final boolean copyJustifications//
            ) {

        if (dst == this)
            throw new IllegalArgumentException();

        // obtain a chunked iterator reading from any access path.
        final IChunkedOrderedIterator<ISPO> itr = getAccessPath(
                getSPORelation().getPrimaryKeyOrder(), filter).iterator();

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

                final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(
                        2);

                /*
                 * Note: we reject using the filter before stmts or
                 * justifications make it into the buffer so we do not need to
                 * apply the filter again here.
                 */

                // set as a side-effect.
                final AtomicLong nwritten = new AtomicLong();

                // task will write SPOs on the statement indices.
                tasks.add(new StatementWriter(this, dst, true/* copyOnly */,
                        itr, nwritten));

                // task will write justifications on the justifications index.
                final AtomicLong nwrittenj = new AtomicLong();

                if (justify) {

                    final IJustificationIterator jitr = new JustificationIterator(
                            getSPORelation().getJustificationIndex(),
                            0/* capacity */, true/* async */);

                    tasks.add(new JustificationWriter(dst, jitr, nwrittenj));

                }

                final List<Future<Long>> futures;
                final long elapsed_SPO;
                final long elapsed_JST;

                try {

                    futures = getIndexManager().getExecutorService().invokeAll(
                            tasks);

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

                if (log.isInfoEnabled())
                    log
                            .info("Copied "
                                    + nwritten
                                    + " statements in "
                                    + elapsed_SPO
                                    + "ms"
                                    + (justify ? (" and " + nwrittenj
                                            + " justifications in "
                                            + elapsed_JST + "ms") : ""));

                return nwritten.get();

            }

        } finally {

            itr.close();

        }

    }

    public IChunkedOrderedIterator<ISPO> bulkFilterStatements(
            final ISPO[] stmts, final int numStmts, boolean present) {

        if (numStmts == 0) {

            return new EmptyChunkedIterator<ISPO>(getSPORelation()
                    .getPrimaryKeyOrder());

        }

        return bulkFilterStatements(new ChunkedArrayIterator<ISPO>(numStmts,
                stmts, null/* keyOrder */), present);

    }

    public IChunkedOrderedIterator<ISPO> bulkFilterStatements(
            final IChunkedOrderedIterator<ISPO> itr, final boolean present) {

        return new ChunkedConvertingIterator<ISPO, ISPO>(itr,
                new BulkFilterConverter(getSPORelation().getPrimaryIndex(),
                        present));

    }

    public IChunkedOrderedIterator<ISPO> bulkCompleteStatements(
            final SPO[] stmts, final int numStmts) {
        
        if (numStmts == 0) {

            return new EmptyChunkedIterator<ISPO>(getSPORelation()
                    .getPrimaryKeyOrder());

        }

        return bulkCompleteStatements(new ChunkedArrayIterator<ISPO>(numStmts,
                stmts, null/* keyOrder */));
        
    }

    public ISPO[] bulkCompleteStatements(final ISPO[] stmts) {

        return new BulkCompleteConverter(getSPORelation().getPrimaryIndex())
                .convert(stmts);

    }
    
    public IChunkedOrderedIterator<ISPO> bulkCompleteStatements(
            final IChunkedOrderedIterator<ISPO> itr) {

        return new ChunkedConvertingIterator(itr, new BulkCompleteConverter(
                getSPORelation().getPrimaryIndex()));

    }

    public long addStatements(final ISPO[] stmts, final int numStmts) {

        if (numStmts == 0)
            return 0;

        return addStatements(new ChunkedArrayIterator<ISPO>(numStmts, stmts,
                null/* keyOrder */), null /* filter */);

    }

    public long addStatements(final ISPO[] stmts, final int numStmts,
            final IElementFilter<ISPO> filter) {

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
     * <i>this</i> database. This is done in a preprocessing stage for each
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

                // @todo raise the filter into the caller's iterator?
                final long numWritten = spoRelation.insert(a, numStmts, filter);

                mutationCount += numWritten;
                
                if (numStmts > 1000) {

                    if(log.isInfoEnabled())
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

    public long removeStatements(final ISPO[] stmts, final int numStmts) {

        return removeStatements(new ChunkedArrayIterator<ISPO>(numStmts, stmts,
                null/* keyOrder */), true);

    }

    public long removeStatements(final IChunkedOrderedIterator<ISPO> itr) {

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
     *       maintenance then this method does NOT guarantee consistency when
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
    public long removeStatements(//
            IChunkedOrderedIterator<ISPO> itr,
            final boolean computeClosureForStatementIdentifiers//
            ) {

        if (itr == null)
            throw new IllegalArgumentException();

        long mutationCount = 0;

        if (statementIdentifiers && computeClosureForStatementIdentifiers) {

            itr = computeClosureForStatementIdentifiers(itr);

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

        // @todo test w/ SPO bloom filter enabled and see if this improves performance.
        properties.setProperty(Options.BLOOM_FILTER, "false");

        // no axioms.
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        final TempTripleStore tmp = new TempTripleStore(getIndexManager()
                .getTempStore(), properties, this);
        
        /*
         * buffer everything in a temp triple store.
         * 
         * Note: copyOnly is false since we need to make sure that the sids are
         * defined for explicit statements before we attempt to compute the
         * fixed point for the statements about statements. This will have the
         * side effect of writing on the lexicon for the database if the caller
         * supplies an explicit SPO that does not exist yet in the database.
         */
        this.addStatements(tmp, false/*copyOnly*/, src, null/*filter*/);

        fixPointStatementIdentifiers(this, tmp);

        /*
         * Note: The returned iterator will automatically release the backing
         * temporary store when it is closed or finalized.
         * 
         * Note: SIDS are only used with triples so the SPO index will exist.
         */
        return new DelegateChunkedIterator<ISPO>(tmp.getAccessPath(
                SPOKeyOrder.SPO).iterator()) {

            public void close() {

                super.close();
                
                tmp.close();
                
            }
            
            protected void finalize() throws Throwable {
                
                super.finalize();
                
                if(tmp.isOpen()) {

                    tmp.close();
                    
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
         * the ability to scale-up from the TempTripleStore (we could create the
         * TempTripleStore on a Journal using Temporary buffer mode and get the
         * concurrency controls).
         */
        long statementCount0;
        long statementCount1;
        final HashSet<IV> sids = new HashSet<IV>();
//        final LongOpenHashSet sids = new LongOpenHashSet();
        int nrounds = 0;
        do {

            // increment the round counter.
            nrounds++;

            // note: count will be exact.
            statementCount0 = tempStore.getStatementCount();

            /*
             * Visit the explicit statements since only they can have statement
             * identifiers.
             * 
             * Note: SIDs are only used with triples so the SPO index will
             * exist.
             */
            final IChunkedOrderedIterator<ISPO> itr = tempStore.getAccessPath(
                    SPOKeyOrder.SPO, ExplicitSPOFilter.INSTANCE).iterator();

            try {
                
                while(itr.hasNext()) {
                    
                    final IV sid = itr.next().getStatementIdentifier();
                    
                    if(sids.contains(sid)) continue;
                    
                    // sid in the subject position.
                    tempStore.addStatements(tempStore, true/* copyOnly */, db
                                    .getAccessPath(sid, null, null).iterator(),
                                    null/* filter */);

                    /*
                     * sid in the predicate position.
                     * 
                     * Note: this case is not allowed by RDF but a TMGraph model
                     * might use it.
                     */
                    tempStore.addStatements(tempStore, true/* copyOnly */, db
                                    .getAccessPath(null, sid, null).iterator(),
                                    null/* filter */);

                    // sid in the object position.
                    tempStore.addStatements(tempStore, true/*copyOnly*/, db
                                    .getAccessPath(null, null, sid).iterator(),
                                    null/* filter */);

                    // finished with this sid.
                    sids.add(sid);
                    
                }
                
            } finally {
                
                itr.close();
                
            }
            
            // note: count will be exact.
            statementCount1 = tempStore.getStatementCount();

            if (log.isInfoEnabled())
                log.info("Finished " + nrounds + " rounds: statementBefore="
                        + statementCount0 + ", statementsAfter="
                        + statementCount1);

        } while (statementCount0 < statementCount1);

    }

// Use getLexiconRelation().getSearchEngine().search(...)
//    /**
//     * <p>
//     * Performs a full text search against literals returning an {@link IHit}
//     * list visiting the term identifiers for literals containing tokens parsed
//     * from the query. Those term identifiers may be used to join against the
//     * statement indices in order to bring back appropriate results.
//     * </p>
//     * <p>
//     * Note: If you want to discover a data typed value, then form the
//     * appropriate data typed {@link Literal} and use
//     * {@link IRawTripleStore#getTermId(Value)}. Likewise, that method is also
//     * more appropriate when you want to lookup a specific {@link URI}.
//     * </p>
//     * 
//     * @param languageCode
//     *            The language code that should be used when tokenizing the
//     *            query (an empty string will be interpreted as the default
//     *            {@link Locale}).
//     * @param text
//     *            The query (it will be parsed into tokens).
//     * 
//     * @return An iterator that visits each term in the lexicon in which one or
//     *         more of the extracted tokens has been found. The value returned
//     *         by {@link IHit#getDocId()} is in fact the <i>termId</i> and you
//     *         can resolve it to the term using {@link #getTerm(long)}.
//     * 
//     * @throws InterruptedException
//     *             if the search operation is interrupted.
//     * 
//     * @todo Abstract the search api so that it queries the terms index directly
//     *       when a data typed literal or a URI is used (typed query).
//     */
//    @SuppressWarnings("unchecked")
//    public Iterator<IHit> textSearch(final String languageCode,
//            final String text) throws InterruptedException {
//
//        return getSearchEngine().search(text, languageCode);
//
//    }

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
                isJustify(), false/* backchain */,
                DefaultEvaluationPlanFactory2.INSTANCE);

    }

    /**
     * 
     * @param solutionFlags
     *            See {@link IJoinNexus#ELEMENT} and friends.
     * @param filter
     *            Optional filter.
     * @return
     */
    public IJoinNexusFactory newJoinNexusFactory(
            final RuleContextEnum ruleContext, final ActionEnum action,
            final int solutionFlags, final IElementFilter filter,
            final boolean justify, final boolean backchain,
            final IEvaluationPlanFactory planFactory) {
        
        return newJoinNexusFactory(ruleContext, action, solutionFlags, filter,
                justify, backchain, planFactory, null/*overrides*/);
        
    }

    /**
     * 
     * @param solutionFlags
     *            See {@link IJoinNexus#ELEMENT} and friends.
     * @param filter
     *            Optional filter.
     * @param overrides
     *            Optional overrides of the properties controlling the rule
     *            execution layer. When given, the property values will override
     *            those inherited from {@link AbstractResource}.
     * @return
     */
    public IJoinNexusFactory newJoinNexusFactory(
            final RuleContextEnum ruleContext, //
            final ActionEnum action,//
            final int solutionFlags,//
            final IElementFilter filter,//
            final boolean justify, //
            final boolean backchain,//
            final IEvaluationPlanFactory planFactory,//
            final Properties overrides//
            ) {
        
        if (ruleContext == null)
            throw new IllegalArgumentException();

        if (action == null)
            throw new IllegalArgumentException();
        
        if(action.isMutation()) {
        	
        	assertWritable();
        	
        }

        // use the timestamp for the database view.
        final long writeTimestamp = getTimestamp();
        
        /*
         * Use the timestamp for the database view.
         * 
         * Note: The choice here effects the behavior for MUTATION only (e.g.,
         * computing or updating the closure of the db) and reflects the use of
         * the UnisolatedReadWriteIndex to allow interleaved reads and writes on
         * the unisolated indices when using a Journal or Temporary(Raw)Store.
         * When running against an IBigdataFederation, the ConcurrencyManager
         * will be interposed and unisolated writes will result in commits.
         * 
         * Note: If we are only reading (Query) then we just use the timestamp
         * of the view.
         * 
         * --- LTS (closure using nested or pipeline joins)
         * 
         * Closure can't use READ_COMMITTED for LTS unless we force commits
         * after each rule executes. Therefore it uses UNISOLATED indices for
         * both reads and writes. This works since (a) the entire JOIN task is
         * executed while holding all required locks; and (b) the
         * UnisolatedReadWriteIndex is used to avoid concurrency problems that
         * would otherwise arise since there are readers running concurrent with
         * the writer that flushes solutions to the relation.
         * 
         * Using UNISOLATED JOINs for LTS means that parallel programs will be
         * (mostly) serialized since they will be ordered by the requirement for
         * resource locks on the various indices, including the head relation
         * (SPORelation) on which the rules will write. However, as noted above,
         * our only other choice is to enforce auto-commit semantics as we do
         * for LDS/EDS/JDS.
         * 
         * ---- Federations and pipeline joins.
         * 
         * Federations (LDS/EDS/JDS) use auto-commit semantics. For EDS/JDS we
         * do NOT use READ_COMMITTED for closure since that can lead to stale
         * locator problems during overflow processing. Instead we advance the
         * readTimestamp before each mutation rule is run. For simplicity, we do
         * this for LDS as well as EDS/JDS, while it is in fact only required
         * for EDS/JDS since LDS does not use locators.
         * 
         * Federation JoinTasks require access to the dataService so that they
         * can obtain the live journal. However, they use read-historical access
         * and you can do that without declaring locks. More to the point, if
         * they attempted to access the UNISOLATED index that would cause a
         * deadlock with the tasks responsible for flushing the generated
         * solutions onto the head relation.
         */
        final long readTimestamp;
        if (action.isMutation()) {

            if (writeTimestamp != ITx.UNISOLATED) {

                // mutation requires the UNISOLATED view.

                throw new UnsupportedOperationException();
                
            }

            if( getIndexManager() instanceof IBigdataFederation) {

                /*
                 * Use historical reads.
                 * 
                 * Note: The read timestamp will be automatically updated before
                 * each mutation step so that all mutation operations will see
                 * the last committed state of the database but will also avoid
                 * problems with stale locators.
                 */
                
                readTimestamp = TimestampUtility.asHistoricalRead(getIndexManager()
                        .getLastCommitTime());
                
            } else {

                /*
                 * LTS closure operations.
                 * 
                 * Note: This means that we use UNISOLATED reads since mutation
                 * requires that the caller is using the UNISOLATED relation.
                 */
                
                readTimestamp = getTimestamp();

            }
            
        } else {

            /*
             * Query.
             * 
             * Use the same timestamp as the relation.
             */
            
            readTimestamp = getTimestamp();
            
        }

        /*
         * true iff owl:sameAs is (a) supported by the axiom model; and (b)
         * there is at least one owl:sameAs assertion in the database.
         */
        final boolean isOwlSameAsUsed = getAxioms().isOwlSameAs()
                && !getAccessPath(null, getVocabulary().get(OWL.SAMEAS), null)
                        .isEmpty();

        final IRuleTaskFactory defaultRuleTaskFactory = isNestedSubquery() //
                ? DefaultRuleTaskFactory.SUBQUERY//
                : DefaultRuleTaskFactory.PIPELINE//
                ;

        // Note: returns a Properties wrapping the resource's properties.
        final Properties tmp = getProperties();

        if (overrides != null) {

            /* FIXME overrides should apply to the properties above here as well!
             * Layer in the overrides.
             * 
             * Note: If the caller passes in a Properties object, then only the
             * top level of that properties object will be copied in.  This can
             * be "fixed" by the caller using PropertyUtil.flatten(Properties).
             */
            tmp.putAll(overrides);

        }
        
        return new RDFJoinNexusFactory(
                ruleContext,//
                action, //
                writeTimestamp,//
                readTimestamp, //
//                isForceSerialExecution(),
//                getMaxParallelSubqueries(), //
                justify,//
                backchain, //
                isOwlSameAsUsed,//
                tmp,//
//                getChunkOfChunksCapacity(), getChunkCapacity(),
//                getChunkTimeout(), getFullyBufferedReadThreshold(),
                solutionFlags, //
                filter, //
                planFactory, //
                defaultRuleTaskFactory//
                );

    }

    /**
     * Specialized {@link IRule} execution using the full text index to identify
     * possible completions of the given literals for which there exists a
     * subject <code>s</code> such that:
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
     * @return An {@link ICloseableIterator} visiting {@link IBindingSet}s. Each
     *         {@link IBindingSet} will have bound {@link BigdataValue}s for
     *         <code>s</code>, <code>t</code>, <code>p</code>, and
     *         <code>lit</code> where those variables are defined per the
     *         pseudo-code JOIN above.
     * 
     * @throws InterruptedException
     *             if the operation is interrupted.
     * 
     *             FIXME quads : Modify match() to allow an optional context
     *             argument. When present, the match would be restricted to the
     *             specified context.
     */
    public ICloseableIterator<IBindingSet> match(final Literal[] lits,
            final URI[] preds, final URI cls) {

        if (lits == null || lits.length == 0)
            throw new IllegalArgumentException();

        if (preds == null || preds.length == 0)
            throw new IllegalArgumentException();
        
        if (cls == null)
            throw new IllegalArgumentException();
        
        /*
         * Batch resolve BigdataValues with their associated term identifiers
         * for the given predicates and cls.
         */
        final BigdataValue[] terms = new BigdataValue[preds.length + 1/* cls */];
        {

            final BigdataValueFactory valueFactory = getValueFactory();
            
            for (int i = 0; i < preds.length; i++) {

                // pred[i] is in terms[i].
                terms[i] = valueFactory.asValue(preds[i]); 
                
            }
            
            // cls is in the last index position.
            terms[preds.length] = valueFactory.asValue(cls);
            
            // batch resolve (readOnly).
            getLexiconRelation()
                    .addTerms(terms, terms.length, true/* readOnly */);

        }
        
        /*
         * Translate the predicates into term identifiers.
         */
        final IConstant<IV>[] _preds = new IConstant[preds.length];
        {

            int nknown = 0;

            for (int i = 0; i < preds.length; i++) {

                final IV iv = terms[i].getIV();

                if (iv != null)
                    nknown++;

                _preds[i] = new Constant<IV>(iv);

            }

            if (nknown == 0) {

                log.warn("No known predicates: preds=" + Arrays.toString(preds));

                return new EmptyChunkedIterator<IBindingSet>(null/* keyOrder */);

            }

        }

        /*
         * Translate the class constraint into a term identifier.
         */
        final IV _cls = terms[preds.length].getIV();

        if (_cls == null) {

            log.warn("Unknown class: class=" + cls);

            return new EmptyChunkedIterator<IBindingSet>(null/* keyOrder */);
            
        }

        /*
         * Generate a program from the possible completions of the literals.
         */
        final IProgram program = getMatchProgram(lits, _preds, _cls);
        
        final int solutionFlags = IJoinNexus.BINDINGS;

		final IJoinNexus joinNexus = newJoinNexusFactory(//
				RuleContextEnum.HighLevelQuery,// 
				ActionEnum.Query,//
				solutionFlags,//
                null // filter
                ).newInstance(getIndexManager());

        /*
         * Resolve ISolutions to their binding sets and efficiently resolves
         * term identifiers in those binding sets to BigdataValues.
         */
        try {
        
            return new BigdataSolutionResolverator(this, joinNexus
                    .runQuery(program)).start(getExecutorService());
            
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
    protected Program getMatchProgram(final Literal[] lits,
            final IConstant<IV>[] _preds, final IV _cls) {
        
        final Iterator<IV> ivIterator = getLexiconRelation().prefixScan(
                lits);

        // the term identifier for the completed literal.
        final IVariable<IV> lit = Var.var("lit");

        // instantiate the rule.
        final Rule r = new MatchRule(getSPORelation().getNamespace(),
                getVocabulary(), lit, _preds, new Constant<IV>(_cls));

//        // bindings used to specialize the rule for each completed literal.
//        final IBindingSet bindings = new ArrayBindingSet(r.getVariableCount());

        final Program program = new Program("match", true/* parallel */);
        
        // specialize and apply to each completed literal.
        while (ivIterator.hasNext()) {

            final IV iv = ivIterator.next();

            final IBindingSet constants = new ArrayBindingSet(1);

            constants.set(lit, new Constant<IV>(iv));

            final IRule tmp = r.specialize(constants, null/* constraints */);

            if (log.isDebugEnabled())
                log.debug(tmp.toString());
            
            program.addStep(tmp);
            
        }

        return program;

    }
    
}
