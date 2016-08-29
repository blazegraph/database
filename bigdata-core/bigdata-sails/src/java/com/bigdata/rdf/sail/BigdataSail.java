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
Portions of this code are:

Copyright Aduna (http://www.aduna-software.com/) 2001-2007

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
/*
 * Created on Jan 2, 2008
 */

package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
//FIXME:  Sesame 2.8 not used for 2.1.4 Release
//import org.openrdf.IsolationLevel;
//import org.openrdf.IsolationLevels;
import org.openrdf.OpenRDFUtil;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.openrdf.sail.UnknownSailTransactionStateException;
import org.openrdf.sail.UpdateContext;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.concurrent.AccessSemaphore.Access;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.changesets.DelegatingChangeLog;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.changesets.StatementWriter;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.BackchainAccessPath;
import com.bigdata.rdf.sail.webapp.DatasetNotFoundException;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.sparql.ast.service.CustomServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.InferredSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BigdataSolutionResolverator;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.rdf.store.BigdataValueIterator;
import com.bigdata.rdf.store.BigdataValueIteratorImpl;
import com.bigdata.rdf.store.EmptyStatementIterator;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Striterator;
import info.aduna.iteration.CloseableIteration;

/**
 * <p>
 * Sesame <code>2.x</code> integration.
 * </p>
 * <p>
 * Read-write operations use {@link #getConnection()}to obtain a mutable view.
 * This delegates to either {@link #getUnisolatedConnection()} or
 * {@link #getReadWriteConnection()} depending on how the Sail is configured.
 * </p>
 * <p>
 * Concurrent unisolated writers are supported if group commit is enabled, but
 * at most one unisolated writer will execute against a given Sail instance at a
 * time - other writers will be serialized. See
 * {@link #getUnisolatedConnection()} for more information on how to enable
 * group commit and how to write code that supports group commit.
 * </p>
 * <p>
 * Concurrent readers are possible, and can be very efficient. However, readers
 * MUST use a database commit point corresponding to a desired state of the
 * store, e.g., after loading some data set and (optionally) after computing the
 * closure of that data set. Use {@link #getReadOnlyConnection()} to obtain a
 * read-only view of the database as of the last commit time, or
 * {@link #getReadOnlyConnection(long)} to obtain a read-only view of the
 * database as of some other historical point. These connections are safe to use
 * concurrently with the unisolated connection from {@link #getConnection()}.
 * </p>
 * <p>
 * Concurrent fully isolated read/write transactions against the same Sail are
 * also implemented in the bigdata SAIL. To turn on read/write transactions, use
 * the option {@link Options#ISOLATABLE_INDICES}. If this option is set to true,
 * then {@link #getConnection()} will return an isolated read/write view of the
 * database. Multiple concurrent read/write transactions are allowed, and the
 * database can resolve add/add conflicts between transactions. Unlike group
 * commit, these fully isolated read/write transactions will in fact execute
 * concurrently against the same Sail instance (rather than being serialized).
 * However, there is additional overhead for fully isolated read/write
 * transactions and graph update patterns can lead to unreconcilable conflicts
 * during updates (in which case one transaction will be a failed during the
 * validation phase). The highest throughput is generally obtained using
 * {@link #getUnisolatedConnection()} in which case you do not need to enabled
 * {@link Options#ISOLATABLE_INDICES}.
 * </p>
 * <p>
 * The {@link BigdataSail} may be configured as as to provide a triple store
 * with statement-level provenance using <em>statement identifiers</em>. See
 * {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} and <a
 * href="http://wiki.blazegraph.com/wiki/index.php/Reification_Done_Right" >
 * Reification Done Right </a>.
 * </p>
 * <p>
 * Quads may be enabled using {@link AbstractTripleStore.Options#QUADS}.
 * However, note that {@link Options#TRUTH_MAINTENANCE} is not supported for
 * {@link AbstractTripleStore.Options#QUADS} at this time. This may change in
 * the future once we decide how to handle eager materialization of entailments
 * with multiple named graphs. The basic problem is that:
 * </p>
 * 
 * <pre>
 * merge(closure(graphA), closure(graphB))
 * </pre>
 * <p>
 * IS NOT EQUALS TO
 * </p>
 * 
 * <pre>
 * closure(merge(graphA, graphB))
 * </pre>
 * 
 * <p>
 * There are two ways to handle this. One is to compute all inferences at query
 * time, in which case we are not doing eager materialization and therefore we
 * are not using Truth Maintenance. The other is to punt and use the merge of
 * their individual closures.
 * </p>
 * 
 * @todo Is there anything to be done with {@link #setDataDir(java.io.File)}?
 *       With {@link #getDataDir()}?
 * 
 * @todo write custom initialization class (Instead of the generic setParameter
 *       method, most Sail implementation now have specific methods for each
 *       parameter. This is much more convenient when creating Sail instances
 *       programmatically. You are free to add whatever initialization method to
 *       your Sail class.)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @openrdf
 */
public class BigdataSail extends SailBase implements Sail {

    private static final String ERR_OPENRDF_QUERY_MODEL = 
            "Support is no longer provided for UpdateExpr or TupleExpr evaluation. Please make sure you are using a BigdataSailRepository.  It will use the bigdata native evaluation model.";
    
    /**
     * Additional parameters understood by the Sesame 2.x SAIL implementation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static interface Options extends com.bigdata.rdf.store.AbstractTripleStore.Options {
    
        /**
         * This optional boolean property may be used to specify whether or not
         * RDFS entailments are maintained by eager closure of the knowledge
         * base (the default is <code>true</code>). This property only effects
         * data loaded through the {@link Sail}.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/591 (Manage
         *      Truth Maintenance)
         */
        public static final String TRUTH_MAINTENANCE = BigdataSail.class
                .getPackage().getName()
                + ".truthMaintenance"; 
    
        public static final String DEFAULT_TRUTH_MAINTENANCE = "true"; 
    
//        /**
//         * The property whose value is the name of the {@link ITripleStore}
//         * implementation that will be instantiated. This may be used to select
//         * either the {@link LocalTripleStore} (default) or the
//         * {@link TempTripleStore} in combination
//         * {@link BigdataSail#BigdataSail(Properties)} ctor.
//         * 
//         * @deprecated 
//         */
//        public static final String STORE_CLASS = "storeClass";
//        
//        /** @deprecated */
//        public static final String DEFAULT_STORE_CLASS = LocalTripleStore.class.getName();
    
        /**
		 * The capacity of the statement buffer used to absorb writes. If this
		 * capacity is exceeded, then an incremental flush will push assertions
		 * and/or retractions to the statement indices.
		 * <p>
		 * Note: With BLGZ-1522, the {@link #QUEUE_CAPACITY} can increase the
		 * effective amount of data that is being buffered quite significantly.
		 * Caution is recommended when overriding the {@link #BUFFER_CAPACITY}
		 * in combination with a non-zero value of the {@link #QUEUE_CAPACITY}.
		 * The best performance will probably come from small (10k - 20k) buffer
		 * capacity values combined with a queueCapacity of 5-20. Larger values
		 * will increase the GC burden and could require a larger heap, but the
		 * net throughput might also increase.
		 * 
		 * @see #DEFAULT_BUFFER_CAPACITY
		 */
        public static final String BUFFER_CAPACITY = BigdataSail.class
                .getPackage().getName()
                + ".bufferCapacity";
    
        public static final String DEFAULT_BUFFER_CAPACITY = "10000";
        
		/**
		 * Optional property specifying the capacity of blocking queue used by
		 * the {@link StatementBuffer} -or- ZERO (0) to disable the blocking
		 * queue and perform synchronous writes (default is
		 * {@value #DEFAULT_QUEUE_CAPACITY} statements). The blocking queue
		 * holds parsed data pending writes onto the backing store and makes it
		 * possible for the parser to race ahead while writer is blocked writing
		 * onto the database indices.
		 * 
		 * @see BLZG-1552
		 */
		String QUEUE_CAPACITY = BigdataSail.class.getPackage().getName() + ".queueCapacity";

		String DEFAULT_QUEUE_CAPACITY = "10";
		
        /**
         * Option (default <code>true</code>) may be used to explicitly disable
         * query-time expansion for entailments NOT computed during closure. In
         * particular, this may be used to disable the query time expansion of
         * (x rdf:type rdfs:Resource) and owl:sameAs.
         * <p>
         * Note: Query time expanders are not supported in scale-out. They
         * involve an expander pattern on the IAccessPath and that is not
         * compatible with local reads against sharded indices. While it is
         * possible to do remote access path reads on the shards and layer the
         * expanders over the remote access path, this is not as efficient as
         * using distributed local access path reads.
         * 
         * @see #DEFAULT_QUERY_TIME_EXPANDER
         */
        public static final String QUERY_TIME_EXPANDER = BigdataSail.class
                .getPackage().getName()
                + ".queryTimeExpander";
        
        public static final String DEFAULT_QUERY_TIME_EXPANDER = "true";
        
        
        /**
         * Option (default <code>false</code>) determines whether the {@link
         * SailConnection#size(Resource[])} method returns an exact size or
         * an upper bound.  Exact size is a very expensive operation.
         */
        public static final String EXACT_SIZE = BigdataSail.class
                .getPackage().getName()
                + ".exactSize";

        public static final String DEFAULT_EXACT_SIZE = "false";
        
        /**
         * Options (default <code>false</code>) added only to pass the Sesame
         * test suites.  DO NOT EVER USE AUTO-COMMIT WITH BIGDATA!
         */
        public static final String ALLOW_AUTO_COMMIT = BigdataSail.class
                .getPackage().getName()
                + ".allowAutoCommit";
        
        public static final String DEFAULT_ALLOW_AUTO_COMMIT = "false";

        
        /**
         * Options (default <code>false</code>) creates the SPO relation with
         * isolatable indices to allow read/write transactions.
         */
        public static final String ISOLATABLE_INDICES = BigdataSail.class
                .getPackage().getName()
                + ".isolatableIndices";
        
        public static final String DEFAULT_ISOLATABLE_INDICES = "false";

        /**
         * Option specifies the namespace of the designed KB instance (default
         * {@value #DEFAULT_NAMESPACE}).
         */
        public static final String NAMESPACE = BigdataSail.class.getPackage()
                .getName()+ ".namespace";

        public static final String DEFAULT_NAMESPACE = "kb";

        /**
         * Option specifies the algorithm used to compute DESCRIBE responses
         * (optional).
         * 
         * @see QueryHints#DESCRIBE_MODE
         * @see QueryHints#DEFAULT_DESCRIBE_MODE
         */
        public static final String DESCRIBE_MODE = BigdataSail.class
                .getPackage().getName() + ".describeMode";

        /**
         * Option specifies the iteration limit for the algorithm used to
         * compute DESCRIBE responses (optional).
         * 
         * @see QueryHints#DESCRIBE_ITERATION_LIMIT
         * @see QueryHints#DEFAULT_DESCRIBE_ITERATION_LIMIT
         */
        public static final String DESCRIBE_ITERATION_LIMIT = BigdataSail.class
                .getPackage().getName() + ".describeIterationLimit";

        /**
         * Option specifies the statement limit for the algorithm used to
         * compute DESCRIBE responses (optional).
         * 
         * @see QueryHints#DESCRIBE_STATEMENT_LIMIT
         * @see QueryHints#DEFAULT_DESCRIBE_STATEMENT_LIMIT
         */
        public static final String DESCRIBE_STATEMENT_LIMIT = BigdataSail.class
                .getPackage().getName() + ".describeIterationStatementLimit";

        /**
         * The name of the default value used for the
         * {@link Journal.Options#FILE} property by the
         * {@link BigdataSail#BigdataSail()} convenience constructor.
         * 
         * @see BigdataSail#BigdataSail()
         */
        public static final String DEFAULT_FILE = "bigdata" + JNL;
        
        /**
         * This optional boolean property may be used to specify whether quads
         * in triple mode are rejected (true) or loaded while silently stripping
         * off the context (false). In the latter case, the fireSailChangedEvent
         * will report back the original statements, i.e. including the context,
         * although the latter was not stored internally. This property only
         * effects data loaded through the {@link Sail}.
         * 
         * @see http://trac.blazegraph.com/ticket/1086
         */
        public static final String REJECT_QUADS_IN_TRIPLE_MODE = BigdataSail.
              class.getPackage().getName() + ".rejectQuadsInTripleMode"; 
        
        public static final String DEFAULT_REJECT_QUADS_IN_TRIPLE_MODE = "false";

    }

    /**
     * Logger.
     */
    final private static Logger log = Logger.getLogger(BigdataSail.class);

//    final protected static boolean INFO = log.isInfoEnabled();

//    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/443 (Logger for
     *      RWStore transaction service and recycler)
     */
    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

    /**
    * Sesame has the notion of a "null" graph which we use for the quad store
    * mode. Any time you insert a statement into a quad store and the context
    * position is not specified, it is actually inserted into this "null" graph.
    * If SPARQL <code>DATASET</code> is not specified, then all contexts are
    * queried and you will see statements from the "null" graph as well as from
    * any other context.
    * {@link BigdataSailConnection#getStatements(Resource, URI, Value, boolean, Resource...)}
    * will return statements from the "null" graph if the context is either
    * unbound or is an array whose sole element is <code>null</code>.
    * 
    * @see BigdataSailConnection#addStatement(Resource, URI, Value, Resource...)
    * @see BigdataSailConnection#getStatements(Resource, URI, Value, boolean,
    *      Resource...)
    */
    public static final transient URI NULL_GRAPH = BD.NULL_GRAPH;

    /**
     * The index manager associated with the database (when there is a focus
     * store and a main database, then this is the focus store index manager).
     */
    final private IIndexManager indexManager;

    /**
     * The as-configured Blazegraph namespace for the {@link BigdataSail}.
     * 
     * @see Options#NAMESPACE
     * 
     * @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested)
     */
    final private String namespace;
    
    /**
     * A value factory associated with the namespace for the {@link BigdataSail}.
     */
    final private BigdataValueFactory valueFactory;
    
    /**
     * <code>true</code> iff the {@link BigdataSail} has been
     * {@link #initialize()}d and not {@link #shutDown()}.
     */
    private boolean openSail;
    
    /**
     * Set <code>true</code> by ctor variants that open/create the database
     * but not by those that connect to an existing database. This helps to
     * provide the illusion of a dedicated purpose SAIL for those ctor variants.
     */
    private boolean closeOnShutdown;
    
    private enum KnownIsolatableEnum {
        /** We do not know whether or not the namespace uses isolated indices. */
        Unknown,
        /** The namespace uses isolated indices. */
        Isolated,
        /** The namespace does not use isolated indices. */
        Unisolated
    }
    
    /**
     * Field is used to cache whether or not the namespace uses isolatable
     * indices. Initially, this is not known. The value becomes known once we
     * successfully resolve the namespace.  It is cleared if the namespace is
     * destroyed.
     */
    final private AtomicReference<KnownIsolatableEnum> knownIsolatable = new AtomicReference<KnownIsolatableEnum>(KnownIsolatableEnum.Unknown); 
    
    /**
     * Transient (in the sense of non-restart-safe) map of namespace prefixes
     * to namespaces.  This map is thread-safe and shared across all
     * {@link BigdataSailConnection} instances and across all transactions.
     */
    private Map<String, String> namespaces;

    /**
     * The query engine.
     */
    final private QueryEngine queryEngine;

    /**
     * Return the blazegraph namespace associated with the {@link BigdataSail}.
     * 
     * @see Options#NAMESPACE
     * @see BLZG-2023: BigdataSail.getUnisolatedConnection() encapsulation 
     */
    public String getNamespace() {
        
        return namespace;
    
    }
    
    /**
     * The index manager associated with the database (when there is a focus
     * store and a main database, then this is the focus store index manager).
     * 
     * @see BLZG-2041: BigdataSail.getUnisolatedConnection() encapsulation 
     */
    public IIndexManager getIndexManager() {

        return indexManager;
        
    }
        
    /* Gone since BLZG-2041: This was accessing the AbstractTripleStore without a Connection.
     */
//    @Deprecated  
//    AbstractTripleStore getDatabase() {
//        
//        return database;
//        
//    }

    /**
     * Defaults various properties.
     */
    private static Properties getDefaultProperties() {
        
        final Properties properties = new Properties();
        
        properties.setProperty(Options.FILE, Options.DEFAULT_FILE);
        
        return properties;
        
    }

    /**
     * Create or re-open a database instance configured using defaults.
     */
    public BigdataSail() {
        
        this(getDefaultProperties());
        
    }
    
    /**
     * Create or open a database instance configured using the specified
     * properties.
     * 
     * @see Options
     */
    public BigdataSail(final Properties properties) {
        
        this(properties.getProperty(Options.NAMESPACE,Options.DEFAULT_NAMESPACE), new Journal(properties));

        closeOnShutdown = true;
        
        if(!exists()) {
            // Create iff it does not exist (backward compatibility, simple constructor mode).
            try {
                create(properties);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
                    }

    }

    /**
    * Check properties to make sure that they are consistent.
    * 
    * @param properties
    *           The properties.
    * @throws UnsupportedOperationException
    */
    public static void checkProperties(final Properties properties) 
            throws UnsupportedOperationException {
    
        final boolean quads = Boolean.parseBoolean(properties.getProperty(
                BigdataSail.Options.QUADS,
                BigdataSail.Options.DEFAULT_QUADS));
        
        final boolean quadsMode = Boolean.parseBoolean(properties.getProperty(
                BigdataSail.Options.QUADS_MODE,
                BigdataSail.Options.DEFAULT_QUADS_MODE));
        
        final boolean isolatable = Boolean.parseBoolean(properties.getProperty(
                BigdataSail.Options.ISOLATABLE_INDICES,
                BigdataSail.Options.DEFAULT_ISOLATABLE_INDICES));
        
        final boolean tm = Boolean.parseBoolean(properties.getProperty(
                BigdataSail.Options.TRUTH_MAINTENANCE,
                BigdataSail.Options.DEFAULT_TRUTH_MAINTENANCE));
        
        final boolean justify = Boolean.parseBoolean(properties.getProperty(
                BigdataSail.Options.JUSTIFY,
                BigdataSail.Options.DEFAULT_JUSTIFY));
        
        final boolean noAxioms = properties.getProperty(
                BigdataSail.Options.AXIOMS_CLASS,
                BigdataSail.Options.DEFAULT_AXIOMS_CLASS).equals(
                        NoAxioms.class.getName());
        
//        final boolean noVocab = properties.getProperty(
//                BigdataSail.Options.VOCABULARY_CLASS,
//                BigdataSail.Options.DEFAULT_VOCABULARY_CLASS).equals(
//                        NoVocabulary.class.getName());
        
        // check for problematic property combinations
        if (isolatable && !quadsMode) {
            
            if (tm) {
                
                throw new UnsupportedOperationException(
                        "Cannot use transactions with truth maintenance. " +
                        "Set option " + Options.TRUTH_MAINTENANCE + 
                        " = false");
                
            }
            
            if (!noAxioms) {
                
                throw new UnsupportedOperationException(
                        "Cannot use transactions with inference. " +
                        "Set option " + Options.AXIOMS_CLASS + 
                        " = " + NoAxioms.class.getName());
                
            }
            
//            if (!noVocab) {
//                
//                throw new UnsupportedOperationException(
//                        "Cannot use transactions with a vocabulary class. " +
//                        "Set option " + Options.VOCABULARY_CLASS + 
//                        " = " + NoVocabulary.class.getName());
//                
//            }

            if (justify) {
                
                throw new UnsupportedOperationException(
                        "Cannot use transactions with justification chains. " +
                        "Set option " + Options.JUSTIFY + 
                        " = " + Boolean.FALSE);
                
            }

        }
        
        if (quads || quadsMode) {

            if (tm) {

                /*
                 * Note: Truth maintenance is not supported for quads at this
                 * time.
                 */
                throw new UnsupportedOperationException(
                        Options.TRUTH_MAINTENANCE
                                + " is not supported with quads ("
                                + Options.QUADS + ")");
                
            }

        }
        
    }
        
    /**
     * Constructor used to wrap an existing {@link AbstractTripleStore}
     * instance.
     * <p>
     * Note: Since BLZG-2041, this delegates through to the core constructor
     * which accepts (namespace, IIndexManager). 
     * 
     * @param database
     *            The instance.
     */
    public BigdataSail(final AbstractTripleStore database) {
     
        this(database, database.getIndexManager());
        
    }

    /**
     * Core ctor. You must use this variant for a scale-out triple store.
     * <p>
     * To create a {@link BigdataSail} backed by an {@link IBigdataFederation}
     * use the {@link ScaleOutTripleStore} ctor and then
     * {@link AbstractTripleStore#create()} the triple store if it does not
     * exist.
     * <p>
     * Note: Since BLZG-2041, this delegates through to the core constructor
     * which accepts (namespace, IIndexManager). 
     * 
     * @param database
     *            An existing {@link AbstractTripleStore}.
     * @param mainIndexManager
     *            When <i>database</i> is a {@link TempTripleStore}, this is the
     *            {@link IIndexManager} used to resolve the
     *            {@link QueryEngine}. Otherwise it must be the same object as
     *            the <i>database</i>.
     */ 
    public BigdataSail(final AbstractTripleStore database,
            final IIndexManager mainIndexManager) {

        this(database.getNamespace(), database.getIndexManager(), mainIndexManager);
    
    }

    /**
     * Constructor used when the namespace and index manager are known (standard
     * use case).
     * 
     * @param namespace The namespace.
     * @param indexManager The index manager (typically a {@link Journal}).
             */
    public BigdataSail(final String namespace, final IIndexManager indexManager) {
        
        this(namespace, indexManager, indexManager);
            
        }
            
    /**
     * Core constructor.
     * <p>
     * Note: Scale-out is shard-wise ACID.
     * To create a {@link BigdataSail} backed by an {@link IBigdataFederation}
     * use the {@link ScaleOutTripleStore} ctor and then
     * {@link AbstractTripleStore#create()} the triple store if it does not
     * exist. See {@link CreateKBTask} which encapsulates this.
     * 
     * @param namespace The namespace.
     * @param indexManager The index manager on which the data is stored.
     * @param mainIndexManager Iff the data is stored on a {@link TempTripleStore} then this is the main index manager and will be used to locate the {@link QueryEngine}.
     * 
     * @see BLZG-2041 BigdataSail should not locate the AbstractTripleStore until a connection is requested
             */
    public BigdataSail(final String namespace, final IIndexManager indexManager, 
            final IIndexManager mainIndexManager) {
            
        if (namespace == null)
            throw new IllegalArgumentException();

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (mainIndexManager == null)
            throw new IllegalArgumentException();
        
        // default to false here and overwritten by some ctor variants.
        this.closeOnShutdown = false;
        
        this.namespace = namespace;
            
        this.indexManager = indexManager;
            
        // Note: The actual namespace used for the ValueFactory is the namespace of the LexiconRelation.
        this.valueFactory = BigdataValueFactoryImpl.getInstance(namespace+"."+LexiconRelation.NAME_LEXICON_RELATION);

//        // Canonical pattern to obtain a lock used make unisolated and read/write tx operations MUTEX.
//        {
////            this.lock = new ReentrantReadWriteLock(false/*fair*/);
//            // Attempt to resolve.
//            ReentrantReadWriteLock lock = locks.get(namespace);
//            if (lock == null ) {
//                final ReentrantReadWriteLock tmp = locks.putIfAbsent(namespace, lock = new ReentrantReadWriteLock(false/*false*/));
//                if(tmp != null) {
//                    // lost data race.
//                    lock = tmp;
//                }
//            }
//            this.lock = lock;
//        }
        
        this.namespaces = 
            Collections.synchronizedMap(new LinkedHashMap<String, String>());
        
        this.queryEngine = QueryEngineFactory.getInstance().getQueryController(mainIndexManager);

//        this.runstate = RunstateEnum.Active;
    }
    
    /**
     * 
     * @throws IllegalStateException
     *             if the {@link BigdataSail} has not been {@link #initialize()}d
     *             or has been {@link #shutDown()}.
     */
    protected void assertOpenSail() {

        if (!openSail)
            throw new IllegalStateException();
        
    }

    /**
     * Return <code>true</code> if the {@link BigdataSail} has been
     * {@link #initialize()}d and has not been {@link #shutDown()}.
     */
    public boolean isOpen() {
        
        return openSail;
        
    }
    
    /**
     * Create a the configured namespace.
     * 
     * @param properties The configuration properties for the namespace.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     * 
     * @see Options#NAMESPACE
     * 
     * @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested)
     */
    public void create(final Properties properties) throws InterruptedException, ExecutionException {
        
        if ( properties == null )
            throw new IllegalArgumentException();
        
        AbstractApiTask.submitApiTask(indexManager,
                new CreateKBTask(namespace, properties)
                ).get();
            
    }

    /**
     * Destroy the configured namespace.
     * 
     * @throws SailException
     * @throws ExecutionException 
     * @throws InterruptedException 
     * 
     * @see Options#NAMESPACE
     * 
     * @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested)
     */
    public void destroy() throws SailException, InterruptedException, ExecutionException {
        
        shutDown();
        
        AbstractApiTask.submitApiTask(indexManager,
                new DestroyKBTask(namespace)
                ).get();
            
    }

    /**
     * Return true iff the namespace exists at the moment when this check is
     * performed.
     * <p>
     * Note: It is not possible to use this with any guarantees that the namespace
     * still exists once control is returned to the caller unless the caller is
     * holding an unisolated connection.
     * 
     * @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested)
     */
    public boolean exists() {

        // Attempt to resolve the namespace.
        return indexManager.getResourceLocator().locate(namespace, ITx.UNISOLATED) != null;
        
    }
    
    /**
     * @throws IllegalStateException
     *             if the sail is already open.
     */
    @Override
    protected void initializeInternal() throws SailException {

        if (openSail)
            throw new IllegalStateException();
        
        /*
         * NOP (nothing to invoke in the SailBase).
         */
        
        if(log.isInfoEnabled()) {
            
            log.info("closeOnShutdown=" + closeOnShutdown);
            
        }
        
        knownIsolatable.set(KnownIsolatableEnum.Unknown);
        
        openSail = true;
        
    }
    
    /**
     * Invokes {@link #shutDown()}.
     */
    @Override
    protected void finalize() throws Throwable {
        
        if(isOpen()) {
            
            if (log.isInfoEnabled())
                log.info("");
            
            shutDown();
            
        }
        
        super.finalize();
        
    }

    @Override
    public void shutDown() throws SailException {
        
        assertOpenSail();

        /*
         * Note: DO NOT shutdown the query engine. It is shared by all
         * operations against the same backing Journal or IBigdataFederation
         * within this JVM!
         */
//        queryEngine.shutdown();
        
//        /*
//         * Indicate Sail shutdown for SailConnection.close() handling
//         */
//        runstate = RunstateEnum.Shutdown;
        
        super.shutDown();
        
//        /*
//         * Check current lock to see if it can be acquired.
//         * If not, then replace lock
//         */
//        if (lock.isWriteLocked()) {
//        	locks.put(namespace, new ReentrantReadWriteLock(false/*false*/));
//        }
    }
    
    /**
     * If the backing database was created/opened by the {@link BigdataSail}
     * then it is closed.  Otherwise this is a NOP.
     */
    @Override
    protected void shutDownInternal() throws SailException {
        
        if (openSail) {
            
            try {
            
                if (closeOnShutdown) {

                    if (log.isInfoEnabled())
                        log.info("Closing the backing database");

                    final BigdataValueFactoryImpl vf = ((BigdataValueFactoryImpl)getValueFactory());

                    /*
                     * Discard the value factory for the lexicon's namespace.
                     * iff the backing Journal will also be closed.
                     * 
                     * Note: This is only possible when the Journal will also be
                     * closed since there could otherwise be concurrently open
                     * AbstractTripleStore instances for the same namespace and
                     * database instance.
                     */
                    vf.remove();

                    /*
                     * Discard all term cache entries for the lexicon's
                     * namespace with the same caveat as above for the 
                     * backing Journal.
                     */
                    LexiconRelation.clearTermCacheFactory(vf.getNamespace());

//                    database.close();
                    if(getIndexManager() instanceof IJournal) {
                        ((IJournal)getIndexManager()).shutdown();
                    }
                    
                }
                
            } finally {

                openSail = false;

            }

        }
        
    }

    /**
     * <strong>DO NOT INVOKE FROM APPLICATION CODE</strong> - this method
     * deletes the KB instance and destroys the backing database instance. It is
     * used to help tear down unit tests.
     */
    public void __tearDownUnitTest() {
        
        closeOnShutdown = false;
        
        try {

            if(isOpen()) shutDown();

//            database.__tearDownUnitTest();
            getIndexManager().destroy();

        } catch (Throwable t) {

            log.error("Problem during shutdown: " + t, t);

        }
        
    }
    
    /**
     * A {@link BigdataValueFactory}
     * <p>
     * {@inheritDoc}
     */
    @Override // FIXME BLZG-2041 LexiconRelation constructor no longer overrides the value factory implementation class on this code path!
    final public ValueFactory getValueFactory() {
        
        return valueFactory;
//        return database.getValueFactory();
        
    }

    /**
     * Return false iff the Sail can not provide a writable connection.
     * <p>
     * {@inheritDoc}
     *  
     *  @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested) 
     */
    @Override
    final public boolean isWritable() throws SailException {

        if(getIndexManager() instanceof IRawStore) {
            // For backends that are willing to report whether or not they are
            // read-only, this returns false if the index manager is read-only.
            return !((IRawStore)getIndexManager()).isReadOnly();
        }
        // Otherwise assume writable (TemporaryStore, Federation).
        return true;
//        return ! database.isReadOnly();
        
    }

    /**
     * Return a read-write {@link SailConnection}. This is used for both
     * read-write transactions and the UNISOLATED connection.
     * <p>
     * Note: There is only one UNISOLATED connection and, when requested, this
     * method will block until that connection is available.
     * 
     * @see #getReadOnlyConnection() for a non-blocking, read-only connection.
     * 
     * @todo many of the stores can support concurrent writers, but there is a
     *       requirement to serialize writers when truth maintenance is enabled.
     */
    @Override
    protected NotifyingSailConnection getConnectionInternal() 
        throws SailException {

        final BigdataSailConnection conn;
        try {

            if ( knownIsolatable.get() == KnownIsolatableEnum.Unknown ) {

                /*
                 * Resolve whether or not the namespace supports isolatation.
                 * 
                 * Note: This pattern is eventually consistent.  At most one
                 * thread will resolve the data race in the case where the 
                 * isolation property is not known on entry.
                 */
                
                final AbstractTripleStore readOnlyTripleStore = (AbstractTripleStore) getIndexManager().getResourceLocator().locate(namespace, ITx.READ_COMMITTED);
    
                if(readOnlyTripleStore == null) {
                    
                    throw new DatasetNotFoundException("namespace="+namespace);
                
                }
                
                final Properties properties = readOnlyTripleStore.getProperties();
                
                final boolean isolatable = Boolean.parseBoolean(properties.getProperty(
                                BigdataSail.Options.ISOLATABLE_INDICES,
                                BigdataSail.Options.DEFAULT_ISOLATABLE_INDICES));
                
                // Set the flag.
                knownIsolatable.compareAndSet(KnownIsolatableEnum.Unknown/*expect*/, isolatable?KnownIsolatableEnum.Isolated:KnownIsolatableEnum.Unisolated);
            
            }

            switch(knownIsolatable.get()) {
            case Isolated:
                conn = getReadWriteConnection();
                break;
            case Unisolated:
                conn = getUnisolatedConnection();
                break;
            case Unknown:
            default:
                throw new UnsupportedOperationException();
            }

            return conn;
            
        } catch (Exception ex) {
            
            throw new SailException(ex);
            
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * If the triple store was provisioned to support full read/write
     * transactions then this is delegated to {@link #getReadWriteConnection()}.
     * Otherwise, is delegated to {@link #getUnisolatedConnection()} which
     * returns the unisolated view of the database. Note that truth maintenance
     * requires only one connection at a time and is therefore not compatible
     * with full read/write transactions.
     * <p>
     * The correct pattern for obtaining an updatable connection, doing work
     * with that connection, and committing or rolling back that update is as
     * follows.
     * 
     * <pre>
     * 
     * BigdataSailConnection conn = null;
     * boolean ok = false;
     * try {
     *     conn = sail.getConnection();
     *     doWork(conn);
     *     conn.commit();
     *     ok = true;
     * } finally {
     *     if (conn != null) {
     *         if (!ok) {
     *             conn.rollback();
     *         }
     *         conn.close();
     *     }
     * }
     * </pre>
     * 
     * This pattern can also be used with {@link #getUnisolatedConnection()}.
     */
    @Override
    public BigdataSailConnection getConnection() throws SailException {
      
        return (BigdataSailConnection) super.getConnection();
        
    }
    
//    /**
//     * Cache for the {@link #lock}s.
//     */
//    final static private transient ConcurrentWeakValueCache<String/*namespace*/, ReentrantReadWriteLock> locks
//    = new ConcurrentWeakValueCacheWithTimeout<String, ReentrantReadWriteLock>(
//            20/*cacheCapacity*/, TimeUnit.MILLISECONDS.toNanos(10000/*cacheTimeout*/));
//    
//    /**
//     * Used to coordinate between read/write transactions and the unisolated
//     * connection.
//     * <p>
//     * In terms of the SAIL, we do need to prevent people from using (and 
//     * writing on) the unisolated statement indices concurrent with full 
//     * transactions.  The issue is that an UnisolatedReadWriteIndex is used 
//     * (by AbstractRelation) to protect against concurrent operations on the 
//     * same index, but the transaction commit protocol on the journal submits a 
//     * task to the ConcurrencyManager, which will execute when it obtains a lock 
//     * for the necessary index.  These two systems ARE NOT using the same locks.
//     * The UnisolatedReadWriteIndex makes it possible to write code which 
//     * operates as if it has a local index object.  If you use the 
//     * ConcurrencyManager, then all unisolated operations must be submitted 
//     * as tasks which get scheduled.  Probably it is dangerous to have these 
//     * two different models coexisting.  We should talk about that at some 
//     * point.
//     *
//     * @see BLZG-2041 This needs to be a canonical lock for the namespace, not per BigdataSail instance.
//     */
//    final private ReentrantReadWriteLock lock;

   /**
    * Return an unisolated connection to the database. The unisolated connection
    * supports fast, scalable updates against the database. The unisolated
    * connection is ACID when used with a local {@link Journal} and shard-wise
    * ACID when used with an {@link IBigdataFederation}.
    * <p>
    * <h3>Group Commit Enabled</h3>
    * <p>
    * If {@link com.bigdata.journal.Journal.Options#GROUP_COMMIT group commit}
    * is enabled and you use the either the REST API or the
    * {@link com.bigdata.rdf.task.AbstractApiTask#submitApiTask(IIndexManager, com.bigdata.rdf.task.IApiTask)}
    * pattern to submit tasks, then concurrent unisolated connections will be
    * granted for different namespaces and multiple unisolated connections for
    * the same namespace MAY be melded into the same commit group (whether they
    * are or not depends on the timing of the tasks and whether they are being
    * submitted by a single client thread or by a pool of clients).
    * <p>
    * <h3>Group Commit Disabled</h3>
    * <p>
    * In this mode the application decides when the database will go through a
    * commit point. In order to guarantee that operations against the unisolated
    * connection are ACID, only one unisolated connection is permitted at a time
    * for a {@link Journal}. If there is an open unisolated connection against a
    * local {@link Journal}, then the open connection must be closed before a
    * new connection can be returned by this method.
    * <p>
    * This constraint that there can be only one unisolated connection is not
    * enforced in scale-out since unisolated operations in scale-out are only
    * shard-wise ACID.
    * <p>
    * The correct pattern for obtaining an updatable connection, doing work with
    * that connection, and committing or rolling back that update is as follows.
    * 
    * <pre>
    * BigdataSailConnection conn = null;
    * boolean ok = false;
    * try {
    *    conn = sail.getUnisolatedConnection();
    *    doWork(conn);
    *    conn.commit();
    *    ok = true;
    * } finally {
    *    if (conn != null) {
    *       if (!ok) {
    *          conn.rollback();
    *       }
    *       conn.close();
    *    }
    * }
    * </pre>
    * 
    * @return The unisolated connection to the database
    * 
    * @see #getConnection()
    * @see <a href="wiki.blazegraph.com/wiki/index.php?title=GroupCommit" > Group
    *      Commit (wiki) </a>
    */
    public BigdataSailConnection getUnisolatedConnection()
            throws InterruptedException {

        final UnisolatedCallable<BigdataSailConnection> task = new UnisolatedCallable<BigdataSailConnection>() {
            @Override
            public BigdataSailConnection call() throws Exception {
                // new unisolated connection.
                final BigdataSailConnection cnxn = new BigdataSailConnection(access).startConn();
                
    			manageConnection(cnxn);
    			
    			return cnxn;
            }};
        try {
            // Create and return the unisolated connection object.  It will 
            // continue to hold the necessary locks.
            return getUnisolatedConnectionLocksAndRunLambda(task);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            /*
             * Note: WE DO NOT RELEASE THE LOCKS ON THIS CODE PATH. THEY ARE
             * HELD BY THE CONNECTION OBJECT THAT WE ARE RETURNING TO THE
             * CALLER.
             */
        }
        
    }

    /**
     * Abstract base class permits some patterns in which the {@link Callable}
     * owns the locks required to obtain the unisolated connection. This is
     * used by the {@link CreateKBTask} which needs those locks, but which can
     * not acquire the {@link BigdataSailConnection} yet since the namespace
     * does not exist yet.
     * 
     * @author bryan
     *
     * @param <T>
     * 
     * @see BLZG-2023, BLZG-2041
     */
    public static abstract class UnisolatedCallable<T> implements Callable<T> {

        private boolean didRun = false;
//        private Lock writeLock;
        private IIndexManager indexManager;
        
        protected Access access;
        
        protected boolean didRun() {
            return didRun;
        }
        
        private void lambdaDidRun() {
            didRun = true; 
         }
         
//        protected Lock getWriteLock() {
//            return writeLock;
//        }
        
        private void init(final Access access, final IIndexManager indexManager) {
            if(indexManager == null)
                throw new IllegalArgumentException();
            
            this.indexManager = indexManager;
            this.access = access;
        }
        
        /**
         * Release any acquired locks.
         */
        /*
         * Note: The code path which obtains the unisolated BigdataSailConnection
         * object does NOT use this method to release the locks. Instead, that is
         * handled in BigdataSailConnection.close().
         */
        public void releaseLocks() {
            if(!didRun) {
                // Did not run, so any locks were already released.
                return;
            }
            if (access != null) {
                access.release();
            }
        }
        
    }
    
    /**
     * Obtain the locks required by the unisolated connection and invoke the
     * caller's lambda.
     * 
     * @param lambda The caller's lambda 
     * 
     * @return The value to which the lamba evaluates.
     * 
     * @throws Exception 
     */
    public <T> T getUnisolatedConnectionLocksAndRunLambda(final UnisolatedCallable<T> lambda) 
            throws Exception {

        if(lambda == null)
            throw new IllegalArgumentException();
        
//        if (lock.writeLock().isHeldByCurrentThread()) {
//            /*
//             * A thread which already holds this lock already has the open
//             * unisolated connection and will deadlock when it attempts to
//             * obtain the permit for that connection from the Journal.
//             */
//            throw new UnisolatedConnectionNotReentrantException();
//        }

        Access access = null;
        boolean ok = false;
        try {
            if (getIndexManager() instanceof Journal) {
            /*
             * acquire permit from Journal.
             * 
             * Note: The [instanceof Journal] test here is correct. What happens
             * is that an unisolated AbstractTask that is submitted through the
             * ConcurrencyManager on the Journal is an IJournal (an
             * IsolatedActionJournal) but not a Journal. Thus the
             * getUnisolatedConnection() code does not attempt to obtain the
             * global semaphore. This allows concurrent execution of tasks
             * requiring unisolated connections for distinct namespaces on the
             * same journal.
             * 
             * @see #1137 (Code review of IJournal vs Journal)
             */
                access = ((Journal) getIndexManager()).acquireUnisolatedConnectionAccess();
            }

            // Set lock and index manager on lambda.
            lambda.init(access, getIndexManager());
            // Invoke lambda.
            final T ret = lambda.call();
            // Note that lambda did run (successfully). This means that the lambda
            // is now responsible for calling releaseLocks().
            lambda.lambdaDidRun();
            // Did run successfully.
            ok = true;
            return ret;
            
        } catch(DatasetNotFoundException ex) {
           throw new RuntimeException(ex);
        } finally {
            if (!ok) {
                if (access != null) {
                    // release permit.
                    access.release();
                }
            }
        }

    }
    
    /**
     * Return a read-only connection based on the last commit point. This method
     * is atomic with respect to the commit protocol.
     * 
     * @return The view.
     */
    public BigdataSailConnection getReadOnlyConnection() {
        
        // Note: This is not atomic with respect to the commit protocol.
//        final long timestamp = database.getIndexManager().getLastCommitTime();
//
//        return getReadOnlyConnection(timestamp);

        return getReadOnlyConnection(ITx.READ_COMMITTED);
        
    }
    
    /**
     * Obtain a read-historical view that reads from the specified commit point.
     * This view is safe for concurrent readers and will not update if there are
     * concurrent writes.
     * 
     * @param timestamp
     *            The commit point.
     * 
     * @return The view.
     */
    public BigdataSailConnection getReadOnlyConnection(final long timestamp) {

    	try {
			
    	    return _getReadOnlyConnection(timestamp);
    	    
		} catch (IOException | DatasetNotFoundException e) {
			
		    throw new RuntimeException(e);
		    
		}
    	
    }

    /**
     * Return a read-only connection backed by a read-only transaction. The
     * transaction will be closed when the connection is closed.
     * 
     * @param timestamp
     *            The timestamp.
     *            
     * @return The transaction.
     * 
     * @throws IOException
    * @throws DatasetNotFoundException 
     * @see ITransactionService#newTx(long)
     */
    private BigdataSailConnection _getReadOnlyConnection(final long timestamp)
            throws IOException, DatasetNotFoundException {
        final long commitTime = timestamp;
//        if(timestamp == ITx.READ_COMMITTED) {
//            commitTime = getIndexManager().getLastCommitTime();
//        } else {
//            commitTime = timestamp;
//        }
//        if(!TimestampUtility.isCommitTime(commitTime))
//            throw new IllegalArgumentException("Not a commitTime: timestamp="+timestamp);
        final ITransactionService txService = getTxService();
        boolean ok = false;
        final long txId = txService.newTx(commitTime);
        try {
            final BigdataSailConnection conn = new BigdataSailReadOnlyConnection(txId,txService).startConn();
            ok = true;
            
			manageConnection(conn);

            return conn;
        } finally {
            if(!ok) {
                try {
                    txService.abort(txId);
                } catch (IOException ex) {
                    log.error(ex, ex);
                }
            }
        }
    }

	/**
	 * Return a connection backed by a read-write transaction.
	 * @throws InterruptedException 
	 * 
	 * @throws DatasetNotFoundException 
	 * 
	 * @throws UnsupportedOperationException
	 *             unless {@link Options#ISOLATABLE_INDICES} was specified when
	 *             the backing triple store instance was provisioned.
	 */
    public BigdataSailConnection getReadWriteConnection() throws IOException, InterruptedException {

//        if (!isolatable) {
//
//            throw new UnsupportedOperationException(
//                "Read/write transactions are not allowed on this database. " +
//                "See " + Options.ISOLATABLE_INDICES);
//
//        }

        final IIndexManager indexManager = getIndexManager();

        if (indexManager instanceof IBigdataFederation<?>) {

        	throw new UnsupportedOperationException("Read/write transactions are not yet supported in scale-out.");

        }

        final ITransactionService txService = getTxService();
        boolean ok = false;
        Access access = null;
        final long txId = txService.newTx(ITx.UNISOLATED); // read/write tx on lastCommitTime.
        try {
            if (getIndexManager() instanceof Journal) {
            /*
             * acquire permit from Journal.
             * 
             * Note: The [instanceof Journal] test here is correct. What happens
             * is that an unisolated AbstractTask that is submitted through the
             * ConcurrencyManager on the Journal is an IJournal (an
             * IsolatedActionJournal) but not a Journal. Thus the
             * getUnisolatedConnection() code does not attempt to obtain the
             * global semaphore. This allows concurrent execution of tasks
             * requiring unisolated connections for distinct namespaces on the
             * same journal.
             * 
             * @see #1137 (Code review of IJournal vs Journal)
             */
                access = ((Journal) getIndexManager()).acquireReadWriteConnectionAccess();
            }
            final BigdataSailConnection conn = new BigdataSailRWTxConnection(access, txId,txService).startConn();
            ok = true;
            
			manageConnection(conn);

            return conn;
        } catch(DatasetNotFoundException ex) {
            throw new RuntimeException(ex);
        } finally {
            if(!ok) {
                if (access != null) {
                    access.release(); // FIXME BLZG-2041 Can we push this into the Journal transaction manager?
                }
                try {
                    txService.abort(txId);
                } catch (IOException ex) {
                    log.error(ex, ex);
                }
            }
        }

    }

    /**
     * Return the {@link ITransactionService}.
     */
	protected ITransactionService getTxService() {

		final IIndexManager indexManager = getIndexManager();

		final ITransactionService txService;

		if (indexManager instanceof IJournal) {

			txService = ((IJournal) indexManager).getLocalTransactionManager()
					.getTransactionService();

		} else {

			txService = ((AbstractFederation<?>) indexManager)
					.getTransactionService();

		}

		return txService;

	}
    
    public QueryEngine getQueryEngine() {
        
        return queryEngine;
        
    }

    /**
     * Inner class implements the {@link SailConnection}. Some additional
     * functionality is available on this class, including
     * {@link #computeClosure()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     *         TODO This should be made into a static class. As it is, there is
     *         a possibility for subtle errors introduced by inheritence of
     *         variables from the {@link BigdataSail}. For example, this was
     *         causing a problem with the {@link #close()} method on this class
     *         and on the classes derived from this class.
     */
    public class BigdataSailConnection implements NotifyingSailConnection {

        /**
         * The database view.
         * 
         * @see #attach(long)
         */
        private AbstractTripleStore database;

        /**
         * True iff the database view is read-only.
         */
        final protected boolean readOnly;

        /**
         * True iff the {@link SailConnection} is open.
         */
        protected boolean openConn;
        
        /**
         * Set for Unisolated and ReadWrite connections.
         */
        protected Access access;
        
        /**
         * <code>true</code> iff the {@link BigdataSail} can support truth
         * maintenance, however truth maintenance may be conditionally suppressed
         * for a {@link BigdataSailConnection} in order to allow people to
         * temporarily disable when batching updates into the Sail.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/591 (Manage Truth
         *      Maintenance)
         */
        private final boolean truthMaintenanceIsSupportable;
        
        /**
         * The configured capacity for the statement buffer(s).
         * 
         * @see Options#BUFFER_CAPACITY
         */
        final private int bufferCapacity;
        
        /**
         * The configured capacity for blocking queue backing the statement buffer(s).
         * 
         * @see Options#QUEUE_CAPACITY
         */
        final private int queueCapacity;
        
        /**
         * When true, the SAIL is in the "quads" mode.
         * 
         * @see AbstractTripleStore.Options#QUADS
         */
        final private boolean quads;
        
        /**
         * When <code>true</code> the SAIL is backed by an
         * {@link IBigdataFederation}.
         */
        final protected boolean scaleOut;
        
        /**
         * When true, SAIL will compute entailments at query time that were excluded
         * from forward closure.
         * 
         * @see Options#QUERY_TIME_EXPANDER
         */
        final private boolean queryTimeExpander;
        
        /**
         * When true, SAIL will compute an exact size in the {@link
         * SailConnection#size(Resource[])} method.
         * 
         * @see Options#EXACT_SIZE
         */
        final private boolean exactSize;
        
        /**
         * When true, auto-commit is allowed. Do not ever use auto-commit, this is
         * purely used to pass the Sesame test suites.
         * 
         * @see Options#ALLOW_AUTO_COMMIT
         */
        final private boolean allowAutoCommit;
        
        /**
         * When true, read/write transactions are allowed.
         * 
         * @see {@link Options#ISOLATABLE_INDICES}
         */
        protected final boolean isolatable;
        
        /**
         *  Specifies whether quads in triple mode are rejected (true) or loaded 
         *  while silently stripping off the context (false). In the latter case, 
         *  the fireSailChangedEvent will report back the original statements,
         *  i.e. including the context, although the latter was not stored
         *  internally.
         */
        final private boolean rejectQuadsInTripleMode;
        
        /**
         * When true, the RDFS closure will be maintained.
         * <p>
         * Note: This property can be changed dynamically.
         * 
         * @see BigdataSail.Options#TRUTH_MAINTENANCE
         * @see #tm
         */
        private boolean truthMaintenance;
        
        /**
         * non-<code>null</code> iff truth maintenance is being performed.
         * 
         * @see BigdataSail.Options#TRUTH_MAINTENANCE
         * @see #truthMaintenance
         */
        private TruthMaintenance tm;
        
        /**
         * Used to buffer statements that are being asserted.
         * 
         * @see #getAssertionBuffer()
         */
        private StatementBuffer<Statement> assertBuffer;
        
        /**
         * Used to buffer statements being retracted.
         * 
         * @see #getRetractionBuffer()
         */
        private StatementBuffer<Statement> retractBuffer;

        /**
         * Set to <code>true</code> if we {@link #flush()} either of the
         * {@link StatementBuffer}s. The flag reflects whether or not the
         * buffered writes were propagated to the underlying indices. For some
         * database modes those writes will be buffered by the indices and then
         * incrementally flushed through to the disk. For others (a federation)
         * the writes are shard wise ACID.
         * <p>
         * Guarded by <code>synchronized(this)</code> (sychronized on the
         * {@link BigdataSailConnection}).
         * 
         * @see #isDirty()
         * @see #assertBuffer
         * @see #retractBuffer
         * @see #flush()
         */
        protected boolean dirty = false;
        
        /**
         * A canonicalizing mapping for blank nodes whose life cycle is the same
         * as that of the {@link SailConnection}.
         * 
         * FIXME bnodes : maintain the dual of this map, {@link #bnodes2}.
         * 
         * FIXME bnodes : resolution of term identifiers to blank nodes for
         * access path scans and CONSTRUCT in {@link BigdataStatementIterator}.
         * 
         * FIXME bnodes : resolution of term identifiers to blank nodes for
         * JOINs in {@link BigdataSolutionResolverator}.
         */
        private Map<String, BigdataBNode> bnodes;

        /**
         * A reverse mapping from the assigned internal values for blank nodes
         * to the {@link BigdataBNodeImpl} object. This is used to resolve blank
         * nodes recovered during query from within the same
         * {@link SailConnection} without loosing the blank node identifier.
         * This behavior is required by the contract for {@link SailConnection}.
         */
        private Map<IV, BigdataBNode> bnodes2;
        
//        /**
//         * Used to coordinate between read/write transactions and the unisolated
//         * view.
//         */
//        private final Lock lock;

		/**
		 * <code>true</code> iff this is the UNISOLATED connection (only one of
		 * those at a time).
		 */
        private final boolean unisolated;
        
        /**
         * Critical section support in case rollback is not completed cleanly,
         * in which case calls to commit() will fail until a clean {@link #rollback()}
         * is made. 
         * 
         * @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
         */
        private final AtomicBoolean rollbackRequired = new AtomicBoolean(false);

        /**
         * The as-configured properties for the {@link BigdataSail}.
         * 
         * @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested)
         */
        final private Properties properties;

        /**
         * Return the as-configured properties for the {@link BigdataSail}.
         * 
         * @see BLZG-2041 (BigdataSail should not locate the AbstractTripleStore until a connection is requested)
         */
        public Properties getProperties() {
            
            return properties;
            
        }

        /**
         * Return <code>true</code> if the SAIL is using a "quads" mode database.
         * 
         * @see AbstractTripleStore.Options#QUADS
         */
        final public boolean isQuads() {

            return quads;
            
        }
        
        /**
         * Return <code>true</code> iff the SAIL is using a namespace that has been
         * configured to support isolatable indices.
         * 
         * @see Options#ISOLATABLE_INDICES
         */
        final public boolean isIsolatable() {
           
           return isolatable;
           
        }

        @Override
        public String toString() {
        	
            return getClass().getName() + "{namespace=" + namespace
                    + ", timestamp=" + TimestampUtility.toString(database.getTimestamp())
                    + ", open=" + openConn
                    + "}";

        }

        public BigdataSail getBigdataSail() {
            
            return BigdataSail.this;
            
        }
        
//        /**
//         * The inference engine if the SAIL is using one.
//         * <p>
//         * Note: Requesting this object will cause the axioms to be written onto the
//         * database if they are not already present. If this is a read-only view and
//         * the mutable view does not already have the axioms defined then this will
//         * cause an exception to be thrown since the indices are not writable by the
//         * read-only view.
//         */
//        private InferenceEngine getInferenceEngine() {
//
//            return database.getInferenceEngine();
//            
//        }
        
        /**
         * Return the assertion buffer.
         * <p>
         * The assertion buffer is used to buffer statements that are being
         * asserted so as to maximize the opportunity for batch writes. Truth
         * maintenance (if enabled) will be performed no later than the commit
         * of the transaction.
         * <p>
         * Note: When non-<code>null</code> and non-empty, the buffer MUST be
         * flushed (a) if a transaction completes (otherwise writes will not be
         * stored on the database); or (b) if there is a read against the
         * database during a transaction (otherwise reads will not see the
         * unflushed statements).
         * <p>
         * Note: if {@link #truthMaintenance} is enabled then this buffer is
         * backed by a temporary store which accumulates the {@link SPO}s to be
         * asserted. Otherwise it will write directly on the database each time
         * it is flushed, including when it overflows.
         */
        synchronized protected StatementBuffer<Statement> getAssertionBuffer() {

            if (assertBuffer == null) {

                if (truthMaintenance) {

                    assertBuffer = new StatementBuffer<Statement>(tm
                            .newTempTripleStore(), database, bufferCapacity, queueCapacity);

                } else {

                    assertBuffer = new StatementBuffer<Statement>(database,
                            bufferCapacity);

                    assertBuffer.setChangeLog(changeLog);

                }

                // FIXME bnodes : must also track the reverse mapping [bnodes2].
                assertBuffer.setBNodeMap(bnodes);
                
            }

            return assertBuffer;
            
        }

        /**
         * Return the retraction buffer (truth maintenance only).
         * <p>
         * The retraction buffer is used by the {@link SailConnection} API IFF
         * truth maintenance is enabled since the only methods available on the
         * {@link Sail} to delete statements,
         * {@link #removeStatements(Resource, URI, Value)} and
         * {@link #removeStatements(Resource, URI, Value, Resource[])}, each
         * accepts a statement pattern rather than a set of statements. The
         * {@link AbstractTripleStore} directly supports removal of statements
         * matching a triple pattern, so we do not buffer retractions for those
         * method UNLESS truth maintenance is enabled.
         */
//        * <p>
//        * Note: you CAN simply obtain the retraction buffer, write on it the
//        * statements to be retracted, and the {@link BigdataSailConnection}
//        * will do the right thing whether or not truth maintenance is enabled.
//        * <p>
//        * When non-<code>null</code> and non-empty the buffer MUST be
//        * flushed (a) if a transaction completes (otherwise writes will not be
//        * stored on the database); or (b) if there is a read against the
//        * database during a transaction (otherwise reads will not see the
//        * unflushed statements).
//        * <p>
//        * Note: if {@link #truthMaintenance} is enabled then this buffer is
//        * backed by a temporary store which accumulates the SPOs to be
//        * retracted. Otherwise it will write directly on the database each time
//        * it is flushed, including when it overflows.
//        * <p>
        synchronized protected StatementBuffer<Statement> getRetractionBuffer() {

            if (retractBuffer == null && truthMaintenance) {

                retractBuffer = new StatementBuffer<Statement>(tm
                        .newTempTripleStore(), database, bufferCapacity, queueCapacity);

                // FIXME bnodes : Must also track the reverse mapping [bnodes2].
                retractBuffer.setBNodeMap(bnodes);

//                    /*
//                     * Note: The SailConnection API will not use the
//                     * [retractBuffer] when truth maintenance is disabled, but
//                     * one is returned anyway so that callers may buffer
//                     * statements which they have on hand for retraction rather
//                     * as a complement to using triple patterns to describe the
//                     * statements to be retracted (which is how you do it with
//                     * the SailConnection API).
//                     */
//
//                    retractBuffer = new StatementBuffer<Statement>(database,
//                            bufferCapacity);
//
//                }
//
//                // FIXME bnodes : Must also track the reverse mapping [bnodes2].
//                retractBuffer.setBNodeMap(bnodes);
                
            }
            
            return retractBuffer;

        }

        /**
         * Any kind of connection (core constructor).
         * 
         * @param timestampOrTxId
         *            Either a commit time or a read/write transaction identifier.
         */
//        * @param lock
        protected BigdataSailConnection(final long timestampOrTxId, final Access access) throws DatasetNotFoundException {

            this.unisolated = TimestampUtility.isUnisolated(timestampOrTxId);
            
            this.readOnly = TimestampUtility.isReadOnly(timestampOrTxId);
            
            this.access = access;

            {
                final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager().getResourceLocator().locate(namespace, timestampOrTxId);
                
                if ( tripleStore == null ) {
    
                    throw new DatasetNotFoundException("namespace=" + namespace);
                    
                }
                    
                this.database = tripleStore;

                this.properties = database.getProperties();

            }

            {
                this.scaleOut = (getIndexManager() instanceof IBigdataFederation);
                
                checkProperties(properties);
                
                {
                    
                    this.quads = database.isQuads();
                    
                    // truthMaintenance
                    if (database.getAxioms() instanceof NoAxioms || quads || scaleOut) {
    
                        /*
                         * If there is no axioms model then inference is not enabled and
                         * truth maintenance is disabled automatically.
                         */
    
                        truthMaintenanceIsSupportable = false;
    
                    } else {
    
                        truthMaintenanceIsSupportable = Boolean.parseBoolean(properties.getProperty(
                                BigdataSail.Options.TRUTH_MAINTENANCE,
                                BigdataSail.Options.DEFAULT_TRUTH_MAINTENANCE));
    
                        if (log.isInfoEnabled())
                            log.info(BigdataSail.Options.TRUTH_MAINTENANCE + "="
                                    + truthMaintenanceIsSupportable);
    
                    }
                
                }
                
                // bufferCapacity
                {
                    
                    bufferCapacity = Integer.parseInt(properties.getProperty(
                            BigdataSail.Options.BUFFER_CAPACITY,
                            BigdataSail.Options.DEFAULT_BUFFER_CAPACITY));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.BUFFER_CAPACITY + "="
                                + bufferCapacity);

                }

                // queueCapacity
                {
                    
                    queueCapacity = Integer.parseInt(properties.getProperty(
                            BigdataSail.Options.QUEUE_CAPACITY,
                            BigdataSail.Options.DEFAULT_QUEUE_CAPACITY));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.QUEUE_CAPACITY + "="
                                + queueCapacity);

                }

                // queryTimeExpander
                if (scaleOut) {
                    
                    /*
                     * Note: Query time expanders are not supported in scale-out. They
                     * involve an expander pattern on the IAccessPath and that is not
                     * compatible with local reads against sharded indices.
                     */
                    queryTimeExpander = false;
                    
                } else {

                    queryTimeExpander = Boolean.parseBoolean(properties.getProperty(
                            BigdataSail.Options.QUERY_TIME_EXPANDER,
                            BigdataSail.Options.DEFAULT_QUERY_TIME_EXPANDER));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.QUERY_TIME_EXPANDER + "="
                                + queryTimeExpander);

                }
                
                // exactSize
                {
                    
                    exactSize = Boolean.parseBoolean(properties.getProperty(
                            BigdataSail.Options.EXACT_SIZE,
                            BigdataSail.Options.DEFAULT_EXACT_SIZE));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.EXACT_SIZE + "="
                                + exactSize);
                    
                }

                // allowAutoCommit
                {
                    
                    allowAutoCommit = Boolean.parseBoolean(properties.getProperty(
                            BigdataSail.Options.ALLOW_AUTO_COMMIT,
                            BigdataSail.Options.DEFAULT_ALLOW_AUTO_COMMIT));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.ALLOW_AUTO_COMMIT + "="
                                + allowAutoCommit);
                    
                }
                
                // isolatable
                { 
                    
                    isolatable = Boolean.parseBoolean(properties.getProperty(
                            BigdataSail.Options.ISOLATABLE_INDICES,
                            BigdataSail.Options.DEFAULT_ISOLATABLE_INDICES));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.ISOLATABLE_INDICES + "="
                                + isolatable);
                    
                }
                
                // allowAutoCommit
                {
                    
                    rejectQuadsInTripleMode = 
                        Boolean.parseBoolean(properties.getProperty(
                            BigdataSail.Options.REJECT_QUADS_IN_TRIPLE_MODE,
                            BigdataSail.Options.DEFAULT_REJECT_QUADS_IN_TRIPLE_MODE));

                    if (log.isInfoEnabled())
                        log.info(BigdataSail.Options.REJECT_QUADS_IN_TRIPLE_MODE + "="
                                + rejectQuadsInTripleMode);
                    
                }
                
            }
            
            /*
             * Notice whether or not truth maintenance is enabled.
             * 
             * Note: This property can be changed dynamically.
             */
            this.truthMaintenance = this.truthMaintenanceIsSupportable
                    && unisolated;

            /*
             * Permit registration of SPARQL UPDATE listeners iff the connection
             * is mutable.
             */
            listeners = readOnly ? null
                    : new CopyOnWriteArraySet<ISPARQLUpdateListener>();
            
        }

        /**
         * Unisolated connections.
         *
         * @param lock
       * @throws DatasetNotFoundException 
         */
        protected BigdataSailConnection(final Access access) throws DatasetNotFoundException {

            this(ITx.UNISOLATED, access);

            attach(ITx.UNISOLATED);

        }
        
        /**
         * Per-connection initialization. This method must be invoked after the
         * constructor so that the connection is otherwise initialized.
         * <p>
         * This method currently will expose a mutable connection to any
         * registered {@link CustomServiceFactory}.
         * 
         * @return The connection.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/754">
         *      Failure to setup SERVICE hook and changeLog for Unisolated and
         *      Read/Write connections </a>
         */
        protected BigdataSailConnection startConn() {

            if (txLog.isInfoEnabled())
                txLog.info("SAIL-START-CONN : conn=" + this);

            if (!isReadOnly()) {

                /*
                 * Give each registered ServiceFactory instance an opportunity to
                 * intercept the start of this connection.
                 */

                final Iterator<CustomServiceFactory> itr = ServiceRegistry
                        .getInstance().customServices();

                while (itr.hasNext()) {

                    final CustomServiceFactory f = itr.next();

                    try {

                        f.startConnection(this);

                    } catch (Throwable t) {

                        log.error(t, t);

                        continue;

                    }

                }

                if (this.changeLog != null) {

                    /*
                     * Note: The read/write tx will also do this after each
                     * commit since it always starts a newTx() when the last one
                     * is done.
                     * 
                     * TODO Is it possible to observe an event here? The
                     * [changeLog] will be [null] when the BigdataSailConnection
                     * constructor is done. startConn() is then invoked
                     * immediately on the connection. Therefore, [changeLog]
                     * SHOULD always be [null] here.
                     */

                    this.changeLog.transactionBegin();

                }

            }

            return this;
            
        }
        
        /**
         * When true, the RDFS closure will be maintained by the
         * {@link BigdataSailConnection} implementation (but not by methods that
         * go around the {@link BigdataSailConnection}).
         * 
         * @see #setTruthMaintenance(boolean)
         */
        public boolean getTruthMaintenance() {

            synchronized(this) {
            
                return truthMaintenance;
            
            }

        }

        public boolean isTruthMaintenanceConfigured() {
        	return truthMaintenanceIsSupportable && unisolated;
        }
        
        /**
         * Enable or suppress incremental truth maintenance for this
         * {@link BigdataSailConnection}. Truth maintenance MUST be configured
         * and this MUST be the {@link ITx#UNISOLATED} connection.
         * <p>
         * For connections that do support truth maintenance, this method is a
         * NOP if the truth maintenance flag would not be changed.
         * <p>
         * If the connection is changed either to or from a state in which it
         * will perform incremental truth maintenance, then any buffered
         * statements are first flushed to the backing database. This ensures
         * that truth maintenance is either performed or not performed (as
         * appropriate) on those statements that were already buffered and that
         * it will be (or will not be as appropriate) performed on those
         * assertions and retractions that follow.
         * <p>
         * Changing the state of this flag from <code>false</code> to
         * <code>true</code> will not force truth maintenance as any buffered
         * writes will first be {@link #flush() flushed}. However, once the flag
         * is <code>true</code>, any writes followed by a subsequent
         * {@link #flush()} WILL force truth maintenance. Therefore, it is
         * critically important that a coherent state is re-established for the
         * entailments BEFORE truth maintenance is re-enabled.
         * 
         * @param newValue
         *            The new value.
         * 
         * @return The old value.
         * 
         * @throws UnsupportedOperationException
         *             unless this is an UNISOLATED connection.
         * @throws UnsupportedOperationException
         *             unless the KB instance has been configured to support
         *             truth maintenance.
         *             
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/591 (Manage
         *      Truth Maintenance)
         */
        public boolean setTruthMaintenance(final boolean newValue) {

            if (!unisolated)
                throw new UnsupportedOperationException(
                        "Not an unisolated connection");

            if (!truthMaintenanceIsSupportable)
                throw new UnsupportedOperationException(
                        "Truth maintenance is not configured");

            /*
             * Note: Lock ensures both visibility and atomicity of the change in
             * the [truthMaintenance] flag state and the same lock is acquired
             * by flush() if we have to actually flush the assertion and/or
             * retraction buffers.
             */
            synchronized(this) {

                if (this.truthMaintenance != newValue) {

                    /*
                     * Since the state of the flag has changed, any buffered
                     * assertions or retractions MUST be synchronously flushed
                     * so they are processed either with or without incremental
                     * truth maintenance as appropriate for the current state of
                     * the truthMaintenance flag.
                     */

                    flush();

                }

                /*
                 * Now that any buffered assertions or retractions have been
                 * flushed, we go ahead and change the state of the flag.
                 */
                
                final boolean oldValue = this.truthMaintenance;

                this.truthMaintenance = newValue;

                return oldValue;

            }
            
        }
        
        /**
       * Attach to a new database view. Useful for transactions.
       * 
       * @param timestampOrTxId The timestamp or the transaction identifier for
       * the view of the database to be attached.
       * 
       * @throws DatasetNotFoundException
       *            if the namespace can not be located for that timestamp or txId.
       */
      protected synchronized void attach(final long timestampOrTxId)
            throws DatasetNotFoundException {

            BigdataSail.this.assertOpenSail();

            /* Note: This duplicates the locate already made in the core
             * BigdataSailConnection ctor, but this simplifies some patterns
             * in how attached is used (less error prone).
             */
            final AbstractTripleStore tripleStore = (AbstractTripleStore) getIndexManager().getResourceLocator().locate(namespace, timestampOrTxId);
          
            if ( tripleStore == null ) {

                throw new DatasetNotFoundException("namespace=" + namespace);
            
            }
            
            this.database = tripleStore;
            
//            readOnly = database.isReadOnly();            
         
            openConn = true;
            
            assertBuffer = null;
            
            retractBuffer = null;
            
            m_listeners = null;
            
            if (database.isReadOnly()) {
                
                if (log.isInfoEnabled())
                    log.info("Read-only view");
                
                tm = null;
                
                bnodes = null;

                bnodes2 = null;
                
            } else {

                if (log.isInfoEnabled())
                    log.info("Read-write view");

                /*
                 * Note: A ConcurrentHashMap is used in case the SAIL has
                 * concurrent threads which are processing RDF/XML statements
                 * and therefore could in principle require concurrent access to
                 * this map. ConcurrentHashMap is preferred to
                 * Collections#synchronizedMap() since the latter requires
                 * explicit synchronization during iterators, which breaks
                 * encapsulation.
                 */
                bnodes = new ConcurrentHashMap<String, BigdataBNode>();
                bnodes2 = new ConcurrentHashMap<IV, BigdataBNode>();
//                bnodes = Collections
//                        .synchronizedMap(new HashMap<String, BigdataBNodeImpl>(
//                                bufferCapacity));
                
                if (truthMaintenance) {

                    /*
                     * Setup the object that will be used to maintain the
                     * closure of the database.
                     */

                    tm = new TruthMaintenance(database.getInferenceEngine());
                    
                } else {
                    
                    tm = null;
                    
                }
                    
            }

        }
        
        /**
         * The implementation object.
         * 
         * @return The backing {@link AbstractTripleStore} or <code>null</code>
         * if an unisolated connection was obtained without requiring the
         * namespace to pre-exist.
         * 
         *  @see BLZG-2023, BLZG-2041
         */
        public AbstractTripleStore getTripleStore() {
            
            return database;
            
        }

        /**
         * When true, SAIL will compute entailments at query time that were excluded
         * from forward closure.
         * 
         * @see Options#QUERY_TIME_EXPANDER
         */
        public final boolean isQueryTimeExpander() {

            return queryTimeExpander;
            
        }

        /**
         * When <code>true</code>, the connection does not permit mutation.
         */
        public final boolean isReadOnly() {
            
            return readOnly;
            
        }

        /**
         * Return <code>true</code> if this is the {@link ITx#UNISOLATED}
         * connection.
         */
        public final boolean isUnisolated() {

            return unisolated;
            
        }

        /**
         * Used by the RepositoryConnection to determine whether or not to allow
         * auto-commits.
         * 
         * @see Options#ALLOW_AUTO_COMMIT
         */
        public boolean getAllowAutoCommit() {
            
            return allowAutoCommit;
            
        }
        
        /*
         * SailConnectionListener support.
         * 
         * FIXME There is _ALSO_ a SailChangedListener that only has booleans
         * for added and removed and which reports the Sail instance that was
         * modified (outside of the SailConnection). I need to restore the old
         * logic that handled this sort of thing and carefully verify that the
         * boolean flags are being set and cleared as appropriate.
         * 
         * It used to be that those notices were deferred until the next commit.
         * Does that still apply for Sesame 2.x?
         * 
         * Note: SailBase appears to handle the SailChangedListener api.
         */

        /**
         * Vector of transient {@link SailConnectionListener}s registered with
         * this SAIL. The field is set back to <code>null</code> whenever
         * there are no more listeners.
         */
        private Vector<SailConnectionListener> m_listeners = null;
        
        /**
         * Note: This method is <strong>strongly discouraged</strong> as it
         * imposes an extremely high burden on the database requiring the
         * materialization at the client of every statement to be added or
         * removed from the database in the scope of this {@link SailConnection}.
         * Further, while the client is only notified for explicit statements
         * added or removed, it is possible that a statement remains entailed in
         * the database regardless of its removal.
         */
        @Override
        synchronized public void addConnectionListener(
                final SailConnectionListener listener) {

            if( m_listeners == null ) {
                
                m_listeners = new Vector<SailConnectionListener>();
                
                m_listeners.add( listener );
                
            } else {
                
                if( m_listeners.contains( listener ) ) {
                    
                    throw new IllegalStateException
                        ( "Already registered: listener="+listener
                          );
                    
                }
                
                m_listeners.add( listener );
                
            }

            log.warn("Adding SailConnectionListener - performance will suffer!");
            
        }

        @Override
        synchronized public void removeConnectionListener(
                final SailConnectionListener listener) {

            if( m_listeners == null ) {
                
                throw new IllegalStateException
                    ( "Not registered: listener="+listener
                      );
                
            }
            
            if( ! m_listeners.remove( listener ) ) {
                
                throw new IllegalStateException
                    ( "Not registered: listener="+listener
                      );
                
            }
            
            if(m_listeners.isEmpty()) {
                
                /*
                 * Note: Since the price of notifying listeners is so high the
                 * listeners vector is explicitly set to null so that we can
                 * test whether or not listeners need to be notified simply by
                 * testing m_listeners != null.
                 */
                
                m_listeners = null;
                
            }

        }

        /**
         * Notifies {@link SailConnectionListener}s if one or more statements have
         * been added to or removed from the repository using the SAIL methods:
         * <ul>
         * 
         * <li> {@link #addStatement(Resource, URI, Value)}
         * <li> {@link #removeStatements(Resource, URI, Value)}
         * <li> {@link #clearRepository()}
         * </ul>
         * 
         * @todo javadoc update.
         */
        synchronized protected void fireSailChangedEvent(final boolean added,
                final Statement stmt) {

            /*
             * Make sure that you test on m_listeners BEFORE hand so that you
             * can avoid the work required to materialize the statements if
             * there are no listeners registered! (The burden is especially high
             * when removing statements).
             */

            if( m_listeners == null ) return;

            /*
             * FIXME Since we are calling the listener for every single
             * statement we really need to eagerly materialize the array of
             * listeners whenever the listeners are added or removed so that we
             * can efficiently process that array here. as long as this method
             * and the add/remove methods are all synchronized then the result
             * will be coherent.
             */
            
            final SailConnectionListener[] listeners = (SailConnectionListener[]) m_listeners
                    .toArray(new SailConnectionListener[] {});

            for (int i = 0; i < listeners.length; i++) {
                
                final SailConnectionListener l = listeners[ i ];
                
                if(added) {
                    
                    l.statementAdded(stmt);
                    
                } else {
                
                    l.statementRemoved(stmt);
                    
                }
                
            }
            
        }
        
        /*
         * Namespace map CRUD.
         * 
         * @todo the namespace API suggests a bi-directional map from namespace
         * to prefix and from prefix to namespace for efficiency. only one
         * direction is actually backed by a Map object - the other uses a scan.
         */
        
        @Override
        public void setNamespace(final String prefix, final String namespace)
                throws SailException {

            if (prefix == null) // per Sesame TCK behavior
                throw new NullPointerException();

            if (namespace == null) // per Sesame TCK behavior
                throw new NullPointerException();

            assertWritableConn();

//            database.addNamespace(namespace,prefix);
            namespaces.put(prefix, namespace);
            
        }

        @Override
        public String getNamespace(final String prefix) {
            
            if (prefix == null) // per Sesame TCK behavior
                throw new NullPointerException();

//            return database.getNamespace(prefix);
            return namespaces.get(prefix);
            
        }
        
        @Override
        public void removeNamespace(final String prefix) {
            
            if (prefix == null) // per Sesame TCK behavior
                throw new NullPointerException();

//            database.removeNamespace(prefix);
            namespaces.remove(prefix);
            
        }

        @Override
        public void clearNamespaces() {
            
//            database.clearNamespaces();
            namespaces.clear();
            
        }
        
        @Override
        public NamespaceIterator getNamespaces() {

            /*
             * Note: You do NOT need to flush the buffer since this does not read
             * statements.
             */
            
            /*
             * Note: the namespaces map on AbstractTripleStore is from
             * namespace to prefix.  The one on the SAIL is from prefix to
             * namespace.  So if you ever change this back to use the database
             * namespaces map, you must also change the NamespaceIterator back
             * to reflect that difference in key-value.
             */
//            return new NamespaceIterator(database.getNamespaces().entrySet().iterator());
            return new NamespaceIterator(namespaces.entrySet().iterator());
            
        }

        /**
         * Namespace iterator.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         */
        private class NamespaceIterator implements CloseableIteration<Namespace,SailException> {

            private final Iterator<Map.Entry<String/*prefix*/,String/*namespace*/>> src;
            
            public NamespaceIterator(Iterator<Map.Entry<String/*prefix*/,String/*namespace*/>> src) {
                
                assert src != null;
                
                this.src = src;
                
            }
            
            public boolean hasNext() {
                
                return src.hasNext();
                
            }

            public Namespace next() {
             
                final Map.Entry<String/*prefix*/,String/*namespace*/> current = src.next();
                
                return new NamespaceImpl(current.getKey(), current.getValue());
                
            }

            public void remove() {
                
                // @todo implement?
                throw new UnsupportedOperationException();
                
            }
            
            public void close() {
                
                // NOP.
                
            }
            
        }
        
        /*
         * Statement CRUD
         */

        /**
         * Sesame has a concept of a "null" graph. Any statement inserted whose
         * context position is NOT bound will be inserted into the "null" graph.
         * Statements inserted into the "null" graph are visible from the SPARQL
         * default graph, when no data set is specified (in this case all
         * statements in all contexts are visible).
         * 
         * @see BigdataSail#NULL_GRAPH
         */
        @Override
        public void addStatement(final Resource s, final URI p, final Value o,
                final Resource... contexts) throws SailException {

            if (log.isDebugEnabled())
                log.debug("s=" + s + ", p=" + p + ", o=" + o + ", contexts="
                        + Arrays.toString(contexts));

            OpenRDFUtil.verifyContextNotNull(contexts);

            if (contexts.length == 0) {

                /*
                 * Operate on just the nullGraph.
                 * 
                 * Note: When no contexts are specified, the intention for
                 * addStatements() is that a statement with no associated
                 * context is added to the store.
                 */

                addStatement(s, p, o, (Resource) null/* c */);

            }

            if (contexts.length == 1 && contexts[0] == null) {

                // Operate on just the nullGraph.

                addStatement(s, p, o, (Resource) null/* c */);

            }

            for (Resource c : contexts) {

                addStatement(s, p, o, c);

            }
            
        }

        private synchronized void addStatement(final Resource s, final URI p, final Value o,
                final Resource c) throws SailException {

            if(!isOpen()) {

                /*
                 * Note: While this exception is not declared by the javadoc,
                 * it is required by the Sesame TCK.
                 */
                throw new IllegalStateException();
                
            }
            
            assertWritableConn();

            // flush any pending retractions first!
            flushStatementBuffers(false/* flushAssertBuffer */, true/* flushRetractBuffer */);

            // in case quads shall be rejected in triples mode by configuration
            // and we encounter such a quad, raise an error
            if (rejectQuadsInTripleMode && !quads && c!=null) {
               throw new SailException(
                  "Quad in triples mode is not allowed by configuration "
                  + "(set parameter rejectQuadsInTripleMode to false to "
                  + "allow for quads data)");
            }
            
            /*
             * Buffer the assertion.
             * 
             * Note: If [c] is null and we are in quads mode, then we set
             * [c==NULL_GRAPH]. Otherwise we leave [c] alone. For the triple
             * store mode [c] will be null. For the provenance mode, [c] will
             * set to a statement identifier (SID) when the statement(s) are
             * flushed to the database.
             */
            getAssertionBuffer().add(s, p, o,
                    c == null && quads ? NULL_GRAPH : c);

            if (m_listeners != null) {

                // Notify listener(s).
                
                fireSailChangedEvent(true, new ContextStatementImpl(s, p, o, c));
                
            }

        }

        @Override
        public synchronized void clear(final Resource... contexts) throws SailException {

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("contexts=" + Arrays.toString(contexts));
            
            assertWritableConn();

            // discard any pending writes.
            clearBuffers();
            
            /*
             * @todo if listeners are registered then we need to materialize
             * everything that is going to be removed....
             */
            
            if (contexts.length == 0) {

                /*
                 * Operates on all contexts.
                 * 
                 * Note: This deliberately removes the statements from each
                 * access path rather than doing a drop/add on the triple/quad
                 * store instance.
                 */
                
                database.getAccessPath((Resource)null/* s */, (URI)null/* p */, 
                        (Value)null/* o */, (Resource) null/* c */).removeAll();

                return;
                
            }

            if (contexts.length == 1 && contexts[0] == null) {

                /*
                 * Operate on just the nullGraph, or on the sole graph if not in
                 * quads mode.
                 */

                database.getAccessPath(null/* s */, null/* p */, null/* o */,
                        quads ? NULL_GRAPH : null)
                        .removeAll();

                return;

            }

            // FIXME parallelize this in chunks as per getStatements()
            long size = 0;
            for (Resource c : contexts) {

                size += database.getAccessPath(null/* s */, null/* p */,
                        null/* o */, (c == null && quads) ? NULL_GRAPH : c
                        ).removeAll();

            }

            return;
            
        }
        
        /**
         * Clears all buffered statements in the {@link #assertBuffer} and in
         * the optional {@link #retractBuffer}. If {@link #truthMaintenance} is
         * enabled, then the backing tempStores are also closed and deleted. The
         * buffer references are set to <code>null</code> and the buffers must
         * be re-allocated on demand.
         */
        private void clearBuffers() {

            if(assertBuffer != null) {
                
                // discard all buffered data.
                assertBuffer.reset();
                
                if(truthMaintenance) {
                    
                    // discard the temp store that buffers assertions.
                    assertBuffer.getStatementStore().close();
                    
                    // must be re-allocated on demand.
                    assertBuffer = null;
                    
                }
                
            }

            if (retractBuffer != null) {

                // discard all buffered data.
                retractBuffer.reset();

                if (truthMaintenance) {

                    // discard the temp store that buffers retractions.
                    retractBuffer.getStatementStore().close();

                    // must be re-allocated on demand.
                    retractBuffer = null;

                }

            }
            
            if(bnodes!=null)
            bnodes.clear();
            
            if(bnodes2!=null)
            bnodes2.clear();

        }

        /**
         * Count the statements in the specified contexts.  Returns an exact
         * size or an upper bound depending on the value of {@link
         * Options#EXACT_SIZE}.  Exact size is an extremely expensive operation,
         * which we turn off by default.  In default mode, an upper bound is
         * given for the total number of statements in the database, explicit
         * and inferred.  In exact size mode, the entire index will be visited
         * and materialized and each explicit statement will be counted.
         * <p>
         * {@inheritDoc}
         * 
         * @see Options#EXACT_SIZE
         */
        @Override
        public long size(final Resource... contexts) throws SailException {
            
            return size(exactSize, contexts);
            
        }
        
        /**
         * Note: This method is quite expensive since it must materialize all
         * statement in either the database or in the specified context(s) and
         * then filter for the explicit statements in order to obtain an exact
         * count. See {@link AbstractTripleStore#getStatementCount()} or
         * {@link IAccessPath#rangeCount()} for efficient methods for reporting
         * on the #of statements in the database or within a specific context.
         * 
         * @see Options#EXACT_SIZE
         */
        public long exactSize(final Resource... contexts) throws SailException {
            
            return size(true, contexts);
            
        }
        
        private synchronized long size(final boolean exactSize,
                final Resource... contexts) throws SailException {

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("contexts=" + Arrays.toString(contexts));

            if (exactSize) {
            
                if (contexts.length == 0 ) {
                    
                    // Operates on all contexts.
                    
                    return database.getExplicitStatementCount(null/* c */);
    
                }
    
                if (contexts.length == 1 && contexts[0] == null) {
    
                    /*
                     * Operate on just the nullGraph (or on the sole graph if not in
                     * quads mode).
                     */
    
                    return database.getExplicitStatementCount(quads ? NULL_GRAPH
                            : null/* c */);
    
                }
    
                // FIXME parallelize this in chunks as per getStatements()
                long size = 0;
    
                for (Resource c : contexts) {
    
                    size += database.getExplicitStatementCount(
                            (c == null && quads) ? NULL_GRAPH : c);
    
                }

                return size;
                
            } else { //exactSize == false
                
                if (contexts.length == 0 ) {
                    
                    // Operates on all contexts.
                    
                    return database.getStatementCount(null/* c */, false/*exact*/);
    
                } else if (contexts.length == 1 && contexts[0] == null) {
    
                    /*
                     * Operate on just the nullGraph (or on the sole graph if not in
                     * quads mode).
                     */
    
                    return database.getStatementCount(quads ? NULL_GRAPH
                            : null/* c */, false/*exact*/);
    
                } else {
        
                    // FIXME parallelize this in chunks as per getStatements()
                    long size = 0;
        
                    for (Resource c : contexts) {
        
                        size += database.getStatementCount(
                                (c == null && quads) ? NULL_GRAPH : c, 
                                        false/* exact */);
        
                    }
    
                    return size;
                
                }
                
            }
            
        }

        @Override
        public void removeStatements(final Resource s, final URI p,
                final Value o, final Resource... contexts) throws SailException {

            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("s=" + s + ", p=" + p + ", o=" + o + ", contexts="
                        + Arrays.toString(contexts));

            if (contexts.length == 0 ) {
                
                // Operates on all contexts.
                
                removeStatements(s, p, o, (Resource) null/* c */);

            } else if (contexts.length == 1 && contexts[0] == null) {

                /*
                 * Operate on just the nullGraph, or on the sole graph if not in
                 * quads mode.
                 */

                removeStatements(s, p, o, quads ? NULL_GRAPH : null/* c */);

            } else {

                // FIXME parallelize this in chunks as per getStatements()
                for (Resource c : contexts) {

                    removeStatements(s, p, o, (c == null && quads) ? NULL_GRAPH
                            : c);

                }

            }
            
        }

        /**
         * Note: The CONTEXT is ignored when in statementIdentifier mode!
         */
        public synchronized int removeStatements(final SPOPredicate pred) 
                throws SailException {
            
            assertWritableConn();

            flushStatementBuffers(true/* flushAssertBuffer */, false/* flushRetractBuffer */);

            if (m_listeners != null) {

                /*
                 * FIXME to support the SailConnectionListener we need to
                 * pre-materialize the explicit statements that are to be
                 * deleted and then notify the listener for each such explicit
                 * statement. Since that is a lot of work, make sure that we do
                 * not generate notices unless there are registered listeners!
                 */

                throw new UnsupportedOperationException();
                
            }

            // #of explicit statements removed.
            long n = 0;

            if (getTruthMaintenance()) {

                /*
                 * Since we are doing truth maintenance we need to copy the
                 * matching "explicit" statements into a temporary store rather
                 * than deleting them directly. This uses the internal API to
                 * copy the statements to the temporary store without
                 * materializing them as Sesame Statement objects.
                 */

                /*
                 * Obtain a chunked iterator using the triple pattern that
                 * visits only the explicit statements.
                 */
                final IChunkedOrderedIterator<ISPO> itr = database.getSPORelation()
                        .getAccessPath(pred.addIndexLocalFilter(
                        ElementFilter.newInstance(ExplicitSPOFilter.INSTANCE)))
                        .iterator();

                // The tempStore absorbing retractions.
                final AbstractTripleStore tempStore = getRetractionBuffer()
                        .getStatementStore();

                // Copy explicit statements to tempStore.
                n = tempStore.addStatements(tempStore, true/* copyOnly */,
                        itr, null/* filter */);

                /*
                 * Nothing more happens until the commit or incremental write
                 * flushes the retraction buffer and runs TM.
                 */
                
            } else {

                /*
                 * Since we are not doing truth maintenance, just remove the
                 * statements from the database (synchronous, batch api, not
                 * buffered).
                 */
                
                final IAccessPath<ISPO> ap = database.getSPORelation().getAccessPath(pred);
                
                if (changeLog == null) {
                    
                    n = ap.removeAll();
                    
                } else {
                
                    final IChunkedOrderedIterator<ISPO> itr = 
                        database.computeClosureForStatementIdentifiers(
                                ap.iterator());
                    
                    // no need to compute closure for sids since we just did it
                    n = StatementWriter.removeStatements(database, itr, 
                            false/* computeClosureForStatementIdentifiers */,
                            changeLog);
                    
                }

            }

            // avoid overflow.
            return (int) Math.min(Integer.MAX_VALUE, n);
            
        }
            
        public synchronized int removeStatements(final SPOPredicate[] preds,
                final int numPreds) throws SailException {
            
            assertWritableConn();

            flushStatementBuffers(true/* flushAssertBuffer */, false/* flushRetractBuffer */);

            if (m_listeners != null) {

                /*
                 * FIXME to support the SailConnectionListener we need to
                 * pre-materialize the explicit statements that are to be
                 * deleted and then notify the listener for each such explicit
                 * statement. Since that is a lot of work, make sure that we do
                 * not generate notices unless there are registered listeners!
                 */

                throw new UnsupportedOperationException();
                
            }

            // #of explicit statements removed.
            long n = 0;

            if (getTruthMaintenance()) {

                /*
                 * Since we are doing truth maintenance we need to copy the
                 * matching "explicit" statements into a temporary store rather
                 * than deleting them directly. This uses the internal API to
                 * copy the statements to the temporary store without
                 * materializing them as Sesame Statement objects.
                 */

                /*
                 * Obtain a chunked iterator using the triple pattern that
                 * visits only the explicit statements.
                 */
                final IChunkedOrderedIterator<ISPO> itr = 
                        combineAndIterate(preds, numPreds);

                // The tempStore absorbing retractions.
                final AbstractTripleStore tempStore = getRetractionBuffer()
                        .getStatementStore();

                // Copy explicit statements to tempStore.
                n = tempStore.addStatements(tempStore, true/* copyOnly */,
                        itr, null/* filter */);

                /*
                 * Nothing more happens until the commit or incremental write
                 * flushes the retraction buffer and runs TM.
                 */
                
            } else {

                /*
                 * Since we are not doing truth maintenance, just remove the
                 * statements from the database (synchronous, batch api, not
                 * buffered).
                 */
                
                if (changeLog == null) {

                    for (int i = 0; i < numPreds; i++) {
                    
                        final SPOPredicate pred = preds[i];
                        
                        final IAccessPath<ISPO> ap = database.getSPORelation().getAccessPath(pred);
                        
                        n = ap.removeAll();
                    }
                    
                    
                } else {
                
                    final IChunkedOrderedIterator<ISPO> itr = 
                        database.computeClosureForStatementIdentifiers(
                                combineAndIterate(preds, numPreds));
                    
                    // no need to compute closure for sids since we just did it
                    n = StatementWriter.removeStatements(database, itr, 
                            false/* computeClosureForStatementIdentifiers */,
                            changeLog);
                    
                }

            }

            // avoid overflow.
            return (int) Math.min(Integer.MAX_VALUE, n);
            
        }
        
        private IChunkedOrderedIterator<ISPO> combineAndIterate(
                final SPOPredicate[] preds, final int numPreds) {
            
            /*
             * Sort preds?
             */
            
            final Striterator sitr = new Striterator(Collections.emptyIterator());
            
            int combined = 0;
            for (int i = 0; i < numPreds; i++) {
                
                final SPOPredicate pred = preds[i];
                
                final IAccessPath<ISPO> ap = database.getSPORelation().getAccessPath(pred);
                
                final IChunkedOrderedIterator<ISPO> it = ap.iterator();
                if (it != null && it.hasNext()) {
                    sitr.append(it);
                    combined++;
                }
            }
            
            if (log.isDebugEnabled()) {
                log.debug("combined " + combined + " APs...");
            }
            
            return new ChunkedWrappedIterator<ISPO>(sitr);
            
        }
            
        /**
         * Note: The CONTEXT is ignored when in statementIdentifier mode!
         */
        public synchronized int removeStatements(final Resource s, final URI p,
                final Value o, final Resource c) throws SailException {
            
            assertWritableConn();

            flushStatementBuffers(true/* flushAssertBuffer */, false/* flushRetractBuffer */);

            if (m_listeners != null) {

                /*
                 * FIXME to support the SailConnectionListener we need to
                 * pre-materialize the explicit statements that are to be
                 * deleted and then notify the listener for each such explicit
                 * statement. Since that is a lot of work, make sure that we do
                 * not generate notices unless there are registered listeners!
                 */

                throw new UnsupportedOperationException();
                
            }

            // #of explicit statements removed.
            long n = 0;

            if (getTruthMaintenance()) {

                /*
                 * Since we are doing truth maintenance we need to copy the
                 * matching "explicit" statements into a temporary store rather
                 * than deleting them directly. This uses the internal API to
                 * copy the statements to the temporary store without
                 * materializing them as Sesame Statement objects.
                 */

                /*
                 * Obtain a chunked iterator using the triple pattern that
                 * visits only the explicit statements.
                 */
                final IChunkedOrderedIterator<ISPO> itr = database
                        .getAccessPath(s, p, o, ExplicitSPOFilter.INSTANCE)
                        .iterator();

                // The tempStore absorbing retractions.
                final AbstractTripleStore tempStore = getRetractionBuffer()
                        .getStatementStore();

                // Copy explicit statements to tempStore.
                n = tempStore.addStatements(tempStore, true/* copyOnly */,
                        itr, null/* filter */);

                /*
                 * Nothing more happens until the commit or incremental write
                 * flushes the retraction buffer and runs TM.
                 */
                
            } else {

                /*
                 * Since we are not doing truth maintenance, just remove the
                 * statements from the database (synchronous, batch api, not
                 * buffered).
                 */
                
                if (changeLog == null) {
                    
                    n = database.removeStatements(s, p, o, c);

                } else {
                
                    final IChunkedOrderedIterator<ISPO> itr = 
                        database.computeClosureForStatementIdentifiers(
                                database.getAccessPath(s, p, o, c).iterator());
                    
                    // no need to compute closure for sids since we just did it
                    n = StatementWriter.removeStatements(database, itr, 
                            false/* computeClosureForStatementIdentifiers */,
                            changeLog);
                    
//                    final IAccessPath<ISPO> ap = 
//                        database.getAccessPath(s, p, o, c);
//    
//                    final IChunkedOrderedIterator<ISPO> itr = ap.iterator();
//                    
//                    if (itr.hasNext()) {
//                        
//                        final BigdataStatementIteratorImpl itr2 = 
//                            new BigdataStatementIteratorImpl(database, bnodes2, itr)
//                                .start(database.getExecutorService()); 
//                        
//                        final BigdataStatement[] stmts = 
//                            new BigdataStatement[database.getChunkCapacity()];
//                        
//                        int i = 0;
//                        while (i < stmts.length && itr2.hasNext()) {
//                            stmts[i++] = itr2.next();
//                            if (i == stmts.length) {
//                                // process stmts[]
//                                n += removeAndNotify(stmts, i);
//                                i = 0;
//                            }
//                        }
//                        if (i > 0) {
//                            n += removeAndNotify(stmts, i);
//                        }
//                        
//                    }
                    
                }

            }

            // avoid overflow.
            return (int) Math.min(Integer.MAX_VALUE, n);
            
        }
        
        /**
         * Note: The CONTEXT is ignored when in statementIdentifier mode!
         */
        public synchronized int removeStatements(final ISPO[] stmts) 
                throws SailException {
            
            return removeStatements(stmts, stmts.length);
            
        }
            
        /**
         * Note: The CONTEXT is ignored when in statementIdentifier mode!
         */
        public synchronized int removeStatements(final ISPO[] stmts, 
                final int numStmts) throws SailException {
            
            assertWritableConn();

            flushStatementBuffers(true/* flushAssertBuffer */, false/* flushRetractBuffer */);

            if (m_listeners != null) {

                /*
                 * FIXME to support the SailConnectionListener we need to
                 * pre-materialize the explicit statements that are to be
                 * deleted and then notify the listener for each such explicit
                 * statement. Since that is a lot of work, make sure that we do
                 * not generate notices unless there are registered listeners!
                 */

                throw new UnsupportedOperationException();
                
            }

            // #of explicit statements removed.
            long n = 0;

            if (getTruthMaintenance()) {

                /*
                 * Since we are doing truth maintenance we need to copy the
                 * matching "explicit" statements into a temporary store rather
                 * than deleting them directly. This uses the internal API to
                 * copy the statements to the temporary store without
                 * materializing them as Sesame Statement objects.
                 */

                /*
                 * Obtain a chunked iterator using the triple pattern that
                 * visits only the explicit statements.
                 */
                final IChunkedOrderedIterator<ISPO> itr = 
                		new ChunkedArrayIterator<ISPO>(numStmts, stmts);

                // The tempStore absorbing retractions.
                final AbstractTripleStore tempStore = getRetractionBuffer()
                        .getStatementStore();

                // Copy explicit statements to tempStore.
                n = tempStore.addStatements(tempStore, true/* copyOnly */,
                        itr, null/* filter */);

                /*
                 * Nothing more happens until the commit or incremental write
                 * flushes the retraction buffer and runs TM.
                 */
                
            } else {

                /*
                 * Since we are not doing truth maintenance, just remove the
                 * statements from the database (synchronous, batch api, not
                 * buffered).
                 */
                
                if (changeLog == null) {
                    
                    n = database.removeStatements(stmts, numStmts);
                    
                } else {
                
                    final IChunkedOrderedIterator<ISPO> itr = 
                        database.computeClosureForStatementIdentifiers(
                        		new ChunkedArrayIterator<ISPO>(numStmts, stmts));
                    
                    // no need to compute closure for sids since we just did it
                    n = StatementWriter.removeStatements(database, itr, 
                            false/* computeClosureForStatementIdentifiers */,
                            changeLog);
                    
//                    final IAccessPath<ISPO> ap = 
//                        database.getAccessPath(s, p, o, c);
//    
//                    final IChunkedOrderedIterator<ISPO> itr = ap.iterator();
//                    
//                    if (itr.hasNext()) {
//                        
//                        final BigdataStatementIteratorImpl itr2 = 
//                            new BigdataStatementIteratorImpl(database, bnodes2, itr)
//                                .start(database.getExecutorService()); 
//                        
//                        final BigdataStatement[] stmts = 
//                            new BigdataStatement[database.getChunkCapacity()];
//                        
//                        int i = 0;
//                        while (i < stmts.length && itr2.hasNext()) {
//                            stmts[i++] = itr2.next();
//                            if (i == stmts.length) {
//                                // process stmts[]
//                                n += removeAndNotify(stmts, i);
//                                i = 0;
//                            }
//                        }
//                        if (i > 0) {
//                            n += removeAndNotify(stmts, i);
//                        }
//                        
//                    }
                    
                }

            }

            // avoid overflow.
            return (int) Math.min(Integer.MAX_VALUE, n);
            
        }
        
//        private long removeAndNotify(final BigdataStatement[] stmts, final int numStmts) {
//            
//            final SPO[] tmp = new SPO[numStmts];
//
//            for (int i = 0; i < tmp.length; i++) {
//
//                final BigdataStatement stmt = stmts[i];
//                
//                /*
//                 * Note: context position is not passed when statement identifiers
//                 * are in use since the statement identifier is assigned based on
//                 * the {s,p,o} triple.
//                 */
//
//                final SPO spo = new SPO(stmt);
//
//                if (log.isDebugEnabled())
//                    log.debug("adding: " + stmt.toString() + " (" + spo + ")");
//                
//                if(!spo.isFullyBound()) {
//                    
//                    throw new AssertionError("Not fully bound? : " + spo);
//                    
//                }
//                
//                tmp[i] = spo;
//
//            }
//            
//            /*
//             * Note: When handling statement identifiers, we clone tmp[] to avoid a
//             * side-effect on its order so that we can unify the assigned statement
//             * identifiers below.
//             * 
//             * Note: In order to report back the [ISPO#isModified()] flag, we also
//             * need to clone tmp[] to avoid a side effect on its order. Therefore we
//             * now always clone tmp[].
//             */
////            final long nwritten = writeSPOs(sids ? tmp.clone() : tmp, numStmts);
//            final long nwritten = database.removeStatements(tmp.clone(), numStmts);
//
//            // Copy the state of the isModified() flag
//            {
//
//                for (int i = 0; i < numStmts; i++) {
//
//                    if (tmp[i].isModified()) {
//
//                        stmts[i].setModified(true);
//                        
//                        changeLog.changeEvent(
//                                new ChangeRecord(stmts[i], ChangeAction.REMOVED));
//
//                    }
//                    
//                }
//                
//            }
//            
//            return nwritten;
//            
//        }

        @Override
        public synchronized CloseableIteration<? extends Resource, SailException> getContextIDs()
                throws SailException {
            
            if (!database.isQuads())
                throw new UnsupportedOperationException();

            if(database.getSPORelation().oneAccessPath) {

                /*
                 * The necessary index does not exist (we would have to scan
                 * everything and filter to obtain a distinct set).
                 */
 
                throw new UnsupportedOperationException();

            }

            // flush before query.
            flushStatementBuffers(true/* assertions */, true/* retractions */);
            
            // Visit the distinct term identifiers for the context position.
            @SuppressWarnings("rawtypes")
            final IChunkedIterator<IV> itr = database.getSPORelation()
                    .distinctTermScan(SPOKeyOrder.CSPO);

            // Resolve the term identifiers to terms efficiently during iteration.
            final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                    database, itr);
            
//            return new CloseableIteration</*? extends*/ Resource, SailException>() {
            return new CloseableIteration<Resource, SailException>() {
                private Resource next = null;
                private boolean open = true;

                @Override
                public void close() throws SailException {
                    if (open) {
                        open = false;
                        next = null;
                        itr2.close();
                    }
                }

                @Override
                public boolean hasNext() throws SailException {
                    if(open && _hasNext())
                        return true;
                    close();
                    return false;
                }

                private boolean _hasNext() throws SailException {
                    if (next != null)
                        return true;
                    while (itr2.hasNext()) {
                        next = (Resource) itr2.next();
                        if (next.equals(BD.NULL_GRAPH)) {
                            next = null;
                            continue;
                        }
                        return true;
                    }
                    return false;
                }

                @Override
                public Resource next() throws SailException {
                    if (next == null)
                        throw new SailException();
                    final Resource tmp = next;
                    next = null;
                    return tmp;
                }

                @Override
                public void remove() throws SailException {
                    /*
                     * Note: remove is not supported. The semantics would
                     * require that we removed all statements for the last
                     * visited context.
                     */
                    throw new UnsupportedOperationException();
//                    itr2.remove();
                }
            };

//            return (CloseableIteration<? extends Resource, SailException>) itr2;

        }

        /*
         * transaction support.
         */
        
        /**
         * Note: The semantics depend on the {@link Options#STORE_CLASS}. See
         * {@link ITripleStore#abort()}.
         */
        @Override
        public synchronized void rollback() throws SailException {
        	// @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
        	boolean success = false;
        	try {
	            assertWritableConn();
	
	            if (txLog.isInfoEnabled())
	                txLog.info("SAIL-ROLLBACK-CONN: " + this);

	            if(changeLog != null) {
	                
	                // Note: Per the API, invoke when preparing to abort() as well as when preparing to commit().
	                // TODO Unit test for conformance with this behavior.  Also, might be nice to pass boolean indicating whether
	                // will abort() or attempt to commit.
	                changeLog.transactionPrepare();
	                
	            }
	
	            // discard buffered assertions and/or retractions.
	            clearBuffers();
	
	            // discard the write set.
	            database.abort();
	            
	            if (changeLog != null) {
	                
	                changeLog.transactionAborted();
	                
	            }
	            
	            dirty = false;
	            
	            success = true; // mark successful rollback
        	} finally { // @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
        		rollbackRequired.set(!success);
        	}
            
        }

        /**
         * Set to <code>true</code> if we {@link #flush()} either of the
         * {@link StatementBuffer}s. The flag reflects whether or not the
         * buffered writes were propagated to the underlying indices. For some
         * database modes those writes will be buffered by the indices and then
         * incrementally flushed through to the disk. For others (a federation)
         * the writes are shard wise ACID.
         */
        public synchronized boolean isDirty() {

            return dirty;

        }
        
        /**
         * Commit the write set.
         * <p>
         * Note: The semantics depend on the {@link Options#STORE_CLASS}. See
         * {@link AbstractTripleStore#commit()}.
         * 
         * @return The timestamp associated with the commit point. This will be
         *         <code>0L</code> if the write set was empty such that nothing
         *         was committed.
         */
        public synchronized long commit2() throws SailException {
        	
        	/**
        	 * If a call to rollback does not complete cleanly, then rollbackRequired will be set and no updates will be allowed.
        	 * @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
        	 */
        	if (rollbackRequired.get())
        		throw new IllegalStateException("Rollback required");

            assertWritableConn();

            /*
             * Flush any pending writes.
             * 
             * Note: This must be done before you compute the closure so that the
             * pending writes will be read by the inference engine when it computes
             * the closure.
             */
            
            flushStatementBuffers(true/* assertions */, true/* retractions */);

            if (changeLog != null) {
             
                changeLog.transactionPrepare();
                
            }

            final long commitTime = database.commit();
            
            if (txLog.isInfoEnabled())
                txLog.info("SAIL-COMMIT-CONN : commitTime=" + commitTime
                        + ", conn=" + this);

            if (changeLog != null) {
                
                changeLog.transactionCommited(commitTime);
                
            }

            dirty = false;
            
            return commitTime;
            
        }

        /**
         * Commit the write set.
         * <p>
         * Note: The semantics depend on the {@link Options#STORE_CLASS}.  See
         * {@link AbstractTripleStore#commit()}.
         */
        @Override
        final public synchronized void commit() throws SailException {
            
            commit2();
            
        }

        @Override
        final public boolean isOpen() throws SailException {

            return openConn;
            
        }

        /**
         * {@inheritDoc}
         * <p>
         * Note: If writes have been applied through the
         * {@link BigdataSailConnection} and neither {@link #commit()} nor
         * {@link #rollback()} has been invoked, then an implicit
         * {@link #rollback()} WILL be performed.
         * <p>
         * Note: This logic to invoke the implicit {@link #rollback()} WILL NOT
         * notice whether changes were applied at the
         * {@link AbstractTripleStore} layer. It bases its decision SOLELY on
         * whether updates were observed at the {@link BigdataSailConnection}.
         * It is possible to make updates at other layers and you are
         * responsible for calling {@link #rollback()} when handling an error
         * condition if you are going around the {@link BigdataSailConnection}
         * for those updates.
         * <p>
         * Note: Since {@link #close()} discards any uncommitted writes it is
         * important to commit the {@link #getDatabase()} made from OUTSIDE of
         * the {@link BigdataSail} before opening a {@link SailConnection} (this
         * artifact arises because the {@link SailConnection} is using
         * unisolated writes on the database).
         */
        @Override
        public synchronized void close() throws SailException {

//            assertOpen();

            if (!openConn) {
                
                return;
                
            }
            
            if (txLog.isInfoEnabled())
                txLog.info("SAIL-CLOSE-CONN: conn=" + this);

            final IIndexManager im = getIndexManager();

            if (isDirty()) {
                /*
                 * Do implicit rollback() of a dirty connection.
                 * 
                 * Note: DO NOT invoke rollback unless the indices are dirty.
                 * Discarding the unisolated indices will throw out a LOT of
                 * cached index pages. You only want to do that when there are
                 * dirty pages which MUST be discarded. There is a significant
                 * negative impact on performance if you discard the unisolated
                 * indices at each commit() point of the Sail!
                 * 
                 * If we have flushed any writes from the assertion or
                 * retraction buffers to the indices, then we should go rollback
                 * the connection before it is closed. rollback() causes the
                 * live indices to be discarded by the backing journal which is
                 * a significant performance hit. This means that if you write
                 * on a SailConnection and do NOT explicitly rollback() the
                 * writes then any writes that were flushed through to the
                 * database will remain there and participate in the next
                 * commit. Really, people should be using a pattern that
                 * guarantees this, but we have often seen code that does not
                 * provide this guarantee.
                 * 
                 * Note: For an AbstractJournal, the writes will have been
                 * buffered on the unisolated views of the indices. Those
                 * indices may have been incrementally flushed to the disk, but
                 * there can still be dirty index pages in memory. It is vitally
                 * important that those views of the indices are discarded and
                 * that the any incrementally flushed index pages are discarded
                 * (the internal views of those indices must be discarded so
                 * they will be reloaded from their last checkpoint and the
                 * allocations associated with those pages must be released
                 * since they will not become committed state).
                 * 
                 * Note: For a federation, those writes will have been made
                 * through shard-wise ACID operations and no rollback is
                 * possible - they are already durable. Further, the client has
                 * only a remote view of the indices so there is no state that
                 * needs to be discarded.
                 * 
                 * Note: TemporaryRawStore does not allow rollback (it does not
                 * have any sense of commit points). This code path will result
                 * in an UnsupportedOperationException.
                 */
                rollback();
            }
            
            if (changeLog != null) {
                changeLog.close();
            }

            try {
                // notify the SailBase that the connection is no longer in use.
                BigdataSail.this.connectionClosed(this);
            } finally {
//                if (lock != null) {
//                    lock.unlock();
//                }
                if (access != null) {
	                access.release();
                }
                openConn = false;
            }
            
        }
        
        /**
         * Must check to see whether the lock is held by the currnet Thread to determine whether it can be unlocked.
         */
        boolean lockHeldByCurrentThread(final Lock lock) {
        	if (lock instanceof ReentrantLock) {
        		return ((ReentrantLock) lock).isHeldByCurrentThread();
        	} else if (lock instanceof ReentrantReadWriteLock.WriteLock) {
        		return ((ReentrantReadWriteLock.WriteLock) lock).isHeldByCurrentThread();
        	}
        	
        	return true; // assume true?
        }
        
        /**
         * Invoke close, which will be harmless if we are already closed. 
         */
        @Override
        protected void finalize() throws Throwable {
            
        	/*
        	 * Note: Automatically closing the connection is vital for the
        	 * UNISOLATED connection.  Otherwise, an application which forgets
        	 * to close() the connection could "lose" the permit required to
        	 * write on the UNISOLATED connection.  By invoking close() from
        	 * within finalize(), we ensure that the permit will be returned
        	 * if a connection is garbage collection without being explicitly
        	 * closed.
        	 */
            close();
            
            super.finalize();

        }

        /**
         * Flush the statement buffers. The {@link BigdataSailConnection}
         * heavily buffers assertions and retractions. Either a {@link #flush()}
         * or a {@link #commit()} is required before executing any operations
         * directly against the backing {@link AbstractTripleStore} so that the
         * buffered assertions or retractions will be written onto the KB and
         * become visible to other methods. This is not a transaction issue --
         * just a buffer issue. The public methods on the
         * {@link BigdataSailConnection} all flush the buffers before performing
         * any queries against the underlying {@link AbstractTripleStore}.
         */
        public void flush() {

            flushStatementBuffers(true/* flushAssertBuffer */, true/* flushRetractBuffer */);
            
        }
        
        /**
         * Flush pending assertions and/or retractions to the database using
         * efficient batch operations. If {@link #getTruthMaintenance()} returns
         * <code>true</code> this method will also handle truth maintenance.
         * <p>
         * Note: This MUST be invoked within any method that will read on the
         * database to ensure that any pending writes have been flushed
         * (otherwise the read operation will not be able to see the pending
         * writes). However, methods that assert or retract statements MUST only
         * flush the buffer on which they will NOT write. E.g., if you are going
         * to retract statements, then first flush the assertions buffer and
         * visa versa.
         */
        protected void flushStatementBuffers(final boolean flushAssertBuffer,
                final boolean flushRetractBuffer) {

            if (readOnly) return;

            synchronized (this) {

                if (flushAssertBuffer && assertBuffer != null) {

                    if (!assertBuffer.isEmpty())
                        dirty = true;

                    // flush statements
                    assertBuffer.flush();

                    if (getTruthMaintenance()) {

                        TempTripleStore statementStore = (TempTripleStore) assertBuffer.getStatementStore();
                        // statementStore could be null if statement buffer was created after disabling entailments,
                        // in this case TM handled manually and does not require execution over assertBuffer
                        if (statementStore != null) {
                        	// do TM, writing on the database.
                        	tm.assertAll(statementStore, changeLog);
                        }

                        // must be reallocated on demand.
                        assertBuffer = null;

                    }

                }

                if (flushRetractBuffer && retractBuffer != null) {

                    if (!retractBuffer.isEmpty())
                        dirty = true;

                    // flush statements.
                    retractBuffer.flush();

                    if (getTruthMaintenance()) {

                        // do TM, writing on the database.
                        tm.retractAll((TempTripleStore) retractBuffer
                                .getStatementStore(), changeLog);

                        // must be re-allocated on demand.
                        retractBuffer = null;

                    }

                    dirty = true;

                }

            }

        }
        
        protected void assertOpenConn() throws SailException {

            if(!openConn) {
                
                throw new SailException("Closed");
                
            }

        }
        
        protected void assertWritableConn() throws SailException {

            assertOpenConn();
            
            if (readOnly) {

                throw new SailException("Read-only");

            }
            
        }

        public CloseableIteration<? extends Statement, SailException> getStatements(
                final Resource s, final URI p, final Value o,
                final Resource context)
                throws SailException {
            return getStatements(s,p,o,true/*includeInferred*/,context==null?
                    new Resource[]{}:new Resource[]{context});
        }
                
        /**
         * Note: if the context is <code>null</code>, then you will see data
         * from each context in a quad store, including anything in the
         * {@link BigdataSail#NULL_GRAPH}.
         */
        @Override
        @SuppressWarnings("unchecked")
        public CloseableIteration<? extends Statement, SailException> getStatements(
                final Resource s, final URI p, final Value o,
                final boolean includeInferred, final Resource... contexts)
                throws SailException {

            if(!isOpen()) {

                /*
                 * Note: While this exception is not declared by the javadoc,
                 * it is required by the Sesame TCK.
                 */
                throw new IllegalStateException();
                
            }
            
            OpenRDFUtil.verifyContextNotNull(contexts);
            
            if (log.isInfoEnabled())
                log.info("s=" + s + ", p=" + p + ", o=" + o
                        + ", includeInferred=" + includeInferred
                        + ", contexts=" + Arrays.toString(contexts));

            if (contexts.length == 0) {
                
                // Operates on all contexts.
                
                return new Bigdata2SesameIteration<Statement, SailException>(
                        getStatements(s, p, o, null/* c */, includeInferred));

            }

            if (contexts.length == 1 && contexts[0] == null) {

                /*
                 * Operate on just the nullGraph, or on the sole graph if not in
                 * quads mode.
                 */
                
                return new Bigdata2SesameIteration<Statement, SailException>(
                        getStatements(s, p, o,
                                quads ? NULL_GRAPH : null/* c */,
                                includeInferred));

            }

            /*
             * Note: The Striterator pattern expands each context in turn. The
             * Striterator itself is wrapped as ICloseableIterator, and that
             * gets wrapped for the Sesame CloseableIteration and returned.
             * 
             * FIXME parallelize this in chunks using a thread pool. See
             * DefaultGraphSolutionExpander for examples. Or we could have a
             * thread pool specifically for this purpose on the Sail, which
             * tends to have a nice long life cycle so that could make sense.
             */

            // See QueryEvaluationIterator for an example of how to do this.
            return new Bigdata2SesameIteration<Statement, SailException>(
                    new CloseableIteratorWrapper<Statement>(
                    new Striterator(Arrays
                    .asList(contexts).iterator()).addFilter(new Expander() {
                private static final long serialVersionUID = 1L;
                @SuppressWarnings("rawtypes")
                @Override
                protected Iterator expand(final Object c) {
                    return getStatements(//
                            s, p, o,//
                            (Resource) ((c == null && quads) ? NULL_GRAPH :c),//
                            includeInferred//
                            );
                }
            })));

        }

        /**
         * Returns an iterator that visits {@link BigdataStatement} objects.
         */
        private synchronized BigdataStatementIterator getStatements(final Resource s,
                final URI p, final Value o, final Resource c,
                final boolean includeInferred) {

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            /*
             * When includedInferred is false we set a filter that causes the
             * access path to only visit the Explicit statements.
             */
            final IElementFilter<ISPO> filter = includeInferred ? null
                    : ExplicitSPOFilter.INSTANCE;

            final IAccessPath<ISPO> accessPath = database.getAccessPath(s, p,
                    o, c, filter, null/* range */);

            if(accessPath instanceof EmptyAccessPath) {
                
                /*
                 * One of the Values was unknown so the access path will be empty.
                 * 
                 * Note: This is true even if we are doing some backchaining.
                 */
                
                return EmptyStatementIterator.INSTANCE;
                
            }
            
            /*
             * Some valid access path.
             */
            
            final IChunkedOrderedIterator<ISPO> src;

            final boolean backchain = database.getAxioms().isRdfSchema()
                    && includeInferred && isQueryTimeExpander(); 
            
//            System.err.println("s=" + s + ", p=" + p + ", o=" + o
//                    + ",\nincludeInferred=" + includeInferred + ", backchain="
//                    + backchain
//                    + ",\nrawAccessPath="
//                    + accessPath
//                    + "\nrangeCount(exact)="
//                    + accessPath.rangeCount(true/*exact*/));
  
            if (backchain) {

                /*
                 * Obtain an iterator that will generate any missing entailments
                 * at query time. The behavior of the iterator depends on how
                 * the InferenceEngine was configured.
                 */
                
                src = new BackchainAccessPath(database, accessPath).iterator();

//                System.err.print("backchainAccessPath");
//                System.err.println(": rangeCount="
//                        + new BackchainAccessPath(database, accessPath)
//                                .rangeCount(true/* exact */));

            } else {

                /*
                 * Otherwise we only return the statements actually present in
                 * the database.
                 * 
                 * Note: An ExplicitSPOFilter is set above that enforces this.
                 */

                src = accessPath.iterator();
                
            }

            /*
             * Resolve SPOs containing term identifiers to BigdataStatementImpls
             * containing BigdataValue objects. The blank node term identifiers
             * will be recognized if they have been seen in the context of this
             * session and will be resolved to the corresponding blank node
             * object in order to preserve their blank node IDs across the scope
             * of the connection.
             * 
             * FIXME bnodes : Also fix in BigdataConstructIterator.
             * 
             * FIXME bnodes : Consider simplifying by passing along the desired
             * valueFactory with the forward and reverse bnode mappings into
             * database.asStatementIterator(src). Note that this is essentially
             * a transactional isolation issue.
             */
            return new BigdataStatementIteratorImpl(database, bnodes2, src)
                    .start(database.getExecutorService());

//            return database.asStatementIterator(src);

        }

        // Note: not part of the Sesame 2 API.
//        public boolean hasStatement(Resource s, URI p, Value o) {
//
//            flushStatementBuffers();
//
//            if( RDF.TYPE.equals(p) && RDFS.RESOURCE.equals(o) ) {
//                
//                if (database.getTermId(s) != NULL) {
//                    
//                    return true;
//                    
//                }
//                
//            }
//            
//            return database.hasStatement(s, p, o);
//            
//        }

      /**
       * Return <code>true</code> iff a statement exists matching the supplied
       * constraints.
       * <p>
       * This code path is optimized for cases where isolatable indices are not
       * in use and where inferences can not appear.
       * 
       * @param s
       * @param p
       * @param o
       * @param includeInferred
       * @param contexts
       * @return
       * @throws SailException
       * 
       * @see <a href="http://trac.bigdata.com/ticket/1178" > Optimize
       *      hasStatement() </a>
       */
       public boolean hasStatement(final Resource s, final URI p, final Value o,
              final boolean includeInferred, final Resource... contexts)
              throws RepositoryException, SailException {

        final AbstractTripleStore tripleStore = getTripleStore();

         /*
          * Figure out whether or not we can use a fast range count to answer
          * the request.
          */
        final boolean fastRangeCountOk;
        if (isIsolatable()) {
           /*
            * The indices are isolatable. This means that fast range counts are
            * not exact. Therefore we have to scan the index.
            */
           fastRangeCountOk = false;
        } else {
           /*
            * The indices are not isolatable so fast range counts are exact.
            */
           if (includeInferred) {
              /*
               * Inferences are being counted so a fast range count will be
               * correct.
               */
              fastRangeCountOk = true;
           } else if (tripleStore.getAxioms().getClass() == NoAxioms.class) {
              /*
               * Inferences are not being maintained so a fast range count will be
               * exact.
               */
              fastRangeCountOk = true;
           } else {
              fastRangeCountOk = false;
           }
        }

         if (fastRangeCountOk) {

            if (contexts.length == 0) {

               // Operates on all contexts.
               return tripleStore.hasStatement(s, p, o, (Resource) null/* c */);

            } else if (contexts.length == 1 && contexts[0] == null) {

               /*
                * Operate on just the nullGraph, or on the sole graph if not in
                * quads mode.
                */

               return tripleStore.hasStatement(s, p, o, quads ? NULL_GRAPH
                     : null/* c */);

            } else {

               boolean found = false;

               for (Resource c : contexts) {

                  found = tripleStore.hasStatement(s, p, o,
                        (c == null && quads) ? NULL_GRAPH : c);

                  if (found)
                     return true;

               }

            }

         }

         /*
          * Run a statement iterator and terminate it once we have at least once
          * matching statement.
          * 
          * TODO This could still be optimized by not materializing the RDF
          * Values.
          */
         final CloseableIteration<? extends Statement, SailException> itr = getStatements(
               s, p, o, includeInferred, contexts);
         try {
            return itr.hasNext();
         } finally {
            itr.close();
         }

      }
        
        /**
         * Computes the closure of the triple store for RDF(S)+ entailments.
         * <p>
         * This computes the closure of the database. This can be used if you do
         * NOT enable truth maintenance and choose instead to load up all of
         * your data first and then compute the closure of the database. Note
         * that some rules may be computed by eager closure while others are
         * computed at query time.
         * <p>
         * Note: If there are already entailments in the database AND you have
         * retracted statements since the last time the closure was computed
         * then you MUST delete all entailments from the database before
         * re-computing the closure.
         * <p>
         * Note: This method does NOT commit the database. See
         * {@link AbstractTripleStore#commit()} and {@link #getTripleStore()}.
         * 
         * @see #removeAllEntailments()
         */
        public synchronized void computeClosure() throws SailException {

            assertWritableConn();

            flushStatementBuffers(true/* assertions */, true/* retractions */);

            database.getInferenceEngine().computeClosure(null/* focusStore */);

        }
        
        /**
         * Removes all "inferred" statements from the database and the proof
         * chains (if any) associated with those inferences (does NOT commit the
         * database).
         */
        public synchronized void removeAllEntailments() throws SailException {
            
            assertWritableConn();
            
            flushStatementBuffers(true/* assertions */, true/* retractions */);

            if (quads) {

                // quads materalized inferences not supported yet. 
                throw new UnsupportedOperationException();
                
            }
            
            @SuppressWarnings("rawtypes")
            final IV NULL = null;
            
            database
                    .getAccessPath(NULL, NULL, NULL, InferredSPOFilter.INSTANCE)
                    .removeAll();
            
        }

        /*
         * Update
         */
        
//        /**
//         * Bigdata now uses an internal query model which differs significantly
//         * from the Sesame query model. Support is not provided for
//         * {@link UpdateExpr} evaluation. SPARQL UPDATE requests must be
//         * prepared and evaluated using a
//         * {@link BigdataSailRepositoryConnection}.
//         * 
//         * @throws SailException
//         *             <em>always</em>.
//         * 
//         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/448">
//         *      SPARQL 1.1 Update </a>
//         */
//        @Override
//        public void executeUpdate(final UpdateExpr updateExpr,
//                final Dataset dataset, final BindingSet bindingSet,
//                boolean includeInferred) throws SailException {
//
//            throw new SailException(ERR_OPENRDF_QUERY_MODEL);
//            
//        }
        
        /*
         * High-level query.
         */

        /**
         * Bigdata now uses an internal query model which differs significantly
         * from the Sesame query model. Support is no longer provided for
         * {@link TupleExpr} evaluation. SPARQL queries must be prepared and
         * evaluated using a {@link BigdataSailRepositoryConnection}.
         * 
         * @throws SailException
         *             <em>always</em>.
         */
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                final TupleExpr tupleExpr, //
                final Dataset dataset,//
                final BindingSet bindings,//
                final boolean includeInferred//
        ) throws SailException {

            throw new SailException(ERR_OPENRDF_QUERY_MODEL);

        }

        /**
         * Evaluate a bigdata query model.
         * 
         * @param queryRoot
         *            The query model.
         * @param dataset
         *            The data set (optional).
         * @param bindings
         *            The initial bindings.
         * @param includeInferred
         *            <code>true</code> iff inferences will be considered when
         *            reading on access paths.
         * 
         * @return The {@link CloseableIteration} from which the solutions may
         *         be drained.
         * 
         * @throws SailException
         * 
         * @deprecated Consider removing this method from our public API. It is
         *             no longer in any code path for the bigdata code base.
         *             Embedded applications requiring high level evaluation
         *             should use {@link BigdataSailRepositoryConnection}. It
         *             does not call through here, but goes directly to the
         *             {@link ASTEvalHelper}.
         */
        public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
                final QueryRoot queryRoot, //
                final Dataset dataset,//
                final BindingSet bindings,//
                final boolean includeInferred//
        ) throws SailException {

            final ASTContainer astContainer = new ASTContainer(queryRoot);
            
            final QueryRoot originalQuery = astContainer.getOriginalAST();

            originalQuery.setIncludeInferred(includeInferred);
            
            try {

                flushStatementBuffers(true/* assertions */, true/* retractions */);

                return ASTEvalHelper.evaluateTupleQuery(getTripleStore(),
                        astContainer, new QueryBindingSet(bindings), dataset);
            
            } catch (QueryEvaluationException e) {
                                
                throw new SailException(e);
                
            }
            
        }
        
        /**
         * Set the change log on the SAIL connection.  See {@link IChangeLog} 
         * and {@link IChangeRecord}.
         * 
         * @param changeLog
         *          the change log
         */
        synchronized public void addChangeLog(final IChangeLog changeLog) {
            
        	if (this.changeLog == null) {
        		
	            this.changeLog = new DelegatingChangeLog();
	            
	            if (assertBuffer != null  && !getTruthMaintenance()) {
	                
	                assertBuffer.setChangeLog(changeLog);
	                
	            }
	            
        	}
        	
        	this.changeLog.addDelegate(changeLog);

        }

        /**
         * Remove a change log from the SAIL connection.  See {@link IChangeLog} 
         * and {@link IChangeRecord}.
         * 
         * @param changeLog
         *          the change log
         */
        synchronized public void removeChangeLog(final IChangeLog changeLog) {
            
            if (this.changeLog != null) {

                this.changeLog.removeDelegate(changeLog);
                
            }

        }

        /**
         * Note: This needs to be visible to
         * {@link BigdataSailRWTxConnection#commit2()}.
         */
        protected DelegatingChangeLog changeLog;

        /*
         * SPARQL UPDATE LISTENER API
         */
        
        /** Registered listeners. */
        private final CopyOnWriteArraySet<ISPARQLUpdateListener> listeners;

        /** Add a SPARQL UDPATE listener. */
        public void addListener(final ISPARQLUpdateListener l) {

            if(isReadOnly())
                throw new UnsupportedOperationException();
            
            if (l == null)
                throw new IllegalArgumentException();

            listeners.add(l);

        }

        /** Remove a SPARQL UDPATE listener. */
        public void removeListener(final ISPARQLUpdateListener l) {
            
            if(isReadOnly())
                throw new UnsupportedOperationException();
            
            if (l == null)
                throw new IllegalArgumentException();
            
            listeners.remove(l);
            
        }

        /**
         * Send an event to all registered listeners.
         */
        public void fireEvent(final SPARQLUpdateEvent e) {
            
            if(isReadOnly())
                throw new UnsupportedOperationException();
            
            if (e == null)
                throw new IllegalArgumentException();
            
            if(listeners.isEmpty()) {
             
                // NOP
                return;

            }

            final ISPARQLUpdateListener[] a = listeners
                    .toArray(new ISPARQLUpdateListener[0]);

            for (ISPARQLUpdateListener l : a) {

                final ISPARQLUpdateListener listener = l;

                try {

                    // send event.
                    listener.updateEvent(e);

                } catch (Throwable t) {

                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                        // Propagate interrupt.
                        throw new RuntimeException(t);

                    }

                    // Log and ignore.
                    log.error(t, t);

                }

            }

        }

        /*
		 * These methods are new with openrdf 2.7. Bigdata uses either
		 * MVCC(full read-write tx) or simply a single writer on the live
		 * indices (unisolated). The latter has more throughput.
		 * 
		 * FIXME Use the MVCC semantics in bigdata do use a prepare()/commit()
		 * pattern and have the notion of active and inactive transactions. This
		 * transaction statement metadata could be exposed to the SailConnection
		 * for the BigdataSailReadOnlyConnection and BigdataSailRWConnection,
		 * but not for the BigdataSailConnection (unisolated connection).
		 * 
		 * BBT - 11/15/2015
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.openrdf.sail.SailConnection#begin()
		 */
        
        /**
         * NOP.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void begin() throws SailException {
            
        }

        /**
         * NOP.
         * <p>
         * {@inheritDoc}
         */
//FIXME:  Sesame 2.8 not used for 2.1.4 Release
//		@Override
//		public void begin(IsolationLevel level)
//				throws UnknownSailTransactionStateException, SailException {
//
//		}

        /**
		 * Always returns <code>true</code>.
		 * <p>
		 * Note: Bigdata does not expose its internal transaction state to the
		 * sail (the concept of active and inactive transactions exists for full
		 * read/write transactions and but those semantics are not exposed to
		 * the sail).
		 * <p>
		 * {@inheritDoc}
		 */
        @Override
        public boolean isActive() throws UnknownSailTransactionStateException {
            
        	return true;
        	
        }

		/**
		 * NOP - the internal transaction model is not exposed to the
		 * {@link SailConnection}.
		 * <p>
		 * Note: Full read/write transactions do have a prepare()/commit() model
		 * interallly based on MVCC, but unisolated connections do not.
		 * <p>
		 * {@inheritDoc}
		 */
        @Override
        public void prepare() throws SailException {
            
        }

        /*
		 * This API is new with openrdf 2.7. It is not supported. Bigdata has a
		 * different logical operator model from openrdf. The UpdateContext is
		 * expressed in terms of the openrdf logical operator model
		 * (UpdateExpr). Therefore it can not be used with bigdata.
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.openrdf.sail.SailConnection#startUpdate(org.openrdf.sail.
		 * UpdateContext)
		 */
        
        /**
         * Unsupported API.
         */
        @Override
        public void startUpdate(UpdateContext op)
            throws SailException
        {
            
            throw new SailException(ERR_OPENRDF_QUERY_MODEL);

        }

        /**
         * Unsupported API.
         */
        @Override
        public void addStatement(UpdateContext op, Resource subj, URI pred, Value obj, Resource... contexts)
            throws SailException
        {
            
            throw new SailException(ERR_OPENRDF_QUERY_MODEL);

        }

        /**
         * Unsupported API.
         */
        @Override
        public void removeStatement(UpdateContext op, Resource subj, URI pred, Value obj, Resource... contexts)
            throws SailException
        {
            
            throw new SailException(ERR_OPENRDF_QUERY_MODEL);

        }

        /**
         * Unsupported API.
         */
        @Override
        public void endUpdate(UpdateContext op)
            throws SailException
        {
            
            throw new SailException(ERR_OPENRDF_QUERY_MODEL);
            
        }

    } // class BigdataSailConnection
   
    /**
     * A connection backed by a read/write transaction.
     */
    private class BigdataSailRWTxConnection extends BigdataSailConnection {

        /**
         * The transaction service.
         */
        private final ITransactionService txService;
        
        /**
         * The transaction id.
         */
        private long tx;

        /**
         * Constructor for a new fully isolated read/write transaction.
         * This constructor uses the caller supplied read/write transaction from the journal's
         * transaction service and attaches this SAIL connection to the new
         * view of the database. 
         * 
         * @throws DatasetNotFoundException 
         */
        public BigdataSailRWTxConnection(final Access access, final long txId, final ITransactionService txService)//, final Lock readLock)
                throws IOException, DatasetNotFoundException {

            super(txId, access);//, false/* unisolated */, false/* readOnly */);

            if (!isolatable) {

                throw new UnsupportedOperationException(
                    "Read/write transactions are not allowed on this database. " +
                    "See " + Options.ISOLATABLE_INDICES);

            }

            this.txService = txService;
            
            newTx(txId);
            
        }

        @Override
        protected synchronized void attach(final long timestampOrTxId)
                throws DatasetNotFoundException {
            
            super.attach(timestampOrTxId);
            
            this.tx = timestampOrTxId;
            
        }
        
        // variant when the caller already has the the tx.
        protected void newTx(final long txId) throws DatasetNotFoundException {
            
            // Attach that transaction view to this SailConnection.
            attach(txId);

            if (txLog.isInfoEnabled())
                txLog.info("SAIL-NEW-TX : txId=" + txId + ", conn=" + this);
            
        }
            
        // variant when the method must create the new tx.
        protected void newTx() throws DatasetNotFoundException, IOException {

            boolean ok = false;
            
            // Open a new read/write transaction.
            final long txId = txService.newTx(ITx.UNISOLATED);

            try {
            
                newTx(txId);
                
                ok = true;
                
            } finally {
                
                if(!ok) {
                try {
                        txService.abort(txId);
                } catch (IOException ex) {
                    log.error(ex, ex);
                }
                }
                
            }
                
        }
        
        /**
         * {@inheritDoc}
         * <p>
         * A specialized commit that goes through the transaction service
         * available on the journal's transaction manager.  Once the commit
         * happens, a new read/write transaction is automatically started
         * so that this connection can continue to absorb writes.
         * <p>
         * Note: writes to the lexicon without dirtying the isolated indices
         * (i.e. writes to the SPO relation) will cause the writes to the
         * lexicon to never be committed.  Probably not a significant issue.
         */
        @Override
        public synchronized long commit2() throws SailException {

            /*
             * don't double commit, but make a note that writes to the lexicon
             * without dirtying the isolated indices will cause the writes to
             * the lexicon to never be committed
             */
            
            assertWritableConn();

            /*
             * Flush any pending writes.
             * 
             * Note: This must be done before you compute the closure so that
             * the pending writes will be read by the inference engine when it
             * computes the closure.
             */
            
            flushStatementBuffers(true/* assertions */, true/* retractions */);
            
            try {
            
                if (changeLog != null) {
                    
                    changeLog.transactionPrepare();
                    
                }

                final long commitTime = txService.commit(tx);
                
                if (txLog.isInfoEnabled())
                    txLog.info("SAIL-COMMIT-CONN : commitTime=" + commitTime
                            + ", conn=" + this);

                if (changeLog != null) {
                    
                    changeLog.transactionCommited(commitTime);
                    
                }

                newTx();

                if (changeLog != null) {
                    
                    changeLog.transactionBegin();
                    
                }

                dirty = false;
                
                return commitTime;
            
            } catch(IOException|DatasetNotFoundException ex) {
                    
                throw new SailException(ex);
                
            }
            
        }
        
        /**
         * A specialized rollback that goes through the transaction service
         * available on the journal's transaction manager.  Once the abort
         * happens, a new read/write transaction is automatically started
         * so that this connection can continue to absorb writes.
         */
        @Override
        public synchronized void rollback() throws SailException {

            if (txLog.isInfoEnabled())
                txLog.info("SAIL-ROLLBACK-CONN: " + this);

            /*
             * Note: DO NOT invoke super.rollback(). That will cause a
             * database (Journal) level abort(). The Journal level abort()
             * will discard the writes buffered on the unisolated indices
             * (the lexicon indices). That will cause lost updates and break
             * the eventually consistent design for the TERM2ID and ID2TERM
             * indices.
             */
//            super.rollback();
            
            try {
            
                txService.abort(tx);
                
                dirty = false;

                newTx();
            
            } catch(IOException|DatasetNotFoundException ex) {
                    
                throw new SailException(ex);
                
            }
            
        }
        
        /**
         * A specialized close that will also abort the current read/write
         * transaction.
         */
        @Override
        public synchronized void close() throws SailException {

            if (!openConn) {
                
                return;
                
            }
            
            super.close();
            
            try {

                txService.abort(tx);
            
            } catch(IOException ex) {
                    
                throw new SailException(ex);
                
            }
            
        }
        
    } // class BigdataSailReadWriteTxConnection
    
    private class BigdataSailReadOnlyConnection extends BigdataSailConnection {

        /**
         * The transaction service.
         */
        private final ITransactionService txService;
        
        /**
         * The transaction id.
         */
        private long tx;

        /**
         * When <code>true</code>, uses a read-historical operation rather than
         * a read-only transaction to read against a cluster.
         * <p>
         * Note: When enabled, the commit time against which the read will be
         * carried out MUST be pinned. E.g., using the NanoSparqlServer or some
         * other application to obtain a single read-only connection which pins
         * that commit time. Queries should then be issued against the desired
         * commit time.
         * <p>
         * Note: This approach allows the metadata and index caches to be
         * reused. Those caches are indexed using a long, which is either a
         * timestamp or a transaction identifier. However, the caches are NOT
         * aware of the commit time associated with a transaction identifier.
         * Each distinct transaction instances against the same commit point
         * will fail when they probe the caches for metadata (partition
         * locators) or data (index views).
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/431 (Read-only
         *      tx per query on cluster defeats cache)
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/266 (Refactor
         *      native long tx id to thin object.)
         */
        private final boolean clusterCacheBugFix;
        
        /**
         * Constructor starts a new transaction. It
         * Obtains a new read-only transaction from the journal's transaction
         * service, and attach this SAIL connection to the new view of the
         * database.
         * 
       * @throws DatasetNotFoundException 
         */
        BigdataSailReadOnlyConnection(final long txId,final ITransactionService txService) throws IOException, DatasetNotFoundException {

            super(txId, null);//, false/* unisolated */, true/* readOnly */);

//            clusterCacheBugFix = BigdataSail.this.database.getIndexManager() instanceof IBigdataFederation;
            clusterCacheBugFix = this.scaleOut; // identical to the code above.

            this.txService = txService;
            
            // Attach that transaction view to this SailConnection.
            attach(txId);
            
        }
        
        @Override
        protected synchronized void attach(final long timestampOrTxId)
                throws DatasetNotFoundException {
            
            super.attach(timestampOrTxId);
            
            this.tx = timestampOrTxId;
                    
                }
                
//        /**
//         * Obtain a new read-only transaction from the journal's transaction
//         * service, and attach this SAIL connection to the new view of the
//         * database.
//         * 
//         * @throws DatasetNotFoundException 
//                     */
//        protected void newTx(final long timestamp) throws IOException, DatasetNotFoundException {
//            
////            // The view of the database *outside* of this connection.
////            final AbstractTripleStore database = BigdataSail.this.database;
////            
////            // The namespace of the triple store.
////            final String namespace = database.getNamespace();
//
//            if (clusterCacheBugFix) {
//
//                /*
//                 * Use a read-historical operation
//                 */
//                this.tx = timestamp;
//
//                // Attach that transaction view to this SailConnection.
//                attach(database);
//
//            } else {
//                            
//                /*
//                 * Obtain a new read-only transaction reading from that
//                 * timestamp.
//                 */
//                this.tx = txService.newTx(timestamp);
//
//                try {
//                    
////                    /*
////                     * Attempt to discover the commit time that this transaction
////                     * is reading on.
////                     */
////                    final long readsOnCommitTime;
////                    if (database.getIndexManager() instanceof Journal) {
////
////                        final Journal journal = (Journal) database
////                                .getIndexManager();
////
////                        final ITx txObj = journal.getTransactionManager()
////                                .getTx(this.tx);
////
////                        if (txObj != null) {
////
////                            // found it.
////                            readsOnCommitTime = txObj.getReadsOnCommitTime();
////                            
////                        } else {
////
////                            /**
////                             * TODO This can happen in HA because the TxState is
////                             * not available yet on the followers.
////                             * 
////                             * @see <a
////                             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/623">
////                             *      HA TXS / TXS Bottleneck </a>
////                             */
////                            readsOnCommitTime = this.tx;
////                            
////                        }
////
////                    } else {
////                        
////                        readsOnCommitTime = timestamp;
////                    
////                    }
//                    
//                    /*
//                     * Locate a view of the triple store isolated by that
//                     * transaction.
//                     * 
//                     * Note: This will use the readsOnCommitTime for the tx when
//                     * that is available. This provides better caching.
//                             * 
//                     * TODO This change does not work. It causes a test failure
//                     * in TestConcurrentKBCreate and may also be causing CI
//                     * problems in TestHA3JournalServer. See r6742, r6743. I
//                     * have not tracked down why using the [readsOnCommitTime]
//                     * causes a problem, but what happens is that the
//                     * DefaultResourceLocator winds up returning [txView :=
//                     * null] and an IllegalArgumentException is then thrown out
//                     * of attach(txView).
//                             */
//                    final AbstractTripleStore txView = (AbstractTripleStore) database
//                            .getIndexManager().getResourceLocator()
//                            .locate(namespace, tx);//readsOnCommitTime);
//
//                    // Attach that transaction view to this SailConnection.
//                    attach(txView);
//    
//                } catch (Throwable t) {
//    
//                    try {
//                        txService.abort(tx);
//                    } catch (IOException ex) {
//                        log.error(ex, ex);
//                    }
//    
//                    throw new RuntimeException(t);
//                            
//                        }
//                        
//            }
//                    
//                    }

        /**
         * NOP
         * 
         * @return <code>0L</code> since nothing was committed.
         */
        @Override
        public synchronized long commit2() throws SailException {

            // NOP.
            return 0L; // Nothing committed.
            
        }
        
        /**
         * NOP
         */
        @Override
        public synchronized void rollback() throws SailException {

            // NOP

            if (txLog.isInfoEnabled())
                txLog.info("SAIL-ROLLBACK-CONN: " + this);

        }
        
        /**
         * A specialized close that will also abort the current read-only
         * transaction.
         */
        @Override
        public synchronized void close() throws SailException {

            if (!openConn) {
                
                return;
                
            }
            
            super.close();
            
            if(!clusterCacheBugFix) {
                
                try {

                    txService.abort(tx);

                } catch (IOException ex) {

                    throw new SailException(ex);

                }
                
            }
            
        }

    } // class BigdataSailReadOnlyConnection

//FIXME:  Sesame 2.8 not used for 2.1.4 Release
//	@Override
//	public List<IsolationLevel> getSupportedIsolationLevels() {
//		return Arrays.<IsolationLevel>asList(IsolationLevels.READ_UNCOMMITTED, IsolationLevels.SNAPSHOT_READ);
//	}

//	@Override
//	public IsolationLevel getDefaultIsolationLevel() {
//		return IsolationLevels.READ_UNCOMMITTED;
//	}
    
}
