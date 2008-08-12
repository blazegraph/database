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
 * Created on Apr 18, 2007
 */

package com.bigdata.rdf.rules;

import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataLiteralImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.GenericChunkedOrderedStriterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.Resolver;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleTestCase extends AbstractInferenceEngineTestCase {
    
    /**
     * 
     */
    public AbstractRuleTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRuleTestCase(String name) {
        super(name);
    }

    protected void applyRule(AbstractTripleStore db, Rule rule,
            long expectedSolutionCount, long expectedMutationCount)
            throws Exception {

        applyRule(db, rule, null/* filter */, expectedSolutionCount,
                expectedMutationCount);

    }
    
    /**
     * Applies the rule, copies the new entailments into the store and checks
     * the expected #of inferences computed and new statements copied into the
     * store.
     * <p>
     * Invoke as <code>applyRule( store.{rule}, ..., ... )</code>
     * 
     * @param rule
     *            The rule, which must be one of those found on {@link #store}
     *            or otherwise configured so as to run with the {@link #store}
     *            instance.
     * 
     * @param expectedSolutionCount
     *            The #of entailments that should be computed by the rule
     *            (tested during Query) or <code>-1</code> if the #of
     *            solutions that will be computed is not known.
     * 
     * @param expectedMutationCount
     *            The #of new elements that should have been inserted into the
     *            db by the rule.
     */
    protected void applyRule(AbstractTripleStore db, Rule rule,
            IElementFilter<SPO> filter, long expectedSolutionCount,
            long expectedMutationCount) throws Exception {

        // Note: Now handled by RDFJoinNexus.
//        /*
//         * Note: Rules run against the committed view.
//         * 
//         * These tests were originally written when rules ran against the
//         * unisolated indices, so a commit is being requested here to ensure
//         * that the test cases all run against a view with access to the data
//         * which they have written on the store.
//         */
//        db.commit();
        
        // dump the database on the console.
        if(log.isInfoEnabled())
            log.info("\ndatabase(before)::\n" + db.dumpStore());

        // run as query.
        {

            final IJoinNexus joinNexus = db.newJoinNexusFactory(
            		RuleContextEnum.HighLevelQuery,
                    ActionEnum.Query, IJoinNexus.ALL, filter).newInstance(
                    db.getIndexManager());

            long n = 0;
            
            final IChunkedOrderedIterator<ISolution> itr = joinNexus.runQuery(rule);
            
            try {

                while(itr.hasNext()) {

                    final ISolution[] chunk = itr.nextChunk();
                    
                    for(ISolution solution : chunk) {
                    
                        n++;
                        
                        log.info("#"+n+" : "+solution.toString());
                        
                    }
                    
                }

            } finally {

                itr.close();
                
            }

            if (log.isInfoEnabled())
                log.info("Computed " + n + " solutions: " + rule.getName() + " :: " + rule);
            
            if (expectedSolutionCount != -1L) {

                assertEquals("solutionCount(Query)", expectedSolutionCount, n);

            }
            
        }

        // run as insert.
        {

            final IJoinNexus joinNexus = db.newJoinNexusFactory(
            		RuleContextEnum.DatabaseAtOnceClosure,
                    ActionEnum.Insert, IJoinNexus.ALL, filter).newInstance(
                    db.getIndexManager());

            final long actualMutationCount = joinNexus.runMutation(rule);

            if (log.isInfoEnabled())
                log.info("Inserted " + actualMutationCount + " elements : "
                        + rule.getName() + " :: " + rule);

            assertEquals("mutationCount(Insert)", expectedMutationCount,
                    actualMutationCount);

        }
                
        // dump the database.
        if(log.isInfoEnabled())
            log.info("\ndatabase(after)::\n" + db.dumpStore());
        
    }

    /**
     * Compares two RDF graphs for equality (same statements). This does NOT
     * handle bnodes, which much be treated as variables for RDF semantics. Note
     * that term identifiers are assigned by the lexicon for each database and
     * will differ between databases. Therefore, the comparison is performed in
     * terms of the externalized RDF {@link Statement}s rather than {@link SPO}s.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     * 
     * FIXME refactor to {@link AbstractTestCase} and make sure that the same
     * logic is not present in the rest of the test suite.
     */
    public static boolean modelsEqual(AbstractTripleStore expected,
            AbstractTripleStore actual) throws SailException {

        int actualSize = 0;
        int notExpecting = 0;
        int expecting = 0;
        boolean sameStatements1 = true;
        {

            BigdataStatementIterator it = actual.asStatementIterator(actual
                    .getInferenceEngine().backchainIterator(NULL, NULL, NULL));

            try {

                while(it.hasNext()) {

                    BigdataStatement stmt = it.next();

                    if (!hasStatement(expected,//
                            (Resource)OptimizedValueFactory.INSTANCE.newValue(stmt.getSubject()),//
                            (URI)OptimizedValueFactory.INSTANCE.newValue(stmt.getPredicate()),//
                            (Value)OptimizedValueFactory.INSTANCE.newValue(stmt.getObject()))//
                            ) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);
                        
                        notExpecting++;

                    }

                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
            
            log("all the statements in actual in expected? " + sameStatements1);

        }

        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            BigdataStatementIterator it = expected.asStatementIterator(expected
                    .getInferenceEngine().backchainIterator(NULL, NULL, NULL));

            try {

                while(it.hasNext()) {

                BigdataStatement stmt = it.next();

                if (!hasStatement(actual,//
                        (Resource)OptimizedValueFactory.INSTANCE.newValue(stmt.getSubject()),//
                        (URI)OptimizedValueFactory.INSTANCE.newValue(stmt.getPredicate()),//
                        (Value)OptimizedValueFactory.INSTANCE.newValue(stmt.getObject()))//
                        ) {

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);
                    
                    expecting++;

                }
                
                expectedSize++; // counts statements actually visited.

                }
                
            } finally {
                
                it.close();
                
            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        final boolean sameSize = expectedSize == actualSize;
        
        log("size of 'expected' repository: " + expectedSize);

        log("size of 'actual'   repository: " + actualSize);

        log("# expected but not found: " + expecting);
        
        log("# not expected but found: " + notExpecting);
        
        return sameSize && sameStatements1 && sameStatements2;

    }

    private static void log(String s) {

        System.err.println(s);

    }

    static private boolean hasStatement(AbstractTripleStore database,
            Resource s, URI p, Value o) {

        if (RDF.TYPE.equals(p) && RDFS.RESOURCE.equals(o)) {

            if (database.getTermId(s) != NULL) {

                return true;

            }

        }

        return database.hasStatement(s, p, o);

    }

    /**
     * Visits {@link SPO}s not found in the target database.
     * 
     * @param src
     * @param tgt
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * @todo rewrite the {@link StatementVerifier} to use the same logic.
     */
    protected Iterator<SPO> notFoundInTarget(//
            final AbstractTripleStore src,//
            final AbstractTripleStore tgt //
            ) throws InterruptedException, ExecutionException {
        
        /*
         * Visit all SPOs in the source, including backchained
         * inferences.
         */
        final IChunkedOrderedIterator<SPO> itr1 = src
                .getInferenceEngine().backchainIterator(NULL, NULL,
                        NULL);

        /*
         * Efficiently convert SPOs to BigdataStatements (externalizes
         * statements).
         */
        final BigdataStatementIterator itr2 = src.asStatementIterator(itr1);

        /*
         * Buffer size (#of statements). 
         */
        final int capacity = 10000; // @todo large capacity for bulk loads.
        
        /*
         * Read only since we do not want to write on the lexicon or the
         * statement indices, just resolve what already exists and look for
         * missing statements.
         */
        final boolean readOnly = true;
        
        /*
         * FIXME Change the striterator heirarchy into an implementation
         * heirarchy (StriteratorImpl) for co-varying generics and a use
         * hierarchy (IStriterator and Striterator) in which only the element
         * type is generic. This should be significantly easier to use and
         * understand.
         */
        
        /*
         * Task that will efficiently convert to BigdataStatements using the
         * lexicon for the target database.
         */
        final StatementResolverTask<SPO> task = new StatementResolverTask<SPO>(
                (Iterator<BigdataStatement>) itr2, readOnly, capacity, tgt) {

            /** bulk filter for SPOs NOT FOUND in the target. */
            @Override
            protected Iterator<SPO> getOutputIterator() {

                return tgt.bulkFilterStatements(
                        new GenericChunkedOrderedStriterator(capacity, this
                                .iterator()).addFilter(new Resolver() {
                            @Override
                            protected Object resolve(Object e) {
                                return new SPO((BigdataStatement) e);
                            }
                        }), false/* present */);

            }
            
        };

        /*
         * Run the task.
         * 
         * Note: We do not obtain the Future. If an error occurs, then we will
         * notice it when the converting iterator is closed.
         */
        tgt.getExecutorService().submit(task);

        // visits SPOs NOT FOUND in the target.
        return task.iterator();

    }

    /**
     * Resolves {@link Statement}s or {@link SPO}s into
     * {@link BigdataStatement}s using the lexicon for the specified database
     * (this can be used to perform bulk copy or existence testing).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract private static class StatementResolverTask<E> implements Callable<Long> {

        /**
         * The source iterator.
         */
        private final Iterator<? extends Statement> sourceIterator;
        
        /**
         * The output iterator.
         */
        private final Iterator<E> outputIterator;
        
        /**
         * The buffer used to bulk converts {@link Statement}s to
         * {@link BigdataStatement}s, resolving term (and optionally statement
         * identifiers) against the target database.
         */
        protected final OuterStatementBuffer<Statement, BigdataStatement> sb;

        /**
         * @param source
         *            An iterator visiting {@link Statement}s (will be closed
         *            if implements a closable interface).
         * @param readOnly
         *            When <code>true</code>, terms will be resolved against
         *            the <i>target</i> but not written onto the <i>target</i>.
         * @param capacity
         *            The capacity of the buffer used to bulk convert
         *            statements.
         * @param target
         *            The target database.
         */
        public StatementResolverTask(Iterator<? extends Statement> source,
                boolean readOnly, int capacity, AbstractTripleStore target) {

            this.sourceIterator = source;
            
            this.sb = new OuterStatementBuffer<Statement, BigdataStatement>(target,
                    readOnly, capacity);
            
            this.outputIterator = getOutputIterator();

        }
        
        /**
         * Return the singleton that will be used as the output iterator. The
         * application will obtain this iterator from {@link #iterator()}. The
         * iterator will be closed when the task completes if it implements a
         * "closeable" interface.
         */
        abstract protected Iterator<E> getOutputIterator();

        /**
         * When the task runs it will consume {@link Statement}s from the
         * {@link #sourceIterator} iterator,
         * {@link OuterStatementBuffer#add(Statement) adding} to an
         * {@link OuterStatementBuffer}. Once the source iterator is exhausted,
         * the buffer is {@link OuterStatementBuffer#flush()}ed and this task
         * is done.
         */
        public Long call() throws Exception {
            
            try {

                while (sourceIterator.hasNext()) {

                    sb.add(sourceIterator.next());

                }

                final long n = sb.flush();

                return Long.valueOf(n);

            } finally {

                close(sourceIterator);

                close(outputIterator);
                
            }

        }

        /**
         * Close the iterator if it implements any of the "closeable"
         * interfaces.
         * 
         * @param itr
         */
        protected void close(Iterator itr) {

            try {

                if (itr instanceof ICloseableIterator) {

                    ((ICloseableIterator) itr).close();

                } else if (itr instanceof CloseableIteration) {

                    ((CloseableIteration) itr).close();

                } else if (itr instanceof Closeable) {

                    ((Closeable) itr).close();

                }

            } catch (Throwable t) {

                log.error(t, t);

            }

        }
        
        /**
         * The output of the task may be read from this iterator.
         */
        public Iterator<E> iterator() {
            
            return outputIterator;
            
        }

    }
    
//    /**
//     * Efficiently convert {@link BigdataStatement}s to {@link SPO}s for the
//     * target database, but DO NOT add unknown terms and DO NOT write the
//     * {@link SPO}s on the target database.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    private static class SPOResolver
//            extends
//            ChunkedFilter<IChunkedIterator<BigdataStatement>, BigdataStatement, SPO> {
//
//        /**
//         * 
//         */
//        private static final long serialVersionUID = -459773229790890577L;
//
//        private final OuterStatementBuffer<Statement,BigdataStatement> sb;
//
//        public SPOResolver(AbstractTripleStore db, int chunkSize,
//                boolean readOnly) {
//
//            this.sb = new OuterStatementBuffer<Statement, BigdataStatement>(db,
//                    readOnly, chunkSize);
//
//        }
//        
//        @Override
//        protected SPO[] filterChunk(BigdataStatement[] chunk) {
//
//            for (int i = 0; i < chunk.length; i++) {
//
//                final BigdataStatement stmt = chunk[i];
//
//                sb.add(stmt);
//
//            }
//            
//            sb.flush();
//
//            final int n = sb.size();
//            
//            final SPO[] a = new SPO[ n ];
//            
//            final ICloseableIterator<? extends BigdataStatement> itr = sb.iterator();
//            try {
//
//                int i = 0;                
//                
//                while (itr.hasNext()) {
//                    
//                    a[i] = new SPO(itr.next());
//                    
//                }
//                
//            } finally {
//                
//                itr.close();
//                
//            }
//
//            return a;
//            
//        }
//
//    }

    /**
     * Class for efficiently converting {@link Statement}s into
     * {@link BigdataStatement}s, including resolving term identifiers (or
     * adding entries to the lexicon for unknown terms) as required. The class
     * does not write the converted {@link BigdataStatement}s onto the
     * database, but that can be easily done using a resolving iterator pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <F>
     *            The generic type of the source {@link Statement}s.
     * @param <G>
     *            The generic type of the converted {@link BigdataStatement}s.
     */
    private static class OuterStatementBuffer<F extends Statement, G extends BigdataStatement> 
    implements IBuffer<F> {

        /**
         * The database against which the {@link Value}s will be resolved (or
         * added). If this database supports statement identifiers, then
         * statement identifiers for the converted statements will be resolved
         * (or added) to the lexicon.
         */
        private final AbstractTripleStore db;
        
        /**
         * When <code>true</code>, {@link Value}s will be resolved against
         * the {@link LexiconRelation} and {@link Statement}s will be resolved
         * against the {@link SPORelation}, but unknown {@link Value}s and
         * unknown {@link Statement}s WILL NOT be inserted into the
         * corresponding relations.
         */
        private final boolean readOnly;

        /**
         * The maximum #of {@link BigdataStatement}s that can be buffered
         * before the buffer {@link #overflow()}s (from the ctor).
         */
        private final int capacity;
        
        /**
         * Buffer for canonical {@link BigdataValue}s. This buffer is cleared
         * each time it overflows.
         */
        private final BigdataValue[] valueBuffer;

        /**
         * The #of elements in {@link #valueBuffer}.
         */
        private int nvalues = 0;
        
        /**
         * Buffer for accepted {@link BigdataStatement}s. This buffer is
         * cleared each time it would overflow.
         */
        protected final G[] statementBuffer;
        
        /**
         * The #of elements in {@link #statementBuffer}.
         */
        private int nstmts = 0;
        
        /**
         * Statements which use blank nodes in their {s,p,o} positions must be
         * deferred when statement identifiers are enabled until (a) either the
         * blank node is observed in the context position of a statement; or (b)
         * {@link #flush()} is invoked, indicating that no more data will be
         * loaded from the current source. Blank nodes that can not be unified
         * with a statement identifier once {@link #flush()} is invoked are
         * interpreted as normal blank nodes rather than statement identifiers.
         * <P>
         * This map is used IFF statement identifers are enabled. When statement
         * identifiers are NOT enabled blank nodes are always blank nodes and we
         * do not need to defer statements, only maintain the canonicalizing
         * {@link #bnodes} mapping.
         */
        final private List<G> deferredStatementBuffer;

        /**
         * Map used to filter out duplicate terms.  The use of this map provides
         * a ~40% performance gain.
         */
        final private Map<Value, BigdataValue> distinctValues;

        /**
         * A canonicalizing map for blank nodes. This map MUST be cleared before you
         * begin to add statements to the buffer from a new "source" otherwise it
         * will co-reference blank nodes from distinct sources. The life cycle of
         * the map is the life cycle of the document being loaded, so if you are
         * loading a large document with a lot of blank nodes the map will also
         * become large.
         */
        final private Map<String, BigdataBNode> bnodes;

        /**
         * @param db
         *            The database against which the {@link Value}s will be
         *            resolved (or added). If this database supports statement
         *            identifiers, then statement identifiers for the converted
         *            statements will be resolved (or added) to the lexicon.
         * @param readOnly
         *            When <code>true</code>, {@link Value}s (and statement
         *            identifiers iff enabled) will be resolved against the
         *            {@link LexiconRelation}, but entries WILL NOT be inserted
         *            into the {@link LexiconRelation} for unknown {@link Value}s
         *            (or for statement identifiers for unknown
         *            {@link Statement}s when statement identifiers are
         *            enabled).
         * @param capacity
         *            The capacity of the backing buffer.
         * 
         * @todo error handling for terms that can not be resolved or leave 0L
         *       and let the target handle it?
         */
        public OuterStatementBuffer(AbstractTripleStore db, boolean readOnly, int capacity) {

            if (db == null)
                throw new IllegalArgumentException();

            if (capacity <= 0)
                throw new IllegalArgumentException();
            
            this.capacity = capacity;
            
            this.db = db;
            
            this.readOnly = readOnly;

            this.valueBuffer = new BigdataValue[capacity * 3];

            this.statementBuffer = (G[])new BigdataStatement[capacity];
            
            this.distinctValues = new HashMap<Value, BigdataValue>(capacity
                    * IRawTripleStore.N);

            this.bnodes = new HashMap<String, BigdataBNode>(capacity);

            this.deferredStatementBuffer = db.getStatementIdentifiers() ? new LinkedList<G>()
                    : null;
                    
            this.itr = new OutputIterator();

        }
        
        /**
         * Return a canonical {@link BigdataValue} instance representing the
         * given <i>value</i>. The scope of the canonical instance is until the
         * next internal buffer overflow ({@link URI}s and {@link Literal}s)
         * or until {@link #flush()} ({@link BNode}s, since blank nodes are
         * global for a given source). The purpose of the canonicalizing mapping
         * is to reduce the buffered {@link BigdataValue}s to the minimum
         * variety required to represent the buffered {@link BigdataStatement}s,
         * which improves throughput significantly (40%) when resolving terms to
         * the corresponding term identifiers using the {@link LexiconRelation}.
         * <p>
         * Note: This is not a true canonicalizing map when statement
         * identifiers are used since values used in deferred statements will be
         * held over until the buffer is {@link #flush()}ed. This relaxation of
         * the canonicalizing mapping is not a problem since the purpose of the
         * mapping is to provide better throughput and nothign relies on a pure
         * canonicalization of the {@link Value}s.
         * 
         * @param value
         *            A value.
         * 
         * @return The corresponding canonical {@link BigdataValue}. The
         *         returned value is NEVER the same as the given value, even
         *         when the given value implements {@link BigdataValue}. This
         *         is done in order to facilitate conversion of
         *         {@link BigdataStatement}s backed by the lexicon for one
         *         {@link AbstractTripleStore} into {@link BigdataStatement}s
         *         backed by the lexicon of a different
         *         {@link AbstractTripleStore}.
         * 
         * @throws IllegalArgumentException
         *             if <i>value</i> is <code>null</code>.
         */
        protected BigdataValue convertValue(Value value) {
            
            if (value == null)
                throw new IllegalArgumentException();
            
            if(value instanceof BNode) {
                
                final BNode a = (BNode) value;

                final String id = a.getID();

                BigdataBNode b = bnodes.get(id);

                if (b == null) {

                    b = new BigdataBNodeImpl(a);

                    // add to canonical map (global scope).
                    bnodes.put(id, b);
                    
                    // Note: DO NOT add to value[]!
                    
                }

                return b;
                
            } else {

                BigdataValue b = distinctValues.get(value);

                if (b == null) {

                    if (value instanceof URI) {

                        b = new BigdataURIImpl((URI) value);

                    } else if (value instanceof Literal) {

                        b = new BigdataLiteralImpl((Literal) value);

                    } else
                        throw new AssertionError();
                    
                    // add to canonical map.
                    distinctValues.put(value, b);

                    // add to array of distinct value refs.
                    valueBuffer[nvalues++] = b;

                }
                
                return b;
                
            }
            
        }
        
        /**
         * <code>true</code> if there are no buffered statements and no
         * buffered deferred statements
         */
        public boolean isEmpty() {

            return nstmts == 0
                    && (deferredStatementBuffer == null ? true
                            : !deferredStatementBuffer.isEmpty());
            
        }

        /**
         * #of buffered statements plus the #of buffered statements that are
         * being deferred.
         */
        public int size() {

            return nstmts
                    + (deferredStatementBuffer == null ? 0
                            : deferredStatementBuffer.size());
            
        }
        
        public void add(int n, F[] a) {

            for(int i=0; i<n; i++) {
                
                add(a[i]);
                
            }
            
        }

        /**
         * Imposes a canonical mapping on the subject, predicate, and objects of
         * the given {@link Statement}s and stores a new
         * {@link BigdataStatement} instance in the internal buffer.
         * <p>
         * Note: Unlike the {@link Value}s, a canonicalizing mapping is NOT
         * imposed for the statements. This is because, unlike the {@link Value}s,
         * there tends to be little duplication in {@link Statement}s when
         * processing RDF.
         */
        @SuppressWarnings("unchecked")
        public void add(F e) {

            if(nstmts == capacity) {
                
                overflow();
                
            }
            
            final G stmt = (G) new BigdataStatementImpl(//
                    (BigdataResource) convertValue(e.getSubject()), //
                    (BigdataURI) convertValue(e.getPredicate()), //
                    convertValue(e.getObject()),//
                    StatementEnum.Explicit);

            if (deferredStatementBuffer != null
                    && e.getSubject() instanceof BNode
                    || e.getObject() instanceof BNode) {

                /*
                 * When statement identifiers are enabled and a blank node
                 * appears in the statement, then we add the statement to a
                 * collection of statements whose processing must be deferred
                 * until
                 * 
                 * Note: blank nodes do not appear in the predicate position.
                 */
                
                deferredStatementBuffer.add( stmt );
                
            } else {
            
                statementBuffer[nstmts++] = stmt;
                
            }
            
        }
        
        /**
         * Efficiently resolves/adds term identifiers for the buffered
         * {@link BigdataValue}s.
         * <p>
         * If {@link #readOnly}), then the term identifier for unknown values
         * will remain {@link IRawTripleStore#NULL}.
         */
        protected void processBufferedValues() {
            
            log.info("");

            // @todo implement.
            throw new UnsupportedOperationException();

        }

        /**
         * Processes any {@link BigdataStatement}s in the
         * {@link #deferredStatementBuffer}, adding them to the
         * {@link #statementBuffer}, which may cause the latter to
         * {@link #overflow()}.
         */
        protected void processDeferredStatements() {

            if (deferredStatementBuffer == null
                    || deferredStatementBuffer.isEmpty())
                return;

            log.info("");

            /*
             * FIXME convert deferred statements, incrementing counter as they
             * are added to the buffered statements and overflowing if
             * necessary.
             */
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Invoked each time the {@link #statementBuffer} buffer would overflow.
         * This method is responsible for bulk resolving / adding the buffered
         * {@link BigdataValue}s against the {@link #db} and adding the fully
         * resolved {@link BigdataStatement}s to the queue on which the
         * {@link #iterator()} is reading.
         */
        @SuppressWarnings("unchecked")
        protected void overflow() {

            if (log.isInfoEnabled())
                log.info("nvalues=" + nvalues + ", nstmts=" + nstmts);

            if (nstmts == 0)
                return;
            
            // resolve/add term identifiers for the buffered values.
            processBufferedValues();

            /*
             * Note: clone the chunk since this class reuses the same array
             * reference for each chunk. Cloning gives us a shallow copy of the
             * caller's array.
             */
            
            // an array with exactly the right #of elements.
            final G[] a = (G[]) new BigdataStatement[nstmts];
            
            // copy references.
            for (int i = 0; i < nstmts; i++) {
                
                a[i] = statementBuffer[i];
                
            }

            // add converted statements to the output iterator.
            itr.add(a);

            // increment the #of processed statements.
            counter += nstmts;
            
            // clear the buffered values and statements.
            clear();
            
        }

        /**
         * Converts any buffered statements and any deferred statements, making
         * the converted {@link BigdataStatement}s available to the
         * {@link #iterator()}.
         * 
         * @return The total #of converted statements processed so far.
         * 
         * @todo an adaptor will normally bulk load into the {@link SPORelation}.
         *       if that adaptor implements {@link IBuffer} then its
         *       {@link IBuffer#flush()} method should return the #of
         *       {@link SPO}s actually written on the {@link SPORelation}.
         */
        public long flush() {

            log.info("");

            processBufferedValues();
            
            processDeferredStatements();
            
            // force everything to the output iterator.
            overflow();
            
            // make a copy of the counter value.
            final long n = counter;
            
            // reset all state.
            reset();
            
            return n;
            
        }
        private long counter = 0L;

        /**
         * Discards all state (term map, bnodes, deferred statements, the
         * buffered statements, and the counter whose value is reported by
         * {@link #flush()}).
         */
        public void reset() {

            log.info("");
            
            bnodes.clear();
            
            deferredStatementBuffer.clear();
            
            counter = 0L;
            
            clear();
            
        }
        
        /**
         * Clears the state associated with the {@link BigdataStatement}s in
         * the internal buffer but does not discard the blank nodes or deferred
         * statements.
         */
        protected void clear() {

            log.info("");

            distinctValues.clear();

            nstmts = nvalues = 0;

        }
        
        /**
         * An asynchronous iterator singleton that reads from the converted
         * statements. {@link BigdataStatement}s are made available to the
         * iterator in chunks each time the buffer {@link #overflow()}s and
         * when it is {@link #flush()}ed. The iterator will block until more
         * {@link BigdataStatement}s are available or until it is
         * {@link ICloseableIterator#close()}d. The iterator is safe for
         * reading by a single thread. The iterator does NOT support removal.
         */
        synchronized public ICloseableIterator<G> iterator() {
            
            return itr;
            
        }
        private final OutputIterator itr;

        /**
         * Extended to ensure that the {@link OutputIterator} is closed.
         */
        protected void finalize() throws Throwable {
            
            super.finalize();
            
            itr.close();
            
        }
        
        /**
         * Iterator for converted {@link BigdataStatement}s.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        private class OutputIterator implements ICloseableIterator<G> {

            /**
             * <code>true</code> iff the iterator is open. When open, the
             * iterator will block if the {@link #queue} is empty.
             * <p>
             * Note: Access to {@link #open} should be synchronized on <i>this</i>
             * so that concurrent changes made in other threads will be visible.
             */
            private boolean open;

            /**
             * Blocking queue containing chunks of converted
             * {@link BigdataStatement}s.
             * <p>
             * Note: The queue has an unbounded capacity. In practice, a bounded
             * and small (~5-10) capacity would be just fine since we normally
             * expect the conversion process to be more expensive than the
             * processing consuming this iterator. However, the unbounded queue
             * makes the {@link OuterStatementBuffer} non-blocking, which seems
             * to be worthwhile.
             */
            private final BlockingQueue<G[]> queue = new LinkedBlockingQueue<G[]>();
            
            /**
             * The current chunk of converted {@link BigdataStatement}s from
             * the {@link #queue}.
             */
            private G[] chunk = null;
            
            /**
             * The index of the next element in {@link #chunk} to be delivered
             * by the iterator.
             */
            private int index = 0;
            
            public OutputIterator() {
                
            }

            /**
             * Adds the chunk to the {@link #queue}. The elements in the chunk
             * will be processed when that chunk is popped off of the
             * {@link #queue}.
             * 
             * @param chunk
             *            A chunk of converted {@link BigdataStatement}s.
             */
            protected void add(G[] chunk) {

                queue.add(chunk);
                
            }

            synchronized public void close() {

                open = false;
                
            }

            private void nextChunk() {
                
                chunk = null;
                
                index = 0;
                
                try {

                    chunk = queue.take();
                    
                    index = 0;
                    
                } catch (InterruptedException e) {
                    
                    log.warn("Interrupted - iterator will be closed");
                    
                    open = false;
                    
                    throw new RuntimeException("Iterator closed by interrupt",
                            e);
                    
                }
                
            }
            
            public boolean hasNext() {

                while (open && chunk == null || index == chunk.length) {

                    // synchronize to make [open] visible.
                    synchronized(this) {
                        
                        nextChunk();
                        
                    }

                }

                return open && chunk != null || index < chunk.length;
                
            }

            public G next() {
                
                if (!hasNext())
                    throw new NoSuchElementException();
                
                return chunk[index++];
                
            }

            public void remove() {
                
                throw new UnsupportedOperationException();
                
            }
            
        }

    }

}
