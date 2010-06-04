package com.bigdata.rdf.rio;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Class for efficiently converting {@link Statement}s into
 * {@link BigdataStatement}s, including resolving term identifiers (or adding
 * entries to the lexicon for unknown terms) as required. The class does not
 * write the converted {@link BigdataStatement}s onto the database, but that
 * can be easily done using a resolving iterator pattern.
 * 
 * @todo In fact, RIO also keeps a blank node map so that it can reuse the same
 *       blank node object if it sees the same ID more than once.
 * 
 * @todo the {@link StatementBuffer} does not appear to correctly canonicalize
 *       terms when statement identifiers are enabled. Per below, this just
 *       needs to be rewritten. The code could be simplified dramatically. If
 *       the value is a BNode, then it goes into a map for canonicalizing blank
 *       nodes with a life cycle of the document being loaded. If a statement
 *       uses blank nodes then it must be deferred (this is true whether or not
 *       statement identifiers are in use) so do NOT make the {s,p,o} canonical
 *       since the statement and its terms will be processed later. Otherwise it
 *       goes into a canonicalizing Set (add iff not found and return, otherwise
 *       return the existing Value). The canonicalized value is used by the
 *       statement. An incremental write will cause all terms in the Value[] to
 *       be assigned term identifiers, so they should be BigdataValue objects.
 *       The statements now have term identifiers and they are written onto the
 *       DB. When the end of the document is reached, there will be deferred
 *       statements iff there were blank nodes. Those are then processed per the
 *       existing code. (If statement identifiers exist, then unify blank nodes
 *       with statment identifiers otherwise just assign term identifiers to
 *       blank nodes.) Note that the Value[] should be empty after each
 *       incremental write. If there are deferred statements, then they already
 *       have BigdataValue objects binding their term identifiers. When we
 *       process the deferred statements we should only be assigning term
 *       identifiers for blank nodes -- everything else should already have its
 *       term identifier assigned for the deferred statements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <F>
 *            The generic type of the source {@link Statement} added to the
 *            buffer by the callers.
 * @param <G>
 *            The generic type of the {@link BigdataStatement}s stored in the
 *            buffer.
 */
abstract public class AbstractStatementBuffer<F extends Statement, G extends BigdataStatement>
        implements IStatementBuffer<F> {

    protected static final Logger log = Logger.getLogger(AbstractStatementBuffer.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
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
    protected final boolean readOnly;

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
     * <p>
     * Note: This is allocated lazily so that we can implement
     * {@link IStatementBuffer#setBNodeMap(Map)}
     */
    private Map<String, BigdataBNode> bnodes = null;

    /**
     * The database from the ctor.
     */
    public AbstractTripleStore getDatabase() {
        
        return db;
        
    }

    /**
     * Note: Returns the same value as {@link #getDatabase()} since the
     * distinction is not captured by this class. This MUST be overriden in
     * derived classes which make this distinction.
     */
    public AbstractTripleStore getStatementStore() {

        return db;
        
    }

    /**
     * The {@link ValueFactory} for {@link Statement}s and {@link Value}s
     * created by this class.
     */
    public BigdataValueFactory getValueFactory() {

        return valueFactory;
        
    }
    
    final private BigdataValueFactory valueFactory;
    
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
     */
    @SuppressWarnings("unchecked")
    public AbstractStatementBuffer(AbstractTripleStore db, boolean readOnly, int capacity) {

        if (db == null)
            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();
        
        this.capacity = capacity;
        
        this.db = db;
        
        this.readOnly = readOnly;

        this.valueBuffer = new BigdataValue[capacity * 3];

        this.statementBuffer = (G[])new BigdataStatement[capacity];
        
//        this.distinctValues = new HashMap<Value, BigdataValue>(capacity
//                * IRawTripleStore.N);
        this.distinctValues = new HashMap<Value, BigdataValue>(capacity
                * db.getSPOKeyArity());

        this.deferredStatementBuffer = db.getStatementIdentifiers() ? new LinkedList<G>()
                : null;
        
        this.valueFactory = db.getValueFactory();

    }

    public void setBNodeMap(Map<String, BigdataBNode> bnodes) {
    
        if (bnodes == null)
            throw new IllegalArgumentException();
        
        if (this.bnodes != null)
            throw new IllegalStateException();
        
        this.bnodes = bnodes;
        
    }
        
    /**
     * Return a canonical {@link BigdataValue} instance representing the given
     * <i>value</i>. The scope of the canonical instance is until the next
     * internal buffer overflow ({@link URI}s and {@link Literal}s) or until
     * {@link #flush()} ({@link BNode}s, since blank nodes are global for a
     * given source). The purpose of the canonicalizing mapping is to reduce the
     * buffered {@link BigdataValue}s to the minimum variety required to
     * represent the buffered {@link BigdataStatement}s, which improves
     * throughput significantly (40%) when resolving terms to the corresponding
     * term identifiers using the {@link LexiconRelation}.
     * <p>
     * Note: This is not a true canonicalizing map when statement identifiers
     * are used since values used in deferred statements will be held over until
     * the buffer is {@link #flush()}ed. This relaxation of the canonicalizing
     * mapping is not a problem since the purpose of the mapping is to provide
     * better throughput and nothign relies on a pure canonicalization of the
     * {@link Value}s.
     * 
     * @param value
     *            A value.
     * 
     * @return The corresponding canonical {@link BigdataValue} for the target
     *         {@link BigdataValueFactory}. This will be <code>null</code>
     *         iff the <i>value</i> is <code>null</code> (allows for the
     *         context to be undefined).
     */
    protected BigdataValue convertValue(Value value) {

        if (value == null)
            return null;

        if (value instanceof BNode) {

            /*
             * Handle blank nodes. The scope of blank nodes is always the scope
             * of the source "document".
             * 
             * @todo Could use a BNodeContextFactory instead and then just use
             * asValue(value) for everything in this method!
             */

            final String id = ((BNode) value).getID();

            if (bnodes == null) {

                // allocate map lazily.
                bnodes = new HashMap<String, BigdataBNode>(capacity);

            }
            
            BigdataBNode b = bnodes.get(id);

            if (b == null) {

                b = valueFactory.createBNode(id);

                // add to canonical map (global scope).
                bnodes.put(id, b);
                
                // Note: DO NOT add to value[]!
                
            }

            return b;
            
        } else {

            /*
             * Handle URIs and Literals.
             */
            
            BigdataValue b = distinctValues.get(value);

            if (b == null) {

                if (value instanceof URI) {

                    b = valueFactory.asValue(value);

                } else if (value instanceof Literal) {

                    b = valueFactory.asValue(value);

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
    
//    public void add(int n, F[] a) {
//
//        for (int i = 0; i < n; i++) {
//
//            add(a[i]);
//
//        }
//
//    }

    /**
     * Imposes a canonical mapping on the subject, predicate, and objects of the
     * given {@link Statement}s and stores a new {@link BigdataStatement}
     * instance in the internal buffer. If the given statement is a
     * {@link BigdataStatement} then its {@link StatementEnum} will be used.
     * Otherwise the new statement will be {@link StatementEnum#Explicit}.
     * <p>
     * Note: Unlike the {@link Value}s, a canonicalizing mapping is NOT imposed
     * for the statements. This is because, unlike the {@link Value}s, there
     * tends to be little duplication in {@link Statement}s when processing
     * RDF.
     */
    @SuppressWarnings("unchecked")
    public void add(F e) {

        if (nstmts == capacity) {
            
            overflow();
            
        }
        
        final G stmt = (G) getValueFactory().createStatement(//
                (BigdataResource) convertValue(e.getSubject()), //
                (BigdataURI)      convertValue(e.getPredicate()), //
                    convertValue(e.getObject()), //
                (BigdataResource) convertValue(e.getContext()),
                (e instanceof BigdataStatement ? ((BigdataStatement) e)
                        .getStatementType() : StatementEnum.Explicit));

        if (deferredStatementBuffer != null
                && (e.getSubject() instanceof BNode || e.getObject() instanceof BNode)) {

            /*
             * When statement identifiers are enabled and a blank node
             * appears in the statement, then we add the statement to a
             * collection of statements whose processing must be deferred
             * until
             * 
             * Note: blank nodes do not appear in the predicate position.
             */
            
            if(INFO) {
                
                log.info("deferred: " + stmt);

            }

            deferredStatementBuffer.add(stmt);

        } else {

            if (INFO) {

                log.info("added=" + stmt);
                
            }
            
            statementBuffer[nstmts++] = stmt;
            
        }
        
    }
    
    @SuppressWarnings("unchecked")
    public void add(Resource s, URI p, Value o) {

        add((F)new StatementImpl(s,p,o));
        
    }

    @SuppressWarnings("unchecked")
    public void add(Resource s, URI p, Value o, Resource c) {
        
        add((F)new ContextStatementImpl(s,p,o,c));
        
    }

    @SuppressWarnings("unchecked")
    public void add(Resource s, URI p, Value o, Resource c, StatementEnum type) {
        
        final G stmt = (G)  getValueFactory().createStatement(//
                (BigdataResource) convertValue(s), //
                (BigdataURI)      convertValue(p), //
                                  convertValue(o), //
                (BigdataResource) convertValue(c), //
                type);
        
        add((F)stmt); 
        
    }

    /**
     * Efficiently resolves/adds term identifiers for the buffered
     * {@link BigdataValue}s.
     * <p>
     * If {@link #readOnly}), then the term identifier for unknown values
     * will remain {@link IRawTripleStore#NULL}.
     */
    protected void processBufferedValues() {
        
        if (INFO)
            log.info("nvalues=" + nvalues);

        db.getLexiconRelation().addTerms(valueBuffer, nvalues, readOnly);

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

        if (INFO)
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
    final protected void overflow() {

        if (INFO)
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

        // update the counter.
        counter += handleProcessedStatements(a);
        
        // clear the buffered values and statements.
        clear();
        
    }

    /**
     * Invoked by {@link #overflow()}.
     * 
     * @param a
     *            An array of processed {@link BigdataStatement}s.
     * 
     * @return The delta that will be added to the {@link #counter} reported by
     *         {@link #flush()}.
     */
    abstract protected int handleProcessedStatements(G[] a);

    /**
     * Converts any buffered statements and any deferred statements and then
     * invokes {@link #overflow()} to flush anything remaining in the buffer.
     * 
     * @return The total #of converted statements processed so far. (The counter
     *         is reset to zero as a side-effect.)
     */
    public long flush() {

        if(INFO)
            log.info("");

        processBufferedValues();

        processDeferredStatements();

        // force everything in the buffer to be processed immediately.
        overflow();

        // save a copy of the counter before it gets cleared.
        final long n = counter;

        // reset all state.
        reset();

        return n;

    }
    private long counter = 0L;

    /**
     * Discards all state (term map, bnodes, deferred statements, the buffered
     * statements, and the counter whose value is reported by {@link #flush()}).
     */
    public void reset() {

        if(INFO)
            log.info("");
        
        bnodes = null;
        
        if (deferredStatementBuffer != null)
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

        if (INFO)
            log.info("");

        distinctValues.clear();

        nstmts = nvalues = 0;

    }
        
//    /**
//     * Provides an iterator for reading resolved statements. The source (
//     * {@link #add(Statement)}ing {@link Statement}s to this buffer) and the
//     * sink (consuming the {@link #iterator()}) MUST be different threads. This
//     * class may also be used to load data into a database simply by passing the
//     * {@link #iterator()} into
//     * {@link IRawTripleStore#addStatements(IChunkedOrderedIterator, IElementFilter)}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <F>
//     *            The generic type of the source {@link Statement} added to the
//     *            buffer by the callers.
//     * @param <G>
//     *            The generic type of the {@link BigdataStatement}s stored in
//     *            the buffer.
//     */
//    public static class StatementResolvingBuffer<F extends Statement, G extends BigdataStatement>
//            extends AbstractStatementBuffer<F, G> {
//
//        /**
//         * @param db
//         *            The database against which the {@link Value}s will be
//         *            resolved (or added). If this database supports statement
//         *            identifiers, then statement identifiers for the converted
//         *            statements will be resolved (or added) to the lexicon.
//         * @param readOnly
//         *            When <code>true</code>, {@link Value}s (and statement
//         *            identifiers iff enabled) will be resolved against the
//         *            {@link LexiconRelation}, but entries WILL NOT be inserted
//         *            into the {@link LexiconRelation} for unknown {@link Value}s
//         *            (or for statement identifiers for unknown
//         *            {@link Statement}s when statement identifiers are
//         *            enabled).
//         * @param capacity
//         *            The capacity of the backing buffer.
//         */
//        public StatementResolvingBuffer(AbstractTripleStore db,
//                boolean readOnly, int capacity) {
//
//            super(db, readOnly, capacity);
//
//            this.itr = new OutputIterator();
//            
//        }
//
//        /**
//         * Adds converted statements to the output iterator.
//         */
//        @Override
//        protected int handleProcessedStatements(G[] a){
//
//            itr.add(a);
//            
//            return a.length;
//
//        }
//        
//        /**
//         * An asynchronous iterator singleton that reads from the converted
//         * statements. {@link BigdataStatement}s are made available to the
//         * iterator in chunks each time the buffer {@link #overflow()}s and
//         * when it is {@link #flush()}ed. The iterator will block until more
//         * {@link BigdataStatement}s are available or until it is
//         * {@link ICloseableIterator#close()}d. The iterator is safe for
//         * reading by a single thread. The iterator does NOT support removal.
//         */
//        synchronized public ICloseableIterator<G> iterator() {
//            
//            return itr;
//            
//        }
//        private final OutputIterator itr;
//
//        /**
//         * Extended to ensure that the {@link OutputIterator} is closed.
//         */
//        protected void finalize() throws Throwable {
//            
//            super.finalize();
//            
//            itr.close();
//            
//        }
//        
//        /**
//         * Iterator for converted {@link BigdataStatement}s.
//         * 
//         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//         * @version $Id$
//         */
//        private class OutputIterator implements ICloseableIterator<G> {
//
//            /**
//             * <code>true</code> iff the iterator is open. When open, the
//             * iterator will block if the {@link #queue} is empty.
//             * <p>
//             * Note: Access to {@link #open} should be synchronized on <i>this</i>
//             * so that concurrent changes made in other threads will be visible.
//             */
//            private boolean open;
//
//            /**
//             * Blocking queue containing chunks of converted
//             * {@link BigdataStatement}s.
//             * <p>
//             * Note: The queue has an unbounded capacity. In practice, a bounded
//             * and small (~5-10) capacity would be just fine since we normally
//             * expect the conversion process to be more expensive than the
//             * processing consuming this iterator. However, the unbounded queue
//             * makes the {@link AbstractStatementBuffer} non-blocking, which seems
//             * to be worthwhile.
//             */
//            private final BlockingQueue<G[]> queue = new LinkedBlockingQueue<G[]>();
//            
//            /**
//             * The current chunk of converted {@link BigdataStatement}s from
//             * the {@link #queue}.
//             */
//            private G[] chunk = null;
//            
//            /**
//             * The index of the next element in {@link #chunk} to be delivered
//             * by the iterator.
//             */
//            private int index = 0;
//            
//            public OutputIterator() {
//                
//            }
//
//            /**
//             * Adds the chunk to the {@link #queue}. The elements in the chunk
//             * will be processed when that chunk is popped off of the
//             * {@link #queue}.
//             * 
//             * @param chunk
//             *            A chunk of converted {@link BigdataStatement}s.
//             */
//            protected void add(G[] chunk) {
//
//                queue.add(chunk);
//                
//            }
//
//            synchronized public void close() {
//
//                open = false;
//                
//            }
//
//            private void nextChunk() {
//                
//                chunk = null;
//                
//                index = 0;
//                
//                try {
//
//                    chunk = queue.take();
//                    
//                    index = 0;
//                    
//                } catch (InterruptedException e) {
//                    
//                    log.warn("Interrupted - iterator will be closed");
//                    
//                    open = false;
//                    
//                    throw new RuntimeException("Iterator closed by interrupt",
//                            e);
//                    
//                }
//                
//            }
//            
//            public boolean hasNext() {
//
//                while (open && (chunk == null || index == chunk.length)) {
//
//                    // synchronize to make [open] visible.
//                    synchronized(this) {
//                        
//                        nextChunk();
//                        
//                    }
//
//                }
//
//                return open && (chunk != null || index < chunk.length);
//                
//            }
//
//            public G next() {
//                
//                if (!hasNext())
//                    throw new NoSuchElementException();
//                
//                return chunk[index++];
//                
//            }
//
//            public void remove() {
//                
//                throw new UnsupportedOperationException();
//                
//            }
//            
//        }
//
//    }

    /**
     * Loads {@link Statement}s into an RDF database.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <F>
     *            The generic type of the source {@link Statement} added to the
     *            buffer by the callers.
     * @param <G>
     *            The generic type of the {@link BigdataStatement}s stored in
     *            the buffer.
     */
    public static class StatementBuffer2<F extends Statement, G extends BigdataStatement>
            extends AbstractStatementBuffer<F, G> {

        final private AbstractTripleStore statementStore;
        
        /**
         * @param lexiconStore
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
         */
        public StatementBuffer2(AbstractTripleStore lexiconStore, boolean readOnly, int capacity) {
        
            this(lexiconStore, lexiconStore, readOnly, capacity);
            
        }

        /**
         * @param lexiconStore
         *            The database against which the {@link Value}s will be
         *            resolved (or added). If this database supports statement
         *            identifiers, then statement identifiers for the converted
         *            statements will be resolved (or added) to the lexicon.
         * @param statementStore
         *            The database against which the {@link Statement}s will be
         *            resolved (or added).
         * @param readOnly
         *            When <code>true</code>, {@link Value}s (and statement
         *            identifiers iff enabled) will be resolved against the
         *            {@link LexiconRelation}, but entries WILL NOT be inserted
         *            into the {@link LexiconRelation} for unknown {@link Value}s
         *            (or for statement identifiers for unknown
         *            {@link Statement}s when statement identifiers are
         *            enabled). Likewise, when <code>true</code>,
         *            {@link Statement}s will not be written onto the
         *            {@link SPORelation}.
         * @param capacity
         *            The capacity of the backing buffer.
         */
        public StatementBuffer2(AbstractTripleStore lexiconStore,
                AbstractTripleStore statementStore, boolean readOnly,
                int capacity) {
        
            super(lexiconStore, readOnly, capacity);
            
            if (statementStore == null)
                throw new IllegalArgumentException();
            
            this.statementStore = statementStore;
            
        }

        /**
         * From the ctor.
         */
        @Override
        public AbstractTripleStore getStatementStore() {

            return statementStore;

        }

        /**
         * Writes the {@link Statement}s onto the {@link #getStatementStore()}.
         * 
         * @return The #of statements actually written on the statement indices.
         */
        @Override
        protected int handleProcessedStatements(G[] a) {

            if (readOnly) {
             
                return 0;
                
            }

            /*
             * Align BigdataStatement[] to SPO[].
             * 
             * @todo if [SPO#override] is added to ISPO then we do not need to
             * allocate the intermediate SPO array.
             */

            final SPO[] b = new SPO[a.length];

            for (int i = 0; i < a.length; i++) {

                b[i] = new SPO(a[i]);

            }

            // Note: max return is int since upper bound is the array length.
            return (int) statementStore.addStatements(b, b.length);
            
        }

        /**
         * @return The total #of statements written on the
         *         {@link #getStatementStore()}. This will be ZERO (0L) if
         *         <code>readOnly == true</code>.
         */
        public long flush() {
            
            return super.flush();
            
        }
        
    }

}
