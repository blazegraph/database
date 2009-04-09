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
 * Created on Jan 27, 2007
 */

package com.bigdata.rdf.rio;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * A write buffer for absorbing the output of the RIO parser or other
 * {@link Statement} source and writing that output onto an
 * {@link AbstractTripleStore} using the batch API.
 * <p>
 * Note: there is a LOT of {@link Value} duplication in parsed RDF and we get a
 * significant reward for reducing {@link Value}s to only the distinct
 * {@link Value}s during processing. On the other hand, there is little
 * {@link Statement} duplication. Hence we pay an unnecessary overhead if we try
 * to make the statements distinct in the buffer.
 * <p>
 * Note: This also provides an explanation for why neither this class nor writes
 * of SPOs do better when "distinct" statements is turned on - the "Value"
 * objects in that case are only represented by long integers and duplication in
 * their values does not impose a burden on either the heap or the index
 * writers. In contrast, the duplication of {@link Value}s in the
 * {@link StatementBuffer} imposes a burden on both the heap and the index
 * writers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementBuffer implements IStatementBuffer<Statement> {

    final protected static Logger log = Logger.getLogger(StatementBuffer.class);
   
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    protected final long NULL = IRawTripleStore.NULL;
    
    /**
     * Buffer for parsed RDF {@link Value}s.
     */
    protected final BigdataValue[] values;
    
    /**
     * Buffer for parsed RDF {@link Statement}s.
     */
    protected final BigdataStatement[] stmts;

    /**
     * #of valid entries in {@link #values}.
     */
    protected int numValues;
    
    /**
     * #of valid entries in {@link #stmts}.
     */
    protected int numStmts;

    /**
     * @todo consider tossing out these counters - they only add complexity to
     * the code in {@link #handleStatement(Resource, URI, Value, StatementEnum)}.
     */
    protected int numURIs, numLiterals, numBNodes;
    
    /**
     * The #of blank nodes which appear in the context position and zero (0) if
     * statement identifiers are not enabled.
     */
    protected int numSIDs;
    
    /**
     * Map used to filter out duplicate terms.  The use of this map provides
     * a ~40% performance gain.
     */
    final private Map<Value, BigdataValue> distinctTermMap;

    /**
     * A canonicalizing map for blank nodes. This map MUST be cleared before you
     * begin to add statements to the buffer from a new "source" otherwise it
     * will co-reference blank nodes from distinct sources. The life cycle of
     * the map is the life cycle of the document being loaded, so if you are
     * loading a large document with a lot of blank nodes the map will also
     * become large.
     */
    private Map<String, BigdataBNodeImpl> bnodes;
    
    /**
     * Statements which use blank nodes in their {s,p,o} positions must be
     * deferred when statement identifiers are enabled until (a) either the
     * blank node is observed in the context position of a statement; or (b)
     * {@link #flush()} is invoked, indicating that no more data will be loaded
     * from the current source and therefore that the blank node is NOT a
     * statement identifier. This map is used IFF statement identifers are
     * enabled. When statement identifiers are NOT enabled blank nodes are
     * always blank nodes and we do not need to defer statements, only maintain
     * the canonicalizing {@link #bnodes} mapping.
     */
    private Set<BigdataStatement> deferredStmts;

    /**
     * <code>true</code> if statement identifiers are enabled.
     * <p>
     * Note: This is set by the ctor but temporarily overriden during
     * {@link #processDeferredStatements()} in order to reuse the
     * {@link StatementBuffer} for batch writes of the deferred statement as
     * well.
     * 
     * @see AbstractTripleStore#getStatementIdentifiers()
     */
    private boolean statementIdentifiers;
    
    /**
     * When non-<code>null</code> the statements will be written on this
     * store. When <code>null</code> the statements are written onto the
     * {@link #database}.
     */
    private final AbstractTripleStore statementStore;

    /**
     * The optional store into which statements will be inserted when non-<code>null</code>.
     */
    public final AbstractTripleStore getStatementStore() {
        
        return statementStore;
        
    }

    /**
     * The database that will be used to resolve terms. When
     * {@link #statementStore} is <code>null</code>, statements will be
     * written into this store as well.
     */
    protected final AbstractTripleStore database;
    
    /**
     * The database that will be used to resolve terms.  When {@link #getStatementStore()}
     * is <code>null</code>, statements will be written into this store as well.
     */
    public final AbstractTripleStore getDatabase() {
        
        return database;
        
    }

    /**
     * Optional {@link BlockingBuffer} used for asynchronous writes.
     */
    private final BlockingBuffer<ISPO[]> writeBuffer;
    
    protected final BigdataValueFactory valueFactory;
    
    /**
     * The maximum #of Statements, URIs, Literals, or BNodes that the buffer can
     * hold. The minimum capacity is three (3) since that corresponds to a
     * single triple where all terms are URIs.
     */
    protected final int capacity;

    /**
     * When true only distinct terms are stored in the buffer (this is always
     * true since this condition always outperforms the alternative).
     */
    protected final boolean distinct = true;
    
    public boolean isEmpty() {
        
        return numStmts == 0;
        
    }
    
    public int size() {
        
        return numStmts;
        
    }

    /**
     * When invoked, the {@link StatementBuffer} will resolve terms against the
     * lexicon, but not enter new terms into the lexicon. This mode can be used
     * to efficiently resolve terms to {@link SPO}s.
     * 
     * FIXME REFACTOR
     * 
     * @todo Use an {@link IBuffer} pattern can be used to make the statement
     *       buffer chunk-at-a-time. The buffer has a readOnly argument and will
     *       visit SPOs for the source statements. When readOnly, new terms will
     *       not be added to the database.
     * 
     * @todo Once we have the {@link SPO}s we can just feed them into whatever
     *       consumer we like and do bulk completion, bulk filtering, write the
     *       SPOs onto the database,etc.
     * 
     * @todo must also support the focusStore patterns, which should not be too
     *       difficult.
     */
    public void setReadOnly() {
        
        this.readOnly = true;
        
    }
    
    private boolean readOnly = false;
    
    /**
     * Create a buffer that converts Sesame {@link Value} objects to {@link SPO}s
     * and writes on the <i>database</i> when it is {@link #flush()}ed. This
     * may be used to perform efficient batch write of Sesame {@link Value}s or
     * {@link Statement}s onto the <i>database</i>. If you already have
     * {@link SPO}s then use
     * {@link IRawTripleStore#addStatements(IChunkedOrderedIterator, IElementFilter)} and
     * friends.
     * 
     * @param database
     *            The database into which the terma and statements will be
     *            inserted.
     * @param capacity
     *            The #of statements that the buffer can hold.
     */
    public StatementBuffer(final AbstractTripleStore database,
            final int capacity) {

        this(null/* statementStore */, database, capacity, null/* writeBuffer */);

    }

    /**
     * Create a buffer that writes on a {@link TempTripleStore} when it is
     * {@link #flush()}ed. This variant is used during truth maintenance since
     * the terms are written on the database lexicon but the statements are
     * asserted against the {@link TempTripleStore}.
     * 
     * @param statementStore
     *            The store into which the statements will be inserted
     *            (optional). When <code>null</code>, both statments and
     *            terms will be inserted into the <i>database</i>. This
     *            optional argument provides the ability to load statements into
     *            a temporary store while the terms are resolved against the
     *            main database. This facility is used during incremental
     *            load+close operations.
     * @param database
     *            The database. When <i>statementStore</i> is <code>null</code>,
     *            both terms and statements will be inserted into the
     *            <i>database</i>.
     * @param capacity
     *            The #of statements that the buffer can hold.
     */
    public StatementBuffer(final TempTripleStore statementStore,
            final AbstractTripleStore database, final int capacity) {
        
        this(statementStore, database, capacity, null/* writeBuffer */);
        
    }
     
    /**
     * Core impl.
     * 
     * @param statementStore
     *            The store into which the statements will be inserted
     *            (optional). When <code>null</code>, both statments and
     *            terms will be inserted into the <i>database</i>. This
     *            optional argument provides the ability to load statements into
     *            a temporary store while the terms are resolved against the
     *            main database. This facility is used during incremental
     *            load+close operations.
     * @param database
     *            The database. When <i>statementStore</i> is <code>null</code>,
     *            both terms and statements will be inserted into the
     *            <i>database</i>.
     * @param capacity
     *            The #of statements that the buffer can hold.
     * @param writeBuffer
     *            When non-<code>null</code>, the {@link ISPO}[] chunks
     *            will be written onto this buffer.
     *            <p>
     *            This option is NOT permitted when statementStore is also
     *            specified. The statementStore is always a local triple store
     *            and there is no reason to pipeline writes onto a local triple
     *            store. Also, since, {@link SPORelation#newWriteBuffer()} is
     *            not parameterized to accept a lexicon other than the one
     *            associated with its container, it can not be used for truth
     *            maintenance where the statementStore must use the database's
     *            lexicon rather than its own.
     */
    public StatementBuffer(final TempTripleStore statementStore,
            final AbstractTripleStore database, final int capacity,
            final BlockingBuffer<ISPO[]> writeBuffer) {
            
        if (database == null)
            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();
        
        this.statementStore = statementStore; // MAY be null.
        
        this.database = database;

        this.valueFactory = database.getValueFactory();
        
        this.capacity = capacity;

        this.writeBuffer = writeBuffer;
        
        values = new BigdataValue[capacity * database.N];
        
        stmts = new BigdataStatement[capacity];

        if (distinct) {

            /*
             * initialize capacity to N times the #of statements allowed. this
             * is the maximum #of distinct terms and would only be realized if
             * each statement used distinct values. in practice the #of distinct
             * terms will be much lower. however, also note that the map will be
             * resized at .75 of the capacity so we want to over-estimate the
             * maximum likely capacity by at least 25% to avoid re-building the
             * hash map.
             */
            
            distinctTermMap = new HashMap<Value, BigdataValue>(capacity * database.N);
            
        } else {
            
            distinctTermMap = null;
            
        }
        
        this.statementIdentifiers = database.getStatementIdentifiers();
        
        if(log.isInfoEnabled()) {
            
            log.info("capacity=" + capacity + ", sids=" + statementIdentifiers
                    + ", statementStore=" + statementStore + ", database="
                    + database);
            
        }
        
    }

    /**
     * Signals the end of a source and causes all buffered statements to be
     * written.
     * <p>
     * Note: The source limits the scope within which blank nodes are
     * co-referenced by their IDs. Calling this method will flush the buffer,
     * cause any deferred statements to be written, and cause the canonicalizing
     * mapping for blank nodes to be discarded.
     * 
     * @todo this implementation always returns ZERO (0).
     */
    public long flush() {
 
        log.info("");

        /*
         * Process deferred statements (NOP unless using statement identifiers).
         */
        processDeferredStatements();

        // flush anything left in the buffer.
        incrementalWrite();
        
        // discard all buffer state (including bnodes and deferred statements).
        reset();
        
        return 0L;

    }
    
    /**
     * Processes the {@link #deferredStmts deferred statements}.
     * <p>
     * When statement identifiers are enabled the processing of statements using
     * blank nodes in their subject or object position must be deferred until we
     * know whether or not the blank node is being used as a statement
     * identifier (blank nodes are not allowed in the predicate position by the
     * RDF data model). If the blank node is being used as a statement
     * identifier then its internal 64-bit identifier will be assigned based on
     * the {s,p,o} triple. If it is being used as a blank node, then the
     * internal identifier is assigned using the blank node ID.
     * <p>
     * Deferred statements are processed as follows:
     * <ol>
     * 
     * <li> Collect all deferred statements whose blank node bindings never show
     * up in the context position of a statement ({@link BigdataBNodeImpl#statementIdentifier}
     * is <code>false</code>). Those blank nodes are NOT statement
     * identifiers so we insert them into the lexicon and the insert the
     * collected statements as well. </li>
     * 
     * <li> The remaining deferred statements are processed in "cliques". Each
     * clique consists of all remaining deferred statements whose {s,p,o} have
     * become fully defined by virtue of a blank node becoming bound as a
     * statement identifier. A clique is collected by a full pass over the
     * remaining deferred statements. This process repeats until no statements
     * are identified (an empty clique or fixed point). </li>
     * 
     * </ol>
     * If there are remaining deferred statements then they contain cycles. This
     * is an error and an exception is thrown.
     * 
     * FIXME write unit tests for the edge cases.
     * 
     * @todo on each {@link #flush()}, scan the deferred statements for those
     *       which are fully determined (bnodes are flagged as statement
     *       identifiers) to minimize the build up for long documents?
     */
    protected void processDeferredStatements() {

        if (!statementIdentifiers || deferredStmts == null
                || deferredStmts.isEmpty()) {

            // NOP.
            
            return;
            
        }

        if (log.isInfoEnabled())
            log.info("processing " + deferredStmts.size()
                    + " deferred statements");

//        incrementalWrite();
        
        try {
            
            // Note: temporary override - clear by finally{}.
            statementIdentifiers = false;
            
            // stage 1.
            {
                
                final int nbefore = deferredStmts.size();
                
                int n = 0;
                
                final Iterator<BigdataStatement> itr = deferredStmts.iterator();
                
                while(itr.hasNext()) {
                    
                    final BigdataStatement stmt = itr.next();
                    
                    if (stmt.getSubject() instanceof BNode
                            && ((BigdataBNodeImpl) stmt.getSubject()).statementIdentifier)
                        continue;

                    if (stmt.getObject() instanceof BNode
                            && ((BigdataBNodeImpl) stmt.getObject()).statementIdentifier)
                        continue;

                    if(DEBUG) {
                        log.debug("grounded: "+stmt);
                    }

                    // fully grounded so add to the buffer.
                    add(stmt);
                    
                    // the statement has been handled.
                    itr.remove();
                    
                    n++;
                    
                }
                
                if (log.isInfoEnabled())
                    log.info(""+ n
                                + " out of "
                                + nbefore
                                + " deferred statements used only blank nodes (vs statement identifiers).");
                
                /*
                 * Flush everything in the buffer so that the blank nodes that
                 * are really blank nodes will have their term identifiers
                 * assigned.
                 */
                
                incrementalWrite();
                
            }
            
            // stage 2.
            if(!deferredStmts.isEmpty()) {
                
                int nrounds = 0;
                
                while(true) {

                    nrounds++;
                    
                    final int nbefore = deferredStmts.size();
                    
                    final Iterator<BigdataStatement> itr = deferredStmts.iterator();
                    
                    while(itr.hasNext()) {
                        
                        final BigdataStatement stmt = itr.next();

                        if (stmt.getSubject() instanceof BNode
                                && ((BigdataBNodeImpl) stmt.getSubject()).statementIdentifier
                                && stmt.s() == NULL)
                            continue;

                        if (stmt.getObject() instanceof BNode
                                && ((BigdataBNodeImpl) stmt.getObject()).statementIdentifier
                                && stmt.o() == NULL)
                            continue;

                        if(DEBUG) {
                            log.debug("round="+nrounds+", grounded: "+stmt);
                        }
                        
                        // fully grounded so add to the buffer.
                        add(stmt);
                        
                        // deferred statement has been handled.
                        itr.remove();
                        
                    }
                    
                    final int nafter = deferredStmts.size();

                    if (log.isInfoEnabled())
                        log.info("round=" + nrounds+" : #before="+nbefore+", #after="+nafter);
                    
                    if(nafter == nbefore) {
                    
                        if (log.isInfoEnabled())
                            log.info("fixed point after " + nrounds
                                    + " rounds with " + nafter
                                    + " ungrounded statements");
                        
                        break;
                        
                    }
                    
                    /*
                     * Flush the buffer so that we can obtain the statement
                     * identifiers for all statements in this clique.
                     */
                    
                    incrementalWrite();
                    
                } // next clique.
                
                final int nremaining = deferredStmts.size();

                if (nremaining > 0) {

                    throw new StatementCyclesException(
                            "" + nremaining
                            + " statements can not be grounded");
                    
                }
                
                
            } // stage 2.

        } finally {

            // Note: restore flag!
            statementIdentifiers = true;

            deferredStmts = null;
            
        }
        
    }
    
    /**
     * Clears all buffered data, including the canonicalizing mapping for blank
     * nodes and deferred provenance statements.
     */
    public void reset() {
        
        log.info("");
        
        _clear();
        
        /*
         * Note: clear the reference NOT the contents of the map! This makes it
         * possible for the caller to reuse the same map across multiple
         * StatementBuffer instances.
         */

        bnodes = null;
        
        deferredStmts = null;
        
    }
    
    /**
     * @todo could be replaced with {@link BigdataValueFactory
     */
    public void setBNodeMap(Map<String, BigdataBNodeImpl> bnodes) {
    
        if (bnodes == null)
            throw new IllegalArgumentException();
        
        if (this.bnodes != null)
            throw new IllegalStateException();
        
        this.bnodes = bnodes;
        
    }
    
    /**
     * Invoked by {@link #incrementalWrite()} to clear terms and statements
     * which have been written in preparation for buffering more writes. This
     * does NOT discard either the canonicalizing mapping for blank nodes NOR
     * any deferred statements.
     */
    protected void _clear() {
        
        for (int i = 0; i < numValues; i++) {

            values[i] = null;

        }

        for (int i = 0; i < numStmts; i++) {

            stmts[i] = null;

        }

        numURIs = numLiterals = numBNodes = numStmts = numValues = 0;

        numSIDs = 0;
        
        if (distinctTermMap != null) {
            
            distinctTermMap.clear();
            
        }

//        clearBNodeMap();
        
    }
    
    /**
     * Batch insert buffered data (terms and statements) into the store.
     */
    protected void incrementalWrite() {

        if (INFO) {
            log.info("numValues=" + numValues + ", numStmts=" + numStmts);
        }

        // Insert terms (batch operation).
        if (numValues > 0) {
            if (DEBUG) {
                for (int i = 0; i < numValues; i++) {
                    log
                            .debug("adding term: "
                                    + values[i]
                                    + " (termId="
                                    + values[i].getTermId()
                                    + ")"
                                    + ((values[i] instanceof BNode) ? "sid="
                                            + ((BigdataBNodeImpl) values[i]).statementIdentifier
                                            : ""));
                }
            }
            addTerms(values, numValues);
            if (DEBUG) {
                for (int i = 0; i < numValues; i++) {
                    log
                            .debug(" added term: "
                                    + values[i]
                                    + " (termId="
                                    + values[i].getTermId()
                                    + ")"
                                    + ((values[i] instanceof BNode) ? "sid="
                                            + ((BigdataBNodeImpl) values[i]).statementIdentifier
                                            : ""));
                }
            }
        }

        // Insert statements (batch operation).
        if (numStmts > 0) {
            if(DEBUG) {
                for(int i=0; i<numStmts; i++) {
                    log.debug("adding stmt: "+stmts[i]);
                }
            }
            addStatements(stmts, numStmts);
            if(DEBUG) {
                for(int i=0; i<numStmts; i++) {
                    log.debug(" added stmt: "+stmts[i]);
                }
            }
        }
        
        // Reset the state of the buffer (but not the bnodes nor deferred stmts).
        _clear();

    }

    protected void addTerms(final BigdataValue[] terms, final int numTerms) {

        database.getLexiconRelation().addTerms(terms, numTerms, readOnly);
        
    }
    
    /**
     * Add an "explicit" statement to the buffer (flushes on overflow, no
     * context).
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI p, Value o) {
        
        add(s, p, o, null, StatementEnum.Explicit);
        
    }
    
    /**
     * Add an "explicit" statement to the buffer (flushes on overflow).
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     */
    public void add(Resource s, URI p, Value o, Resource c) {
        
        add(s, p, o, c, StatementEnum.Explicit);
        
    }
    
    /**
     * Add a statement to the buffer (core impl, flushes on overflow).
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    public void add(final Resource s, final URI p, final Value o,
            final Resource c, final StatementEnum type) {
        
        if (nearCapacity()) {

            // bulk insert the buffered data into the store.
            incrementalWrite();
            
        }
        
        // add to the buffer.
        handleStatement(s, p, o, c, type);

    }
    
    public void add(final Statement e) {

        add(e.getSubject(), e.getPredicate(), e.getObject(), e.getContext(),
                (e instanceof BigdataStatement ? ((BigdataStatement) e)
                        .getStatementType() : null));

    }

    /**
     * Adds the statements to each index (batch api, NO truth maintenance).
     * <p>
     * Pre-conditions: The {s,p,o} term identifiers for each
     * {@link BigdataStatement} are defined.
     * <p>
     * Note: If statement identifiers are enabled and the context position is
     * non-<code>null</code> then it will be unified with the statement
     * identifier assigned to that statement. It is an error if the context
     * position is a URI (since it can not be unified with the assigned
     * statement identifier). It is an error if the context position is a blank
     * node which is already bound to a term identifier whose value is different
     * from the statement identifier assigned/reported by the {@link #database}.
     * 
     * @param stmts
     *            An array of statements in any order.
     * 
     * @return The #of statements written on the database.
     */
    final protected long addStatements(final BigdataStatement[] stmts,
            final int numStmts) {

        final SPO[] tmp = new SPO[numStmts];

        for (int i = 0; i < tmp.length; i++) {

            final BigdataStatement stmt = stmts[i];
            
            /*
             * Note: context position is not passed when statement identifiers
             * are in use since the statement identifier is assigned based on
             * the {s,p,o} triple.
             */

            final SPO spo = new SPO(stmt);

            if (DEBUG)
                log.debug("adding: " + stmt.toString() + " (" + spo + ")");
            
            if(!spo.isFullyBound()) {
                
                throw new AssertionError("Not fully bound? : " + spo);
                
            }
            
            tmp[i] = spo;

        }
        
        /*
         * When true, we will be handling statement identifiers.
         * 
         * Note: this is based on the flag on the database rather than the flag
         * on the StatementBuffer since the latter is temporarily overriden when
         * processing deferred statements.
         */
        final boolean sids = database.getStatementIdentifiers();
        
        /*
         * Note: When handling statement identifiers, we clone tmp[] to avoid a
         * side-effect on its order so that we can unify the assigned statement
         * identifiers below.
         */
        final long nwritten = writeSPOs(sids ? tmp.clone() : tmp, numStmts);

        if( sids ) {

            /*
             * Unify each assigned statement identifier with the context
             * position on the corresponding statement.
             */

            for (int i = 0; i < numStmts; i++) {
                
                final SPO spo = tmp[i];
                
                final BigdataStatement stmt = stmts[i];

                // verify that the BigdataStatement and SPO are the same triple.
                assert stmt.s() == spo.s;
                assert stmt.p() == spo.p;
                assert stmt.o() == spo.o;
                
                final BigdataResource c = stmt.getContext();
                
                if (c == null)
                    continue;

                if (c instanceof URI) {

                    throw new UnificationException(
                            "URI not permitted in context position when statement identifiers are enabled: "
                                    + stmt);
                    
                }
                
                if( c instanceof BNode) {

                    final long sid = spo.getStatementIdentifier();
                    
                    if(c.getTermId() != NULL) {
                        
                        if (sid != c.getTermId()) {

                            throw new UnificationException(
                                    "Can not unify blankNode "
                                            + c
                                            + "("
                                            + c.getTermId()
                                            + ")"
                                            + " in context position with statement identifier="
                                            + sid + ": " + stmt + " (" + spo
                                            + ")");
                            
                        }
                        
                    } else {
                        
                        // assign the statement identifier.
                        c.setTermId(sid);
                        
                        if(DEBUG) {
                            
                            log.debug("Assigned statement identifier: " + c
                                    + "=" + sid);
                            
                        }

                    }
                    
                }
                
            }
                
        }

        return nwritten;
        
    }

    /**
     * Adds the statements to each index (batch api, NO truth maintenance).
     * 
     * @param stmts
     *            An array of {@link SPO}s
     * 
     * @return The #of statements written on the database.
     * 
     * @see AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)
     */
    protected long writeSPOs(final SPO[] stmts, final int numStmts) {

        final IChunkedOrderedIterator<ISPO> itr = new ChunkedArrayIterator<ISPO>(
                numStmts, stmts, null/* keyOrder */);

        final AbstractTripleStore sink = statementStore != null ? statementStore
                : database;

        if (log.isInfoEnabled()) {

            log.info("writing " + numStmts + " on "
                    + (statementStore != null ? "statementStore" : "databasse")
                    + ": " + statementStore);

        }

        if (writeBuffer != null) {

            // make sure that the SPO[] chunk is dense.
            final SPO[] a;

            if (stmts.length != numStmts) {

                a = new SPO[stmts.length];

                System.arraycopy(stmts, 0/* srcPos */, a, 0/* dstPos */,
                        numStmts);

            } else {

                a = stmts;

            }

            // add to the buffer (asynchronous write on the target).
            writeBuffer.add(a);
            
            /*
             * Note: When using the write buffer we do not know how many
             * statements are written since the writes are asynchronous.
             */
            return 0L;
            
        } else {

            // synchronous write on the target.
            return database
                    .addStatements(sink, false/* copyOnly */, itr, null /* filter */);
        }
        
// if (statementStore != null) {
//
//            // Writing statements on the secondary store.
//
//            if (log.isInfoEnabled()) {
//
//                log.info("writing " + numStmts + " on statementStore: "
//                        + statementStore);
//
//            }
//
//            nwritten = database.addStatements(statementStore,
//                    false/* copyOnly */, itr, null /* filter */);
//
//        } else {
//
//            // Write statements on the primary database.
//
//            if (log.isInfoEnabled()) {
//
//                log.info("writing " + numStmts + " on database: " + database);
//
//            }
//
//            nwritten = database.addStatements(database, false/* copyOnly */,
//                    itr, null /* filter */);
//
//        }
//
//        return nwritten;

    }

    /**
     * Returns true if the bufferQueue has less than three slots remaining for
     * any of the value arrays (URIs, Literals, or BNodes) or if there are no
     * slots remaining in the statements array. Under those conditions adding
     * another statement to the bufferQueue could cause an overflow.
     * 
     * @return True if the bufferQueue might overflow if another statement were
     *         added.
     */
    public boolean nearCapacity() {

        if (numStmts + 1 > capacity)
            return true;

        if (numValues + database.N > values.length)
            return true;

        return false;
        
    }
    
    /**
     * Canonicalizing mapping for a term.
     * <p>
     * Note: Blank nodes are made canonical with the scope of the source from
     * which the data are being read. See {@link #bnodes}. All other kinds of
     * terms are made canonical within the scope of the buffer's current
     * contents in order to keep down the demand on the heap with reading either
     * very large documents or a series of small documents.
     * 
     * @param term
     *            A term.
     * 
     * @return Either the term or the pre-existing term in the buffer with the
     *         same data.
     */
    protected BigdataValue getDistinctTerm(BigdataValue term) {

        assert distinct == true;
        
        if (term instanceof BNode) {

            /*
             * Canonicalizing map for blank nodes.
             * 
             * Note: This map MUST stay in effect while reading from a given
             * source and MUST be cleared (or set to null) before reading from
             * another source.
             */
            
            final BigdataBNodeImpl bnode = (BigdataBNodeImpl)term;
            
            // the BNode's ID.
            final String id = bnode.getID();

            if (bnodes == null) {

                // allocating canonicalizing map for blank nodes.
                bnodes = new HashMap<String, BigdataBNodeImpl>(capacity);

                // insert this blank node into the map.
                bnodes.put(id, bnode);

            } else {

                // test canonicalizing map for blank nodes.
                final BigdataBNode existingBNode = bnodes.get(id);

                if (existingBNode != null) {

                    // return existing blank node with same ID.
                    return existingBNode;

                }

                // insert this blank node into the map.
                bnodes.put(id, bnode);
                
            }
            
            return term;
            
        }
        
        /*
         * Other kinds of terms use a map whose scope is limited to the terms
         * that are currently in the buffer. This keeps down the heap demand
         * when reading very large documents.
         */
        
        final BigdataValue existingTerm = distinctTermMap.get(term);
        
        if(existingTerm != null) {
            
            // return the pre-existing term.
            
            if(log.isDebugEnabled()) {
                
                log.debug("duplicate: "+term);
                
            }
            
            return existingTerm;
            
        }

        // put the new term in the map.
        if (distinctTermMap.put(term, term) != null) {
            
            throw new AssertionError();
            
        }
        
        // return the new term.
        return term;
        
    }
    
    /**
     * Adds the values and the statement into the buffer.
     * 
     * @param s
     *            The subject.
     * @param p
     *            The predicate.
     * @param o
     *            The object.
     * @param c
     *            The context (may be null).
     * @param type
     *            The statement type.
     * 
     * @throws IndexOutOfBoundsException
     *             if the buffer capacity is exceeded.
     * 
     * @see #nearCapacity()
     */
    public void handleStatement( Resource s, URI p, Value o, Resource c, StatementEnum type ) {
        
        s = (Resource) valueFactory.asValue(s);
        p = (URI)      valueFactory.asValue(p);
        o =            valueFactory.asValue(o);
        c = (Resource) valueFactory.asValue(c);
        
        boolean duplicateS = false;
        boolean duplicateP = false;
        boolean duplicateO = false;
        boolean duplicateC = false;
        
        if (distinct) {
            {
                final BigdataValue tmp = getDistinctTerm((BigdataValue) s);
                if (tmp != s) {
                    duplicateS = true;
                }
                s = (Resource) tmp;
            }
            {
                final BigdataValue tmp = getDistinctTerm((BigdataValue) p);
                if (tmp != p) {
                    duplicateP = true;
                }
                p = (URI) tmp;
            }
            {
                final BigdataValue tmp = getDistinctTerm((BigdataValue) o);
                if (tmp != o) {
                    duplicateO = true;
                }
                o = (Value) tmp;
            }
            if (c != null) {
                final BigdataValue tmp = getDistinctTerm((BigdataValue) c);
                if (tmp != c) {
                    duplicateC = true;
                }
                c = (Resource) tmp;
            }
        }

        /*
         * Form the BigdataStatement object now that we have the bindings.
         */

        final BigdataStatement stmt;
        {
          
            stmt = valueFactory
                    .createStatement((BigdataResource) s, (BigdataURI) p,
                            (BigdataValue) o, (BigdataResource) c, type);

            if (statementIdentifiers
                    && (s instanceof BNode || o instanceof BNode)) {

                /*
                 * When statement identifiers are enabled a statement with a
                 * blank node in the subject or object position must be deferred
                 * until the end of the source so that we determine whether it
                 * is being used as a statement identifier or a blank node (if
                 * the blank node occurs in the context position, then we know
                 * that it is being used as a statement identifier).
                 */
                
                if (deferredStmts == null) {

                    deferredStmts = new HashSet<BigdataStatement>(stmts.length);

                }

                deferredStmts.add(stmt);

                if (INFO)
                    log.info("deferred: "+stmt);
                
            } else {

                // add to the buffer.
                stmts[numStmts++] = stmt;

            }
            
        }

        /*
         * Update counters.
         */
        
        if (!duplicateS) {// && ((_Value) s).termId == 0L) {

            if (s instanceof URI) {

                numURIs++;

                values[numValues++] = (BigdataValue) s;

            } else {

                if (!statementIdentifiers) {

                    numBNodes++;

                    values[numValues++] = (BigdataValue) s;

                }
                
            }
            
        }

        if (!duplicateP) {//&& ((_Value) s).termId == 0L) {
            
            values[numValues++] = (BigdataValue)p;

            numURIs++;

        }

        if (!duplicateO) {// && ((_Value) s).termId == 0L) {

            if (o instanceof URI) {

                numURIs++;

                values[numValues++] = (BigdataValue) o;

            } else if (o instanceof BNode) {

                if (!statementIdentifiers) {

                    numBNodes++;

                    values[numValues++] = (BigdataValue) o;

                }

            } else {

                numLiterals++;

                values[numValues++] = (BigdataValue) o;

            }
            
        }

        if (c != null && !duplicateC && ((BigdataValue) s).getTermId() == NULL) {

            if (c instanceof URI) {

                numURIs++;

                values[numValues++] = (BigdataValue) c;

            } else {

                if (!database.getStatementIdentifiers()) {

                    /*
                     * We only let the context node into the buffer when
                     * statement identifiers are disabled for the database.
                     * 
                     * Note: This does NOT test [statementIdentifiers] as that
                     * flag is temporarily overriden when processing deferred
                     * statements.
                     */
                    
                    values[numValues++] = (BigdataValue) c;

                    numBNodes++;

                } else {

                    /*
                     * Flag the blank node as a statement identifier since it
                     * appears in the context position.
                     * 
                     * Note: the blank node is not inserted into values[] since
                     * it is a statement identifier and will be assigned when we
                     * insert the statement rather than based on the blank
                     * node's ID.
                     */

                    ((BigdataBNodeImpl) c).statementIdentifier = true;

                }

            }
            
        }

    }

    /**
     * An instance of this exception is thrown if cycles are detected amoung
     * statements. A cycle can exist only when statement identifiers are enabled
     * and a statement is made either directly about itself or indirectly via
     * one or more statements about itself.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StatementCyclesException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 3506732137721004208L;
        
        public StatementCyclesException(String msg) {
            
            super(msg);
            
        }
        
    }
    
    /**
     * An instance of this exception is thrown when the same blank node appears
     * in the context position of two or more statements having a distinct
     * subject predicate, and object. This is an error because it implies that
     * two statements with different bindings are the same statement.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class UnificationException extends RuntimeException {
        
        /**
         * 
         */
        private static final long serialVersionUID = 6430403508687043789L;

        public UnificationException(String msg) {
            
            super(msg);
            
        }
        
    }

}
