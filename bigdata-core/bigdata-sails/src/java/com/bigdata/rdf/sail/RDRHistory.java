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
package com.bigdata.rdf.sail;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedResolvingIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * This is an experimental feature that captures history using the change log
 * mechanism and RDR. For each relevant change event (subclasses can decide
 * relevance), a new triple is added to the database, where the subject is the
 * original statement, the predicate is the change action (added or removed),
 * and the object is the commit time.
 * 
 * <pre>
 * {@code
 * << <s> <p> <o> >> <added>/<removed> "commitTime"^^xsd:long .
 * }
 * </pre>
 */
public class RDRHistory implements IChangeLog {
    
    private static final Logger log = Logger.getLogger(RDRHistory.class);
    

    /**
     * Vocab terms to use for the "added" and "removed" predicates.
     */
    public interface Vocab {
    
        URI ADDED = new URIImpl("blaze:history:added"); 
                
        URI REMOVED = new URIImpl("blaze:history:removed"); 
        
    }            
    
    /**
     * The database.
     */
    protected final AbstractTripleStore database;
    
    /**
     * The temp store used to hold change records until the commit.
     */
    protected TempTripleStore tempStore = null;
    
    /**
     * The spo buffer backed by the temp store.
     */
    protected Buffer buffer = null;
    
    /**
     * IV for the "added" term.
     */
    private IV<?,?> added = null;
    
    /**
     * IV for the "removed" term.
     */
    private IV<?,?> removed = null;
    
    /**
     * Dummy timestamp to use as change events come in.  Will be replaced by
     * actual commit time when the transaction is committed.
     */
    private IV<?,?> nullTime = null;
    
    public RDRHistory(final AbstractTripleStore database) {
        
        if (!database.isStatementIdentifiers()) {
            throw new IllegalArgumentException("database must be in sids mode");
        }
        
        this.database = database;
    }
    
    /**
     * Give subclasses the opportunity for any initialization, including
     * obtaining the IVs for the "added" and "removed" terms.
     */
    @SuppressWarnings("rawtypes")
    public void init() {
        
        try {
            
            final IV<?,?>[] ivs = resolveTerms(
                    new URI[] { Vocab.ADDED, Vocab.REMOVED });
            added = ivs[0];
            removed = ivs[1];
            
            nullTime = new XSDNumericIV(0l);
            
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
    }
    
    /**
     * Helper method to resolve added and removed terms.
     */
    protected IV<?,?>[] resolveTerms(final URI[] terms) throws Exception {
        
        final BigdataValueFactory vf = database.getValueFactory();
        
        final BigdataValue[] values = new BigdataValue[terms.length];
        for (int i = 0; i < terms.length; i++) {
            values[i] = vf.asValue(terms[i]);
        }
        
        database.addTerms(values);
        
        final IV<?,?>[] ivs = new IV[terms.length];
        for (int i = 0; i < values.length; i++) {
            ivs[i] = values[i].getIV();
        }
        
        return ivs;
        
    }
    
    /**
     * Stolen from {@link TruthMaintenance#newTempTripleStore()}.
     */
    public TempTripleStore newTempTripleStore() {

        final Properties properties = database.getProperties();

        // // turn off justifications for the tempStore.
        // properties.setProperty(Options.JUSTIFY, "false");

        // turn off the lexicon since we will only use the statement indices.
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.LEXICON,
                "false");

        // only keep the SPO index
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.ONE_ACCESS_PATH,
                "true");
        
        /*
         * @todo MikeP : verify that turning off the bloom filter on the
         * temporary triple store is a good idea. we might actually benefit from
         * it substantially.
         */
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.BLOOM_FILTER,
                "false");
        
        /*
         * Turn off history for the temp store.
         */
        properties.setProperty(
                com.bigdata.rdf.store.AbstractTripleStore.Options.RDR_HISTORY_CLASS,
                "");
        
        final TempTripleStore tempStore = new TempTripleStore(database
                .getIndexManager().getTempStore(), properties, database);

        return tempStore;
        
    }

    
    /**
     * Write the change record to the temp store.
     */
    @Override
    public void changeEvent(final IChangeRecord record) {

        if (log.isTraceEnabled()) {
            log.trace(record);
        }
        
        /*
         * Avoid recursion.  We do not keep history of the history.
         */
        {
            final IV<?,?> p = record.getStatement().p();
            if (added.equals(p) || removed.equals(p)) {
                return;
            }
        }
        
        /*
         * Give subclasses a chance to decide what history is relevant.
         */
        if (!accept(record)) {
            return;
        }

        final ChangeAction action = record.getChangeAction();
        if (!(action == ChangeAction.INSERTED ||
                action == ChangeAction.REMOVED)) {
            /*
             * We do not log truth maintenance updates.
             */
            return;
        }
        final IV<?,?> p = action == ChangeAction.INSERTED ? added : removed;
                
        @SuppressWarnings("rawtypes")
        final SidIV sid = new SidIV(record.getStatement());
        
        /*
         * Use the null time (0l) until we have the commit time. 
         */
        final ISPO spo = new SPO(sid, p, nullTime, StatementEnum.Explicit);
        
        if (log.isTraceEnabled()) {
            log.trace(spo);
        }
        
        /*
         * Drop it on the buffer.
         */
        getOrCreateBuffer().add(spo);
        
    }
    
    /**
     * Subclasses can override this to only record history on certain change
     * events.
     * 
     * @param record
     *          change event
     * @return
     *          true if history should be recorded
     */
    protected boolean accept(final IChangeRecord record) {
        return true;
    }

    /**
     * Get the buffer, create it if necessary.
     */
    protected Buffer getOrCreateBuffer() {
        
        if (buffer == null) {
            
            if (log.isInfoEnabled()) {
                log.info("starting rdr history");
            }
            
            tempStore = newTempTripleStore();
            buffer = new Buffer();
        }
        
        return buffer;
        
    }

    /**
     * Noop.
     */
    @Override
    public void transactionBegin() {
    }
    
    /**
     * Noop.
     */
    @Override
    public void transactionPrepare() {
    }

    /**
     * Copy the statements from the temp store into the database, replacing the
     * object position of each statement with the commit time, then close
     * out the temp store.
     */
    @Override
    public void transactionCommited(final long commitTime) {
        
        if (log.isDebugEnabled()) {
            log.debug("commit time: " + commitTime);
        }
        
        if (buffer == null) {
            /*
             * Nothing written to the buffer.
             */
            return;
        }
        
        try {
            
            buffer.flush();
            
            if (log.isDebugEnabled()) {
                log.debug("# of adds: " + buffer.counter);
            }
            
            final RemoveBuffer removes = new RemoveBuffer();
            
            final IChunkedOrderedIterator<ISPO> it =
                    tempStore.getAccessPath(SPOKeyOrder.SPO).iterator();
            
            /*
             * If we get two SPOs in a row with the same S we know we have two
             * change events that cancel each other (add and remove of the same 
             * thing) and we can remove both from the temp store .  
             */
            ISPO last = null;
            while (it.hasNext()) {
                final ISPO curr = it.next();
                if (last != null && last.s().equals(curr.s())) {
                    removes.add(last);
                    removes.add(curr);
                    last = null;
                } else {
                    last = curr;
                }
            }
            
            removes.flush();
            
            if (log.isDebugEnabled()) {
                log.debug("# of removes: " + removes.counter);
            }
            
            if ((buffer.counter - removes.counter) == 0) {
                /*
                 * Nothing written (net) to the temp store.
                 */
                return;
            }
            
            /*
             * This will create an inline IV for the commit time.
             */
            final IV<?,?> timestamp = database.addTerm(
                    database.getValueFactory().createXSDDateTime(commitTime));
            
            /*
             * Copy the temp store into the database, replacing the null time
             * term with the commit time along the way.
             */
            database.addStatements(new ChunkedResolvingIterator<ISPO,ISPO>(
                    tempStore.getAccessPath(SPOKeyOrder.SPO).iterator()) {
    
                /**
                 * Replace the null time in the object position with the commit
                 * time.
                 */
                @Override
                protected ISPO resolve(final ISPO spo) {
                    
                    final ISPO timestamped = new SPO(
                            spo.s(), spo.p(), timestamp, 
                            StatementEnum.Explicit);
                    
                    if (log.isTraceEnabled()) {
                        log.trace(spo);
                        log.trace(timestamped);
                    }
                    
                    return timestamped;
                }
                
            }, null);
            
            database.commit();
            
        } finally {
        
            close();
            
        }
        
    }

    /**
     * Close out the temp store.
     */
    @Override
    public void transactionAborted() {

        close();
        
    }
    
    /**
     * Close out the temp store.
     */
    @Override
    public void close() {
        
        if (tempStore != null) {

            if (log.isInfoEnabled()) {
                log.info("closing rdr history");
            }
            
            tempStore.close();
            tempStore = null;
            buffer.reset();
            buffer = null;
        }
        
    }
    
    /**
     * SPO buffer backed by the temp store.
     */
    private class Buffer extends AbstractArrayBuffer<ISPO> {

        /*
         * Use a default capacity of 10000.
         */
        private static final int capacity = 10000;
        
        private long counter = 0;
        
        public Buffer() {
            super(capacity, ISPO.class, null);
        }

        @Override
        protected long flush(final int n, final ISPO[] a) {
            final long l = tempStore.addStatements(a, n);
            counter += l;
            return l;
        }

    }

    /**
     * SPO buffer backed by the temp store.
     */
    private class RemoveBuffer extends AbstractArrayBuffer<ISPO> {

        /*
         * Use a default capacity of 10000.
         */
        private static final int capacity = 10000;
        
        private long counter = 0;
        
        public RemoveBuffer() {
            super(capacity, ISPO.class, null);
        }

        @Override
        protected long flush(final int n, final ISPO[] a) {
            final long l = tempStore.removeStatements(
                    new ChunkedArrayIterator<ISPO>(n, a, null), false);
            counter += l;
            return l;
        }

    }

}
