package com.bigdata.rdf.changesets;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.log4j.Logger;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * This is a very simple implementation of a change log.  NOTE: This is not
 * a particularly great implementation.  First of all it ends up storing
 * two copies of the change set.  Secondly it needs to be smarter about
 * concurrency, or maybe we can be smart about it when we do the
 * implementation on the other side (the SAIL connection can just write
 * change events to a buffer and then the buffer can be drained by 
 * another thread that doesn't block the actual read/write operations,
 * although then we need to be careful not to issue the committed()
 * notification before the buffer is drained).
 * 
 * @author mike
 *
 */
public class InMemChangeLog implements IChangeLog {

    protected static final Logger log = Logger.getLogger(InMemChangeLog.class);
    
    /**
     * Running tally of new changes since the last commit notification.
     */
    private final Map<ISPO,IChangeRecord> changeSet = 
        new HashMap<ISPO, IChangeRecord>();
    
    /**
     * Keep a record of the change set as of the last commit.
     */
    private final Map<ISPO,IChangeRecord> committed = 
        new HashMap<ISPO, IChangeRecord>();
    
    /**
     * See {@link IChangeLog#changeEvent(IChangeRecord)}.
     */
    public synchronized void changeEvent(final IChangeRecord record) {
        
        if (log.isInfoEnabled()) 
            log.info(record);
        
        changeSet.put(record.getStatement(), record);
        
    }
    
    /**
     * See {@link IChangeLog#transactionCommited()}.
     */
    public synchronized void transactionCommited() {
    
        if (log.isInfoEnabled()) 
            log.info("transaction committed");
        
        committed.clear();
        
        committed.putAll(changeSet);
        
        changeSet.clear();
        
    }
    
    /**
     * See {@link IChangeLog#transactionAborted()}.
     */
    public synchronized void transactionAborted() {

        if (log.isInfoEnabled()) 
            log.info("transaction aborted");
        
        changeSet.clear();
        
    }
    
    /**
     * Return the change set as of the last commmit point.
     * 
     * @return
     *          a collection of {@link IChangeRecord}s as of the last commit
     *          point
     */
    public Collection<IChangeRecord> getLastCommit() {
        
        return committed.values();
        
    }
    
    /**
     * Return the change set as of the last commmit point, using the supplied
     * database to resolve ISPOs to BigdataStatements.
     * 
     * @return
     *          a collection of {@link IChangeRecord}s as of the last commit
     *          point
     */
    public Collection<IChangeRecord> getLastCommit(final AbstractTripleStore db) {
        
        return resolve(db, committed.values());
        
    }
    
    /**
     * Use the supplied database to turn a set of ISPO change records into
     * BigdataStatement change records.  BigdataStatements also implement
     * ISPO, the difference being that BigdataStatements also contain
     * materialized RDF terms for the 3 (or 4) positions, in addition to just
     * the internal identifiers (IVs) for those terms.
     * 
     * @param db
     *          the database containing the lexicon needed to materialize
     *          the BigdataStatement objects
     * @param unresolved
     *          the ISPO change records that came from IChangeLog notification
     *          events
     * @return
     *          the fully resolves BigdataStatement change records
     */
    private Collection<IChangeRecord> resolve(final AbstractTripleStore db, 
            final Collection<IChangeRecord> unresolved) {
        
        final Collection<IChangeRecord> resolved = 
            new LinkedList<IChangeRecord>();

        // collect up the ISPOs out of the unresolved change records
        final ISPO[] spos = new ISPO[unresolved.size()];
        int i = 0;
        for (IChangeRecord rec : unresolved) {
            spos[i++] = rec.getStatement();
        }
        
        // use the database to resolve them into BigdataStatements
        final BigdataStatementIterator it = 
            db.asStatementIterator(
                    new ChunkedArrayIterator<ISPO>(i, spos, null/* keyOrder */));
        
        /* 
         * the BigdataStatementIterator will produce BigdataStatement objects
         * in the same order as the original ISPO array
         */
        for (IChangeRecord rec : unresolved) {
            
            final BigdataStatement stmt = it.next();
            
            resolved.add(new ChangeRecord(stmt, rec.getChangeAction()));
            
        }
        
        return resolved;
        
    }
    
    
    
}
