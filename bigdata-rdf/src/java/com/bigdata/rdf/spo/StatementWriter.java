package com.bigdata.rdf.spo;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord.ChangeAction;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Writes statements on all the statement indices in parallel.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link SPORelation#insert(IChunkedOrderedIterator)}? (Still
 *             used by copyStatements)
 */
public class StatementWriter implements Callable<Long>{

    protected static final Logger log = Logger.getLogger(StatementWriter.class);
    
    private final AbstractTripleStore database;
    private final AbstractTripleStore statementStore;
    private final boolean copyOnly;
    private final IChunkedOrderedIterator<ISPO> itr;
    
    /**
     * Incremented by the #of statements written on the statements indices.
     */
    public final AtomicLong nwritten;
    
    private final IChangeLog changeLog;
    
    /**
     * @param database
     *            The database. If statement identifiers are being generated
     *            then they will always be added using the lexicon associated
     *            with the database.
     * @param statementStore
     *            The store on which the statements will be written (the focus
     *            store if you are doing truth maintenance and otherwise the
     *            database).
     * @param copyOnly
     *            See
     *            {@link AbstractTripleStore#addStatements(AbstractTripleStore, boolean, IChunkedOrderedIterator, IElementFilter)
     * @param itr
     *            The source iterator for the {@link SPO}s to be written.
     * @param nwritten
     *            Incremented by the #of statements written on the statement
     *            indices as a side-effect.
     * 
     * @todo allow an optional {@link IElementFilter} here.
     */
    public StatementWriter(AbstractTripleStore database,
            AbstractTripleStore statementStore, boolean copyOnly,
            IChunkedOrderedIterator<ISPO> itr, AtomicLong nwritten) {

        this(database, statementStore, copyOnly, itr, nwritten, 
                null/* changeLog */);
        
    }
        
    public StatementWriter(final AbstractTripleStore database,
            final AbstractTripleStore statementStore, final boolean copyOnly,
            final IChunkedOrderedIterator<ISPO> itr, final AtomicLong nwritten,
            final IChangeLog changeLog) {
        
        if (database == null)
            throw new IllegalArgumentException();
        
        if (statementStore == null)
            throw new IllegalArgumentException();
        
        if (itr == null)
            throw new IllegalArgumentException();
        
        if (nwritten == null)
            throw new IllegalArgumentException();
        
        this.database = database;
        
        this.statementStore = statementStore;

        this.copyOnly = copyOnly;

        this.itr = itr;

        this.nwritten = nwritten;
        
        this.changeLog = changeLog;
        
    }

    /**
     * Writes on the statement indices (parallel, batch api).
     * 
     * @return The elapsed time for the operation.
     */
    public Long call() throws Exception {

        final long begin = System.currentTimeMillis();

        final long n;
        
        if (changeLog == null) {
            
            n = database.addStatements(statementStore, copyOnly,
                    itr, null/* filter */);
            
        } else {

            n = com.bigdata.rdf.changesets.StatementWriter.addStatements(
                    database, 
                    statementStore, 
                    copyOnly, 
                    null/* filter */, 
                    itr, 
                    changeLog);
            
        }
        
        nwritten.addAndGet(n);
        
        return System.currentTimeMillis() - begin;

    }
    
}
