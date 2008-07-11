package com.bigdata.rdf.spo;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * Writes statements on all the statement indices in parallel.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementWriter implements Callable<Long>{

    private final AbstractTripleStore database;
    private final AbstractTripleStore statementStore;
    private final boolean copyOnly;
    private final IChunkedOrderedIterator<SPO> itr;
    
    /**
     * Incremented by the #of statements written on the statements indices.
     */
    public final AtomicLong nwritten;

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
     * 
     * @deprecated by {@link SPORelation#insert(IChunkedOrderedIterator)}? (Still used by 
     * copyStatements)
     */
    public StatementWriter(AbstractTripleStore database,
            AbstractTripleStore statementStore, boolean copyOnly,
            IChunkedOrderedIterator<SPO> itr, AtomicLong nwritten) {
    
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

    }

    /**
     * Writes on the statement indices (parallel, batch api).
     * 
     * @return The elapsed time for the operation.
     */
    public Long call() throws Exception {

        final long begin = System.currentTimeMillis();

        nwritten.addAndGet(database.addStatements(statementStore, copyOnly,
                itr, null/* filter */));

        final long elapsed = System.currentTimeMillis() - begin;

        return elapsed;

    }

}
