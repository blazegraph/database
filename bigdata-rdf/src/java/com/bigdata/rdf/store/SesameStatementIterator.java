package com.bigdata.rdf.store;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.sesame.sail.StatementIterator;

import com.bigdata.rdf.spo.ISPOIterator;

/**
 * Wraps the raw iterator that traverses a statement index and exposes each
 * visited statement as a {@link Statement} object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SesameStatementIterator implements StatementIterator {

    private final AbstractTripleStore db;
    private final ISPOIterator src;
    
    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator.
     */
    public SesameStatementIterator(AbstractTripleStore db,ISPOIterator src) {

        if (db == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.db = db;
        
        this.src = src;

    }
    
    public boolean hasNext() {
        
        return src.hasNext();
        
    }

    /**
     * Note: Returns instances of {@link StatementWithType}.
     */
    public Statement next() {

        return db.asStatement( src.next() );
        
    }
    
    public void close() {
        
        src.close();
        
    }

}