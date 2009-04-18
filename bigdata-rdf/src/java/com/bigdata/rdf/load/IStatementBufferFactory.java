package com.bigdata.rdf.load;

import org.openrdf.model.Statement;

import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;

/**
 * A factory for {@link StatementBuffer}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatementBufferFactory<S extends Statement> {

    /**
     * Return the {@link StatementBuffer} to be used for a task (some factories
     * will recycle statement buffers, but that is not necessary or implied).
     */
    public IStatementBuffer<S> newStatementBuffer();
    
}