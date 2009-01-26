package com.bigdata.rdf.load;

import com.bigdata.rdf.rio.StatementBuffer;

/**
 * A factory for {@link StatementBuffer}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatementBufferFactory {

    /**
     * Return the {@link StatementBuffer} to be used for a task.
     */
    public StatementBuffer getStatementBuffer();
    
}