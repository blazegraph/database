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
package com.bigdata.rdf.rio;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import com.bigdata.rdf.store.ITripleStore;

/**
 * Statement handler for the RIO RDF Parser that writes on a
 * {@link StatementBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PresortRioLoader extends BasicRioLoader implements RDFHandler
{

    /**
     * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser (the value is supplied by the ctor). 
     */
    final protected StatementBuffer buffer;

    private boolean flush = true;
    
    public boolean setFlush(boolean newValue) {
        
        boolean ret = this.flush;
        
        this.flush = newValue;
        
        return ret;
        
    }
    
    /**
     * When <code>true</code> the {@link #buffer} will be
     * {@link IStatementBuffer#flush() flushed} to the backing store once the
     * document has been successfully processed by the parser (default is
     * <code>true</code>). When <code>false</code> the caller is
     * responsible for flushing the {@link #buffer}.
     * <p>
     * This behavior MAY be disabled if you want to chain load a bunch of small
     * documents without flushing to the backing store after each document. This
     * can be much more efficient, approximating the throughput for large
     * document loads. However, the caller MUST insure that the {@link #buffer}
     * is flushed if all documents are loaded successfully. If an error occurs
     * during the processing of one or more documents then the entire data load
     * should be discarded by calling {@link ITripleStore#abort()} since data
     * MAY have been written on the store.
     * 
     * @return The current value.
     * 
     * FIXME The use of this flag can introduce unexpected dependencies between
     * source documents since they will use the same canonicalizing mapping for
     * blank nodes and, when statement identifiers are used, statements using
     * blank nodes will be deferred beyond the end of the source document.
     */
    public boolean getFlush() {
        
        return flush;
        
    }
    
    /**
     * Sets up parser to load RDF.
     * 
     * @param buffer
     *            The buffer used to collect, sort, and write statements onto
     *            the database.
     */
    public PresortRioLoader(StatementBuffer buffer) {

        assert buffer != null;
                
        this.buffer = buffer;
        
    }
        
    /**
     * bulk insert the buffered data into the store.
     */
    protected void success() {

        if(buffer != null && flush) {
            
            buffer.flush();
            
        }

    }

    protected void error(Exception ex) {
        
        if(buffer != null) {
            
            // discard all buffered data.
            buffer.clear();
            
        }

        super.error( ex );
        
    }
    
    // Let the caller clear the buffer!!!
//    /**
//     * Clear the buffer.
//     */
//    protected void cleanUp() {
//
//        buffer.clear();
//
//    }

    public RDFHandler newRDFHandler() {
        
        return this;
        
    }

    public void handleStatement( Statement stmt ) {

        if(DEBUG) {
            
            log.debug(stmt);
            
        }
        
        // buffer the write (handles overflow).
        buffer.add( stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext() );

        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
        }
        
    }

    public void endRDF() throws RDFHandlerException {
        
    }

    public void handleComment(String arg0) throws RDFHandlerException {
        
    }

    public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
        
    }

    public void startRDF() throws RDFHandlerException {
        
    }
    
}
