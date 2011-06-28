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

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import com.bigdata.rdf.model.BigdataURI;

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
    final protected IStatementBuffer<?> buffer;

    /**
     * The value that will be used for the graph/context co-ordinate when
     * loading data represented in a triple format into a quad store.
     */
    private BigdataURI defaultGraphURI = null ;

    /**
     * Sets up parser to load RDF.
     * 
     * @param buffer
     *            The buffer used to collect, sort, and write statements onto
     *            the database.
     */
    public PresortRioLoader(final IStatementBuffer<?> buffer) {

        super(buffer.getDatabase().getValueFactory());

        this.buffer = buffer;
        
    }

    /**
     * bulk insert the buffered data into the store.
     */
    protected void success() {

        if (buffer != null) {
            
            buffer.flush();
            
        }

    }

    protected void error(final Exception ex) {
        
        if(buffer != null) {
            
            // discard all buffered data.
            buffer.reset();
            
        }

        super.error( ex );
        
    }
    
    public RDFHandler newRDFHandler() {
        
        defaultGraphURI =   null != defaultGraph && 4 == buffer.getDatabase ().getSPOKeyArity ()
        	              ? buffer.getDatabase ().getValueFactory ().createURI ( defaultGraph )
        			      : null
        			      ;
        return this;
    }

    public void handleStatement( final Statement stmt ) {

        if(log.isDebugEnabled()) {
            
            log.debug(stmt);
            
        }

        Resource graph = stmt.getContext() ;
        if (    null == graph
        	 && null != defaultGraphURI ) // only true when we know we are loading a quad store
        	graph = defaultGraphURI ;
        // buffer the write (handles overflow).
        buffer.add( stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), graph );

        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
        }
        
    }

    public void endRDF() throws RDFHandlerException {
        
        // @todo why not invoke buffer#force()?
        
    }

    public void handleComment(String arg0) throws RDFHandlerException {
        
    }

    public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
        
    }

    public void startRDF() throws RDFHandlerException {
        
    }
    
}
