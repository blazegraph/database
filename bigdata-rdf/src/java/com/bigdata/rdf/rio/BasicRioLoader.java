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

import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.Vector;

import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import com.bigdata.rdf.model.OptimizedValueFactory;

/**
 * Parses data but does not load it into the indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BasicRioLoader implements IRioLoader {

    long stmtsAdded;
    
    long insertTime;
    
    long insertStart;
    
    Vector<RioLoaderListener> listeners;

    public BasicRioLoader() {
        
    }
    
    final public long getStatementsAdded() {
        
        return stmtsAdded;
        
    }
    
    final public long getInsertTime() {
        
        return insertTime;
        
    }
    
    final public long getInsertRate() {
        
        return (long) ((stmtsAdded * 1000d) / (double) insertTime);
        
    }

    final public void addRioLoaderListener( RioLoaderListener l ) {
        
        if ( listeners == null ) {
            
            listeners = new Vector<RioLoaderListener>();
            
        }
        
        listeners.add( l );
        
    }
    
    final public void removeRioLoaderListener( RioLoaderListener l ) {
        
        listeners.remove( l );
        
    }
    
    final protected void notifyListeners() {
        
        RioLoaderEvent e = new RioLoaderEvent
            ( stmtsAdded,
              System.currentTimeMillis() - insertStart
              );
        
        for ( Iterator<RioLoaderListener> it = listeners.iterator(); 
              it.hasNext(); ) {
            
            it.next().processingNotification( e );
            
        }
        
    }
    
    /**
     * Choose the parser based on the {@link RDFFormat} specified to the
     * constructor.
     * 
     * @return The parser.
     * 
     * @todo reuse parser instances for the same {@link RDFFormat}?
     */
    final protected RDFParser getParser(RDFFormat rdfFormat) {

        final ValueFactory valFactory = OptimizedValueFactory.INSTANCE;
        
        final RDFParser parser = Rio.createParser(rdfFormat, valFactory);
        
        return parser;

    }

    final public void loadRdf(InputStream is, String baseURI,
            RDFFormat rdfFormat, boolean verifyData) throws Exception {

        loadRdf2(is, baseURI, rdfFormat, verifyData);
        
    }
    
    final public void loadRdf(Reader reader, String baseURI,
            RDFFormat rdfFormat, boolean verifyData) throws Exception {
        
        loadRdf2(reader, baseURI, rdfFormat, verifyData);
        
    }

    /**
     * Core implementation.
     * 
     * @param source
     *            A {@link Reader} or {@link InputStream}.
     * @param baseURI
     * @param rdfFormat
     * @param verifyData
     * 
     * @throws Exception
     */
    protected void loadRdf2(Object source, String baseURI,
            RDFFormat rdfFormat, boolean verifyData) throws Exception {
        
        if (source == null)
            throw new IllegalArgumentException();

        if (!(source instanceof Reader) && !(source instanceof InputStream)) {

            throw new IllegalArgumentException();
            
        }
        
        log.info("format="+rdfFormat+", verify="+verifyData);
        
        final RDFParser parser = getParser(rdfFormat);
        
        // FIXME it should be easier to configure these features on the underlying RIO parser.
//        parser.setPreserveBNodeIDs(true);
        
        parser.setVerifyData( verifyData );
        
        parser.setRDFHandler( newRDFHandler() );
    
        // Note: reset to that rates reflect load times not clock times.
        insertStart = System.currentTimeMillis();
        insertTime = 0; // clear.
        
        // Note: reset so that rates are correct for each source loaded.
        stmtsAdded = 0;
                
        try {

            before();

            log.info("Starting parse.");

            // Parse the data.
            if(source instanceof Reader) {

                parser.parse((Reader)source, baseURI);
                
            } else if( source instanceof InputStream){
                
                parser.parse((InputStream)source, baseURI);
                
            } else throw new AssertionError();

            insertTime = System.currentTimeMillis() - insertStart;

            log.info("parse complete: elapsed=" + insertTime
                    + "ms, toldTriples=" + stmtsAdded + ", tps="
                    + getInsertRate());
            
            success();

        } catch (RuntimeException ex) {

            insertTime += System.currentTimeMillis() - insertStart;
            
            log.error("While parsing: " + ex, ex);

            try {
                error(ex);
            } catch (Exception ex2) {
                log.error("Ignoring: " + ex2);
            }
            
            throw ex;
            
        } finally {

            cleanUp();
            
        }
        
    }

    /**
     * Invoked before parse (default is NOP).
     */
    protected void before() {
        
    }
    
    /**
     * Invoked after successful parse (default is NOP).
     */
    protected void success() {
        
    }
    
    /**
     * Invoked if the parse fails (default is NOP).
     */
    protected void error(Exception ex) {
        
    }
    
    /**
     * Invoked from finally clause after parse regardless of success or failure.
     */
    protected void cleanUp() {
        
    }
    
    /**
     * Note: YOU MUST override this method to install a different
     * {@link RDFHandler}. The default is the {@link BasicRDFHandler} which
     * does NOTHING.
     */
    public RDFHandler newRDFHandler() {
        
        return new BasicRDFHandler();
        
    }
    
    private class BasicRDFHandler implements RDFHandler
    {

        public BasicRDFHandler() {
            
        }

        public void endRDF() throws RDFHandlerException {
            
        }

        public void handleComment(String arg0) throws RDFHandlerException {

        }

        public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
            
        }

        /**
         * Counts the #of statements.
         */
        public void handleStatement(Statement arg0) throws RDFHandlerException {
            
            stmtsAdded++;           
            
        }

        public void startRDF() throws RDFHandlerException {
            
        }
        
    }

}
