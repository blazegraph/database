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

import java.io.Reader;
import java.util.Iterator;
import java.util.Vector;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.rdfxml.RdfXmlParser;
import org.openrdf.rio.turtle.TurtleParser;
import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.rdf.model.OptimizedValueFactory;

/**
 * Parses data but does not load it into the indices.
 * 
 * @todo do a variant that uses the non-batch, non-bulk apis to load each term
 *       and statement as it is parsed into the indices. this will be
 *       interesting as a point of comparison with the other loaders.
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
    final protected Parser getParser(RDFFormat rdfFormat) {

        final ValueFactory valFactory = OptimizedValueFactory.INSTANCE;
        
        final Parser parser;
        
        if (RDFFormat.RDFXML.equals(rdfFormat)) {
            
            parser = new RdfXmlParser(valFactory);
            
        } else if (RDFFormat.NTRIPLES.equals(rdfFormat)) {
            
            parser = new NTriplesParser(valFactory);
            
        } else if (RDFFormat.TURTLE.equals(rdfFormat)) {
            
            parser = new TurtleParser(valFactory);
            
        } else {
            
            throw new IllegalArgumentException("Format not supported: "
                    + rdfFormat);
            
        }
        
        return parser;

    }
    
//        InputStream rdfStream = getClass().getResourceAsStream(ontology);
//
//    if (rdfStream == null) {
//
//        /*
//         * If we do not find as a Resource then try as a URL.
//         * 
//         */
//        try {
//            
//            rdfStream = new URL(ontology).openConnection().getInputStream();
//            
//        } catch (IOException ex) {
//            
//            ex.printStackTrace(System.err);
//            
//            return false;
//            
//        }
//        
//    }
//
//    rdfStream = new BufferedInputStream(rdfStream);
//
//    ...
//    
//    finally {
//    rdfStream.close();
//    }
//    

    final public void loadRdf(Reader reader, String baseURI,
            RDFFormat rdfFormat, boolean verifyData) throws Exception {
        
        log.info("format="+rdfFormat+", verify="+verifyData);
        
        Parser parser = getParser(rdfFormat);
        
        parser.setVerifyData( verifyData );
        
        parser.setStatementHandler( newStatementHandler() );
    
        // Note: reset to that rates reflect load times not clock times.
        insertStart = System.currentTimeMillis();
        insertTime = 0; // clear.
        
        // Note: reset so that rates are correct for each source loaded.
        stmtsAdded = 0;
                
        try {

            before();

            log.info("Starting parse.");

            // Parse the data.
            parser.parse(reader, baseURI);

            insertTime = System.currentTimeMillis() - insertStart;

            log.info("parse complete: elapsed=" + insertTime
                    + "ms, toldTriples=" + stmtsAdded + ", tps="
                    + getInsertRate());
            
            success();

        } catch (RuntimeException ex) {

            insertTime += System.currentTimeMillis() - insertStart;
            
            log.error("While parsing: " + ex, ex);

            throw ex;
            
        } finally {

            cleanUp();
            
        }
        
    }

    /**
     * NOP.
     */
    protected void before() {
        
    }
    
    /**
     * NOP.
     */
    protected void success() {
        
    }
    
    /**
     * NOP.
     */
    protected void cleanUp() {
        
    }
    
    /**
     * Note: YOU MUST override this method to install a different
     * {@link StatementHandler}. The default is the
     * {@link BasicStatementHandler} which does NOTHING.
     */
    public StatementHandler newStatementHandler() {
        
        return new BasicStatementHandler();
        
    }
    
    private class BasicStatementHandler implements StatementHandler
    {

        public BasicStatementHandler() {
            
        }

        /**
         * Counts the #of statements.
         */
        public void handleStatement( Resource s, URI p, Value o ) {
            
//            if ( s instanceof BNode || 
//                 p instanceof BNode || 
//                 o instanceof BNode ) 
//                return;
            
            // log.info( s + ", " + p + ", " + o );
            
            // store.addStatement( s, p, o );
            
            stmtsAdded++;
            
        }
        
    }

}
