/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
import com.sun.swing.internal.plaf.basic.resources.basic;

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
        
        return (long) 
            ( ((double)stmtsAdded) / ((double)insertTime) * 1000d );
        
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

            insertTime += System.currentTimeMillis() - insertStart;

            log.info("parse complete: elapsed="
                    + (System.currentTimeMillis() - insertStart)
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
