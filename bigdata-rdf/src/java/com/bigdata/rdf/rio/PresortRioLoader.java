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
import org.openrdf.model.Statement;
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
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.ITripleStore;

/**
 * Statement handler for the RIO RDF Parser that collects values and statements
 * in batches and inserts ordered batches into the {@link ITripleStore}.  The
 * default batch size is large enough to absorb many ontologies in a single
 * batch.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PresortRioLoader implements IRioLoader, StatementHandler
{

    /**
     * The default buffer size.
     * <p>
     * Note: I am seeing a 1000 tps performance boost at 1M vs 100k for this
     * value.
     */
    static final int DEFAULT_BUFFER_SIZE = 1000000;
    
    /**
     * Terms and statements are inserted into this store.
     */
    protected final AbstractTripleStore store;
    
    /**
     * The RDF syntax to be parsed.
     */
    protected final RDFFormat rdfFormat;

    /**
     * Controls the {@link Parser#setVerifyData(boolean)} option.
     */
    protected final boolean verifyData;
    
    /**
     * The bufferQueue capacity -or- <code>-1</code> if the {@link StatementBuffer}
     * object is signaling that no more buffers will be placed onto the
     * queue by the producer and that the consumer should therefore
     * terminate.
     */
    protected final int capacity;

    /**
     * When true only distinct terms and statements are stored in the buffer.
     */
    protected final boolean distinct;

    long stmtsAdded;
    
    long insertTime;
    
    long insertStart;
    
    Vector<RioLoaderListener> listeners;
    
    /**
     * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    StatementBuffer buffer;

    /**
     * Used as the value factory for the {@link Parser}.
     */
    OptimizedValueFactory valueFac = OptimizedValueFactory.INSTANCE;
    
    /**
     * Sets up parser to load RDF/XML - {@link #verifyData} is NOT enabled.
     * 
     * @param store
     *            The store into which to insert the loaded statements.
     */
    public PresortRioLoader(AbstractTripleStore store) {
        
        this(store, RDFFormat.RDFXML, false /*verifyData*/);
        
    }
    
    /**
     * Sets up parser to load the indicated RDF interchange syntax.
     * 
     * @param store
     *            The store into which to insert the loaded statements.
     * @param rdfFormat
     *            The RDF interchange syntax to be parsed.
     * @param verifyData
     *            Controls the {@link Parser#setVerifyData(boolean)} option.
     */
    public PresortRioLoader(AbstractTripleStore store, RDFFormat rdfFormat,
            boolean verifyData) {

        this(store, rdfFormat, verifyData, DEFAULT_BUFFER_SIZE, true/* distinct */);

    }

    /**
     * Sets up parser to load RDF.
     * 
     * @param store
     *            The store into which to insert the loaded statements.
     * @param rdfFormat
     *            The RDF interchange syntax to be parsed.
     * @param verifyData
     *            Controls the {@link Parser#setVerifyData(boolean)} option.
     * @param capacity
     *            The capacity of the buffer.
     * @param distinct
     *            Whether or not terms and statements are made distinct in the
     *            buffer.
     *            <p>
     *            Note: performance is generally substantially better with
     *            <code>distinct := true</code>.
     */
    public PresortRioLoader(AbstractTripleStore store, RDFFormat rdfFormat,
            boolean verifyData, int capacity, boolean distinct) {

        assert store != null;

        assert rdfFormat != null;
        
        assert capacity > 0;

        this.store = store;
        
        this.rdfFormat = rdfFormat;
        
        this.verifyData = verifyData;
        
        this.capacity = capacity;
        
        this.distinct = distinct;
        
        this.buffer = new StatementBuffer(store, capacity, distinct );
        
    }
    
    public long getStatementsAdded() {
        
        return stmtsAdded;
        
    }
    
    public long getInsertTime() {
        
        return insertTime;
        
    }
    
    public long getTotalTime() {
        
        return insertTime;
        
    }
    
    public long getInsertRate() {
        
        return (long) 
            ( ((double)stmtsAdded) / ((double)getTotalTime()) * 1000d );
        
    }
    
    public void addRioLoaderListener( RioLoaderListener l ) {
        
        if ( listeners == null ) {
            
            listeners = new Vector<RioLoaderListener>();
            
        }
        
        listeners.add( l );
        
    }
    
    public void removeRioLoaderListener( RioLoaderListener l ) {
        
        listeners.remove( l );
        
    }
    
    protected void notifyListeners() {
        
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
     * @param valFactory
     *            The value factory.
     * 
     * @return The parser.
     */
    protected Parser newParser(ValueFactory valFactory) {

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
        
        parser.setVerifyData( verifyData );
        
        parser.setStatementHandler( this );
        
        return parser;

    }
    
//    InputStream rdfStream = getClass().getResourceAsStream(ontology);
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
    
    /**
     * We need to collect two (three including bnode) term arrays and one
     * statement array. These should be buffers of a settable size.
     * <p>
     * Once the term buffers are full (or the data is exhausted), the term
     * arrays should be sorted and batch inserted into the LocalTripleStore.
     * <p>
     * As each term is inserted, its id should be noted in the Value object, so
     * that the statement array is sortable based on term id.
     * <p>
     * Once the statement buffer is full (or the data is exhausted), the
     * statement array should be sorted and batch inserted into the LocalTripleStore.
     * Also the term buffers should be flushed first.
     * 
     * @param reader
     *            the RDF/XML source
     * @param baseURI
     *            The baseURI or "" if none is known.
     */
    public void loadRdf( Reader reader, String baseURI ) throws Exception {
        
        Parser parser = newParser(valueFac);
        
        // Note: reset to that rates reflect load times not clock times.
        insertStart = System.currentTimeMillis();
        insertTime = 0; // clear.
        
        // Note: reset so that rates are correct for each source loaded.
        stmtsAdded = 0;
        
        // Allocate the initial buffer for parsed data.
        if(buffer == null) {
            
            buffer = new StatementBuffer(store,capacity,distinct);
            
        }

        try {

            // Parse the data.
            parser.parse(reader, baseURI);

            // bulk insert the buffered data into the store.
            if(buffer != null) {
                
                buffer.flush();
                
            }

        } catch (RuntimeException ex) {

            log.error("While parsing: " + ex, ex);

            throw ex;
            
        } finally {

            // clear the old buffer reference.
            buffer = null;

        }

        insertTime += System.currentTimeMillis() - insertStart;
        
    }
    
    public void handleStatement( Resource s, URI p, Value o ) {

        // buffer the write (handles overflow).
        buffer.add(s, p, o);

        stmtsAdded++;
        
        if ( stmtsAdded % 100000 == 0 ) {
            
            notifyListeners();
            
        }
        
    }
    
}
