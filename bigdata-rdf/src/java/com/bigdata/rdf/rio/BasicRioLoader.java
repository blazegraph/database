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
import org.openrdf.rio.Parser;
import org.openrdf.rio.StatementHandler;
import org.openrdf.rio.rdfxml.RdfXmlParser;

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
    
    public long getStatementsAdded() {
        
        return stmtsAdded;
        
    }
    
    public long getInsertTime() {
        
        return insertTime;
        
    }
    
    public long getInsertRate() {
        
        return (long) 
            ( ((double)stmtsAdded) / ((double)insertTime) * 1000d );
        
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
    
    public void loadRdf( Reader reader, String baseURI ) throws Exception {
        
        Parser parser = new RdfXmlParser();
        
        parser.setVerifyData( false );
        
        BasicStatementHandler stmtHandler = new BasicStatementHandler();
        
        parser.setStatementHandler( stmtHandler );
    
        insertStart = System.currentTimeMillis();
        
        parser.parse( reader, baseURI );
        
        insertTime += System.currentTimeMillis() - insertStart;
        
    }
    
    private class BasicStatementHandler implements StatementHandler
    {

        public BasicStatementHandler() {
            
        }
        
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
