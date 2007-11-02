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

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.StatementHandler;

/**
 * Statement handler for the RIO RDF Parser that writes on a
 * {@link StatementBuffer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PresortRioLoader extends BasicRioLoader implements IRioLoader, StatementHandler
{

    /**
     * Used to buffer RDF {@link Value}s and {@link Statement}s emitted by
     * the RDF parser.
     */
    final IStatementBuffer buffer;

    /**
     * Sets up parser to load RDF.
     * 
     * @param buffer
     *            The buffer used to collect, sort, and write statements onto
     *            the database.
     */
    public PresortRioLoader(IStatementBuffer buffer) {

        assert buffer != null;
                
        this.buffer = buffer;
        
    }
        
    /**
     * bulk insert the buffered data into the store.
     */
    protected void success() {

        if(buffer != null) {
            
            buffer.flush();
            
        }

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

    public StatementHandler newStatementHandler() {
        
        return this;
        
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
