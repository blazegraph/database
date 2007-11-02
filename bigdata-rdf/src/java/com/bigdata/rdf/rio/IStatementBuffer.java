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
/*
 * Created on Nov 2, 2007
 */

package com.bigdata.rdf.rio;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Abstraction for buffering statements.
 * 
 * @todo review javadoc here and on implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatementBuffer {

    /**
     * The optional store into which statements will be inserted when non-<code>null</code>.
     */
    public AbstractTripleStore getStatementStore();

    /**
     * The database that will be used to resolve terms.  When {@link #getStatementStore()}
     * is <code>null</code>, statements will be written into this store as well.
     */
    public AbstractTripleStore getDatabase();

    /**
     * True iff the buffer is empty.
     */
    public boolean isEmpty();

    /**
     * The #of statements in the buffer.
     */
    public int size();
    
    /**
     * Resets the state of the buffer (any pending writes are discarded).
     */
    public void clear();

    /**
     * Batch insert buffered data (terms and statements) into the store.
     */
    public void flush();

    /**
     * Add an "explicit" statement to the buffer.
     * 
     * @param s
     * @param p
     * @param o
     */
    public void add(Resource s, URI p, Value o);

}
