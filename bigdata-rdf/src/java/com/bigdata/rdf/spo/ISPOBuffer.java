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
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.spo;

import com.bigdata.rdf.store.IRawTripleStore;

/**
 * A buffer for {@link SPO}s.
 * <p>
 * Note: {@link ISPOBuffer}s are used to collect {@link SPO}s into chunks that
 * can be sorted in order to support efficient batch operations on statement
 * indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISPOBuffer {

    /**
     * The #of statements currently in the buffer (if duplicates are not being
     * filtered then this count will include any duplicate statements).
     */
    public int size();

    /**
     * True iff there are no statements in the buffer.
     */
    public boolean isEmpty();

    /**
     * Adds an {@link SPO} together with an optional justification for that
     * {@link SPO}.
     * 
     * @param spo
     *            The {@link SPO}.
     * @param justification
     *            The justification for that {@link SPO} (optional, depending on
     *            the TM strategy and only if the {@link SPO} is an inference).
     * 
     * @return true if the buffer will store the statement.
     */
    public boolean add(SPO spo, Justification justification);

    /**
     * Flush any buffered statements to the backing store.
     * 
     * @return The #of statements that were written on the indices (a statement
     *         that was previously an axiom or inferred and that is converted to
     *         an explicit statement by this method will be reported in this
     *         count as well as any statement that was not pre-existing in the
     *         database).
     *         
     * @see IRawTripleStore#addStatements(ISPOIterator, ISPOFilter)
     */
    public int flush();
    
}
