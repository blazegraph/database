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
 * Created on Oct 24, 2007
 */

package com.bigdata.rdf.spo;

import java.util.Iterator;

import com.bigdata.rdf.util.KeyOrder;

/**
 * Iterator visits {@link SPO}s.
 * 
 * @todo verify that all {@link ISPOIterator}s are being closed within a
 *       <code>finally</code> clause.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISPOIterator extends Iterator<SPO> {

//    /**
//     * Reset the iterator. You can re-read the same statements after the
//     * iterator has been reset.
//     * 
//     * @todo the purpose of this is to make the iterator reusable, especially
//     *       when it is fully buffered. if we have rules that reuse the same
//     *       iterator then reset() can drammatically reduce the cost of the
//     *       rule. If we don't do this, then reset() is not worth the candle.
//     *       <p>
//     *       Note: if you reset() an iterator that is not fully buffered then
//     *       this does not help performance particularly.
//     * 
//     * @todo If we guarentee that statements that have been deleted
//     *       {@link Iterator#remove()} are NOT be present in a revisit then the
//     *       iterator will be forced to hold a hash set of the deleted SPOs or
//     *       set a flag on the SPO object.  In any case, this will limit scaling
//     *       when 
//     */
//    public void reset();
    
    /**
     * Closes the iterator, releasing any associated resources. This method MAY
     * be invoked safely if the iterator is already closed.
     */
    public void close();

    /**
     * Return the next "chunk" of statements from the iterator. The statements
     * will be in the same order that they would be visited by
     * {@link Iterator#next()}. The size of the chunk is up to the
     * implementation.
     * <p>
     * This is designed to make it easier to write methods that use the batch
     * APIs but do not require the statements matching some triple pattern to be
     * fully materialized. You can use {@link #nextChunk()} instead of
     * {@link Iterator#next()} to break down the operation into N chunks, where
     * N is determined dynamically based on how much data the iterator returns
     * in each chunk and how much data there is to be read.
     * 
     * @return The next chunk of statements.
     * 
     * @see #nextChunk(KeyOrder)
     */
    public SPO[] nextChunk();

    /**
     * Return the next "chunk" of statements from the iterator. The statements
     * will be in the specified order. If {@link #getKeyOrder()} would return
     * non-<code>null</code> and the request order corresponds to the value
     * that would be returned by {@link #getKeyOrder()} then the statements in
     * the next chunk are NOT sorted. Otherwise the statements in the next chunk
     * are sorted before they are returned. The size of the chunk is up to the
     * implementation.
     * 
     * @param keyOrder
     *            The order for the statements in the chunk.
     * 
     * @return The next chunk of statements in the specified order.
     */
    public SPO[] nextChunk(KeyOrder keyOrder);

    /**
     * The {@link KeyOrder} in which statements are being visited and
     * <code>null</code> if not known.
     * 
     * @return The order in which statemetns are being visited -or-
     *         <code>null</code> if not known.
     */
    public KeyOrder getKeyOrder();
    
}
