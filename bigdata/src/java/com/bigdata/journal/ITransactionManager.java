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
 * Created on Feb 19, 2007
 */

package com.bigdata.journal;

import com.bigdata.objndx.IIndex;

/**
 * A client-facing interface for managing transaction life cycles.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITransactionManager extends ITimestampService {

    /**
     * Create a new fully-isolated read-write transaction.
     * 
     * @return The transaction start time, which serves as the unique identifier
     *         for the transaction.
     */
    public long newTx();

    /**
     * Create a new fully-isolated transaction.
     * 
     * @param readOnly
     *            When true, the transaction will reject writes.
     * 
     * @return The transaction start time, which serves as the unique identifier
     *         for the transaction.
     */
    public long newTx(boolean readOnly);

    /**
     * Create a new read-committed transaction. The transaction will reject
     * writes. Any data committed by concurrent transactions will become visible
     * to indices isolated by this transaction (hence, "read comitted").
     * <p>
     * This provides more isolation than "read dirty" since the concurrent
     * transactions MUST commit before their writes become visible to the a
     * read-committed transaction.
     * 
     * @return The transaction start time, which serves as the unique identifier
     *         for the transaction.
     */
    public long newReadCommittedTx();

    /**
     * Return the named index as isolated by the transaction.
     * 
     * @param name
     *            The index name.
     * @param ts
     *            The transaction start time, which serves as the unique
     *            identifier for the transaction.
     * 
     * @return The isolated index.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public IIndex getIndex(String name, long ts);

    /**
     * Abort the transaction.
     * 
     * @param ts
     *            The transaction start time, which serves as the unique
     *            identifier for the transaction.
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public void abort(long ts);

    /**
     * Commit the transaction.
     * 
     * @param ts
     *            The transaction start time, which serves as the unique
     *            identifier for the transaction.
     *            
     * @return The commit timestamp assigned to the transaction.
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public long commit(long ts);

}
