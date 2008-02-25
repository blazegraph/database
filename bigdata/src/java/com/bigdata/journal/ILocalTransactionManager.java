/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Feb 19, 2008
 */

package com.bigdata.journal;

import com.bigdata.btree.IIndex;

/**
 * Interface for managing local transactions.
 * 
 * @todo does this need shutdown() and shutdownNow()?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILocalTransactionManager extends ITransactionManager {

//    /** #of active transactions. */
//    public int getActiveTxCount();
//    
//    /** #of pending transactions. */
//    public int getPreparedTxCount();
    
    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * @todo test for transactions that have already been completed? that would
     *       represent a protocol error in the transaction manager service.
     */
    public void activateTx(ITx tx) throws IllegalStateException;
    
    /**
     * Notify the journal that a transaction has prepared (and hence is no
     * longer active).
     * 
     * @param tx
     *            The transaction
     * 
     * @throws IllegalStateException
     */
    public void prepared(ITx tx) throws IllegalStateException;

    /**
     * Notify the journal that a transaction is completed (either aborted or
     * committed).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    public void completedTx(ITx tx) throws IllegalStateException;

    /**
     * Lookup an active or prepared transaction (exact match).
     * 
     * @param startTime
     *            The start timestamp for the transaction.
     * 
     * @return The transaction with that start time or <code>null</code> if
     *         the start time is not mapped to either an active or prepared
     *         transaction.
     */
    public ITx getTx(long startTime);
    
}
