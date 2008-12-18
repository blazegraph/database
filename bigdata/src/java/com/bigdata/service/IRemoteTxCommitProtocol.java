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
/*
 * Created on Mar 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;

import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ValidationError;

/**
 * Remote interface by which the centralized {@link ITransactionManager} manages
 * the state of transactions on the distributed {@link IDataService}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRemoteTxCommitProtocol extends Remote {

    /**
     * Notify a data service that it may release data required to support views
     * for the specified release time (basically, this releases any read locks
     * on views for timestamps up to and including the specified release time).
     * 
     * @param releaseTime
     *            The new release time (strictly advanced by the transaction
     *            manager).
     * 
     * @todo when the release time is advanced any active transactions (or
     *       lightweight reads) GTE the new release time should be interrupted.
     */
    public void setReleaseTime(long releaseTime) throws IOException;

    /**
     * Request preparation of a read-write transaction for a 2-phase commit.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     * @throws ValidationError
     *             if validation fails.
     * @throws IOException
     */
    public void prepare(long tx) throws ValidationError, IOException;

    /**
     * Request commit of the transaction by the data service.
     * 
     * @param tx
     *            The transaction identifier.
     * @param commitTime
     *            The commit time assigned to that transaction.
     * 
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     * @throws IllegalArgumentException
     *             if the <i>commitTime</i> is LTE the last commit time
     *             performed on this data service.
     * @throws IllegalStateException
     *             if the transaction is a read-write transaction and it has not
     *             been {@link #prepare(long) prepared}
     * @throws IllegalStateException
     *             if the transaction is a read-write transaction and a timeout
     *             has invalidated {@link #prepare(long) prepared} commit.
     */
    public void commit(long tx, long commitTime) throws IOException;

    /**
     * Request abort of the transaction by the data service.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     */
    public void abort(long tx) throws IOException;

}
