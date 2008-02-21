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
import java.rmi.RemoteException;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ValidationError;

/**
 * Remote interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo reconcile API with {@link ITransactionManager} which is declared by
 *       {@link AbstractJournal}. I do not want to have IOException on all of
 *       the bigdata interfaces, but you need either that or
 *       {@link RemoteException} for an interface that will be exposed by a jini
 *       service (unless you use a smart proxy?).
 */
public interface IRemoteTxCommitProtocol extends Remote {

    /**
     * Request commit of the transaction write set.
     */
    public long commit(long tx) throws ValidationError, IOException;

    /**
     * Request abort of the transaction write set.
     */
    public void abort(long tx) throws IOException;

}
