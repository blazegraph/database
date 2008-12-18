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
 * Created on Dec 17, 2008
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.service.DataService;

/**
 * Additional methods for the global transaction manager of a distributed
 * database that are not required for a purely local database (for a local
 * database, we do not use 2-phase commits).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IGlobalTransactionManager extends ITransactionManager {

    /**
     * Invoked on the behalf of tasks executing a read-write transaction to
     * notify the global transaction manager of their intention to write on a
     * {@link DataService}. When it comes time to validate and prepare the
     * transaction, only those {@link DataService}s on which it has written
     * will partitipate in the 2-phase commit.
     * 
     * @param tx
     *            The transaction identifier.
     * @param dataServiceUUID
     *            The {@link UUID} of a logical {@link DataService} on which the
     *            transaction will write.
     */
    public void wroteOn(long tx, UUID dataService);

}
