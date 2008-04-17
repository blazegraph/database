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
 * Created on Apr 17, 2008
 */

package com.bigdata.service;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.journal.ITx;

/**
 * Client-side {@link IIndex} interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IClientIndex extends IIndex {

    /**
     * Either the startTime of an active transaction, {@link ITx#UNISOLATED} for
     * the current unisolated index view, {@link ITx#READ_COMMITTED} for a
     * read-committed view, or the <code>timestamp</code> for a historical
     * view no later than the specified timestamp.
     */
    public long getTimestamp();

    /**
     * The name of the scale-out index.
     */
    public String getName();
    
    /**
     * Counters are local to a specific index partition and are only available
     * to unisolated procedures running locally on an {@link IDataService}.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public ICounter getCounter();
    
}
