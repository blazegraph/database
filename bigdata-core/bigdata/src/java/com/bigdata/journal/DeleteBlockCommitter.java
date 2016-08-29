/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.journal;

import com.bigdata.btree.IndexInconsistentError;
import com.bigdata.rwstore.IRWStrategy;

/**
 * Defines the callback object called on commit that enables the deferred 
 * delete blocks to be associated with a CommitRecord.
 * 
 * @author Martyn Cutcher
 *
 */
public class DeleteBlockCommitter implements ICommitter {

	private final IRWStrategy m_strategy;
	private volatile Throwable error = null;
	
	public DeleteBlockCommitter(final IRWStrategy strategy) {
	
	    m_strategy = strategy;
	    
	}

    @Override
    public long handleCommit(final long commitTime) {

        if (error != null)
            throw new IndexInconsistentError(error);

	    return m_strategy.saveDeferrals();
	    
	}

    @Override
    public void invalidate(final Throwable t) {

        if (t == null)
            throw new IllegalArgumentException();

        if (error == null)
            error = t;

    }

}
