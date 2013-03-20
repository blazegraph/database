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
package com.bigdata.journal.jini.ha;

import java.util.concurrent.TimeUnit;

import com.bigdata.journal.IRootBlockView;

/**
 * The default restore policy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DefaultRestorePolicy implements IRestorePolicy {

    private final long millis;
    
    /**
     * The default is to keep local backups on hand for 7 days.
     */
    public DefaultRestorePolicy() {

        this(TimeUnit.DAYS.toMillis(7));
        
    }

    /**
     * Create a policy that will keep local backups on hand for the specified
     * number of milliseconds.
     * 
     * @param millis
     *            The #of milliseconds of state that can be restored from local
     *            backups.
     */
    public DefaultRestorePolicy(final long millis) {
        
        if (millis < 0)
            throw new IllegalArgumentException();

        this.millis = millis;
        
    }

    /**
     * This finds and returns the commit counter for the most recent snapshot
     * whose commit time is LTE <code>now - millis</code>, where <i>millis</i>
     * is the #of milliseconds specified by the constructor for this policy. The
     * return value will be ZERO (0) if there are no commit points.
     */
    @Override
    public long getEarliestRestorableCommitPoint(final HAJournal jnl) {

        final long now = System.currentTimeMillis();
        
        final long then = now - millis;

        final IRootBlockView rootBlock = jnl.getSnapshotManager().find(then);

        if (rootBlock == null) {

            // There are no snapshots.
            return 0L;
            
        }
        
        return rootBlock.getCommitCounter();
        
    }

}
