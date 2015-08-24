/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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


/**
 * A policy that never permits the release of backups such that you can always
 * restore any commit point. This policy will require unbounded disk space if
 * there are continuing update transactions against the database. However, it is
 * perfectly reasonable if you are using a write-once, read-many deployment
 * strategy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ForeverRestorePolicy implements IRestorePolicy {

    /**
     * {@inheritDoc}
     * <p>
     * This policy always returns ZERO to prevent backups from being released.
     */
    @Override
    public long getEarliestRestorableCommitPoint(final HAJournal jnl) {

        return 0;
        
    }

}
