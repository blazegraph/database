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


/**
 * The restore policy identifies the first commit point whose backups MUST NOT
 * be released. The policy may be based on the age of the commit point, the
 * number of intervening commit points, etc. A policy that always returns ZERO
 * (0) will never release any backups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IRestorePolicy {

    /**
     * Return the earlist commit point whose supporting backup files MUST NOT be
     * released in order to preseve the ability to restore that commit point
     * from local backup files.
     * 
     * @param jnl
     *            The journal.
     * @return The earliest commit point whose supporting backup files must not
     *         be released.
     */
    long getEarliestRestorableCommitPoint(HAJournal jnl);
    
}
