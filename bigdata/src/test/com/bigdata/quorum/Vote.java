/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.UUID;

/**
 * Member services which participate in a quorum <em>vote</em> to decide the
 * <em>lastCommitTime</em> (if any) for which there is an agreement of at least
 * (k+1)/2 quorum members. The <i>lastCommitTime</i> itself provides a summary
 * of the persistent state of the service. Two quorum members with the same
 * <i>lastCommitTime</i> are assumed to have the same persistent state. That
 * assumption is validated during the synchronization protocol when the services
 * <i>join</i> with the quorum (their current root blocks will be inspected to
 * ensure that there is a detailed agreement).
 * <p>
 * The natural order of a {@link Vote} is ascending last commit time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface Vote extends Comparable<Vote> {

    /**
     * The last commit time.
     */
    long getLastCommitTime();

    /**
     * The {@link UUID}s of the physical service instances having that last
     * commit time.
     */
    UUID[] getBallots();

}
