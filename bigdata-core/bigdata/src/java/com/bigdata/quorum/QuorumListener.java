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
/*
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

/**
 * An interface for receiving informational quorum events. These events are
 * intended for clients interested in quorum state changes. Services that are HA
 * aware use a more intimate API to handle the state changes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumListener {

    /**
     * Notify the client of a quorum event.
     * <p>
     * The listener MUST NOT take any event that could block. In particular, it
     * MUST NOT wait on another thread that will access the {@link Quorum} as
     * that will cause a deadlock around the internal lock maintained by the
     * {@link Quorum}.
     */
    void notify(QuorumEvent e);

}
