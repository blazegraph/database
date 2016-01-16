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
package com.bigdata.ha;

import java.util.Set;
import java.util.UUID;

/**
 * Interface providing an atomic snapshot of the services that are joined with a
 * met quorum (and the services that are not joined with a met quorum) as of
 * some point in the GATHER or PREPARE protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IJoinedAndNonJoinedServices {

    /** The services joined with the met quorum, in their join order. */
    public UUID[] getJoinedServiceIds();
    
    /** The services in the write pipeline (in any order). */
    public Set<UUID> getNonJoinedPipelineServiceIds();

}
