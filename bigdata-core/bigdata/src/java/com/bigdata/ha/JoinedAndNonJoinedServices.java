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

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import com.bigdata.quorum.Quorum;

/**
 * Helper class finds all joined and non-joined services for the quorum
 * client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public class JoinedAndNonJoinedServices implements Serializable,
        IJoinedAndNonJoinedServices {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    // The services joined with the met quorum, in their join order.
    private final UUID[] joinedServiceIds;
    
    // The services in the write pipeline (in any order).
    private final Set<UUID> nonJoinedPipelineServiceIds;

    public JoinedAndNonJoinedServices(
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        // The services joined with the met quorum, in their join order.
        joinedServiceIds = quorum.getJoined();

        // The UUID for this service.
        final UUID serviceId = quorum.getClient().getServiceId();

        if (joinedServiceIds.length == 0
                || !joinedServiceIds[0].equals(serviceId)) {

            /*
             * Sanity check. Verify that the first service in the join order
             * is *this* service. This is a precondition for the service to
             * be the leader.
             */

            throw new RuntimeException("Not leader: serviceId=" + serviceId
                    + ", joinedServiceIds="
                    + Arrays.toString(joinedServiceIds));

        }

        // The services in the write pipeline (in any order).
        nonJoinedPipelineServiceIds = new LinkedHashSet<UUID>(
                Arrays.asList(quorum.getPipeline()));

        // Remove all services that are joined from this collection.
        for (UUID joinedServiceId : joinedServiceIds) {

            nonJoinedPipelineServiceIds.remove(joinedServiceId);

        }

    }

    @Override
    public UUID[] getJoinedServiceIds() {
        return joinedServiceIds;
    }

    @Override
    public Set<UUID> getNonJoinedPipelineServiceIds() {
        return nonJoinedPipelineServiceIds;
    }

    @Override
    public String toString() {
        return super.toString() + "{#joined=" + joinedServiceIds.length
                + ", #nonJoined=" + nonJoinedPipelineServiceIds.size()
                + ", joinedServices=" + Arrays.toString(joinedServiceIds)
                + ", nonJoined=" + nonJoinedPipelineServiceIds + "}";
    }

}
