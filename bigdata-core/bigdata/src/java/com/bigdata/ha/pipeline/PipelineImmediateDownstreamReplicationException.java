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

package com.bigdata.ha.pipeline;

import java.util.UUID;

/**
 * Exception thrown when there is a problem with write replication from a
 * service to its downstream service, including a problem with RMI to the
 * downstream service or socket level write replication to the downstream
 * service. This typed exception enables the leader is then able to take action
 * intended to cure the pipeline by forcing the problem service from the quorum.
 * The problem service can then attempt to re-enter the quorum.
 */
public class PipelineImmediateDownstreamReplicationException extends
        AbstractPipelineException {

    /**
     * Generated ID
     */
    private static final long serialVersionUID = 8019938954269914574L;

    /**
     * The {@link UUID} of the service reporting a problem replicating writes to
     * its downstream service.
     */
    private final UUID serviceId;

    /**
     * The prior and next service {@link UUID}s for the service reporting the
     * problem. The problem is with the communication to the "next" service.
     */
    private final UUID[] priorAndNext;

    /**
     * 
     * @param serviceId
     *            The {@link UUID} of the service reporting a problem
     *            replicating writes to its downstream service.
     * @param priorAndNext
     *            The prior and next service {@link UUID}s for the service
     *            reporting the problem. The problem is with the communication
     *            to the "next" service.
     * @param t
     *            The root cause exception.
     */
    public PipelineImmediateDownstreamReplicationException(final UUID serviceId,
            final UUID[] priorAndNext, final Throwable t) {

        super(t);

        this.serviceId = serviceId;

        this.priorAndNext = priorAndNext;

    }

    /**
     * Return the {@link UUID} of the service reporting the problem. This is the
     * service that attempted to replicate a payload to the downstream service
     * and was unable to replicate the payload due to the reported root cause.
     */
    public UUID getReportingServiceId() {

        return serviceId;

    }

    /**
     * The prior and next service {@link UUID}s for the service reporting the
     * problem. The problem is with the communication to the "next" service.
     */
    public UUID[] getPriorAndNext() {

        return priorAndNext;

    }

    /**
     * Return the {@link UUID} of the downstream service - the problem is
     * reported for the communication channel between the reporting service and
     * this downstream service. The downstream service is the service that
     * should be forced out of the quorum in order to "fix" the pipeline.
     */
    public UUID getProblemServiceId() {

        return priorAndNext[1];

    }

}
