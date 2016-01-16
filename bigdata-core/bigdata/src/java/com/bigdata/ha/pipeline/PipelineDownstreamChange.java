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
 * Created on Jun 7, 2013.
 */
package com.bigdata.ha.pipeline;

import java.util.concurrent.CancellationException;

import com.bigdata.ha.QuorumPipelineImpl;
import com.bigdata.quorum.QuorumException;

/**
 * Exception thrown when the downstream service is changed by a pipeline
 * reconfiguration. This exception was introduced so retrySend() in
 * {@link QuorumPipelineImpl} could differentiate between normal termination of
 * a service (which will interrupt the {@link HAReceiveService} and thus
 * propagate a {@link CancellationException} to the upstream service) and a
 * pipeline change which requires retrySend() to retransmit the message and
 * payload from the leader along the reconfigured write pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PipelineDownstreamChange extends AbstractPipelineChangeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public PipelineDownstreamChange() {
    }

    public PipelineDownstreamChange(String message) {
        super(message);
    }

    public PipelineDownstreamChange(Throwable cause) {
        super(cause);
    }

    public PipelineDownstreamChange(String message, Throwable cause) {
        super(message, cause);
    }

}
