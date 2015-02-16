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
/*
 * Created on Jun 7, 2013.
 */
package com.bigdata.ha.pipeline;

import java.util.concurrent.CancellationException;

import com.bigdata.ha.QuorumPipelineImpl;

/**
 * Exception thrown when the upstream service is changed by a pipeline
 * reconfiguration. This exception was introduced so retrySend() in
 * {@link QuorumPipelineImpl} could differentiate between normal termination of
 * a service (which will interrupt the {@link HAReceiveService} and thus
 * propagate a {@link CancellationException} to the upstream service) and a
 * pipeline change which requires retrySend() to retransmit the message and
 * payload from the leader along the reconfigured write pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PipelineUpstreamChange extends AbstractPipelineChangeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public PipelineUpstreamChange() {
    }

    public PipelineUpstreamChange(String message) {
        super(message);
    }

    public PipelineUpstreamChange(Throwable cause) {
        super(cause);
    }

    public PipelineUpstreamChange(String message, Throwable cause) {
        super(message, cause);
    }

}
