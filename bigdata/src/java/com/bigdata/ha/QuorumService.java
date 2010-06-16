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
 * Created on Jun 1, 2010
 */

package com.bigdata.ha;

import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to support service specific high availability operations such as
 * reading on another member of the quorum, the 2-phase quorum commit protocol,
 * replicating writes, etc.
 * <p>
 * A {@link QuorumService} participates in a write pipeline, which replicates
 * cache blocks from the leader and relays them from service to service in the
 * {@link Quorum#getPipeline() pipeline order}. The write pipeline is
 * responsible for ensuring that the followers will have the same persistent
 * state as the leader at each commit point. Services which are synchronizing
 * may also join the write pipeline in order to receive updates before they join
 * a met quorum. See {@link QuorumPipeline}.
 * <p>
 * The leader uses a 2-phase commit protocol to ensure that the quorum enters
 * each commit point atomically and that followers can read historical commit
 * points, including on the prior commit point. See {@link QuorumCommit}.
 * <p>
 * Persistent services use pre-record checksums to detect read errors and can
 * handle read failures by reading on another service joined with the quorum.
 * See {@link QuorumRead}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumService<S extends HAGlue> extends QuorumMember<S>,
        QuorumRead<S>, QuorumCommit<S>, QuorumPipeline<S> {

}
