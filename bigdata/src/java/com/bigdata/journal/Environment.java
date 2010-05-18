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
package com.bigdata.journal;

import java.net.InetSocketAddress;

import com.bigdata.journal.ha.HADelegate;
import com.bigdata.journal.ha.QuorumManager;

/**
 * The Environment object is a general hook accessible to all layers
 * (Journal,Strategy,WriteCacheService etc..) within a service.
 * 
 * The incentive for this has been driven by the HA design and the need to
 * co-ordinate actions across layers that had not necessarily been developed
 * with this in mind.
 * 
 * Specifically, the Environment provides will provide access to the Quorum
 * as the first link into the HA functionality.
 * 
 * @author Martyn Cutcher
 */
public interface Environment {

	public InetSocketAddress getWritePipelineAddr();

	public HADelegate getHADelegate();

    /** @deprecated */
	public AbstractJournal getJournal();

	public QuorumManager getQuorumManager();

	public boolean isHighlyAvailable();

    /** @deprecated */
    public long getActiveFileExtent();

    /** @deprecated */
    public IBufferStrategy getStrategy();

}
