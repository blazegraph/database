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
 * Created on Jun 6, 2010
 */
package com.bigdata.quorum;

import java.rmi.Remote;
import java.util.UUID;

/**
 * An interface that effects various changes in the distributed quorum state
 * required to execute the intention of a {@link QuorumMember} service and its
 * cognizant {@link AbstractQuorum}.
 * 
 * @author thompsonbry@users.sourceforge.net
 * @see QuorumWatcher
 */
public interface QuorumActor<S extends Remote, C extends QuorumClient<S>> {

	/**
	 * The {@link Quorum}.
	 */
	public Quorum<S, C> getQuourm();

	/**
	 * The service on whose behalf this class is acting.
	 * 
	 * @return
	 */
	public C getQuorumMember();

	/**
	 * The {@link UUID} of the service on whose behalf this class is acting.
	 */
	public UUID getServiceId();

	/**
	 * Join the service method with the quorum (for example, by adding the
	 * ephemeral znode for the service to the "joined" services znode for this
	 * quorum).
	 */
	public void joinService(final long lastCommitTime);

}
