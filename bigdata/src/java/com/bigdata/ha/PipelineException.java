/**

Copyright (C) SYSTAP, LLC 2006-2013.  All rights reserved.

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

package com.bigdata.ha;

import java.util.UUID;

/**
 * PipelineException is thrown from RMI calls to communicate
 * the root cause of a pipeline problem.  The caller is then able
 * to take action: for example to remove the problem service
 * from the quorum.
 */
public class PipelineException extends RuntimeException {
	
	/**
	 * Generated ID
	 */
	private static final long serialVersionUID = 8019938954269914574L;
	
	/** The UUID of the service that could not be reached. */
	private final UUID serviceId;

	public PipelineException(final UUID serviceId, final Throwable t) {
		super(t);
		
		this.serviceId = serviceId;
	}
	
	/** Return the UUID of the service that could not be reached. */
	public UUID getProblemServiceId() {
		return serviceId;
	}

}
