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

import java.util.UUID;

import com.bigdata.ha.msg.IHAMessage;

/**
 * Message requesting a pipeline reset on a service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHAPipelineResetRequest extends IHAMessage {

    /**
     * The quorum token in effect on the leader when this request was generated.
     */
    long token();

    /**
     * The {@link UUID} of the service that the leader has forced from the
     * pipeline
     * 
     * @return The {@link UUID} of the problem service -or- <code>null</code> if
     *         the leader did not identify a problem service.
     */
    UUID getProblemServiceId();
    
    /**
     * How long to await the state where the problem service is no longer part
     * of the write pipeline for a service that is upstream or downstream of the
     * problem service.
     */
    long getTimeoutNanos();

}
