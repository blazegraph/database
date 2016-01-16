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
package com.bigdata.ha.msg;

import java.util.UUID;

/**
 * Message requesting a root block for a store on a remote service.
 */
public interface IHARootBlockRequest extends IHAMessage {

    /**
     * The {@link UUID} of the journal whose root block will be returned
     * (optional, defaults to the current Journal).
     * <p>
     * Note: This parameter is intended for scale-out if there is a need to
     * fetch the root block of a historical journal (versus the live journal).
     */
    UUID getStoreUUID();

    /**
     * When <code>true</code> the request should be non-blocking. Otherwise the
     * request should obtain the lock that guards the update of the root block
     * in the commit protocol such that the caller can not observe a root block
     * that has been updated but where the commit protocol is still in its
     * critical section.
     * <p>
     * Note: The non-blocking form of the request is used in some context where
     * the avoidence of a deadlock is necessary. The blocking form is used in
     * some contexts where we need to await a specific commit point on the
     * service (typically under test suite control).
     */
    boolean isNonBlocking();
    
}
