/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.service.jini;

import java.rmi.Remote;

import net.jini.admin.Administrable;

/**
 * Extends {@link Remote} for RMI compatibility.
 * 
 * @todo There are other jini interfaces that we do not yet implement. Note: You
 *       need to extend Remote in order for these APIs to be exported!
 * 
 * <pre>
 * public interface RemoteJoinAdmin extends Remote, JoinAdmin {
 * 
 * }
 * 
 * public interface RemoteDiscoveryAdmin extends Remote, DiscoveryAdmin {
 * 
 * }
 * </pre>
 */
public interface RemoteAdministrable extends Remote, Administrable {

}
