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
 * Created on Aug 12, 2016
 */
package com.bigdata.rdf.sail;

/**
 * An instance of this is thrown if a thread already owning the unisolated 
 * connection for a Sail attempts to acquire a 2nd unisolated connection for
 * the same Sail.
 * 
 * @author bryan
 */
public class UnisolatedConnectionNotReentrantException extends IllegalStateException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

}
