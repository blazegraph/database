/*

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
 * Created on Mar 15, 2007
 */
package com.bigdata.journal;

/**
 * A marker interface for logic that can declare whether or not it is read-only.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IReadOnly {

    /**
     * Return <code>true</code> iff the procedure asserts that it will not
     * write on the index. When <code>true</code>, the procedure may be run
     * against a view of the index that is read-only or which allows concurrent
     * processes to read on the same index object. When <code>false</code> the
     * procedure will be run against a mutable view of the index (assuming that
     * the procedure is executed in a context that has access to a mutable index
     * view).
     */
    public boolean isReadOnly();
    
}
