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
 * Created on May 31, 2011
 */

package com.bigdata.rdf.internal;

/**
 * Interface for {@link IV}s which have inline Unicode components in their
 * representation.  This interface provides a means to cache the byteLength
 * of the {@link IV} at the time that the compressed Unicode representation
 * is computed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IInlineUnicode {

    /**
     * Cache the byteLength on the {@link IV}.
     * 
     * @param byteLength
     *            The byteLength.
     *            
     * @throws IllegalArgumentException
     *             if the argument is LT ZERO (0).
     * @throws IllegalStateException
     *             if the byteLength has already been set to a different value.
     */
    public void setByteLength(int byteLength);
    
}
