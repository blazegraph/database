/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 19, 2009
 */

package com.bigdata.resources;

import com.bigdata.service.IMetadataService;

/**
 * An interface which shields callers from handling RMI exceptions and which
 * allows the use of mock implementations when you do not really want to assign
 * partition identifiers using the
 * {@link IMetadataService#nextPartitionId(String)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IMetadataService#nextPartitionId(String)
 */
public interface IPartitionIdFactory {

    /**
     * Return the next index partition identifier for the given scale-out index.
     * 
     * @param scaleOutIndexName
     *            The name of the scale-out index.
     *            
     * @return The next partition identifier for that scale-out index.
     */
    public int nextPartitionId(final String scaleOutIndexName);

}
