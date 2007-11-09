/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.service.mapReduce;

import java.util.Iterator;

/**
 * Interface responsible for identifying input sources for map tasks. For
 * example, by scanning the files in a local directory on the map server.
 * 
 * @todo this needs to be generalized to handle more kinds of inputs, including
 *       reading from an index, a scale-out index (whether hash or range
 *       partitioned), or a split that can be divided into "splits".
 */
public interface IMapSource {
    
    /**
     * An iterator that will visit each source to be read by the map task in
     * turn.
     */
    public Iterator<Object> getSources();

}