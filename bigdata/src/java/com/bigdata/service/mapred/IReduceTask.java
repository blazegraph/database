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
package com.bigdata.service.mapred;

import java.util.Iterator;
import java.util.UUID;

import com.bigdata.service.IDataService;

/**
 * Interface for a reduce task to be executed on a map/reduce client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IReduceTask extends ITask {

    /**
     * The {@link IDataService}  from which the reduce task will read its
     * input.
     */
    public UUID getDataService();
    
    /**
     * Each reduce task will be presented with a series of key-value pairs
     * in key order. However, the keys will be distributed across the N
     * reduce tasks by the used defined hash function, so this is NOT a
     * total ordering over the intermediate keys.
     * <p>
     * Note: This method is never invoked for a key for which there are no
     * values.
     * 
     * @param key
     *            A key.
     * @param vals
     *            An iterator that will visit the set of values for that
     *            key.
     */
    public void reduce(byte[] key, Iterator<byte[]> vals) throws Exception;
    
}
