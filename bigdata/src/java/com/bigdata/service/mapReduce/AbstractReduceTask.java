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

import java.util.UUID;

/**
 * Abstract base class for reduce tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractReduceTask implements IReduceTask {

    /**
     * The task identifier.
     */
    final public UUID uuid;
    
    /**
     * The data service identifier. The task will read its data from the
     * index on the data service that is named by the task identifier.
     */
    final private UUID dataService;
    
    /**
     * 
     * @param uuid
     *            The task identifier.
     * @param dataService
     *            The data service identifier. The task will read its data
     *            from the index on the data service that is named by the
     *            task identifier.
     */
    protected AbstractReduceTask(UUID uuid, UUID dataService) {
        
        if(uuid==null) throw new IllegalArgumentException();
        
        if(dataService==null) throw new IllegalArgumentException();
        
        this.uuid = uuid;
        
        this.dataService = dataService;
        
    }
    
    public UUID getUUID() {
        
        return uuid;
        
    }

    public UUID getDataService() {
        
        return dataService;
        
    }
    
}