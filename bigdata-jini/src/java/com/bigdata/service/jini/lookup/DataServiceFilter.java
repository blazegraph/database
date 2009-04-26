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
package com.bigdata.service.jini.lookup;

import java.io.Serializable;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;

/**
 * Filter rejects items that implement {@link IMetadataService} so as to only
 * select those {@link IDataService}s that are being used as data services rather
 * than metadata services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceFilter implements ServiceItemFilter, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 8403645355677682562L;

    protected static final transient Logger log = Logger
            .getLogger(DataServiceFilter.class);

    public static final transient DataServiceFilter INSTANCE = new DataServiceFilter();
    
    /**
     * Use {@link #INSTANCE} if there are no other constraints.
     */
    protected DataServiceFilter() {

    }

    /**
     * @return <code>true</code> if the item is an {@link IDataService} that
     *         does NOT implement the {@link IMetadataService} interface.
     */
    public boolean check(final ServiceItem item) {

        if (item.service == null) {

            log.warn("Service is null: " + item);

            return false;
            
        }
        
        if(!(item.service instanceof IMetadataService)) {
           
            if (log.isDebugEnabled())
                log.debug("Matched: " + item);
            
            return true;
            
        }

        if (log.isDebugEnabled())
            log.debug("Ignoring: " + item);
        
        return false;
        
    }
    
}
