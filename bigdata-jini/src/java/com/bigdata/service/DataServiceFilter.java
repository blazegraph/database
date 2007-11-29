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
package com.bigdata.service;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

import org.apache.log4j.Logger;

/**
 * Filter matches a {@link DataService} but not a {@link MetadataService}.
 * <p>
 * 
 * @todo This explicitly filters out service variants that extend
 *       {@link DataService} but which are not tasked as a
 *       {@link DataService} by the {@link MetadataService}. It would be
 *       easier if we refactored the interface hierarchy a bit so that there
 *       was a common interface and abstract class extended by both the
 *       {@link DataService} and the {@link MetadataService} such that we
 *       could match on their specific interfaces without the possibility of
 *       confusion.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DataServiceFilter implements ServiceItemFilter {

    public static final transient Logger log = Logger
            .getLogger(DataServiceFilter.class);

    public boolean check(ServiceItem item) {

        if(item.service==null) {
            
            log.warn("Service is null: "+item);

            return false;
            
        }
        
        if(!(item.service instanceof IMetadataService)) {
           
            log.info("Matched: "+item);
            
            return true;
            
        }

        log.debug("Ignoring: "+item);
        
        return false;
        
    }
    
}