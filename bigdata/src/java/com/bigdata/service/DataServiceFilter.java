package com.bigdata.service;

import org.apache.log4j.Logger;

import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceItemFilter;

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