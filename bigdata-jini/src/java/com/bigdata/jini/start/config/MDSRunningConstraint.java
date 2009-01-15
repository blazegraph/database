package com.bigdata.jini.start.config;

import com.bigdata.service.IMetadataService;
import com.bigdata.service.jini.JiniFederation;

/**
 * The {@link IMetadataService} must be discovered.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MDSRunningConstraint extends ServiceDependencyConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = -1983273198622764005L;

    public boolean allow(JiniFederation fed) throws Exception {

        if (fed.getMetadataService() == null) {
        
            if (ServiceDependencyConstraint.INFO)
                ServiceDependencyConstraint.log.info("Not discovered: "
                        + IMetadataService.class.getName());
            
            return false;
            
        }
        
        return true;
        
    }
    
}