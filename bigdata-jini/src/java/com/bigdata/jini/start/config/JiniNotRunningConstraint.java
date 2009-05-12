package com.bigdata.jini.start.config;

import com.bigdata.service.jini.JiniFederation;

/**
 * Constraint that jini must be running (one or more service registrars must
 * have been discovered).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JiniNotRunningConstraint extends ServiceDependencyConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 5490323726279787743L;

    public boolean allow(JiniFederation fed) throws Exception {

        // Get the #of discovered service registrars.
        final int ndiscovered = fed.getDiscoveryManagement().getRegistrars().length;
        
        if ( ndiscovered > 0) {

            if (log.isInfoEnabled())
                log.info(ndiscovered+" registrars have been discovered");

            return false;

        }

        // return false if NO registrars have been discovered.
        return true;

    }

}
