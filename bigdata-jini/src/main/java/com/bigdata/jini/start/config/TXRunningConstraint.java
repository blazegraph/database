package com.bigdata.jini.start.config;

import com.bigdata.journal.ITransactionService;
import com.bigdata.service.jini.JiniFederation;

/**
 * The {@link ITransactionService} must be discovered.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TXRunningConstraint extends ServiceDependencyConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 6590113180404519952L;

    public boolean allow(JiniFederation fed) throws Exception {

        if (fed.getTransactionService() == null) {
        
            if(log.isInfoEnabled())
                log.info("Not discovered: "
                        + ITransactionService.class.getName());

            return false;

        }

        return true;

    }

}