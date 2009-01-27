package com.bigdata.resources;

import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.ITransactionService;

/**
 * Mock implementation used by some of the unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockLocalTransactionManager extends AbstractLocalTransactionManager {

    private final ITransactionService txService;
    
    public MockLocalTransactionManager(ITransactionService txService) {

        this.txService = txService;

    }

    public ITransactionService getTransactionService() {

        return txService;
        
    }

}