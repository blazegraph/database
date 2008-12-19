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

    public MockLocalTransactionManager() {

        super();

    }

    public ITransactionService getTransactionService() {
        // TODO Auto-generated method stub
        return null;
    }

}