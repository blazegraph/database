/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Jan 27, 2009
 */

package com.bigdata.resources;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

import com.bigdata.service.AbstractFederation;
import com.bigdata.service.AbstractTransactionService;

/**
 * Mock implementation supporting only those features required to bootstrap the
 * resource manager test suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockTransactionService extends AbstractTransactionService {

    /**
     * @param properties
     */
    public MockTransactionService(Properties properties) {
   
        super(properties);
   
    }

    @Override
    protected void abortImpl(TxState state) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    protected long commitImpl(TxState state) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected long findCommitTime(long timestamp) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected long findNextCommitTime(long commitTime) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLastCommitTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public AbstractFederation getFederation() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean committed(long tx, UUID dataService) throws IOException,
            InterruptedException, BrokenBarrierException {
        // TODO Auto-generated method stub
        return false;
    }

    public long prepared(long tx, UUID dataService) throws IOException,
            InterruptedException, BrokenBarrierException {
        // TODO Auto-generated method stub
        return 0;
    }

}
