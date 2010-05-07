/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Apr 28, 2010
 */

package com.bigdata.journal.ha;

import com.bigdata.journal.Journal;
import com.bigdata.journal.ProxyTestCase;

/**
 * Unit tests bootstrap 3 journals in a specified failover chain and demonstrate
 * pipelined writes and the commit protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHAWritePipeline extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestHAWritePipeline() {
    }

    /**
     * @param name
     */
    public TestHAWritePipeline(String name) {
        super(name);
    }

    public void test_writePipeline() {

        final Journal store = getStore(getProperties());

        try {

            System.err.println("a");            
            
            try {
				Thread.sleep(1000);  // allow pipe setup
			} catch (InterruptedException e) {
				// ignore
			}
            store.commit();
            try {
				Thread.sleep(4000);  // wait for acks
			} catch (InterruptedException e) {
				// ignore
			}
            System.err.println("b");
            
        } finally {

            store.destroy();
            
        }
        
    }
    
}
