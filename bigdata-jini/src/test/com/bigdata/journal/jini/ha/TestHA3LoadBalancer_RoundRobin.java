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
package com.bigdata.journal.jini.ha;

import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;
import com.bigdata.rdf.sail.webapp.lbs.policy.RoundRobinLBSPolicy;


/**
 * Test suite for the HA load balancer. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/624"> HA Load Balancer </a>
 */
public class TestHA3LoadBalancer_RoundRobin extends AbstractHA3LoadBalancerTestCase {

    public TestHA3LoadBalancer_RoundRobin() {
    }

    public TestHA3LoadBalancer_RoundRobin(final String name) {

        super(name);
        
    }

    @Override
    protected IHALoadBalancerPolicy newTestPolicy() {

        return new RoundRobinLBSPolicy();
        
    }
    
}