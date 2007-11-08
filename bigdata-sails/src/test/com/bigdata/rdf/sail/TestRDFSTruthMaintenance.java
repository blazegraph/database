/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Nov 1, 2007
 */

package com.bigdata.rdf.sail;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for truth maintenance when statements are deleted from the
 * database.
 * 
 * @todo write some tests that load up a 2-part data set, then retract one part,
 *       then add it back in again and verify the correct closures at each
 *       stage.
 * 
 * @todo is there an efficient way to prove that two {@link AbstractTripleStore}s
 *       are the same? We have to materialize the terms in order to verify that
 *       the same terms are bound for the statements. Also, if the terms were
 *       created in a different order, then the term identifiers will differ. In
 *       this case the statement indices will not visit the statements in the
 *       same order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRDFSTruthMaintenance extends AbstractInferenceEngineTestCase {

    public TestRDFSTruthMaintenance() {
        
    }
    
    public TestRDFSTruthMaintenance(String name) {
        super(name);
    }
    
    public void test_nothing() {
        
        fail("write tests");
        
    }
    
}
