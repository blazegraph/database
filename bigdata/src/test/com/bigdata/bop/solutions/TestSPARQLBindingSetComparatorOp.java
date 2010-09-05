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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.solutions;

import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.bop.solutions.SparqlBindingSetComparatorOp;

import junit.framework.TestCase2;

/**
 * Unit tests for the {@link SparqlBindingSetComparatorOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPARQLBindingSetComparatorOp extends TestCase2 {

    /**
     * 
     */
    public TestSPARQLBindingSetComparatorOp() {
    }

    /**
     * @param name
     */
    public TestSPARQLBindingSetComparatorOp(String name) {
        super(name);
    }

    /**
     * @todo This test should just focus on the correctness of the binding set
     *       comparator. We are relying on the {@link ValueComparator} to get
     *       the SPARQL ordering correct for RDF {@link Value} objects.
     */
    public void test_something() {

        fail("write tests");
        
    }
    
}
