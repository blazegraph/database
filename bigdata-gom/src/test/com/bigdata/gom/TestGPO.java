/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
package com.bigdata.gom;

import java.net.URL;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFFormat;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;

public class TestGPO extends LocalGOMTestCase {

	/**
	 * The initial state rdf store is defined in the testgom.n3 file
	 */
	protected void doLoadData() {
		final URL n3 = TestGOM.class.getResource("testgom.n3");

		try {
			load(n3, RDFFormat.N3);
		} catch (Exception e) {
			fail("Unable to load test data");
		}
	}

	public void testHashCode() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI clssuri = vf.createURI("gpo:#1");
	    IGPO clssgpo = om.getGPO(clssuri);
		
	    assertTrue(clssgpo.hashCode() == clssgpo.getId().hashCode());
	    assertTrue(clssgpo.hashCode() == clssuri.hashCode());
	}
	
	public void testLinkSetsIn() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI clssuri = vf.createURI("gpo:#1");
	    IGPO clssgpo = om.getGPO(clssuri);
		
	    ILinkSet ls = clssgpo.getLinksIn(vf.createURI("attr:/type"));
	    
	    checkLinkSet(ls, 2);
	}

	/**
	 * Checks consistency with added and removed values
	 */
	public void testLinkSetsOut() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
		
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    checkLinkSet(ls, 2);
	    
	    final URI gpo678uri = vf.createURI("gpo:#678");
	    workergpo.addValue(worksFor, gpo678uri);
	    
	    checkLinkSet(ls, 3);
	    
	    workergpo.removeValue(worksFor, gpo678uri);

	    checkLinkSet(ls, 2);
	    
	    workergpo.removeValues(worksFor);
	    
	    checkLinkSet(ls, 0);
	}
	
	/**
	 * Checks linkSet membership
	 */
	public void testMembership() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
		
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    checkLinkSet(ls, 2);

	    final URI companyuri = vf.createURI("gpo:#456");
	    IGPO companygpo = om.getGPO(companyuri);
	    
	    assertTrue(companygpo.isMemberOf(ls));

	}

	public void testValues() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    final URI notWorksFor = vf.createURI("attr:/employee#notWorksFor");
		
	    assertTrue(workergpo.getValues(worksFor).size() > 0);
	    
	    assertTrue(workergpo.getValues(notWorksFor).isEmpty());
		
	}

	public void testBound() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    final URI notWorksFor = vf.createURI("attr:/employee#notWorksFor");
		
	    assertTrue(workergpo.isBound(worksFor));
	    
	    assertFalse(workergpo.isBound(notWorksFor));
	}
	
	public void testRemoveValues() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
		
	    assertFalse(workergpo.getValues(worksFor).isEmpty());
	    workergpo.removeValues(worksFor);
	    assertTrue(workergpo.getValues(worksFor).isEmpty());
	}
	
	public void testRemoveValue() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
		
	    assertFalse(workergpo.getValues(worksFor).isEmpty());
	    Value old = workergpo.getValue(worksFor);
	    while (old != null) {
	    	workergpo.removeValue(worksFor, old);
	    	old = workergpo.getValue(worksFor);
	    }
	    assertTrue(workergpo.getValues(worksFor).isEmpty());
	}
	
	public void testRemove() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    final ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    assertTrue(ls.size() > 0);
	    
	    final IGPO employer = ls.iterator().next();
	    final int lssize = employer.getLinksIn(worksFor).size();
	    
	    assertTrue(lssize > 0);
	    
	    workergpo.remove();
	    
	    checkLinkSet(employer.getLinksIn(worksFor), lssize - 1);
	    
	    try {
	    	ls.size();
	    	fail("Expected exception after removing GPO");
	    } catch (IllegalStateException ise) {
	    	// expected
	    }
	    
	    try {
	    	workergpo.getLinksOut(worksFor);
	    	fail("Expected exception after removing GPO");
	    } catch (IllegalStateException ise) {
	    	// expected
	    }
	}
	
	/**
	 * Checks for reverse linkset referential integrity when property removed
	 */
	public void testLinkSetConsistency1() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    final ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    assertTrue(ls.size() > 0);
	    
	    final IGPO employer = ls.iterator().next();
	    
	    final ILinkSet employees = employer.getLinksIn(worksFor);
	    assertTrue(employees.contains(workergpo));
	    
	    workergpo.removeValue(worksFor, employer.getId());
	    
	    assertFalse(employees.contains(workergpo));
	}
	
	/**
	 * Checks for reverse linkset referential integrity when property replaced
	 */
	public void testLinkSetConsistency2() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    final ILinkSet ls = workergpo.getLinksOut(worksFor);
	    
	    assertTrue(ls.size() > 0);
	    
	    final IGPO employer = ls.iterator().next();
	    
	    final ILinkSet employees = employer.getLinksIn(worksFor);
	    assertTrue(employees.contains(workergpo));
	    
	    // set property to new URI
	    final URI newuri = vf.createURI("gpo:#999");
	    workergpo.setValue(worksFor, newuri);
	    
	    assertFalse(employees.contains(workergpo));
	    assertTrue(om.getGPO(newuri).getLinksIn(worksFor).contains(workergpo));
	}
	
	/**
	 * Checks for consistency as link set is created and modified
	 * 
	 * Dependent: setValue, getValue, removeValue, getLinksIn
	 */
	public void testLinkSetConsistency3() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final IGPO worker1 = om.getGPO(vf.createURI("gpo:#1000"));
	    final IGPO worker2 = om.getGPO(vf.createURI("gpo:#1001"));
	    final IGPO worker3 = om.getGPO(vf.createURI("gpo:#1002"));

	    final IGPO employer = om.getGPO(vf.createURI("gpo:#1003"));
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    
	    final ILinkSet ls = employer.getLinksIn(worksFor);
	    
	    checkLinkSet(ls, 0);
	    
	    worker1.setValue(worksFor, employer.getId());
	    checkLinkSet(ls, 1);
	    
	    // repeat - should be void
	    worker1.setValue(worksFor, employer.getId());
	    assertTrue(worker1.getValue(worksFor).equals(employer.getId()));
	    checkLinkSet(ls, 1);

	    worker2.setValue(worksFor, employer.getId());
	    checkLinkSet(ls, 2);

	    worker3.setValue(worksFor, employer.getId());
	    checkLinkSet(ls, 3);
	    
	    // now check on removal
	    worker2.removeValue(worksFor, employer.getId());
	    assertTrue(worker2.getValue(worksFor) == null);
	    checkLinkSet(ls, 2);
	    
	    assertTrue(om.getDirtyObjectCount() == 3);

	}
}
