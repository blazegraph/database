/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import junit.extensions.proxy.IProxyTest;
import junit.framework.Test;
import junit.framework.TestCase;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;

import com.bigdata.gom.gpo.IGPO;
import com.bigdata.gom.gpo.ILinkSet;
import com.bigdata.gom.om.IObjectManager;
import com.bigdata.gom.om.ObjectManager;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class TestGPO extends ProxyGOMTest {

    protected void checkLinkSet(final ILinkSet ls, int size) {
    	assertTrue(ls.size() == size);
    	Iterator<IGPO> values = ls.iterator();
    	int count = 0;
    	while (values.hasNext()) {
    		count++;
    		values.next();
    	}
    	assertTrue(count == size);
    }

    /**
	 * The initial state rdf store is defined in the testgom.n3 file
	 */
	protected void doLoadData() {
		final URL n3 = TestGOM.class.getResource("testgom.n3");

		try {
			((IGOMProxy) m_delegate).load(n3, RDFFormat.N3);
		} catch (Exception e) {
			fail("Unable to load test data");
		}
	}

	public TestGPO() {
		
	}
	
	public TestGPO(String testName) {
		super(testName);
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
		
	    final URI linkURI = vf.createURI("attr:/type");
	    ILinkSet ls = clssgpo.getLinksIn(linkURI);
	    
	    assertTrue(ls.getOwner() == clssgpo);
	    assertTrue(ls.isLinkSetIn());
	    assertTrue(ls.getLinkProperty().equals(linkURI));
	    
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
	    
	    assertTrue(ls.getOwner() == workergpo);
	    assertFalse(ls.isLinkSetIn());
	    assertTrue(ls.getLinkProperty().equals(worksFor));
	    
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

	public void testStatements() {
		doLoadData();
		
		final ValueFactory vf = om.getValueFactory();

	    final URI workeruri = vf.createURI("gpo:#123");
	    IGPO workergpo = om.getGPO(workeruri);
	    
	    assertTrue(workergpo.getStatements().size() == 6);
	    
	    final URI worksFor = vf.createURI("attr:/employee#worksFor");
	    workergpo.removeValues(worksFor);

	    assertTrue(workergpo.getStatements().size() == 4);
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
		
	    assertFalse(workergpo.getLinksOut(worksFor).isEmpty());
	    assertFalse(workergpo.getValues(worksFor).isEmpty());
	    workergpo.removeValues(worksFor);
	    assertTrue(workergpo.getValues(worksFor).isEmpty());
	    assertTrue(workergpo.getLinksOut(worksFor).isEmpty());
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
	    
	    // assertTrue(om.getDirtyObjectCount() == 3);

	}

}
