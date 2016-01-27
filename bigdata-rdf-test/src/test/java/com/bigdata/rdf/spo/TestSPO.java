/*

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
/*
 * Created on Sep 18, 2009
 */

package com.bigdata.rdf.spo;

import junit.framework.TestCase2;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.StatementEnum;

/**
 * Test suite for the {@link SPO} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPO extends TestCase2 {

    /**
     * 
     */
    public TestSPO() {
    }

    /**
     * @param name
     */
    public TestSPO(String name) {
        super(name);
    }

    public void test1() {
    	// TODO: write tests for SPO
    }

    public void test_serializable() {

    	final IV<?,?> s = new TermId<BigdataURI>(VTE.URI, 1);
    	final IV<?,?> p = new TermId<BigdataURI>(VTE.URI, 2);
    	final IV<?,?> o = new TermId<BigdataURI>(VTE.URI, 3);
    	
    	final SPO expected = new SPO(s, p, o);
    	
    	doRoundTripTest(expected);
    	
    }
    
    public void test_serializable_sidIV() {

    	final IV<?,?> s = new TermId<BigdataURI>(VTE.URI, 1);
    	final IV<?,?> p = new TermId<BigdataURI>(VTE.URI, 2);
    	final IV<?,?> o = new TermId<BigdataURI>(VTE.URI, 3);
    	
    	final SPO expected = new SPO(s, p, o, StatementEnum.Explicit);
    	
    	doRoundTripTest(expected);

//    	expected.setStatementIdentifier(true);
    	
    	doRoundTripTest(expected);

    	final IV<?,?> p1 = new TermId<BigdataURI>(VTE.URI, 4);
    	final IV<?,?> o1 = new TermId<BigdataURI>(VTE.URI, 5);
    	
		final SPO expected2 = new SPO(expected.c(), p1, o1,
				StatementEnum.Explicit);

    	doRoundTripTest(expected2);

    }
    
    private void doRoundTripTest(final SPO expected) {
    	
    	final byte[] b = SerializerUtil.serialize(expected);
    	
    	final SPO actual = (SPO)SerializerUtil.deserialize(b);

		assertEquals(expected, actual);
    	
    }
    
}
