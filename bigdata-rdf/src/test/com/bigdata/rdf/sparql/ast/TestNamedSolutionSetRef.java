/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 31, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.NamedSolutionSetRef;

/**
 * Test suite for {@link NamedSolutionSetRef}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNamedSolutionSetRef extends TestCase2 {

    /**
     * 
     */
    public TestNamedSolutionSetRef() {
    }

    /**
     * @param name
     */
    public TestNamedSolutionSetRef(String name) {
        super(name);
    }

    public void test_ctor() {
        
        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");

        final UUID queryId = UUID.randomUUID();
        final String namedSet = "namedSet1";
        final IVariable[] joinVars = new IVariable[] { x, y };

        final NamedSolutionSetRef ref = new NamedSolutionSetRef(queryId,
                namedSet, joinVars);

        try {
            new NamedSolutionSetRef(null/*queryId*/, namedSet, joinVars);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }


        try {
            new NamedSolutionSetRef(queryId, null/*namedSet*/, joinVars);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }


        try {
            new NamedSolutionSetRef(queryId, namedSet, null/*joinVars*/);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Expecting: " + IllegalArgumentException.class);
        }

    }

    public void test_valueOf() {

        final IVariable x = Var.var("x");
        final IVariable y = Var.var("y");

        final UUID queryId = UUID.randomUUID();
        final String namedSet = "namedSet1";
        final IVariable[] joinVars = new IVariable[] { x, y };

        final NamedSolutionSetRef ref = new NamedSolutionSetRef(queryId,
                namedSet, joinVars);

        final String s = ref.toString();
        
        System.err.println(s);
        
        final NamedSolutionSetRef ref2 = NamedSolutionSetRef.valueOf(s);
        
        System.err.println(ref2.toString());

        assertEquals(ref,ref2);
        
    }
    
}
