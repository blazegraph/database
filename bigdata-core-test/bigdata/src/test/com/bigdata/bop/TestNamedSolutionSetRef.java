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
/*
 * Created on Aug 31, 2011
 */

package com.bigdata.bop;

import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.NamedSolutionSetRef;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.Var;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.journal.ITx;

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
    public TestNamedSolutionSetRef(final String name) {
        super(name);
    }

    public void test_ctor() {
        
        @SuppressWarnings("rawtypes")
        final IVariable x = Var.var("x");
        @SuppressWarnings("rawtypes")
        final IVariable y = Var.var("y");

        final UUID queryId = UUID.randomUUID();
        final String namedSet = "namedSet1";
        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = new IVariable[] { x, y };
        
        {
            final INamedSolutionSetRef ref = new NamedSolutionSetRef(queryId,
                    namedSet, joinVars);

            assertEquals("namedSet", namedSet, ref.getLocalName());
            assertTrue("queryId", queryId.equals(ref.getQueryId()));
            assertTrue("joinVars", Arrays.deepEquals(joinVars, ref.getJoinVars()));
        }

        // Allowed by BLZG-1493. Used to scope the query attributes to the current query.
//        try {
//            new NamedSolutionSetRef(null/*queryId*/, namedSet, joinVars);
//            fail("Expecting: " + IllegalArgumentException.class);
//        } catch (IllegalArgumentException ex) {
//            if (log.isInfoEnabled())
//                log.info("Expecting: " + IllegalArgumentException.class);
//        }

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

        @SuppressWarnings("rawtypes")
        final IVariable x = Var.var("x");
        @SuppressWarnings("rawtypes")
        final IVariable y = Var.var("y");

        final UUID queryId = UUID.randomUUID();
        final String namedSet = "namedSet1";
        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars = new IVariable[] { x, y };

        final String namespace = "kb";
        
        final long timestamp = ITx.UNISOLATED;
        
        /*
         * First, test a reference for a named solution set attached to a query.
         */
        
        final INamedSolutionSetRef ref = NamedSolutionSetRefUtility
                .newInstance(queryId, namedSet, joinVars);

        final String s = ref.toString();

        if (log.isInfoEnabled())
            log.info(s);

        final INamedSolutionSetRef ref2 = NamedSolutionSetRefUtility.valueOf(s);

        if (log.isInfoEnabled())
            log.info(ref2.toString());

        assertTrue(ref.equals(ref2));
        assertTrue(ref2.equals(ref));
        assertTrue(ref.equals(ref));
        assertTrue(ref2.equals(ref2));

        /*
         * Now test a reference for a named solution set associated with a KB
         * (either durable or cached).
         */

        final INamedSolutionSetRef refX = NamedSolutionSetRefUtility
                .newInstance(namespace, timestamp, namedSet, joinVars);

        final String sX = refX.toString();

        if (log.isInfoEnabled())
            log.info(sX);

        final INamedSolutionSetRef refX2 = NamedSolutionSetRefUtility.valueOf(sX);

        if (log.isInfoEnabled())
            log.info(refX2.toString());

        assertTrue(refX.equals(refX2));
        assertTrue(refX2.equals(refX));
        assertTrue(refX.equals(refX));
        assertTrue(refX2.equals(refX2));

        /*
         * Verify that ref and refX are not equals!
         */
        assertFalse(ref.equals(refX));
        assertFalse(refX.equals(ref));
        
    }
    
}
