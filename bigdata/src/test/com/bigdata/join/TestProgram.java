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
 * Created on Jun 23, 2008
 */

package com.bigdata.join;

import java.util.Iterator;

import com.bigdata.join.rdf.ISPO;

/**
 * Test suite for {@link Program} and common rule/program re-writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestProgram extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestProgram() {
    }

    /**
     * @param name
     */
    public TestProgram(String name) {
        super(name);
    }

    public void test_emptyProgram() {

        {
            final IProgram program = new Program("p1",false/* parallel */,false/*closure*/);

            assertEquals("p1",program.getName()); 
            
            assertFalse("isParallel", program.isParallel());

            assertFalse("isClosure", program.isClosure());
            
            assertEquals(0,program.stepCount());
            
            assertFalse("isEmpty", program.steps().hasNext());

            assertEquals(0,program.toArray().length);
            
        }

        {
        
            final IProgram program = new Program("p2",true/*parallel*/,false/*closure*/);

            assertEquals("p2",program.getName()); 
            
            assertTrue("isParallel", program.isParallel());

            assertFalse("isClosure", program.isClosure());
            
            assertEquals(0,program.stepCount());
            
            assertFalse("isEmpty", program.steps().hasNext());

            assertEquals(0,program.toArray().length);

        }

        {
            
            final IProgram program = new Program("p3",true/*parallel*/,true/*closure*/);

            assertEquals("p3",program.getName());            
            
            assertTrue("isParallel", program.isParallel());

            assertTrue("isClosure", program.isClosure());

            assertEquals(0,program.stepCount());

            assertFalse("isEmpty", program.steps().hasNext());

            assertEquals(0,program.toArray().length);

        }
        
    }

    public void test_simpleProgram() {
        
        final Program program = new Program("p1", false/* parallel */, false/* closure */);
        
        final IRelation<ISPO> relation = new MockRelation<ISPO>();
        
        final IRule rule = new TestRuleRdfs04a(relation);
        
        program.addStep( rule );

        {

            Iterator<IProgram> itr = program.steps(); 
            
            assertTrue(itr.hasNext());
            
            assertTrue(itr.next() == rule);
            
            assertFalse(itr.hasNext());
        
            assertEquals(1,program.stepCount());
            
        }
        
    }
    
}
