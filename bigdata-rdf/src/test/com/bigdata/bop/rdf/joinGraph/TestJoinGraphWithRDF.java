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
 * Created on Jan 18, 2011
 */

package com.bigdata.bop.rdf.joinGraph;

import junit.framework.TestCase2;

import com.bigdata.bop.controller.JoinGraph;

/**
 * A test suite for the {@link JoinGraph} against RDF data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJoinGraphWithRDF extends TestCase2 {

    /**
     * 
     */
    public TestJoinGraphWithRDF() {
    }

    /**
     * @param name
     */
    public TestJoinGraphWithRDF(String name) {
        super(name);
    }

    /**
     * Test the ability to dynamically attach a constraint to the first join in
     * a join path at which all of the variables in that constraint would be
     * bound (the join graph only considers non-optional joins).
     */
    public void test_constraintAttachment() {
        fail("write test");
    }
    
}
