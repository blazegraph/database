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
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.util.ArrayList;
import java.util.List;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rdf.internal.IV;

/**
 * Test suite for {@link IVSolutionSetEncoder} and {@link IVSolutionSetDecoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestIVSolutionSetEncoder extends AbstractBindingSetEncoderTestCase {

    /**
     * 
     */
    public TestIVSolutionSetEncoder() {
    }

    /**
     * @param name
     */
    public TestIVSolutionSetEncoder(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
     
        super.setUp();

        // The encoder under test.
        encoder = new IVSolutionSetEncoder();
        
        // The decoder under test (same object as the encoder).
        decoder = new IVSolutionSetDecoder();
        
    }

//    protected void tearDown() throws Exception {
//        
//        super.tearDown();
//        
//        // Clear references.
//        encoder.release();
//        encoder = null;
//        decoder = null;
//        
//    }

    /**
     * Unit test of the stream-oriented API.
     */
    @SuppressWarnings("rawtypes")
    public void test_streamAPI() {
        
        final List<IBindingSet> expectedSolutions = new ArrayList<IBindingSet>();

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            expectedSolutions.add(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            expectedSolutions.add(expected);
        }

        doEncodeDecodeTest(expectedSolutions);

    }

    /**
     * Multiple solutions where an empty solution appears in the middle of the
     * sequence.
     */
    @SuppressWarnings("rawtypes")
    public void test_streamAPI2() {

        final List<IBindingSet> expectedSolutions = new ArrayList<IBindingSet>();
        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));

            expectedSolutions.add(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();

            expectedSolutions.add(expected);
        }

        {
            final IBindingSet expected = new ListBindingSet();
            expected.set(Var.var("x"), new Constant<IV>(termId));
            expected.set(Var.var("y"), new Constant<IV>(blobIV));

            expectedSolutions.add(expected);
        }

        doEncodeDecodeTest(expectedSolutions);

    }

    protected void doEncodeDecodeTest(final List<IBindingSet> expectedSolutions) {

        final int nsolutions = expectedSolutions.size();
        
        final DataOutputBuffer out = new DataOutputBuffer();
        
        for (IBindingSet bset : expectedSolutions) {
        
            ((IVSolutionSetEncoder) encoder).encodeSolution(out, bset);
            
        }
        
        final DataInputBuffer in = new DataInputBuffer(out.array());

        for (int i = 0; i < nsolutions; i++) {

            final IBindingSet expected = expectedSolutions.get(i);
            
            final IBindingSet actual = ((IVSolutionSetDecoder) decoder)
                    .decodeSolution(in, true/* resolveCachedValues */);

            assertEquals(expected, actual, true/* testCache */);
            
        }
        
    }
    
}
