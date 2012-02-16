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
/*
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase2;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueSerializer;

/**
 * Test suite for {@link IVSolutionSetEncoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVSolutionSetEncoder extends TestCase2 {

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

    public void test_emptySolutionSet() {
        final IBindingSet[] expected = new IBindingSet[0];
        doEncodeDecodeTest(expected);
    }
    
    public void test_oneSolutionInSet() {
        
    }
    
    public void test_twoSolutionsInSet() {
        
    }
    
    public void test_solutionsWithCachedIVs() {
        
    }
    
    public void test_solutionsWithSIDs() {
        
    }

    private void doEncodeDecodeTest(final IBindingSet[] expected
            ) {

        final IBindingSet[] actual = null;
        
        
    }

    /**
     * Encoder for solution sets.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public class IVSolutionSetEncoder implements IBindingSetEncoder {

        private final IVBindingSetEncoder encoder;
        private final Map<IV<?, ?>, BigdataValue> sharedCache;
        private final Map<IV<?, ?>, BigdataValue> localCache;
        
        public IVSolutionSetEncoder() {

            this.encoder = new IVBindingSetEncoder(false/*filter*/);
            
            this.sharedCache = new HashMap<IV<?,?>, BigdataValue>();
            
            this.localCache = new HashMap<IV<?,?>, BigdataValue>();
            
        }

        public int encode(final OutputStream os, final IBindingSet bset) {
            
            /*
             * First, figure out how many bound and unbound variables there are and
             * how many of those bindings are IVs with cached BigdataValues. Unbound
             * variables will be represented by a NullIV. Bound variables will be
             * represented by the IV. If the bound variable has a cached
             * BigdataValue, then it is inlined into the record the first time it is
             * encountered.
             */

            localCache.clear();
//            final byte[] a = encoder.encodeSolution(localCache, bset);
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] encodeSolution(IBindingSet bset) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public byte[] encodeSolution(IBindingSet bset, boolean updateCache) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void flush() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void release() {
            // TODO Auto-generated method stub
            
        }
        
    }

    /**
     * Decoder for {@link IVSolutionSetEncoder}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static class IVSolutionSetDecoder implements IBindingSetDecoder {

        private final BigdataValueFactory valueFactory;

        private final BigdataValueSerializer<BigdataValue> valueSer;

        private final byte[] data;

        private int off;

        /**
         * @param filter
         */
        public IVSolutionSetDecoder(final BigdataValueFactory valueFactory,
                final byte[] data, final int off) {

//            super(false/* filter */);

            if (valueFactory == null)
                throw new IllegalArgumentException();

            if (data == null)
                throw new IllegalArgumentException();

            if (off < 0 || off >= data.length)
                throw new IllegalArgumentException();

            this.valueFactory = valueFactory;

            this.valueSer = valueFactory.getValueSerializer();
            
            this.data = data;

            this.off = off;

        }

        public IBindingSet decodeNext() {
            throw new UnsupportedOperationException();
        }

        /*
         * Note: The decode needs to update the internal cache regardless of
         * whether or not the IVCache is resolved on the decoded IVs.
         */
        @Override
        public IBindingSet decodeSolution(byte[] val, int off, int len,
                boolean resolveCachedValues) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void release() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void resolveCachedValues(IBindingSet bset) {
            // TODO Auto-generated method stub
            
        }

    }

}
