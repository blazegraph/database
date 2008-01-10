/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 13, 2007
 */

package com.bigdata.btree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.io.DataOutputBuffer;

/**
 * Test suite for {@link ByteArrayValueSerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestByteArrayValueSerializer extends TestCase2 {

    /**
     * 
     */
    public TestByteArrayValueSerializer() {
    }

    /**
     * @param arg0
     */
    public TestByteArrayValueSerializer(String arg0) {
        super(arg0);
    }
    
    public void assertEquals(byte[][] expected, byte[][] actual) {
        
        assertEquals("length",expected.length, actual.length);
        
        for(int i=0; i<expected.length; i++) {
            
            try {

                assertEquals(expected[i],actual[i]);
                
            } catch(AssertionFailedError ex) {
                
                fail("Index="+i+", expecting: "+expected[i]+", actual="+actual[i], ex);
                
            }
            
        }
        
    }

    public void test_zeroValues() {
        
        doRoundTripTest(new byte[][]{});
        
    }
    
    public void test_oneValue_nonNull() {
        
        doRoundTripTest(new byte[][]{new byte[]{1}});
        
    }
    
    public void test_oneValue_null() {
        
        doRoundTripTest(new byte[][]{null});
        
    }
    
    Random r = new Random();
    
    public byte[] getRandomValue() {
        
        boolean nullValue = r.nextBoolean();
        
        byte[] data = (nullValue?null:new byte[r.nextInt(255)]);
        
        if(!nullValue) {
           
            r.nextBytes(data);
            
        }
        
        return data;
        
    }
    
    public void test_stress() {

        final int ntrials = 1000;
        
        for(int trial=0;trial<ntrials; trial++) {
            
            final int n = r.nextInt(1000);
            
            byte[][] expected = new byte[n][];
            
            for(int i=0; i<n; i++) {
               
                expected[i] = getRandomValue();
                
            }
            
            doRoundTripTest(expected);
            
        }
        
    }
    
    public void doRoundTripTest(byte[][] values) {
        
        IValueSerializer ser = ByteArrayValueSerializer.INSTANCE;

        try {
            
            final byte[] data;
            {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//            DataOutputStream dos = new DataOutputStream(baos);
                
                DataOutputBuffer dos = new DataOutputBuffer();

                ser.putValues(dos, values, values.length);
            
//            dos.flush();
            
                data = dos.array();
            
            }

            byte[][] actual = new byte[values.length][];
            
            {
            
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
             
                DataInputStream dis = new DataInputStream(bais);
                
                ser.getValues(dis, actual, values.length);
                
            }
            
            assertEquals(values,actual);

        } catch (IOException ex) {

            fail("Not expecting: " + ex);

        }

    }

}
