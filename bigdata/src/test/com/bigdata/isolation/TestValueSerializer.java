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

package com.bigdata.isolation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.io.DataOutputBuffer;

/**
 * Test suite for {@link Value.Serializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestValueSerializer extends TestCase2 {

    /**
     * 
     */
    public TestValueSerializer() {
    }

    /**
     * @param arg0
     */
    public TestValueSerializer(String arg0) {
        super(arg0);
    }
    
    public void assertEquals(Value expected, Value actual) {
        
        assertEquals("versionCounter", expected.versionCounter,
                actual.versionCounter);

        assertEquals("deleted", expected.deleted, actual.deleted);

        assertEquals("datum", expected.datum, actual.datum);
   
    }
    
    public void assertEquals(Value[] expected, Value[] actual) {
        
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
        
        doRoundTripTest(new Value[]{});
        
    }
    
    public void test_oneValue_nonNull() {
        
        doRoundTripTest(new Value[]{new Value((short)2,false,new byte[]{12})});
        
    }
    
    public void test_oneValue_null() {
        
        doRoundTripTest(new Value[]{new Value((short)3,false,null)});
        
    }
    
    public void test_oneValue_deleted() {
        
        doRoundTripTest(new Value[]{new Value((short)4,true,null)});
        
    }

    Random r = new Random();
    
    public Value getRandomValue() {
        
        short versionCounter = (short)r.nextInt(Short.MAX_VALUE+1);
        
        boolean deleted = r.nextBoolean();
        
        byte[] data = (deleted?null:new byte[r.nextInt(255)]);
        
        if(!deleted) {
           
            r.nextBytes(data);
            
        }
        
        return new Value(versionCounter,deleted,data);
        
    }
    
    public void test_stress() {

        final int ntrials = 1000;
        
        for(int trial=0;trial<ntrials; trial++) {
            
            final int n = r.nextInt(1000);
            
            Value[] expected = new Value[n];
            
            for(int i=0; i<n; i++) {
               
                expected[i] = getRandomValue();
                
            }
            
            doRoundTripTest(expected);
            
        }
        
    }
    
    public void doRoundTripTest(Value[] values) {
        
        Value.Serializer ser = Value.Serializer.INSTANCE;

        try {
            
            final byte[] data;
            {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//            DataOutputStream dos = new DataOutputStream(baos);
                
                DataOutputBuffer dos = new DataOutputBuffer();

            ser.putValues(dos, values, values.length);
            
//            dos.flush();
            
            data = dos.toByteArray();
            }

            Value[] actual = new Value[values.length];
            
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
