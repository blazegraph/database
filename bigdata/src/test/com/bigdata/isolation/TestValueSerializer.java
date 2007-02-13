/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 13, 2007
 */

package com.bigdata.isolation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

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
        
        assertEquals(expected.versionCounter,actual.versionCounter);
        
        assertEquals(expected.deleted,actual.deleted);
        
        assertEquals(expected.value,actual.value);
        
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
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(baos);

            ser.putValues(dos, values, values.length);
            
            dos.flush();
            
            data = baos.toByteArray();
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
