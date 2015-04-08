/* 
 * Licensed to the SYSTAP, LLC under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * SYSTAP, LLC licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed  under the  License is distributed on an "AS IS" BASIS,
 * WITHOUT  WARRANTIES OR CONDITIONS  OF ANY KIND, either  express  or
 * implied.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package junit.framework;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Properties;
import java.util.Random;

/**
 * Test case for {@link TestCase2}.<p>
 *
 * Note: These tests are not very through.<p>
 */

public class TestCase2TestCase
    extends TestCase2
{

    public static Test suite()
    {

        TestSuite suite = new TestSuite();
	
	suite.addTestSuite
	    ( TestCase2TestCase.class
	      );
	
	return suite;

    }

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * Unit test for {@link #getRandomOrder( int n )}.
     *
     */
    public void test_getRandomOrder()
    {

	final int n = 6;

	final int[] order = getRandomOrder( n );

	// Verify #of elements.
	assertEquals
	    ( "length",
	      n,
	      order.length
	      );

	// Verify that the values in order[] are all in the expected
	// range [0:n-1].

	for( int i=0; i<n; i++ ) {

	    if( order[i] < 0 || order[i] >= n ) {
		
		// An index value in order[] is out of the expected
		// index range.
		
		fail( "Expecting value in [0:"+(n-1)+"]"+
		      ", but found value="+order[i]+" in position="+i
		      );
		
	    }
	    
	    System.err.println( "order[i="+i+"] = "+order[i]);

	}

	boolean[] consumed = new boolean[ n ];

	for( int i=0; i<n; i++ ) {
	    
	    consumed[ i ] = false;

	}

	// We are expecting to find the indices [0..n-1] in a random
	// ordering within order[].  [i] is the index that we are
	// looking for one each pass.  We search the elements of
	// order[] in the inner loop on [j].  Each time we find the
	// index that we are looking for in some position within
	// order[] we mark that position as consumed so that we do not
	// revisit it.

	for( int i=0; i<n; i++ ) {
		
	    boolean found = false;

	    for( int j=0; j<n; j++ ) {

		if( consumed[j] ) continue;

		if( order[j] == i ) {

		    consumed[j] = true;

		    found = true;

		    break;

		}

	    }

	    if( ! found ) {

		fail( "Could not find index="+i+" in "+order
		      );

	    }

	}

	// This is a sanity check on the test itself.  It makes sure
	// that each index in order[] was consumed.

	for( int i=0; i<n; i++ ) {

	    if( ! consumed[i] ) {

		fail( "order[i="+i+"] was not matched." );

	    }

	}

    }

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * <p>
     * Tests some of the property loader behaviors.
     * </p>
     * <p>
     * Note: This test will fail unless your <code>build.properties</code>
     * defines <code>junit.framework.TestCase2.enabled=false</code>.  This
     * file must be located in <code>${user.home}</code>, wherever that is
     * for your system.
     * </p>
     */

    public void test_getProperties()
    {

	Properties p = getProperties();
				// force loading of properties.
	
	// This is bound locally in TestCase2Test.properties.
	
	assertEquals
	    ( "red",
	      p.getProperty( "color" )
	      );
	
	// This is enabled in our .properties file but disabled in
	// _my_ ${user.home}/build.properties.  Since the latter
	// overrides the former, the property should be reported as
	// disabled.  If _your_ build.properties does not define this
	// property then this test will fail.
	
	final String userHome = System.getProperty("user.home");
	System.err.println("user.home = "+userHome);
	final String buildProperties = userHome+File.pathSeparator+"build.properties";
	if( ! new File(buildProperties).exists() ) {
	    log.warn("File not found: "+buildProperties);
	}
	final String actual = p.getProperty("junit.framework.TestCase2.enabled");
	assertEquals("false", actual);
	
    }

    /**
     * Tests correct reading of a local text resource.
     */

    public void test_getTestResource()
    {

	final String prefix = "junit/framework/";

	String data = getTestResource
	    ( prefix+"test-entity-1.txt"
	      );

	assertEquals
	    ( data,
	      "A text resource."
	      );

    }

    public void test_getTestInputStream()
    {

	final String prefix = "junit/framework/";

	final int expected = 30996;	// #of bytes that we are expecting.

	int actual = 0;

	try {
	    
	    InputStream is = getTestInputStream
		( prefix+"test-entity-2.bin"
		  );
	    
	    while( true ) {
		
		int b = is.read();
		
		if( b == -1 ) break;
		
		actual++;
		
	    }
	    
	}

	catch( IOException ex ) {

	    fail( "Not expecting: "+ex
		  );

	}

	assertEquals
	    ( "Read wrong number of bytes",
	      expected,
	      actual
	      );

    }

    public void test_assertEquals_double_array()
    {
    
        assertEquals
            ( new double[]{4d,5d},
              new double[]{4d,5d}
              );                    

        try {
            
            assertEquals
            	( new double[]{4d,5d},
                  new double[]{4.1d,5.1d}
            	  );
            
            // We can't throw AssertionFailedError here since that
            // is what we expect the method under test to throw!
            throw new RuntimeException
            	( "Expected exception: "+AssertionFailedError.class
                  );
            
        }
        
        catch( AssertionFailedError ex ) {
            
            log.info
               ( "Ignoring expected exception: "+ex
                 );
            
        }
        
    }    

    /**
     * TODO Do test with different object types for the array.
     */

    public void test_assertEquals_Object_array()
    {
    
        assertEquals
        ( new Object[]{"Hi there","Joe"},
          new Object[]{"Hi there","Joe"}
          );                    

        try {
            
            assertEquals
            	( new Object[]{"Hi there","Joe"},
                  new Object[]{"Hi there","Mary"}
            	  );
            
            // We can't throw AssertionFailedError here since that
            // is what we expect the method under test to throw!
            throw new RuntimeException
            	( "Expected exception: "+AssertionFailedError.class
                  );
            
        }
        
        catch( AssertionFailedError ex ) {
            
            log.info
               ( "Ignoring expected exception: "+ex
                 );
            
        }

        // Verify that the array class is ignored.
            assertEquals
            	( new String[]{"Hi there","Joe"},
                  new Object[]{"Hi there","Joe"}
            	  );
        
    }

    public void test_getInnerCause_correctRejection() {

        try {
            getInnerCause(null, null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            getInnerCause(null, RuntimeException.class);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            getInnerCause(new RuntimeException(), null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * Finds cause when it is on top of the stack trace and the right type.
     */
    public void test_getInnerCause01_find_exact() {

        Throwable t = new RuntimeException();
         
        assertTrue(t == getInnerCause(t, RuntimeException.class));
            
    }

    /**
     * Find cause when it is on top of the stack trace and a subclass of the
     * desired type.
     */
    public void test_getInnerCause01_find_subclass() {

        Throwable t = new IOException();
         
        assertTrue(t == getInnerCause(t, Exception.class));
        
    }

    /**
     * Does not find cause that is a super class of the desired type.
     */
    public void test_getInnerCause01_reject_superclass() {

        Throwable t = new Exception();
         
        assertNull(getInnerCause(t, IOException.class));
        
    }
    
    /**
     * Does not find cause when it is on top of the stack trace and not either
     * the desired type or a subclass of the desired type.
     */
    public void test_getInnerCause01_reject_otherType() {

        Throwable t = new Throwable();
         
        assertNull(getInnerCause(t, Exception.class));
        
    }

    /**
     * Finds inner cause that is the exact type.
     */
    public void test_getInnerCause02_find_exact() {

        Throwable cause = new Exception();

        Throwable t = new Throwable(cause);

        assertTrue(cause == getInnerCause(t, Exception.class));

    }

    /**
     * Finds inner cause that is a derived type (subclass).
     */
    public void test_getInnerCause02_find_subclass() {

        Throwable cause = new IOException();

        Throwable t = new Throwable(cause);

        assertTrue(cause == getInnerCause(t, Exception.class));

    }

    /**
     * Does not find inner cause that is a super class of the desired type.
     */
    public void test_getInnerCause02_reject_superclass() {

        Throwable cause = new Exception();
        
        Throwable t = new RuntimeException(cause);
         
        assertNull( getInnerCause(t, IOException.class));
        
    }
    
    /**
     * Does not find an inner cause that is neither the specified type nor a
     * subtype of the specified type.
     */
    public void test_getInnerCause03_reject_otherType() {

        Throwable cause = new RuntimeException();

        Throwable t = new Exception(cause);

        assertNull( getInnerCause(t, IOException.class) );

    }
    
    /**
     * Test helper generates the binary entity read by one of the test cases
     * above.
     * 
     * @param args
     */
    public static void main( String[] args )
    	throws IOException
    {

	final String prefix = "src/test/junit/framework/";

	final int expected = 30996;	// #of bytes that we are expecting.

	OutputStream os = new BufferedOutputStream(new FileOutputStream(prefix
                + "test-entity-2.bin",false));
	
	Random r = new Random();
	
	for( int i=0; i<expected; i++ ) {
	    
	    byte b = (byte) r.nextInt(255); 
	    
	    os.write( b );
	    
	}
	
	os.flush();
	
	os.close();
	
    }
    
}
