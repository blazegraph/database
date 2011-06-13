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

// Test resource loader.
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import junit.util.PropertyUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Extension of {@link TestCase} that supports logging, loading test
 * resources, and hierarchical properties.<p>
 *
 * Note: When using Maven (or Ant), you need to be aware that that
 * your <code>project.xml</code> has an impact on which files are
 * recognized as tests to be submitted to a test runner.  For example,
 * it is common practice that all files beginning or ending with
 * "Test" will be submitted to JUnit, though I suggest that the
 * practice of naming things "FooTestCase" will serve you better.
 * Also note that the maven test plugin explictly skips files whose
 * name matches the pattern "*AbstractTestSuite".<p>
 */

abstract public class TestCase2
    extends TestCase
{

    public TestCase2() {
        super();
    }
    
    public TestCase2(String name) {
        super(name);
    }

    /**
     * The default {@link Logger} for this class, which is named
     * "junit.framework.Test".
     */

    protected static final Logger log = Logger.getLogger
	( junit.framework.Test.class
	  );

    /**
     * This convenience method provides a version of {@link #fail(
     * String Message )} that also excepts a {@link Throwable} cause
     * for the test failure.<p>
     */

    static public void fail
	( String message,
	  Throwable t
	  )
    {

	AssertionFailedError ex = new AssertionFailedError
	    ( message
	      );

	ex.initCause( t );

	throw ex;

    }

    //************************************************************
    //************************************************************ 
    //************************************************************

    static public void assertEquals( boolean[] expected, boolean[] actual )
    {
        assertEquals( null, expected, actual );
    }

    /**
     * Compares arrays of primitive values.
     */

    static public void assertEquals( String msg, boolean[] expected, boolean[] actual )
    {
    
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == actual ) {
            
            return;
            
        } else
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        } else
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        } else
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            if (expected[i] != actual[i]) {
                /*
                 * Only do message construction if we know that the assert will
                 * fail.
                 */
                assertEquals(msg + "values differ: index=" + i, expected[i],
                        actual[i]);
            }
            
        }
        
    }

    /**
     * <p>
	 * Compares byte[]s by value (not reference).
	 * </p>
	 * <p>
	 * Note: This method will only be invoked if both arguments can be typed as
	 * byte[] by the compiler. If either argument is not strongly typed, you
	 * MUST case it to a byte[] or {@link #assertEquals(Object, Object)} will be
	 * invoked instead.
	 * </p>
	 * 
     * @param expected
     * @param actual
     */
    static public void assertEquals( byte[] expected, byte[] actual )
    {

        assertEquals( null, expected, actual );
        
    }

    /**
	 * <p>
	 * Compares byte[]s by value (not reference).
	 * </p>
	 * <p>
	 * Note: This method will only be invoked if both arguments can be typed as
	 * byte[] by the compiler. If either argument is not strongly typed, you
	 * MUST case it to a byte[] or {@link #assertEquals(Object, Object)} will be
	 * invoked instead.
	 * </p>
	 * 
	 * @param msg
	 * @param expected
	 * @param actual
	 */
    static public void assertEquals( String msg, byte[] expected, byte[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            if (expected[i] != actual[i]) {
                /*
                 * Only do the message construction if we know that the assert
                 * will fail.
                 */
                assertEquals(msg + "values differ: index=" + i, expected[i],
                        actual[i]);
            }
            
        }
        
    }

    static public void assertEquals( char[] expected, char[] actual )
    {
        assertEquals( null, expected, actual );
    }
    
    static public void assertEquals( String msg, char[] expected, char[] actual )
    {
    
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
        
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            if (expected[i] != actual[i]) {
                /*
                 * Only do the message construction if we know that the assert
                 * will fail.
                 */
                assertEquals(msg + "values differ: index=" + i, expected[i],
                        actual[i]);
            }
            
        }
        
    }

    static public void assertEquals( short[] expected, short[] actual )
    {
        assertEquals( null, expected, actual );
    }

    static public void assertEquals( String msg, short[] expected, short[] actual )
    {
        
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == null && actual == null ) {
            
            return;
            
        }

        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }

        for( int i=0; i<expected.length; i++ ) {
            
            if (expected[i] != actual[i]) {
                /*
                 * Only do the message construction if we know that the assert
                 * will fail.
                 */
                assertEquals(msg + "values differ: index=" + i, expected[i],
                        actual[i]);
            }
            
        }
        
    }

    static public void assertEquals( int[] expected, int[] actual )
    {

        assertEquals( null, expected, actual );
        
    }
    
    static public void assertEquals( String msg, int[] expected, int[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == null && actual == null ) {
            
            return;
            
        }

        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }

        for( int i=0; i<expected.length; i++ ) {
            
            if (expected[i] != actual[i]) {
                /*
                 * Only do the message construction if we know that the assert
                 * will fail.
                 */
                assertEquals(msg + "values differ: index=" + i, expected[i],
                        actual[i]);
            }
            
        }
        
    }

    static public void assertEquals( long[] expected, long[] actual )
    {
        assertEquals( null, expected, actual );
    }

    static public void assertEquals( String msg, long[] expected, long[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
        
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            if (expected[i] != actual[i]) {
                /*
                 * Only do the message construction if we know that the assert
                 * will fail.
                 */
                assertEquals(msg + "values differ: index=" + i, expected[i],
                        actual[i]);

            }
             
        }
        
    }

    static public void assertEquals( float[] expected, float[] actual )
    {
        assertEquals( null, expected, actual );
    }
    
    /**
     * TODO Use smarter floating point comparison and offer parameter
     * for controlling the floating point comparison.
     */
    static public void assertEquals( String msg, float[] expected, float[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
     
            float delta = 0f;
            
            try {

                assertEquals(expected[i], actual[i], delta);
                
            } catch (AssertionFailedError ex) {

                /*
                 * Only do the message construction once the assertion is known
                 * to fail.
                 */
                fail(msg + "values differ: index=" + i, ex);

            }

        }
        
    }
    
    static public void assertEquals( double[] expected, double[] actual )
    {
        assertEquals( null, expected, actual );
    }

    /**
     * TODO Use smarter floating point comparison and offer parameter
     * for controlling the floating point comparison.
     */
    static public void assertEquals( String msg, double[] expected, double[] actual )
    {
    
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            double delta = 0.0d;

            try {

                assertEquals(expected[i], actual[i], delta);
                
            } catch (AssertionFailedError ex) {

                /*
                 * Only do the message construction once the assertion is known
                 * to fail.
                 */
                fail(msg + "values differ: index=" + i, ex);

            }

        }
        
    }

    static public void assertEquals( Object[] expected, Object[] actual )
    {
        assertEquals( null, expected, actual );
    }
    
    /**
     * Compares arrays of {@link Object}s.  The length of the arrays
     * must agree, and each array element must agree.  However the 
     * class of the arrays does NOT need to agree, e.g., an Object[]
     * MAY compare as equals with a String[].
     */

    static public void assertEquals( String msg, Object[] expected, Object[] actual )
    {
    
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        if (expected.length != actual.length) {
            /*
             * Only do message construction if we know that the assert will
             * fail.
             */
            assertEquals(msg + "length differs.", expected.length,
                    actual.length);
        }
        
        for( int i=0; i<expected.length; i++ ) {
            
            try {

                assertEquals(expected[i], actual[i]);
                
            } catch (AssertionFailedError ex) {
                
                /*
                 * Only do the message construction once the assertion is known
                 * to fail.
                 */
                
                fail(msg + "values differ: index=" + i, ex);
                
            }
            
        }
        
    }

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * Test helper that can correctly compare arrays of primitives and
     * arrays of objects as well as primitives and objects.
     */
    static public void assertSameValue( Object expected, Object actual )
    {
        assertSameValue( null, expected, actual );
    }
    
    /**
     * Test helper that can correctly compare arrays of primitives and
     * arrays of objects as well as primitives and objects.
     *
     * TODO This will throw a {@link ClassCastException} if
     * <i>actual</i> is of a different primitive array type.  Change
     * the code to throw {@link AssertionFailedError} instead.
     */
    static public void assertSameValue( String msg, Object expected, Object actual )
    {

	if( expected != null && expected.getClass().isArray() ) {

	    if( expected.getClass().getComponentType().isPrimitive() ) {

		Class componentType = expected.getClass().getComponentType();

		if( componentType.equals( Boolean.TYPE ) ) {

		    assertEquals
			( msg,
			  (boolean[]) expected,
			  (boolean[]) actual
			  );
		    
		} else if( componentType.equals( Byte.TYPE ) ) {

		    assertEquals
			( msg,
			  (byte[]) expected,
			  (byte[]) actual
			  );
		
		} else if( componentType.equals( Character.TYPE ) ) {

		    assertEquals
			( msg,
			  (char[]) expected,
			  (char[]) actual
			  );
		
		} else if( componentType.equals( Short.TYPE ) ) {

		    assertEquals
			( msg,
			  (short[]) expected,
			  (short[]) actual
			  );
		
		} else if( componentType.equals( Integer.TYPE ) ) {

		    assertEquals
			( msg,
			  (int[]) expected,
			  (int[]) actual
			  );
		
		} else if( componentType.equals( Long.TYPE ) ) {

		    assertEquals
			( msg,
			  (long[]) expected,
			  (long[]) actual
			  );
		
		} else if( componentType.equals( Float.TYPE ) ) {

		    assertEquals
			( msg,
			  (float[]) expected,
			  (float[]) actual
			  );

		} else if( componentType.equals( Double.TYPE ) ) {

		    assertEquals
			( msg,
			  (double[]) expected,
			  (double[]) actual
			  );

		} else {

		    throw new AssertionError();

		}

	    } else {
		
		assertTrue
		    ( msg,
		      java.util.Arrays.equals
		      ( (Object[]) expected,
			(Object[]) actual
	    	        )
		      );

	    }
	    
	} else {
	    
	assertEquals
	   ( msg,
	     expected,
	     actual
	     );

	}

    }

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * Method verifies that the <i>actual</i> {@link Iterator}
     * produces the expected objects in the expected order.  Objects
     * are compared using {@link Object#equals( Object other )}.  Errors
     * are reported if too few or too many objects are produced, etc.
     */

    static public void assertSameIterator
	( Object[] expected,
	  Iterator actual
	  )
    {
        
        assertSameIterator
            ( "",
              expected,
              actual
              );
        
    }

    /**
     * Method verifies that the <i>actual</i> {@link Iterator}
     * produces the expected objects in the expected order.  Objects
     * are compared using {@link Object#equals( Object other )}.  Errors
     * are reported if too few or too many objects are produced, etc.
     */

    static public void assertSameIterator
	( String msg,
	  Object[] expected,
	  Iterator actual
	  )
    {

	int i = 0;

	while( actual.hasNext() ) {

	    if( i >= expected.length ) {

		fail( msg+": The iterator is willing to visit more than "+
		      expected.length+
		      " objects."
		      );

	    }

	    Object g = actual.next();

//        if (!expected[i].equals(g)) {
        try {
            assertSameValue(expected[i],g);
        } catch(AssertionFailedError ex) {
                /*
                 * Only do message construction if we know that the assert will
                 * fail.
                 */
                fail(msg + ": Different objects at index=" + i + ": expected="
                        + expected[i] + ", actual=" + g);
            }

	    i++;

	}

	if( i < expected.length ) {

	    fail( msg+": The iterator SHOULD have visited "+expected.length+
		  " objects, but only visited "+i+
		  " objects."
		  );

	}

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIteratorAnyOrder(final Object[] expected,
            final Iterator actual) {

        assertSameIteratorAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    @SuppressWarnings("unchecked")
    static public void assertSameIteratorAnyOrder(final String msg,
            final Object[] expected, final Iterator actual) {

        // Populate a map that we will use to realize the match and
        // selection without replacement logic.

        final int nrange = expected.length;

        java.util.Map range = new java.util.HashMap();

        for (int j = 0; j < nrange; j++) {

            range.put(expected[j], expected[j]);

        }

        // Do selection without replacement for the objects visited by
        // iterator.

        for (int j = 0; j < nrange; j++) {

            if (!actual.hasNext()) {

                fail(msg + ": Index exhausted while expecting more object(s)"
                        + ": index=" + j);

            }

            Object actualObject = actual.next();

            if (range.remove(actualObject) == null) {

                fail("Object not expected" + ": index=" + j + ", object="
                        + actualObject);

            }

        }

        if (actual.hasNext()) {

            fail("Iterator will deliver too many objects.");

        }

    }
    
//    /**
//     * Verifies that the iterator visits the specified objects in some
//     * arbitrary ordering and that the iterator is exhausted once all
//     * expected objects have been visited.  The implementation uses a
//     * selection without replacement "pattern".
//     */
//
//    static public void assertSameIteratorAnyOrder
//	( Comparable[] expected,
//	  Iterator actual
//	  )
//    {
//        
//        assertSameIteratorAnyOrder
//           ( "",
//             expected,
//             actual
//             );
//        
//    }
//
//    /**
//     * Verifies that the iterator visits the specified objects in some
//     * arbitrary ordering and that the iterator is exhausted once all
//     * expected objects have been visited.  The implementation uses a
//     * selection without replacement "pattern".
//     * 
//     * FIXME Write test cases for this one.
//     * 
//     * FIXME Can we we this without requiring the objects to implement
//     * {@link Comparable}?
//     */
//
//    static public void assertSameIteratorAnyOrder
//	( String msg,
//	  Comparable[] expected,
//	  Iterator actual
//	  )
//    {
//
//	// Populate a map that we will use to realize the match and
//	// selection without replacement logic.
//
//	final int nrange = expected.length;
//	
//	java.util.Map range = new java.util.TreeMap();
//	
//	for( int j=0; j<nrange; j++ ) {
//	    
//	    range.put
//		( expected[ j ],
//		  expected[ j ]
//		  );
//	    
//	}
//	
//	// Do selection without replacement for the objects visited by
//	// iterator.
//	
//	for( int j=0; j<nrange; j++ ) {
//	    
//	    if( ! actual.hasNext() ) {
//		
//		fail( msg+": Index exhausted while expecting more object(s)"+
//		      ": index="+j
//		      );
//		
//	    }
//	    
//	    Object actualObject = actual.next();
//	    
//	    if( range.remove( actualObject ) == null ) {
//		
//		fail( "Object not expected"+
//		      ": index="+j+
//		      ", object="+actualObject
//		      );
//		
//	    }
//	    
//	}
//
//	if( actual.hasNext() ) {
//
//	    fail( "Iterator will deliver too many objects."
//		  );
//
//	}
//
//    }

    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * Test helper produces a random sequence of indices in the range [0:n-1]
     * suitable for visiting the elements of an array of n elements in a random
     * order. This is useful when you want to randomize the presentation of
     * elements from two or more arrays. For example, known keys and values can
     * be generated and their presentation order randomized by indexing with the
     * returned array.
     */
    
    public static int[] getRandomOrder( final int n )
    {

	final class Pair
	    implements Comparable<Pair>
	{
	    final public double r = Math.random();
	    final public int val;
	    public Pair( int val ) {this.val = val;}
	    public int compareTo(final Pair other)
	    {
		if( this == other ) return 0;
		if( this.r < ((Pair)other).r ) return -1;
		else return 1;
	    }

	}

	final Pair[] pairs = new Pair[ n ];

	for( int i=0; i<n; i++ ) {

	    pairs[ i ] = new Pair( i );

	}

	java.util.Arrays.sort( pairs );

	int order[] = new int[ n ];
	
	for( int i=0; i<n; i++ ) {

	    order[ i ] = pairs[ i ].val;

	}

	return order;
	
    }

    /**
     * Random number generator used by {@link #getNormalInt( int range
     * )}.
     */

    private Random m_random = new Random();

    /**
     * Returns a random integer normally distributed in [0:range].
     */

    public int getNormalInt( int range )
    {

	final double bound = 3d;        
	
	double rand = m_random.nextGaussian();
	
	if( rand < -bound ) rand = -bound;
	else if( rand > bound ) rand = bound;
	
	rand = ( rand + bound ) / ( 2 * bound );
				// normal distribution in [0:1].
	
	if( rand < 0d ) rand = 0d;
				// make sure it is not slightly
				// negative
	
	int r = (int)( rand * range );
	
	if( r > range ) {
	    
	    throw new AssertionError();
	    
	}
	
	return r;

	// TODO develop a test case for this method based on the code
	// in comments below and migrate into TestCase2 (junit-ext
	// package).

//        int[] r = new int[10000];

//         for( int i=0; i<r.length; i++ ) {

// 	    r[ i ] = getNormalInt( len );
            
//         }
        
//         java.util.Arrays.sort( r );
        

    }

    /**
     * Returns a random but unique string of Unicode characters with a maximum
     * length of len and a minimum length. Only alphanumeric characters are
     * present in the string so you can form a unique string by appending a
     * delimiter and an one-up counter.
     * 
     * @param len
     *            The maximum length of the string. Each generated literal will
     *            have a mean length of <code>len/2</code> and the lengths
     *            will be distributed using a normal distribution (bell curve).
     * 
     * @param id
     *            A unique index used to obtain a unique string. Typically this
     *            is a one up identifier.
     */
    public String getRandomString(int len, int id) {

        // final String data =
        // "0123456789!@#$%^&*()`~-_=+[{]}\\|;:'\",<.>/?QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm";
        final String data = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm";

        int[] order = getRandomOrder(data.length());

        int n = getNormalInt(len);

        StringBuilder sb = new StringBuilder(n);

        int index = 0;

        for (int i = 0; i < n; i++) {

            sb.append(data.charAt(order[index++]));

            if (index == order.length) {

                index = 0;

            }

        }

        sb.append(id);

        return sb.toString();

    }
    
    /**
     * Used to create objects of random type.
     * 
     * TODO support generation of random array types and dates.
     * 
     * @author thompsonbry
     */
    protected class RandomType
    {

        /**
         * An explicit NULL property value.
         */
//        final public static short NULL = 0;
        
        final public static short BOOLEAN = 20;
        final public static short BYTE    = 21;
        final public static short CHAR    = 22;
        final public static short SHORT   = 23;
        final public static short INT     = 24;
        final public static short LONG    = 25;
        final public static short FLOAT   = 26;
        final public static short DOUBLE  = 27;
        final public static short STRING  = 28;

        /**
         * Java Object (must implement {@link Serializable} or {@link
         * Externalizable}).
         */
        final public static short OBJECT  = 30;

        /**
         * Array of Java objects (but not Java primitives).
         */
        final public static short OBJECT_ARRAY   = 31;

        /**
         * Array of Java primitives.
         */
        final public static short BOOLEAN_ARRAY = 40;
        final public static short BYTE_ARRAY    = 41;
        final public static short CHAR_ARRAY    = 42;
        final public static short SHORT_ARRAY   = 43;
        final public static short INT_ARRAY     = 44;
        final public static short LONG_ARRAY    = 45;
        final public static short FLOAT_ARRAY   = 46;
        final public static short DOUBLE_ARRAY  = 47;

        final public int[] randomType = new int[] {
//            LINK,
//            BLOB,
            BOOLEAN, BYTE, CHAR, SHORT, INT, LONG, FLOAT, DOUBLE,
            STRING,
            OBJECT,
//            OBJECT_ARRAY,
//            BOOLEAN_ARRAY,
//            BYTE_ARRAY,
//            CHAR_ARRAY,
//            SHORT_ARRAY,
//            INT_ARRAY,
//            LONG_ARRAY,
//            FLOAT_ARRAY,
//            DOUBLE_ARRAY,
//            NULL // Note: null at end to make easy to exclude.
        };

        /**
         * Return the type code for the specified class.
         * 
         * @param cls
         *            A class.
         * @return The type code.
         * 
         * @throws UnsupportedOperationException
         *             if the class is not one of those that is supported by
         *             {@link RandomType}.
         */
        public int getType(Class cls) {
            
            if(cls==null) {
                
                throw new IllegalArgumentException();
                
            }
            
            if(cls.equals(Boolean.class)) return BOOLEAN;
            if(cls.equals(Byte.class)) return BYTE;
            if(cls.equals(Character.class)) return CHAR;
            if(cls.equals(Short.class)) return SHORT;
            if(cls.equals(Integer.class)) return INT;
            if(cls.equals(Long.class)) return LONG;
            if(cls.equals(Float.class)) return FLOAT;
            if(cls.equals(Double.class)) return DOUBLE;
            if(cls.equals(String.class)) return STRING;
            if(cls.equals(Object.class)) return OBJECT;

            throw new UnsupportedOperationException("class="+cls);
            
        }
        
        /**
         * For random characters.
         */
        static final private String alphabet = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm";
        
        /**
         * Returns an object with a random type and random value.
         * 
         * @param rnd
         *            The random# generator to use.
         * 
         * @param allowNull
         *            When true, a null object reference may be returned.
         * 
         * @return The random object.
         */
        public Object nextObject( Random rnd, boolean allowNull )
        {

            int[] types = randomType;
            
            int range = types.length;  
            
            if( ! allowNull ) range--; // exclude null from types.
            
            int type = types[ rnd.nextInt( range ) ];

            return nextObject( rnd, type , allowNull );

        }

        /**
         * Return an instance of the specified type with a random value.
         * 
         * @param rnd
         *            The random number generator.
         * 
         * @param cls
         *            The class of the random instance to be returned -or-
         *            <code>null</code> to return an instance of any of the
         *            classes that can be generated by this method.
         * 
         * @param allowNull
         *            When true, a null object reference may be returned.
         * 
         * @return The random object.
         */
        public Object nextObject(Random rnd, Class cls, boolean allowNull) {
            
            if(cls==null) {
                
                // random object type.
                return nextObject(rnd, allowNull);

            } else {

                // specific object type.
                return nextObject(rnd, getType(cls), allowNull);
                
            }
            
        }
        
        /**
         * Return an instance of the specified type with a random value.
         * 
         * @param rnd
         *            The random number generator.
         * 
         * @param type
         *            One of the values declared by this class.
         * 
         * @param allowNull
         *            When true, a null object reference may be returned.
         * 
         * @return The random object.
         */
        public Object nextObject( Random rnd, int type, boolean allowNull ) {

            Object obj = null;
            
            switch( type ) {

//            case NULL: {
//                return null;
//            }
            case BOOLEAN: {
                obj = ( rnd.nextBoolean()
                        ? Boolean.TRUE
                        : Boolean.FALSE
                        );
                break;
            }
            case BYTE: {
                obj = Byte.valueOf( (byte) ( rnd.nextInt( 255 ) - 127 ) );
                break;
            }
            case CHAR: {
                obj = Character.valueOf( alphabet.charAt( rnd.nextInt( alphabet.length() ) ) );
                break;
            }
            case SHORT: {
                obj = Short.valueOf( (short) rnd.nextInt() );
                break;
            }
            case INT: {
                obj = Integer.valueOf( rnd.nextInt() );
                break;
            }
            case LONG: {
                obj = Long.valueOf( rnd.nextLong() );
                break;
            }
            case FLOAT: {
                obj = Float.valueOf( rnd.nextFloat() );
                break;
            }
            case DOUBLE: {
                obj = Double.valueOf( rnd.nextDouble() );
                break;
            }
            case STRING: {
                obj = getRandomString( 40, rnd.nextInt() );
                break;
            }
            case OBJECT: {
                obj = getRandomString( 40, rnd.nextInt() );
                break;
            }
//            OBJECT_ARRAY,
//                BOOLEAN_ARRAY,
//                BYTE_ARRAY,
//                CHAR_ARRAY,
//                SHORT_ARRAY,
//                INT_ARRAY,
//                LONG_ARRAY,
//                FLOAT_ARRAY,
//                DOUBLE_ARRAY
            default: {
                throw new AssertionError( "unknown type="+type );
            }
            
            }

            return obj;
        
        }
        
    }

    protected RandomType _randomType = new RandomType();
    
    /**
     * Returns an object with a random type and random value.
     * 
     * @param allowNull When true, a null object reference may be
     * returned.
     * 
     * @return The random object.
     */

    public Object getRandomObject( boolean allowNull )
    {

        return _randomType.nextObject( m_random, allowNull );
        
    }

    /**
     * Returns an object with a random type and random value.
     *
     * @param rnd The random generator.
     *  
     * @param allowNull When true, a null object reference may be
     * returned.
     * 
     * @return The random object.
     */

    public Object getRandomObject( Random rnd, boolean allowNull )
    {

        return _randomType.nextObject( rnd, allowNull );
        
    }
    
    //************************************************************
    //****************** Loading Test Resources ******************
    //************************************************************

    /**
     * Returns an {@link InputStream} that may be used to read from a
     * test resource found along the CLASSPATH.<p>
     *
     * @param resourceName A resource identified by a series of '/'
     * delimited path components <strong>without</strong> a leading
     * '/'.  Such resources are normally part of the JAR in which the
     * test is bundled and correspond to the package naming convention
     * with the exception that the '/' delimiter is used in place of
     * the '.' delimiter used in package names.
     *
     * @exception AssertionError If the resource could not be read for
     * any reason.
     *
     * @see ClassLoader#getResourceAsStream( String resourceName )
     */

    public InputStream getTestInputStream
	( // Object context,
	  String resourceName
	  )
    {

//      * @param context An arbitrary object whose {@link ClassLoader}
//      * will be used to locate the identified resource.
//      *

	Object context = this;

	log.info
	    ( "Getting input stream for: "+resourceName
	      );

	try {

	    /* Used to locate test resources.
	     */

	    ClassLoader cl = context.getClass().getClassLoader();

	    if( cl == null ) {

		throw new AssertionError
		    ( "Could not get ClassLoader"+
		      ": "+context.getClass()
		      );

	    }

	    InputStream is = cl.getResourceAsStream
		( resourceName
		  );

	    if( is == null ) {

		log.error
		    ( "Could not locate resource: '"+resourceName+"'"
		      );

		throw new AssertionError
		    ( "Could not locate resource: '"+resourceName+"'"
		      );

	    }

	    return is;

	}

	catch( Exception ex ) {

	    log.error
		( "Could not read from "+resourceName,
		  ex
		  );

	    throw new AssertionFailedError
		( "Could not read from "+resourceName+
		  " : "+ex.toString()
		  );

	}

    }

    /**
     * Read a test character resource from along the CLASSPATH.<p>
     *
     * @param resourceName A resource identified by a series of path
     * components beginning <strong>without</strong> a leading '/'.
     * Such resources are normally part of the JAR in which the test
     * is bundled.
     *
     * @param encoding The character set encoding that will be used to
     * read the text resource.  If <code>null</code> then the platform
     * default Java encoding will be used (which is not recommended as
     * it is non-portable).<p>
     *
     * @exception AssertionError If the resource could not be read for
     * any reason.
     *
     * @see ClassLoader#getResourceAsStream( String resourceName )
     */

    public String getTestResource
	( // Object context,
	  String resourceName,
	  String encoding
	  )
    {

	log.info
	    ( "Reading local test resource: "+resourceName
	      );

	try {
	    
	    InputStream is = getTestInputStream
		( // context,
		  resourceName
		  );

	    // Note: There is no way to know the character encoding
	    // for the resource we are reading.  We have to assume
	    // that it is the default Java character encoding unless
	    // the caller has explicitly specified the encoding to be
	    // used.

	    final Reader r = 
		( encoding == null
		  ? new InputStreamReader
		      ( is
			)
		  : new InputStreamReader
		      ( is,
			encoding
			)
		  );

	    try {
	    final StringBuilder sb = new StringBuilder();

	    while( true ) {

		final int ch = r.read();

		if( ch == -1 ) break;

		sb.append( (char)ch );

	    }

	    if(log.isInfoEnabled())
	        log.info
		( "Read "+sb.length()+" characters from "+
		  resourceName
		  );

	    return sb.toString();

	    } finally {
	        r.close();
	    }

	}

	catch( Exception ex ) {

	    log.error
		( "Could not read from "+resourceName,
		  ex
		  );

	    throw new AssertionFailedError
		( "Could not read from "+resourceName+
		  " : "+ex.toString()
		  );

	}

    }

    /**
     * Convenience method for {@link #getTestResource( String
     * resourceName, String encoding )} that uses the platform
     * specific default encoding, which is NOT recommended since it is
     * non-portable.
     */

    public String getTestResource
	( String resourceName
	  )
    {
	
	return getTestResource
	    ( resourceName,
	      null
	      );
	
    }

    //************************************************************
    //********************** Properties **************************
    //************************************************************

    /**
     * Reads in the configuration properties for the test from a
     * variety of resources and returns a properties hierarchy.  If a
     * property name is not bound at the top-level of the returned
     * {@link Properties} object, then the lower levels of the
     * hierarchy are recursively searched.<p>
     *
     * The hierarchy is constructed from the following properties
     * files in the following order.  The first property file in this
     * list corresponds to the top of the property hierarchy.  The
     * last property file in this list corresponds to the bottom of
     * the property hierarchy.  The property resources are:<ol>
     *
     * <li> The System properties specified using the -D argument on
     * the command line.
     * 
     * <li> ${user.home}/build.properties.  ${user.home} is the value
     * of the Java system environment variable named "user.home" and
     * corresponds to the user home directory.  A warning is logged if
     * this properties file does not exist.  <em>Note: The JUnit UI
     * can wind up with a different value for this property than when
     * you execute tests without the UI.</em> It is an error if the
     * properties file exists but can not be read.
     *
     * <li> {class}.properties, where {class} is the class name
     * reported by Java reflection, is loaded iff it is found.
     *
     * <li> {class} is repeatedly truncated by dropping the last
     * package name component to generate a package name.  If a
     * properties file named "test.properties" exists in that package
     * then those properties are loaded.
     *
     * </ol>
     *
     * @return A {@link Properties} object that supplies bindings for
     * property names according to the described hierarchy among
     * property resources.  The returned {@link Properties} is NOT
     * cached.
     *
     * TODO This does not handle the recursive truncation of the
     * class name to search for "test.properties" yet.
     */

    public Properties getProperties()
    {

        MyProperties m_properties = null;
        
        if( m_properties == null ) {
        
	    try {
		
		log.info
		    ( "Will read properties."
		      );
		
		/* Setup a bottom layer of the default system.  This
		 * is just an empty map.
		 *
		 * Note that we go in backwards order in order to
		 * setup the default hierarchy correctly, so this
		 * empty map winds up on the bottom of the hierarchy.
		 */
		
		m_properties = new MyProperties
		    ( "<none>"
		      );
		
		/* Use reflection to the get name of the concrete
		 * instance of this class, which will be the TestCase
		 * that is being used to run the test suite.
		 */
		
		String className = this.getClass().getName();
		
		/* If a resource exists named <class>.properties then
		 * we create a new layer in the properties hierarchy
		 * and load the properties from that resource into the
		 * new properties layer.
		 */
		
		classNameProperties: {
		    
		    /* Convert the class name into a resource identifier.
		     */
		    
		    String resourceName =
			className.replace('.','/') + ".properties"
			;

            // Note: requires Java 1.5.
//            String resourceName = this.getClass().getSimpleName();

		    // Try to load properties from the resource into
		    // the new layer.  This uses a helper method that
		    // searchs along the CLASSPATH, rather than the
		    // file system, so that it can find the resource
		    // in a JAR if necessary.
	
		    log.info
			( "Will try to read properties from resource: "+
			  resourceName
			  );
		    
		    ClassLoader cl = getClass().getClassLoader();
		    
		    InputStream is = cl.getResourceAsStream
			( resourceName
			  );
		    
		    if( is != null ) {
			
			// Add a new properties layer that inherits
			// from the last properties layer.
			
			MyProperties newLayer = new MyProperties
			    ( resourceName,
			      m_properties
			      );
			
			newLayer.load
			    ( is
			      );
			
			// Update the reference iff everything
			// succeeded.
			
			m_properties = newLayer;
			
		    } else {
			
			log.debug
			    ( "No properties resource named: "+resourceName
			      );
			
		    }
		    
		} // classNameProperties
		
		/* The last thing that we do is incorporate the properties
		 * file named by ${user.home}/build.properties so that it
		 * will wind up on top of the properties hierarchy.
		 */
		
		user_home: {
		    
		    String userHome = System.getProperty
			    ( "user.home"
			      );

		    if( userHome == null ) {

			log.error
			    ( "The System property 'user.home' is not defined."
			      );
			
			throw new AssertionError
			    ( "The System property 'user.home' is not defined."
			      );
			
		    }
		    
		    File file = new File
			( userHome+File.separator+"build.properties"
			  );
		    
		    log.info
			( "Looking for properties file in home directory: "+
			  file
			  );
		    
		    if( ! file.exists() ) {
			
			log.warn
			    ( "File does not exist: "+file
			      );
			
// 			throw new IOException
// 			    ( "File does not exist: "+file
// 			      );
			
		    } else {
		    
		    if( ! file.isFile() ) {
			
			log.error
			    ( "Not a normal file: "+file
			      );
			
			throw new IOException
			    ( "Not a normal file: "+file
			      );
			
		    }
		    
		    if( ! file.canRead() ) {
			
			log.error
			    ( "Can not read file: "+file
			      );
			
			throw new IOException
			    ( "Can not read file: "+file
			      );
			
		    }
		    
		    // New layer in the hierarchy inherits from the old
		    // layer.
		    
		    m_properties = new MyProperties
			( file.toString(),
			  m_properties
			  );
		    
		    // Read in properties from the file.
		    
		    m_properties.load
			( new FileInputStream( file )
			  );

		    }
		    
		} // user.home.
		
		if( true ) {
		    
		    // New layer in the hierarchy inherits from the old
		    // layer.
		    
		    m_properties = new MyProperties
			( "System.getProperties()",
			  m_properties
			  );
		    
		    m_properties.putAll( PropertyUtil.flatten( System.getProperties() ) );
		    
		}
		
		// Log the properties that we just loaded.
		
		if( isDEBUG() ) {
		
		    logProperties
		    ( m_properties,
		      0
		      );
		    
		}
		
	    }

	    catch( IOException ex ) {

		/* Hide the IOException as a RuntimeException.
		 */
		
	        ex.printStackTrace();
            
		RuntimeException ex2 = new RuntimeException
		    ( "Could not load properties."
		      );
		
		ex2.initCause( ex );
		
		throw ex2;
		
	    }

	} // load property hierarchy.

	return m_properties;

    }

//    MyProperties m_properties;

    /**
     * Helper class gives us access to the default {@link Properties}.
     * We use this class exclusively when loading properties so that
     * we can correctly report what properties are bound at what level
     * and what resource or file the properties were loaded from.
     */

    protected static class MyProperties
	extends Properties
    {

    	/**
		 * 
		 */
		private static final long serialVersionUID = 3980787538394715754L;
		
	String m_source = null;

	/**
	 * The file or CLASSPATH resource from which the properties
	 * were loaded.
	 */

	public String getSource() {return m_source;}

	/**
	 * Exposes the protected member in the {@link Properties}
	 * class.
	 */

	public MyProperties getDefaults()
	{

	    return (MyProperties) defaults;

	}

	public MyProperties( String source )
	{
	    super();

	    m_source = source;
	}

	public MyProperties( String source, MyProperties defaults )
	{
	    super( defaults );

	    m_source = source;
	}

    }

    /**
     * Recursively logs properties together with the resource or file
     * in which they were defined at the DEBUG level.  The properties
     * at a given level are presented in alpha ordering in order to
     * make it easier to locate a specific binding.<p>
     *
     * This method is invoked by {@link #getProperties()}.<p>
     */

    protected void logProperties
	( MyProperties properties,
	  int level
	  )
    {

	StringBuilder sb = new StringBuilder();

	sb.append
	    ( "Properties:: [ source = '"+properties.getSource()+"' ]\n"
	      );

	TreeMap map = new TreeMap // sorted view.
	    ( properties
	      );

	Iterator itr = map.entrySet().iterator(); // in sorted order.

	while( itr.hasNext() ) {

	    Map.Entry entry = (Map.Entry) itr.next();

	    String name = (String) entry.getKey();

	    String value = (String) entry.getValue();

	    sb.append
		( indent(level)+
		  "'"+name+"'"+
		  " = "+
		  ( value != null
		    ? "'"+value+"'"
		    : "<none>"
		    )+
		  "\n"
		  );

	}

	log.info
	    ( sb.toString()
	      );

	/* If there are defaults for these properties, then
	 * recursively log those as well.
	 */

	MyProperties defaults = properties.getDefaults();

	if( defaults != null ) {

	    logProperties
		( defaults,
		  level + 1
		  );

	}
	
    }

    /**
     * Generates an indentation string.
     */

    private String indent( int level )
    {

	final String s = "...";

	if( level == 0 ) return "";

	StringBuilder sb = new StringBuilder
	    ( level * s.length()
	      );

	for( int i=0; i<level; i++ ) {

	    sb.append( s );

	}

	return s.toString();

    }

    /**
     * Returns the project build directory, even in a multiproject
     * build.  This is normally the <i>target</i> subdirectory beneath
     * the project root directory.<p>
     *
     * Note that the current working directory in a multiproject build
     * is normally NOT the same as the individual project directory
     * when running the unit tests.  This method is designed to give
     * you access to project local resources (whether consumed or
     * created) during unit test, even when running a multiproject
     * goal.<p>
     *
     * In order for this method to succeed, you MUST declare
     * <i>maven.junit.sysproperties="maven.build.dir ..."</i> when you
     * invoke the test suite.  See the properties page for the <a
     * href="http://maven.apache.org/reference/plugins/test/properties.html">
     * test-plugin </a> for more information on how to specify this
     * property.<p>
     *
     * @throws UnsupportedOperationException if the project build
     * directory could NOT be determined since the
     * <i>maven.junit.sysproperties</i> property was NOT defined.
     *
     * @return The path to the project build directory.
     */

    static public String getProjectBuildPath()
	throws UnsupportedOperationException
    {

	String x = System.getProperty
	    ( "maven.build.dir"
	      );

	if( x == null ) {

	    final String msg = "The 'maven.build.dir' property is NOT defined.  Did you forget to defined 'maven.junit.sysproperties=maven.build.dir ...'";

	    log.error
		( msg
		  );

	    throw new UnsupportedOperationException
		( msg
		  );

	} else {

	    log.info
		( "maven.build.dir='"+x+"'"
		  );

	    return x;

	}

    }

    /**
     * Utility method returns true iff the effective logger level is
     * DEBUG or less.  This may be used to make logging requiring 
     * significant formatting conditional on the logging level (so
     * that the formatting is not done if the message would not be
     * logged).
     *  
     * @param log The logger.
     * 
     * @return True if the logger would log messages at the DEBUG level.
     */
    
    static final public boolean isDEBUG( Logger log )
    {
    
        return log.getEffectiveLevel().toInt() <= Level.DEBUG.toInt();

    }
    
    /**
     * Utility method returns true iff the effective logger level is
     * INFO or less.  This may be used to make logging requiring 
     * significant formatting conditional on the logging level (so
     * that the formatting is not done if the message would not be
     * logged).
     *  
     * @param log The logger.
     * 
     * @return True if the logger would log messages at the INFO level.
     */
    
    static final public boolean isINFO( Logger log )
    {
    
        return log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    }

    /**
     * @return True if the logger would log messages at the DEBUG level.
     */

    final public boolean isDEBUG() {return isDEBUG( log );}
    
    /**
     * @return True if the logger would log messages at the INFO level.
     */

    final public boolean isINFO() {return isINFO( log );}
    
    //************************************************************
    //************************************************************
    //************************************************************

    /**
     * Derived from <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     * 
     * @param A
     *            A floating point value.
     * @param B
     *            Another floating point value
     */
//    * @param maxUlps
//    *            The maximum error in terms of Units in the Last Place. This
//    *            specifies how big an error we are willing to accept in terms
//    *            of the value of the least significant digit of the floating
//    *            point number's representation. <i>maxUlps</i> can also be
//    *            interpreted in terms of how many representable floats we are
//    *            willing to accept between A and B. This function will allow
//    *            <code>maxUlps-1</code> floats between <i>A</i> and <i>B</i>.
    static protected int getUlps(float A, float B) {

        int aInt = Float.floatToIntBits(A); // *(int*)&A;

        // Make aInt lexicographically ordered as a twos-complement
        // int.

        if (aInt < 0) {

            aInt = 0x80000000 - aInt;

        }

        // Make bInt lexicographically ordered as a twos-complement
        // int.

        int bInt = Float.floatToIntBits(B); // *(int*)&B;

        if (bInt < 0) {

            bInt = 0x80000000 - bInt;

        }

        int intDiff = Math.abs(aInt - bInt);

        return intDiff;

    }

    /**
     * Derived from <a
     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
     * Comparing floating point numbers </a> by Bruce Dawson.
     * 
     * @param A
     *            A double precision floating point value.
     * @param B
     *            Another double precision floating point value
     */
//    * @param maxUlps
//    *            The maximum error in terms of Units in the Last Place. This
//    *            specifies how big an error we are willing to accept in terms
//    *            of the value of the least significant digit of the floating
//    *            point number's representation. <i>maxUlps</i> can also be
//    *            interpreted in terms of how many representable doubles we are
//    *            willing to accept between A and B. This function will allow
//    *            <code>maxUlps-1</code> doubles between <i>A</i> and <i>B</i>.
    static protected long getUlps(double A, double B) {

        long aLong = Double.doubleToLongBits(A); // *(int*)&A;

        // Make aInt lexicographically ordered as a twos-complement
        // long.

        if (aLong < 0) {

            aLong = 0x8000000000000000L - aLong;

        }

        // Make bInt lexicographically ordered as a twos-complement
        // int.

        long bLong = Double.doubleToLongBits(B); // *(int*)&B;

        if (bLong < 0) {

            bLong = 0x8000000000000000L - bLong;

        }

        long longDiff = Math.abs(aLong - bLong);

        return longDiff;

    }

    /**
     * Verify zero ULPs difference between the values.
     * @param f1
     * @param f2
     */
    protected void assertZeroUlps(float f1, float f2) {

        int ulps = getUlps(f1,f2);
        
        if( ulps != 0 ) {

            fail("Expecting zero ulps, but found: "+ulps+"; f1="+f1+", f2="+f2);
            
        }
        
    }

    /**
     * Verify zero ULPs difference between the values.
     * @param d1
     * @param d2
     */
    protected void assertZeroUlps(double d1, double d2) {

        long ulps = getUlps(d1,d2);
        
        if( ulps != 0L ) {

            fail("Expecting zero ulps, but found: "+ulps+"; f1="+d1+", f2="+d2);
            
        }
        
    }

//    /**
//     * Derived from <a
//     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
//     * Comparing floating point numbers </a> by Bruce Dawson.
//     *
//     * @param A A floating point value.
//     * @param B Another floating point value
//     * @param maxUlps The maximum error in terms of Units in the Last
//     * Place. This specifies how big an error we are willing to accept
//     * in terms of the value of the least significant digit of the
//     * floating point numbers representation.  <i>maxUlps</i> can
//     * also be interpreted in terms of how many representable floats
//     * we are willing to accept between A and B.  This function will
//     * allow <code>maxUlps-1</code> floats between <i>A</i> and
//     * <i>B</i>.
//     */
//
//    static protected int getUlps
////     static public boolean AlmostEqual2sComplement
//    ( float A,
//      float B
//// ,
////    int maxUlps
//      )
//    {
//    
////  // Make sure maxUlps is non-negative and small enough that the
////  // default NAN won't compare as equal to anything.
//    
////  if( ! (maxUlps > 0 && maxUlps < 4 * 1024 * 1024) ) {
//
////      throw new IllegalArgumentException
////      ( "maxUlps is out of range: "+maxUlps
////        );
//
////  }
//
//    int aInt = Float.floatToIntBits( A ); // *(int*)&A;
//
//    // Make aInt lexicographically ordered as a twos-complement
//    // int.
//    
//    if (aInt < 0) {
//        
//        aInt = 0x80000000 - aInt;
//
//    }
//    
//    // Make bInt lexicographically ordered as a twos-complement
//    // int.
//    
//    int bInt = Float.floatToIntBits( B ); // *(int*)&B;
//    
//    if (bInt < 0) {
//        
//        bInt = 0x80000000 - bInt;
//
//    }
//
//    int intDiff = Math.abs
//        ( aInt - bInt
//          );
//
//    return intDiff;
//    
////  if (intDiff <= maxUlps) {
//        
////      return true;
//
////  }
//    
////  return false;
//    
//    }
//
//    /**
//     * Derived from <a
//     * href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
//     * Comparing floating point numbers </a> by Bruce Dawson.
//     *
//     * @param A A double precision floating point value.
//     * @param B Another double precision floating point value
//     * @param maxUlps The maximum error in terms of Units in the Last
//     * Place. This specifies how big an error we are willing to accept
//     * in terms of the value of the least significant digit of the
//     * floating point numbers representation.  <i>maxUlps</i> can
//     * also be interpreted in terms of how many representable doubles
//     * we are willing to accept between A and B.  This function will
//     * allow <code>maxUlps-1</code> doubles between <i>A</i> and
//     * <i>B</i>.
//     */
//
//    static protected long getUlps
////     static public boolean AlmostEqual2sComplement
//    ( double A,
//      double B
//// ,
////    long maxUlps
//      )
//    {
//    
//    // Make sure maxUlps is non-negative and small enough that the
//    // default NAN won't compare as equal to anything.
//    //
//    // TODO The range check here is for float.  The max possible
//    // legal range is larger for double.
//    
////  if( ! (maxUlps > 0 && maxUlps < 4 * 1024 * 1024) ) {
//
////      throw new IllegalArgumentException
////      ( "maxUlps is out of range: "+maxUlps
////        );
//
////  }
//
//    long aLong = Double.doubleToLongBits( A ); // *(int*)&A;
//
//    // Make aInt lexicographically ordered as a twos-complement
//    // long.
//    
//    if (aLong < 0) {
//        
//        aLong = 0x8000000000000000L - aLong;
//
//    }
//    
//    // Make bInt lexicographically ordered as a twos-complement
//    // int.
//    
//    long bLong = Double.doubleToLongBits( B ); // *(int*)&B;
//    
//    if (bLong < 0) {
//        
//        bLong = 0x8000000000000000L - bLong;
//
//    }
//
//    long longDiff = Math.abs
//        ( aLong - bLong
//          );
//    
//
//    return longDiff;
//
////  if (longDiff <= maxUlps) {
//        
////      return true;
//
////  }
//    
////  return false;
//    
//    }

    /**
     * Provides a proper basis for comparing floating point numbers (the values
     * must be within 10 ulps of one another).
     * 
     * @see #getUlps(float, float)
     */
    public static void assertEquals(float expected, float actual) {
        assertEquals("", expected, actual);
    }

    /**
     * Provides a proper basis for comparing floating point numbers (the values
     * must be within 10 ulps of one another).
     * 
     * @see #getUlps(float, float)
     */
    public static void assertEquals(String message, float expected, float actual) {

        if (expected == actual)
            return;

        final int maxUlps = 10;

        int ulps = getUlps(expected, actual);

        if (ulps <= maxUlps) {

            return;

        }

        fail("Expecting " + expected + ", not " + actual + ": abs(difference)="
                + Math.abs(expected - actual) + ": ulps=" + ulps);

    }

    /**
     * Provides a proper basis for comparing floating point numbers (the values
     * must be within 10 ulps of one another).
     * 
     * @param expected
     * @param actual
     * 
     * @see #getUlps(double, double)
     */
    public static void assertEquals(double expected, double actual) {
        assertEquals("", expected, actual);
    }

    /**
     * Provides a proper basis for comparing floating point numbers (the values
     * must be within 10 ulps of one another).
     * 
     * @param expected
     * @param actual
     * 
     * @see #getUlps(double, double)
     */
    public static void assertEquals(String message, double expected,
            double actual) {

        assertEqualsWithinUlps(message, expected, actual, 10/*maxUlps*/);
        
    }

    /**
     * Provides a proper basis for comparing floating point numbers (the values
     * must be within <i>maxUlps</i> of one another).
     * 
     * @param expected
     * @param actual
     * @param maxUlps The maximum #of ulps difference which will be considered
     * as equivalent.
     * 
     * @see #getUlps(double, double)
     */
    public static void assertEqualsWithinUlps(String message, double expected,
            double actual, final long maxUlps) {

        if (expected == actual)
            return;
        
        long ulps = getUlps(expected, actual);

        if (ulps <= maxUlps) {

            return;

        }

        fail("Expecting " + expected + ", not " + actual + ": abs(difference)="
                + Math.abs(expected - actual) + ", ulps=" + ulps);

    }

    public static void assertSameBigInteger(BigInteger expected,
            BigInteger actual) {

        assertSameBigInteger("", expected, actual);

    }

    public static void assertSameBigInteger(String message,
            BigInteger expected, BigInteger actual) {

        assertTrue(message + ": expected " + expected + ", not " + actual,
                expected.equals(actual));

    }

    /**
     * This uses {@link BigDecimal#compareTo( Object other )}, which
     * considers that two {@link BigDecimal}s that are equal in
     * <i>value</i> but have a different <i>scale</i> are the same.
     * (This is NOT true of {@link BigDecimal#equals( Object other )},
     * which also requires the values to have the same <i>scale</i>.)
     */
    public static void assertSameBigDecimal(BigDecimal expected,
            BigDecimal actual) {

        assertEquals("", expected, actual);

    }

    /**
     * This uses {@link BigDecimal#compareTo( Object other )}, which
     * considers that two {@link BigDecimal}s that are equal in
     * <i>value</i> but have a different <i>scale</i> are the same.
     * (This is NOT true of {@link BigDecimal#equals( Object other )},
     * which also requires the values to ave the same <i>scale</i>.)
     */

    public static void assertSameBigDecimal(String message,
            BigDecimal expected, BigDecimal actual) {

        assertTrue(message + ": expected " + expected + ", not " + actual,
                expected.compareTo(actual) == 0);

    }

    /**
     * Helper method verifies that the arrays are consistent in length
     * and that their elements are consistent under {@link
     * Object#equals( Object other )}.
     */

    public static void assertSameArray(Object[] expected, Object[] actual) {
        assertSameArray("", expected, actual);
    }

    /**
     * Helper method verifies that the arrays are consistent in length
     * and that their elements are consistent under {@link
     * Object#equals( Object other )}.
     */

    public static void assertSameArray(String message, Object[] expected,
            Object[] actual) {

        if (expected == null && actual == null) {

            return;

        }

        if (expected != null && actual == null) {

            fail(message + " : expecting actual to be non-null.");

        }

        if (expected == null && actual != null) {

            fail(message + " : expecting actual to be null.");

        }

        assertEquals(message + " : length is wrong.", expected.length,
                actual.length);

        for (int i = 0; i < expected.length; i++) {

            if (expected[i] == null && actual[i] == null) {

                continue;

            }

            if (expected[i] != null && actual[i] == null) {

                fail(message + " : expecting actual[" + i + "] to be non-null");

            }

            if (expected[i] == null && actual[i] != null) {

                fail(message + " : expecting actual[" + i + "] to be null.");

            }

            assertTrue(message + " : expected[" + i + "]=" + expected[i]
                    + ", but actual[" + i + "]=" + actual[i], expected[i]
                    .equals(actual[i]));

        }

    }

    /**
     * Examines a stack trace for an instance of the specified cause nested to
     * any level within that stack trace.
     * 
     * @param t
     *            The stack trace.
     * @param cls
     *            The class of exception that you are looking for in the stack
     *            trace.
     *            
     * @return An exception that is an instance of that class iff one exists in
     *         the stack trace and <code>null</code> otherwise.
     *         
     * @throws IllegalArgumentException
     *             if any parameter is null.
     */
    static public Throwable getInnerCause(Throwable t, Class cls) {
        // Note: Use of generics commented out for 1.4 compatibility.
//        static public Throwable getInnerCause(Throwable t, Class<? extends Throwable> cls) {
        
        if (t == null)
            throw new IllegalArgumentException();

        if (cls == null)
            throw new IllegalArgumentException();
        
        {
            Class x = t.getClass();
            while(x != null){
                if( x == cls) 
                    return t;
                x = x.getSuperclass();
            }
            
        }
         
        t = t.getCause();

        if (t == null)
            return null;

        return getInnerCause(t, cls);

    }

    /**
     * Examines a stack trace for an instance of the specified cause nested to
     * any level within that stack trace.
     * 
     * @param t
     *            The stack trace.
     * @param cls
     *            The class of exception that you are looking for in the stack
     *            trace.
     * 
     * @return <code>true</code> iff an exception that is an instance of that
     *         class iff one exists in the stack trace.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is null.
     */
    static public boolean isInnerCause(Throwable t, Class cls) {
        // Note: Use of generics commented out for 1.4 compatibility.
//        static public boolean isInnerCause(Throwable t, Class<? extends Throwable>cls) {
        
        return getInnerCause(t, cls) != null;
        
    }
    
}
