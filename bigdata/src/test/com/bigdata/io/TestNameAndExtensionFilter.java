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
 * Created on Feb 1, 2006
 */
package com.bigdata.io;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import junit.framework.TestCase;

/**
 * Test suite for {@link NameAndExtensionFilter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNameAndExtensionFilter extends TestCase
{

    /**
     * 
     */
    public TestNameAndExtensionFilter() {
        super();
    }

    /**
     * @param name
     */
    public TestNameAndExtensionFilter(String name) {
        super(name);
    }

    /**
     * Verifies that the same files are present in each {@link File}[]. The
     * order in which the files are listed does not matter.
     * 
     * @param expected
     * @param actual
     */
    public void assertSameFiles( File[] expected, File[] actual )
    {
        
        if( expected == null ) {
            
            throw new AssertionError( "expected is null.");
            
        }

        if( actual == null ) {
            
            fail( "actual is null.");
            
        }

        assertEquals( "#of files", expected.length, actual.length );
        
        // Insert the expected files into a set.
        Set expectedSet = new HashSet();
        
        for( int i=0; i<expected.length; i++ ) {

            File expectedFile = expected[ i ];
            
            if( expectedFile == null ) {

                throw new AssertionError( "expected file is null at index="+i );
                
            }

            if( ! expectedSet.add( expectedFile.toString() ) ) {
            
                throw new AssertionError( "expected File[] contains duplicate: expected["+i+"]="+expectedFile );
                
            }
            
        }

        /*
         * Verify that each actual file occurs in the expectedSet using a
         * selection without replacement policy.
         */
        
        for( int i=0; i<actual.length; i++ ) {
            
            File actualFile = actual[ i ];
            
            if( actualFile  == null ) {
            
                fail( "actual file is null at index="+i );
                
            }
            
            if( ! expectedSet.remove( actual[ i ].toString() ) ) {
                
                fail( "actual file="+actualFile+" at index="+i+" was not found in expected files." );
                
            }
            
        }
        
    }
    
    /**
     * Test verifies that no files are found using a guarenteed unique basename.
     */
    public void test_filter_001() throws IOException
    {
     
        final File basefile = File.createTempFile(getName(),"-test");        
        basefile.deleteOnExit();
        
        final String basename = basefile.toString();
        System.err.println( "basename="+basename );
        
        NameAndExtensionFilter logFilter = new NameAndExtensionFilter( basename, ".log" );
        
        assertSameFiles( new File[]{}, logFilter.getFiles() );
        
    }

    /**
     * Test verifies that N files are found using a guarenteed unique basename.
     */
    public void test_filter_002() throws IOException {

        int N = 100;
        
        final File logBaseFile = File.createTempFile(getName(),"-test");
        logBaseFile.deleteOnExit();
        
        final String basename = logBaseFile.toString();
        System.err.println( "basename="+basename );
        
        NameAndExtensionFilter logFilter = new NameAndExtensionFilter( basename, ".log" );

        Vector v = new Vector( N );
        
        for( int i=0; i<N; i++ ) {

            File logFile = new File( basename+"."+i+".log" );
            logFile.deleteOnExit();
            logFile.createNewFile();
//          System.err.println( "logFile="+logFile );
            
            v.add( logFile );
        
        }
        
        File[] expectedFiles = (File[]) v.toArray(new File[]{});
        
        assertSameFiles( expectedFiles, logFilter.getFiles() );

    }

}
