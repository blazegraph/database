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
 * Created on Feb 1, 2006
 */
package com.bigdata.scaleup;

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
