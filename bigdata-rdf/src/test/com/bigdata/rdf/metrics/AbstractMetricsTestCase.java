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
 * Created on Feb 5, 2007
 */

package com.bigdata.rdf.metrics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Vector;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.AbstractTripleStoreTestCase;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractMetricsTestCase extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public AbstractMetricsTestCase() {
    }

    /**
     * @param name
     */
    public AbstractMetricsTestCase(String name) {
        super(name);
    }

    protected BufferMode getBufferMode() {
        
        return BufferMode.Disk;
        
    }
    
//    /**
//     * Returns the size of the store file.
//     * <p>
//     * 
//     * Note: This only works for local stores.
//     * <p>
//     * 
//     * Note: Some stores (e.g., jdbm) are stable once the data is on the log and
//     * but may migrate data from the log to the database only when the store is
//     * closed. Therefore you must first shutdown the repository in order to
//     * obtain an accurate measure of the store size.
//     * <p>
//     * 
//     * @param properties
//     *            Used to determine the name of the store file.
//     * 
//     * @return The size of the store or <code>-1L</code> if the size of the
//     *         store could not be determined for the repository under test.
//     */
//
//    protected long getStoreSize()
//    {
//
//        if( m_storeDataFiles.length == 0 ) {
//            
//            return -1L;
//            
//        }
//        
//        long nbytes = 0L;
//        
//        for( int i=0; i<m_storeDataFiles.length; i++ ) {
//
//            String filename = m_storeDataFiles[ i ];
//            
//            File file = new File( filename );
//            
//            long len = file.length();
//            
//            System.err.println
//                ( "store: "+filename+", length="+len
//                 );
//            
//            nbytes += len;
//            
//        }
//
//        return nbytes;
//        
//    }
    
    /**
     * Returns the quantity <i>n</i> expressed as a per-second rate or "N/A" if
     * the elapsed time is zero.
     */
    static final public String perSec(final int n, final long elapsed) {

        if (n == 0)
            return "0";

        return ((elapsed == 0 ? "N/A" : "" + (int) (n / (elapsed / 1000.))));

    }

    /**
     * Returns n <i>per</i> m or "N/A" if <i>m == 0</i>.
     * 
     * @param n
     *            Some value.
     * 
     * @param m
     *            Some other value.
     * 
     * @return
     */
    static final public String nPerM(final long n, final long m) {

        if (n == 0)
            return "0";

        if (m == 0)
            return "N/A";

        int percent = ((int) (((double) n / (double) m) * 100d * 100d)) / 100;

        return "" + percent;

    }

    /**
     * Returns a writer named by the test and having the specified filename
     * extension.
     */
    public Writer getWriter(String ext) throws IOException {

        return new BufferedWriter(new FileWriter(getName() + ext));

    }

// /**
// * Reports the time required to read all triples in the repository.
//     * <p>
//     * Note: You should either run this on a newly opened repository or re-open
//     * the repository before running this method in order to flush cache.
//    
//     * @return The time in milliseconds.
//     */
//
//    public long doReadPerformanceTest()
//    {
//
//            long begin = System.currentTimeMillis();
//            
//            int count = 0;
//            
//            StatementIterator itr = m_repo.getStatements( null, null, null );
//            
//            while( itr.hasNext() ) {
//                
//                Statement stmt = itr.next();
//                
//                count++;
//                
//                // Materialize the bindings.
//                stmt.getSubject();
//                stmt.getPredicate();
//                stmt.getObject();
//
//                if( false && count < 200 ) {
//                    
//                    System.out.println( stmt.toString() );
//                    
//                }
//                
//            }
//            
//            long elapsed = System.currentTimeMillis() - begin;
//            
//            System.out.println( "Read "+count+" statements in "+(elapsed/1000)+" seconds." );
//   
//            return elapsed;
//        
//    }

    /**
     * Parses a string defining a load sequence for zero or more RDF data
     * sources.  Each new source is introduced by the "-rdf" keyword which
     * is followed by the filename to be loaded.  The filename may be optionally
     * followed by the baseURI.
     * 
     * @param s The grammar is <code>-rdf <filename> (<baseURI>)}+</code>
     * 
     * @return The array of sources.
     */
    public static FileAndBaseURI[] getSources( String s )
    {

        String[] args = s.split( "\\s+" );
        
        Vector v = new Vector();
        
        for( int argno=0; argno<args.length; argno++ ) {

                String arg = args[ argno ];

                if( arg.equals( "-rdf" ) ) {

                // Specify the filename and optional baseURI of an RDF
                // datasource.  We require that the next argument identify
                // a filename.  The argument after than may optionally
                // identify a baseURI.  If it is NOT a URI, then we do not
                // consume it here.

                String filename = args[ ++ argno ]; // consume next arg.

//         The file existance test is disabled right now since Sesame appears
//         to load from the CLASSPATH rather than the file system.

//              if( ! new File( filename ).exists() ) {

//                  log.fatal
//                  ( "File does not exist: "+filename
//                    );

//                  System.exit( 1 );

//              }

                String baseURI = "";

                if( argno+1 < args.length ) {

                    // Don't consume args that start with '-' since
                    // they begin the next command line argument.

                    if( ! args[ argno + 1].startsWith( "-" ) ) {

                    try {
                        
                        // test next arg for URI.

                        new URI( args[ argno + 1 ] );
                        
                        // consume that arg as the baseURI.

                        baseURI = args[ ++ argno ];

                    }
                    
                    catch( URISyntaxException ex ) {
                        
                        // Ingore - arg is not a URI.
                        
                    }

                    }

                }

                // Add to the ordered list of RDF files to be uploaded to
                // the repository.

                log.info
                    ( "Will load: "+filename+
                      ( baseURI != null && baseURI.length() > 0
                    ? " ("+baseURI+")"
                    : ""
                    )
                      );

                v.add( new FileAndBaseURI( filename, baseURI ) );

                } else {

                // Anything else is a command line argument that we do not
                // (or did not) understand.

                throw new RuntimeException( "Could not parse: "+s );

                }

            }

        //
        // Get the RDF file(s) to be loaded.  This respects the order
        // in which they are given on the command line.
        //

        FileAndBaseURI[] sources = (FileAndBaseURI[]) v.toArray
            ( new FileAndBaseURI[]{}
              );

        if( sources.length == 0 ) {

            log.warn
            ( "No RDF source(s) were specified."
              );

        } else {

            log.info
            ( "Will load "+sources.length+" files."
              );

        }

        return sources;
        
    }
    
    /**
     * Trivial helper class for RDF filename and optional base URI used to
     * describe the files to upload to the repository during a test run.
     */
    public static class FileAndBaseURI {

        String m_filename;

        String m_baseURI;

        public String getFilename() {
            return m_filename;
        }

        public String getBaseURI() {
            return m_baseURI;
        }

        public FileAndBaseURI(String filename) {

            this(filename, "");

        }

        public FileAndBaseURI(String filename, String baseURI) {

            if (filename == null) {

                throw new IllegalArgumentException("filename");

            }

            if (baseURI == null) {

                throw new IllegalArgumentException("baseURI");

            }

            m_filename = filename;

            m_baseURI = baseURI;

        }

        public static String[] getFileNames(FileAndBaseURI[] sources) {

            int len = sources.length;
            String[] ret = new String[len];
            for (int i = 0; i < len; i++) {
                ret[i] = sources[i].getFilename();
            }
            return ret;

        }

        public static String[] getBaseURIs(FileAndBaseURI[] sources) {
            int len = sources.length;
            String[] ret = new String[len];
            for (int i = 0; i < len; i++) {
                ret[i] = sources[i].getBaseURI();
            }
            return ret;
        }

    }

    /**
     * If the file exists, then attempts to delete the file. If the file could
     * not be deleted, then an exception is thrown.
     * 
     * @param filename
     *            The filename.
     */
    protected void deleteFile(String filename) {

        File file = new File(filename);

        if (file.exists()) {

            if (!file.delete()) {

                throw new RuntimeException("Could not delete: file=" + file);

            }

        }

    }

}
