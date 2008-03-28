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
package com.bigdata.rdf.store;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.rdf.model.OptimizedValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;

/**
 * A test for measuring the possible insert rate for a triple store based on a
 * journal and btree using a synthetic data generator.
 * <p>
 * 
 * The s:p:o of the statements can use random selection without replacement from
 * a class space, a property space, and a literal space. Those spaces can be
 * pre-populated such that we can insert up to ( #class * #prop * #value )
 * distinct statements if we select subjects from the class space, predicates
 * from the property space, and objects from the literal space and the class
 * space. However, we should also select objects from the class space, which
 * produces additional distinct statements. The literal space should be divided
 * into plain literals, literals with language tags, and typed literals. The
 * space of types are the predefined XSD types plus those defined by RDFS
 * (rdf:xml).
 * <p>
 * 
 * In order to test plain RDF insert, we do not need to do anything beyond this.
 * <p>
 * 
 * In order to test RDFS insert, there needs to be some ontology. This can be
 * introduced by creating a class hierarchy from the class space and a property
 * heirarchy from the property space. Such a hierarchy could be formed by either
 * by inserting or removing rdfs:subClassOf (or rdfs:subPropertyOf) assertions
 * from a fully connected matrix in the appropriate space.
 * <p>
 * 
 * FIXME modify to use variable length unsigned byte[] keys and the
 * {@link UnsignedByteArrayComparator} and see how that effects performance -
 * the performance will be the base line on which I can then improve. Once I
 * have that baseline I can then go into the btree code and strip out the
 * polymorphic keys (except maybe int and long) and add in support for prefix
 * btrees and choosing short separators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInsertRate extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestInsertRate() {
    }

    /**
     * @param name
     */
    public TestInsertRate(String name) {
        super(name);
    }

    /**
     * Large scale insert test.
     * 
     * @param args
     *            unused - just edit the code.
     */
    public static void main(String[] args) throws Exception {

//        // small
//        int nclass = 30;
//        int nproperty = 20;
//        int nliteral = 20;
//        int litsize = 100;

        // moderate
//        int nclass = 300; // @todo at 300 this will force the journal to be extended on commit.
//        int nproperty = 20;
//        int nliteral = 20;
//        int litsize = 100;

      // large
      int nclass = 5000;
      int nproperty = 20;
      int nliteral = 30;
//      int nliteral = 0;
      int litsize = 300;
    
        TestInsertRate test = new TestInsertRate("TestInsertRate");
        test.setUp();
        test.doTest( nclass, nproperty, nliteral, litsize );
        test.tearDown();
            
    }

    /**
     * Defines a variety of URIs relevant to the XML Schema Datatypes
     * specification.
     */

    static public class XMLSchema {

        /**
         * The namespace name, commonly associated with the prefix "rdf", whose
         * value is "http://www.w3.org/1999/02/22-rdf-syntax-ns#".
         */

        public static final String NAMESPACE_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

        /**
         * The namespace name, commonly associated with the prefix "rdfs", whose
         * value is "http://www.w3.org/2000/01/rdf-schema#".
         */

        public static final String NAMESPACE_RDFS = "http://www.w3.org/2000/01/rdf-schema#";

        /**
         * The URI,commonly written as rdf:XMLLiteral, whose value is
         * "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral".
         */

        public static final String RDF_XMLLiteral = "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral";

        /**
         * The namespace name, commonly associated with the prefix "xsd", whose
         * value is "http://www.w3.org/2001/XMLSchema#".
         * 
         * @todo [http://www.w3.org/2001/XMLSchema-datatypes] is a synonym for
         *       the same namespace....
         */

        public static final String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema#";

        /**
         * The XSDatatype URI for "boolean".
         */

        public static final String xsBoolean = NAMESPACE_XSD + "boolean";

        /**
         * The XSDatatype URI for "byte".
         */

        public static final String xsByte = NAMESPACE_XSD + "byte";

        /**
         * The XSDatatype URI for "short".
         */

        public static final String xsShort = NAMESPACE_XSD + "short";

        /**
         * The XSDatatype URI for "int".
         */

        public static final String xsInt = NAMESPACE_XSD + "int";

        /**
         * The XSDatatype URI for "lomg".
         */

        public static final String xsLong = NAMESPACE_XSD + "long";

        /**
         * The XSDatatype URI for "float".
         */

        public static final String xsFloat = NAMESPACE_XSD + "float";

        /**
         * The XSDatatype URI for "double".
         */

        public static final String xsDouble = NAMESPACE_XSD + "double";

        /**
         * The XSDatatype URI for "integer" - used for {@link
         * java.math.BigInteger} and natively by the XSD library for
         * {@link com.sun.msv.datatype.xsd.IntegerValueType}.
         */

        public static final String xsInteger = NAMESPACE_XSD + "integer";

        /**
         * The XSDatatype URI for "decimal" - used for {@link
         * java.math.BigDecimal}
         */

        public static final String xsDecimal = NAMESPACE_XSD + "decimal";

        /**
         * The XSDatatype URI for "string".
         */

        public static final String xsString = NAMESPACE_XSD + "string";

        /**
         * The XSDatatype URI for "anyURI".
         */

        public static final String xsAnyURI = NAMESPACE_XSD + "anyURI";

    }

    /**
     * Primary driver for the insert rate test.
     * 
     * @param nclass
     *            The #of distinct classes.
     * 
     * @param nproperty
     *            The #of distinct properties.
     * 
     * @param nliteral
     *            The #of plain literals, the #of literals for each language
     *            type, and the #of typed literals for each datatype URI.
     * 
     * @param litsize
     *            The average size of a literal. The generated literals use a
     *            normal distribution with this as their mean length (in
     *            characters).
     */
    public void doTest(final int nclass, final int nproperty,
            final int nliteral, final int litsize) throws IOException {

        AbstractTripleStore store = getStore();
        
        try {

            doTest(store, nclass, nproperty, nliteral, litsize);
            
        } finally {
            
            store.closeAndDelete();
        }

    }
    
    private void doTest(AbstractTripleStore store, final int nclass,
            final int nproperty, final int nliteral, final int litsize)
            throws IOException {

        final URI[] cspace = new URI[nclass];
        final URI[] pspace = new URI[nproperty];
        final URI[] tspace = new URI[] {
                // uncomment to get data typed literals.
// new URIImpl( XMLSchema.xsInteger ),
//        new URIImpl( XMLSchema.xsFloat )
        };
        final String[] langSpace = new String[]{
                // uncomment to get language typed literals.
//                "en","de"
                };
        final int nliteral2 =
                nliteral + 
            nliteral * tspace.length +
            nliteral * langSpace.length
            ;
        final Literal[] lspace = new Literal[nliteral2];

        final int nvalues = nclass + nproperty + nliteral2;

        if( true ) {

            final long begin = System.currentTimeMillis();

            final ValueFactory fac = OptimizedValueFactory.INSTANCE;
            
            log.info( "\nCreating "+nvalues+" values..." );
        
            for( int i=0; i<cspace.length; i++ ) {
            
            // @todo random class names
            cspace[ i ] = fac.createURI( "http://class/"+i );
               
            }
        
            for( int i=0; i<pspace.length; i++ ) {

            // @todo random property names      
            pspace[ i ] = fac.createURI( "http://property/"+i );
               
            }

            if( true ) {

            int index = 0; 
        
            for( int i=0; i<nliteral; i++ ) {

                lspace[ index++ ] = fac.createLiteral
                ( getRandomString( litsize, index )
                  );

                for( int j=0; j<langSpace.length; j++ ) {

                lspace[ index++ ] = fac.createLiteral
                    ( getRandomString( litsize, index ),
                      langSpace[ j ]
                      );

                }

                for( int j=0; j<tspace.length; j++ ) {
                
                lspace[ index++ ] = fac.createLiteral
                    ( getRandomType( tspace[ j ], index ),
                      tspace[ j ]
                      );
                
                }
            
            }

            }

            /* Note: a commit is not necessary here since we are not using
             * the persistence capable backend to create the values, just
             * the default memory-resident implementation classes provided
             * by openrdf.
             */
//            commit();
            
            long elapsed = System.currentTimeMillis() - begin;

            log.info( "\nCreated "+nvalues+" values"+
                  ": elapsed="+elapsed+
                  ", value/sec="+perSec(nvalues,elapsed)
                  );

        }

        // The object space contains everything in the class space plus
        // everything in the literal space.

        final int nobject = lspace.length + cspace.length;
        
        Value[] ospace = new Value[ nobject ];

        if( true ) {

            int index = 0;

            for( int i=0; i<cspace.length; i++ ) {

            ospace[ index++ ] = cspace[ i ];

            }

            for( int i=0; i<lspace.length; i++ ) {

            ospace[ index++ ] = lspace[ i ];

            }

        }

        final int nstmts = nclass * nproperty * nobject;

        Writer w = getWriter( ".out" );

        w.write // description of test.
            ( "Test: "+getName()+
              ", #class="+nclass+
              ", #property="+nproperty+
              ", #literal="+nliteral+
              ", #languages="+langSpace.length+
              ", #datatypes="+tspace.length+
              ", #maxlitsize="+litsize+
              ", #literals(all)="+nliteral2+
              ", #objects="+nobject+
              ", #statements="+nstmts+"\n"
              );

            w.write // header for columns.
        ( "#stmts\t#stmts(interval)\telapsed(interval)\tstmts/sec(interval)\n"
          );

        w.flush();

        log.info
            ( "\nTest: "+getName()+
              ", #class="+nclass+
              ", #property="+nproperty+
              ", #literal="+nliteral+
              ", #languages="+langSpace.length+
              ", #datatypes="+tspace.length+
              ", #maxlitsize="+litsize+
              ", #literals(all)="+nliteral2+
              ", #objects="+nobject+
              ", #statements="+nstmts
              );

        /*
         * Note: when true, statement are presented in random order. When false
         * they are presented in the order in which they were generated which
         * should have good locality for the indices.
         * 
         * @todo rename cspace as sspace?
         */
        boolean randomOrder = false;
        
        // Get random orderings for selecting from the class, property
        // and value spaces.
        final int corder[] = getRandomOrder( cspace.length );
        final int porder[] = getRandomOrder( pspace.length );
        final int oorder[] = getRandomOrder( ospace.length );

        //
        // Generate and insert random statements.
        //
        // @todo This has a strong order effect bias since all statements
        // about the same subject are inserted at once, hence [randomOrder]
        // does not truely randomize the presentation of triples to the store.
        //
        
        long begin = System.currentTimeMillis();

        long begin2 = begin;
        
        int index = 0, index2 = 0;
        
        StatementBuffer sbuf = new StatementBuffer(store,10000);
        
        for( int i=0; i<cspace.length; i++ ) {
            
            for( int j=0; j<pspace.length; j++ ) {
                
                for( int k=0; k<ospace.length; k++ ) {
                    
                    URI s = randomOrder ? cspace[corder[i]] : cspace[i];
                    URI p = randomOrder ? pspace[porder[j]] : pspace[j];
                    Value o = randomOrder ? ospace[oorder[k]] : ospace[k]; 

//                  System.err.println
//                      ( "index="+index+", "+
//                        s+" : "+
//                        p+" : "+
//                        o
//                        );

                    sbuf.add( s, p, o );

                    // Progress marker and incremental statistics.

                    if( index > 0 && index % 10000 == 0 ) {
                        
                        System.err.print( "." );

                        if( index % 100000 == 0 ) {

                    long now = System.currentTimeMillis();

                    long elapsed = now - begin2;

                    begin2 = now; // reset.

                            w.write
                    ( ""+index+"\t"+index2+"\t"+elapsed+"\t"+perSec(index2,elapsed)+"\n"
                      );

                    w.flush();

                    log.info
                    ( "\nCurrent insert rate"+
                      ": #statements(so far)="+index+
                      ": #statements(interval)="+index2+
                      ", elapsed(interval)="+elapsed+
                      ", stmts/sec="+perSec(index2,elapsed)
                      );

                    index2 = 0; // reset.

//                    m_repo.startTransaction();

                        }                   
                        
                    }
                    
                    index++;
                index2++;
                    
                }
                
            }
            
        }
        
        sbuf.flush();
        
        long elapsed = System.currentTimeMillis() - begin;

        w.write
            ( "Sustained insert rate"+
              ": #statements="+index+
              ", elapsed="+elapsed+
              ", stmts/sec="+perSec(index,elapsed)+"\n"
              );

        log.info
            ( "\nSustained insert rate"+
              ": #statements="+index+
              ", elapsed="+elapsed+
              ", stmts/sec="+perSec(index,elapsed)
              );

        w.flush();

        w.close();
        
        }

        /**
         * Returns a random but unique value within the identified type
         * space.
         *
         * @param t The data type URI.
         *
         * @param id A unique index used to obtain a unique value in the
         * identified type space.  Typically this is a one up identifier.
         */

        private String getRandomType( URI t, int id )
        {

        // FIXME This needs to be type sensitive.  For some types, the
        // size of the type space is of necessity limited.  For such
        // types I imagine that this method needs to recycle values,
        // which results in a net reduction in the size of the overal
        // literal space and hence the #of distinct statements that
        // can be made.

        return ""+id;

        }

        /**
         * Returns the quantity <i>n</i> expressed as a per-second rate or
         * "N/A" if the elapsed time is zero.
         */

        static final public String perSec( final int n, final long elapsed )
        {

        if( n == 0 ) return "0";

        return ((elapsed==0?"N/A":""+(int)(n/(elapsed/1000.))));

        }


        /**
         * Returns a writer named by the test and having the specified
         * filename extension.
         */

        public Writer getWriter( String ext )
            throws IOException
        {

        return new BufferedWriter
            ( new FileWriter
              ( getName()+ext
            )
              );

        }
                
        /**
         * @todo write tests for the individual indices, restart safety,
         *       concurrent writers, and concurrent writes with concurrent
         *       query.
         */
        public void test_tiny() throws IOException {

            // tiny
            int nclass = 3;
            int nproperty = 2;
            int nliteral = 2;
            int litsize = 100;

            doTest( nclass, nproperty, nliteral, litsize );

        }

        public void test_small() throws IOException {

            int nclass = 30;
            int nproperty = 20;
            int nliteral = 20;
            int litsize = 100;
      
            doTest( nclass, nproperty, nliteral, litsize );
    
        }

//        /**
//         * This tests nearly a million triples.
//         * 
//         * <pre>
//         * Sustained insert rate: #statements=880000, elapsed=7422, stmts/sec=118566
//         * </pre>
//         */
//        public void test_moderate() throws IOException {
//
//            int nclass = 200; // @todo at 300 this will force the journal to be extended on commit.
//            int nproperty = 20;
//            int nliteral = 20;
//            int litsize = 100;
//      
//            doTest( nclass, nproperty, nliteral, litsize );
//            
////          // moderate.
////          int nclass = 5000;
////          int nproperty = 20;
////          int nliteral = 30;
//////          int nliteral = 0;
////          int litsize = 300;
//
//        }

}
