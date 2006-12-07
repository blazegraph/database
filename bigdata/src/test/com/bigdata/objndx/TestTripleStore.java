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
 * Created on Dec 6, 2006
 */

package com.bigdata.objndx;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

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
 * @todo add logic to actually insert stuff into indices. this will require a
 *       refactor of the btree package to support generic keys with comparator
 *       methods.
 * 
 * @todo Note that a very interesting solution for RDF places all data into a
 *       statement index and then uses block compression techniques to remove
 *       frequent terms, e.g., the repeated parts of the value. Also note that
 *       there will be no "value" for an rdf statement since existence is all.
 *       The key completely encodes the statement. So, another approach is to
 *       bit code the repeated substrings found within the key in each leaf. *
 *       This way the serialized key size reflects only the #of distinctions.
 * 
 * @todo add logic to parse from rio into the database.
 * 
 * @todo measure the effect of batching (absorbing and then sorting a batch and
 *       doing bulk inserts into the various indices). try this with and without
 *       bulk inserts and also try with just "perfect" index builds and
 *       compacting sort-merges (i.e., a bulk load that runs outside of the
 *       transaction mechanisms).
 * 
 * @todo I've been thinking about rdfs stores in the light of the work on
 *       bigdata. Transactional isolation for rdf is really quite simple. Since
 *       lexicons (uri, literal or bnode indices) do not (really) support
 *       deletion, the only acts are asserting and retracting statements. since
 *       a statement always merges with an existing statement, inserts never
 *       cause conflicts. Hence the only possible write-write conflict is a
 *       write-delete conflict. quads do not really make this more complex (or
 *       expensive) since merges only occur when there is a context match.
 *       however entailments can cause twists depending on how they are
 *       realized.
 * 
 * If we do a pure RDF layer (vs RDF over GOM over bigdata), then it seems that
 * we could simple use a statement index (no lexicons for URIs, etc). Normally
 * this inflates the index size since you have lots of duplicate strings, but we
 * could just use block compression to factor out those strings when we evict
 * index leaves to disk. Prefix compression of keys will already do great things
 * for removing repetitive strings from the index nodes and block compression
 * will get at the leftover redundancy.
 * 
 * So, one dead simple architecture is one index per access path (there is of
 * course some index reuse across the access paths) with the statements inline
 * in the index using prefix key compression and block compression to remove
 * redundancy. Inserts on this architecture would just send triples to the store
 * and the various indices would be maintained by the store itself. Those
 * indices could be load balanced in segments across a cluster.
 * 
 * Since a read that goes through to disk reads an entire leaf at a time, the
 * most obvious drawback that I see is caching for commonly used assertions, but
 * that is easy to implement with some cache invalidation mechanism coupled to
 * deletes.
 * 
 * I can also see how to realize very large bulk inserts outside of a
 * transactional context while handling concurrent transactions -- you just have
 * to reconcile as of the commit time of the bulk insert and you get to do that
 * using efficient compacting sort-merges of "perfect" bulk index segments. The
 * architecture would perform well on concurrent apstars style document loading
 * as well as what we might normally consider a bulk load (a few hundred
 * megabytes of data) within the normal transaction mechanisms, but if you
 * needed to ingest uniprot you would want to use a different technique :-)
 * outside of the normal transactional isolation mechanisms.
 * 
 * I'm not sure what the right solution is for entailments, e.g., truth
 * maintenance vs eager closure. Either way, you would definitely want to avoid
 * tuple at a time processing and batch things up so as to minimize the #of
 * index tests that you had to do. So, handling entailments and efficient joins
 * for high-level query languages would be the two places for more thought. And
 * there are little odd spots in RDF - handling bnodes, typed literals, and the
 * concept of a total sort order for the statement index.
 * 
 * @todo verify that we are not generating heap churn by needless allocations.
 * 
 * @todo Note that the target platform will use a 200M journal extent and freeze
 *       the extent when it gets full, opening a new extent. Once a frozen
 *       journal contains committed state, we can evict the indices from the
 *       journal into perfect index range files. Reads then read through the
 *       live journal, any frozen journals that have not been evicted yet, and
 *       lastly the perfect index range segments on the disk. For the latter,
 *       the index nodes are buffered so that one read is require to get a leaf
 *       from the disk. Perfect index range segments get compacted from time to
 *       time. A bloom filter in front of an index range (or a segment of an
 *       index range) could reduce IOs further.
 * 
 * @todo test with the expected journal modes, e.g., direct buffered, once the
 *       page buffering feature is in place.
 * 
 * @todo a true bulk loader does not need transactional isolation and this class
 *       does not use transactional isolation. When using isolation there are
 *       two use cases - small documents where the differential indicies will be
 *       small (document sized) and very large loads where we need to use
 *       persistence capable differential indices and the load could even span
 *       more than one journal extent.
 * 
 * @todo What about BNodes? These need to get in here somewhere....
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStore extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestTripleStore() {
    }

    /**
     * @param name
     */
    public TestTripleStore(String name) {
        super(name);
    }

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                    .toString());

        }

        return properties;

    }

    private Properties properties;
    
    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor) {

        try {
            
            Properties properties = getProperties();

            Journal journal = new Journal(properties);

            // A modest leaf queue capacity.
            final int leafQueueCapacity = 500;
            
            final int nscan = 10;

            BTree btree = new BTree(journal, branchingFactor,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            leafQueueCapacity, nscan),
                    new SimpleEntry.Serializer());

            return btree;

        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        // @todo parameterized runs or bulk loads in main()
        
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
         * Note: This uses the value factory for the repository, which means
         * that the URI, Literal, etc. objects are all specific to the backend
         * under test. This is a bit of an optimization for repositories that
         * have a persistent object semantics. It is unlikely to benefit
         * repositories based on a remote database connection as much as those
         * based on a local persistence store.
         * <p>
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
         *            The average size of a literal. The generated literals use
         *            a normal distribution with this as their mean length (in
         *            characters).
         */

        public void doTest
        ( final int nclass,
          final int nproperty,
          final int nliteral,
          final int litsize
          )
            throws IOException
        {

        final URI[] cspace = new URI[ nclass ];
        final URI[] pspace = new URI[ nproperty ];
        final URI[] tspace = new URI[] {
        new URIImpl( XMLSchema.xsInteger ),
        new URIImpl( XMLSchema.xsFloat )
        };
        final String[] langSpace = new String[]{"en","de"};
        final int nliteral2 =
                nliteral + 
            nliteral * tspace.length +
            nliteral * langSpace.length
            ;
        final Literal[] lspace = new Literal[nliteral2];

        final int nvalues = nclass + nproperty + nliteral2;

        if( true ) {

            final long begin = System.currentTimeMillis();

            final ValueFactory fac = new ValueFactoryImpl();
            
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

            commit();
            
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

        // Get random orderings for selecting from the class, property
        // and value spaces.
        //
        // @todo rename cspace as sspace?

        final int corder[] = getRandomOrder( cspace.length );
        final int porder[] = getRandomOrder( pspace.length );
        final int oorder[] = getRandomOrder( ospace.length );

        //
        // Generate and insert random statements.
        //
        // @todo This has a strong order effect bias since all statements
        // about the same subject are inserted at once.
        //
        
        long begin = System.currentTimeMillis();

        long begin2 = begin;
        
        int index = 0, index2 = 0;
        
        for( int i=0; i<cspace.length; i++ ) {
            
            for( int j=0; j<pspace.length; j++ ) {
                
                for( int k=0; k<ospace.length; k++ ) {
                    
                    URI s = cspace[corder[i]];
                    URI p = pspace[porder[j]];
                    Value o = ospace[oorder[k]]; 

//                  System.err.println
//                      ( "index="+index+", "+
//                        s+" : "+
//                        p+" : "+
//                        o
//                        );

                    addStatement( s, p, o );

                    // Progress marker and incremental statistics.

                    if( index > 0 && index % 1000 == 0 ) {
                        
                        System.err.print( "." );

                        if( index % 5000 == 0 ) {

                    commit();

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

        commit();

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

        public void doTest()
            throws IOException
        {

//            int nclass = 300;
//            int nproperty = 20;
//            int nliteral = 200;
//            int litsize = 100;
            
            int nclass = 5000;
            int nproperty = 20;
//            int nliteral = 30;
            int nliteral = 0;
            int litsize = 300;
            
        doTest( nclass, nproperty, nliteral, litsize );

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
        
        // FIXME add to indices.
        public void addStatement(Resource s, URI p, Value o ) {
            
//            System.err.println("("+s+":"+p+":"+o+")");
            
        }

        // FIXME flush indices.
        public void commit() {
            
            System.err.println("incremental commit");

//            ndx.uris.commit();
//            ndx.lits.commit();
//            ndx_stmts.commit();
            
        }

        public void test_small() throws IOException {
            
      int nclass = 30;
      int nproperty = 20;
      int nliteral = 20;
      int litsize = 100;
      
//      int nclass = 5000;
//      int nproperty = 20;
//      int nliteral = 30;
////      int nliteral = 0;
//      int litsize = 300;
      
    doTest( nclass, nproperty, nliteral, litsize );
    
        }

}
