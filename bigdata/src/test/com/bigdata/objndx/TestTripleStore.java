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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Properties;

import org.CognitiveWeb.extser.LongPacker;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Bytes;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.objndx.IndexEntrySerializer.ByteBufferInputStream;
import com.bigdata.objndx.IndexEntrySerializer.ByteBufferOutputStream;

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

//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
//                    .toString());

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());
            properties.setProperty(Options.SEGMENT, "0");
            properties.setProperty(Options.FILE, getName()+".jnl");

        }

        return properties;

    }

    private Properties properties;

    /**
     * A persistent index for RDF {@link Statement}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SPOIndex extends BTree {

        /**
         * Create a new statement index.
         * 
         * @param store
         *            The backing store.
         */
        public SPOIndex(IRawStore store) {
            super(store,
                    ArrayType.OBJECT, // generic keys
                    DEFAULT_BRANCHING_FACTOR,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            DEFAULT_LEAF_QUEUE_CAPACITY,
                            DEFAULT_LEAF_QUEUE_SCAN),
                    null, // NEGINF
                    SPOComparator.INSTANCE,
                    KeySerializer.INSTANCE,
                    ValueSerializer.INSTANCE
                    );
        }
        
        /**
         * Load a statement index from the store.
         * 
         * @param store
         *            The backing store.
         * @param metadataId
         *            The metadata record identifier for the index.
         */
        public SPOIndex(IRawStore store, long metadataId) {
            super(  store,
                    metadataId,
                    new HardReferenceQueue<PO>( new DefaultEvictionListener(), DEFAULT_LEAF_QUEUE_CAPACITY, DEFAULT_LEAF_QUEUE_SCAN),
                    null, // NEGINF
                    SPOComparator.INSTANCE,
                    KeySerializer.INSTANCE,
                    ValueSerializer.INSTANCE
                    );
        }

        /**
         * Places statements into a total ordering for the SPO index.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class SPOComparator implements Comparator<SPO> {

            static final Comparator INSTANCE = new SPOComparator();
            
            public int compare(SPO o1, SPO o2) {
                
                return o1.compareTo(o2);
                
            }
            
        }
        
        /**
         * Key class for the SPO index. The key is comprised of long integer
         * identifies assigned by a term index to each term in the statement.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class SPO implements Comparable<SPO>{
            final long s;
            final long p;
            final long o;
            public SPO(long s,long p,long o) {
                this.s = s;
                this.p = p;
                this.o = o;
            }
            public int compareTo(SPO t) {
                long ret = s - t.s;
                if( ret == 0 ) {
                    ret = p - t.p;
                    if( ret == 0 ) {
                        ret = o - t.o;
                    }
                }
                if( ret == 0 ) return 0;
                if( ret > 0 ) return 1;
                return -1;
            }
        }

        /**
         * Key serializer for the SPO index.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class KeySerializer implements IKeySerializer {

            static final IKeySerializer INSTANCE = new KeySerializer();
            
            public int getSize(int n) {
                
                return (Bytes.SIZEOF_LONG * 3) * n;
                
            }

            public void getKeys(ByteBuffer buf, Object keys, int nkeys) {

                Object[] a = (Object[])keys;
                
                DataInputStream is = new DataInputStream( new ByteBufferInputStream(buf) );
                
                try {

                    for (int i = 0; i < nkeys; i++) {

                        long s = LongPacker.unpackLong(is);
                        long p = LongPacker.unpackLong(is);
                        long o = LongPacker.unpackLong(is);
                        
                        a[i] = new SPO(s, p, o);
                        
                    }

                }

                catch (EOFException ex) {

                    RuntimeException ex2 = new BufferUnderflowException();

                    ex2.initCause(ex);

                    throw ex2;

                }

                catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

            public void putKeys(ByteBuffer buf, Object keys, int nkeys) {

                Object[] a = (Object[])keys;
                
                DataOutputStream os = new DataOutputStream(
                        new ByteBufferOutputStream(buf));

                try {

                    for (int i = 0; i < nkeys; i++) {

                        SPO key = (SPO)a[i];

                        LongPacker.packLong(os, key.s);
                        LongPacker.packLong(os, key.p);
                        LongPacker.packLong(os, key.o);

                    }

                    os.flush();

                }

                catch (EOFException ex) {

                    RuntimeException ex2 = new BufferOverflowException();

                    ex2.initCause(ex);

                    throw ex2;

                }

                catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

        }

        /**
         * Note: There is no additional data serialized with a statement at this
         * time so the value serializer is essentially a nop. All the
         * information is in the keys.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         * 
         * @todo could serialize the statement values with the statement to
         *       avoid reverse lookup indices.
         *       
         * @todo could mark inferred vs explicit vs axiom statements.
         */
        public static class ValueSerializer implements IValueSerializer {

            static final IValueSerializer INSTANCE = new ValueSerializer();
            
            public int getSize(int n) {
                return 0;
            }

            public void getValues(ByteBuffer buf, Object[] values, int n) {
                return;
            }

            public void putValues(ByteBuffer buf, Object[] values, int n) {
                return;
            }
            
        }
        
    }
    
    /**
     * A persistent index for {@link String} keys.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StringIndex extends BTree {

        /**
         * The next identifier to be assigned to a string inserted into this
         * index.
         * 
         * @todo this needs to be (a) shared across all transactional instances
         *       of this index; (b) restart safe; and (c) set into a namespace
         *       that is unique to the journal so that multiple writers on
         *       multiple journals for a single distributed database can not
         *       collide.
         */
        protected long nextId = 1;
        
        /**
         * Create a new index.
         * 
         * @param store
         *            The backing store.
         */
        public StringIndex(IRawStore store) {
            super(store,
                    ArrayType.OBJECT, // generic keys
                    DEFAULT_BRANCHING_FACTOR,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            DEFAULT_LEAF_QUEUE_CAPACITY,
                            DEFAULT_LEAF_QUEUE_SCAN),
                    null, // NEGINF
                    StringComparator.INSTANCE,
                    KeySerializer.INSTANCE,
                    ValueSerializer.INSTANCE
                    );
        }
        
        /**
         * Load an index from the store.
         * 
         * @param store
         *            The backing store.
         * @param metadataId
         *            The metadata record identifier for the index.
         */
        public StringIndex(IRawStore store, long metadataId) {
            super(  store,
                    metadataId,
                    new HardReferenceQueue<PO>( new DefaultEvictionListener(), DEFAULT_LEAF_QUEUE_CAPACITY, DEFAULT_LEAF_QUEUE_SCAN),
                    null, // NEGINF
                    StringComparator.INSTANCE,
                    KeySerializer.INSTANCE,
                    ValueSerializer.INSTANCE
                    );
        }

        public long insert(String s) {
            
            Long id = (Long)lookup(s);
            
            if( id == null ) {
                
                id = nextId++;
                
                insert(s,id);
                
            }
            
            return id;
            
        }
        
        /**
         * Places URIs into a total ordering.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class StringComparator implements Comparator<String> {

            static final Comparator INSTANCE = new StringComparator();
            
            public int compare(String o1, String o2) {
                
                return o1.compareTo(o2);
                
            }
            
        }
        
        /**
         * Key serializer.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        public static class KeySerializer implements IKeySerializer {

            static final IKeySerializer INSTANCE = new KeySerializer();
            
            /**
             * FIXME There is no fixed upper limit for Ustrings in general,
             * therefore the btree may have to occasionally resize its buffer to
             * accomodate very long variable length keys.
             */
            public int getSize(int n) {
                
                return 4096*n;
                
            }

            public void getKeys(ByteBuffer buf, Object keys, int nkeys) {

                Object[] a = (Object[])keys;

                DataInputStream is = new DataInputStream( new ByteBufferInputStream(buf) );
                
                try {

                    for (int i = 0; i < nkeys; i++) {

                        a[i] = is.readUTF();

                    }

                }

                catch (EOFException ex) {

                    RuntimeException ex2 = new BufferUnderflowException();

                    ex2.initCause(ex);

                    throw ex2;

                }

                catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

            public void putKeys(ByteBuffer buf, Object keys, int nkeys) {

                if( nkeys == 0 ) return;
                
                Object[] a = (Object[]) keys;

                DataOutputStream os = new DataOutputStream(
                        new ByteBufferOutputStream(buf));

                try {

                    for (int i = 0; i < nkeys; i++) {

                        os.writeUTF((String)a[i]);

                    }

                    os.flush();

                }

                catch (EOFException ex) {

                    RuntimeException ex2 = new BufferOverflowException();

                    ex2.initCause(ex);

                    throw ex2;

                }

                catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

        }

        /**
         * Note: There is no additional data serialized with a String. All the
         * information is in the keys.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        public static class ValueSerializer implements IValueSerializer {

            static final IValueSerializer INSTANCE = new ValueSerializer();
            
            public int getSize(int n) {
                return 0;
            }

            public void getValues(ByteBuffer buf, Object[] values, int n) {
                return;
            }

            public void putValues(ByteBuffer buf, Object[] values, int n) {
                return;
            }
            
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
                // uncomment to get data typed literals.
//        new URIImpl( XMLSchema.xsInteger ),
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

//                    commit(); // @todo restore use of incremental commit?

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
        journal.commit();

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
         * add to indices.
         * 
         * @todo optimize by sorting terms into buffers and doing batch lookup/
         *       insert on terms, then create buffer of long[] for each
         *       statement based on the assigned termIds, sort the statement
         *       buffer, and do a batch insert on the statement index.
         */
        public void addStatement(Resource s, URI p, Value o ) {

//          System.err.println("("+s+":"+p+":"+o+")");
            
            long _s = addTerm(s);
            long _p = addTerm(p);
            long _o = addTerm(o);

            SPOIndex.SPO spo = new SPOIndex.SPO(_s,_p,_o);
            
            ndx_spo.insert(spo, spo);
            
        }

        /**
         * Add a term into the appropriate term index.
         * 
         * @param t
         *            The term.
         * 
         * @return The assigned term identifier.
         * 
         * @todo the practice of assigning one up identifiers to terms will
         *       require a means to allocate those identifiers in a highly
         *       concurrent fashion, e.g., by allocating a block of identifiers
         *       at a time to a transaction. This is a bit distasteful, but it
         *       could be handled by reconciling within a distinct transaction
         *       for term inserts vs statement inserts (with appropriate
         *       buffering or nested transactions, all of which seems very heavy
         *       weight). Maybe a nicer solution is to give each journal a
         *       unique identifier and to then assigned term ids within a
         *       namespace formed by the journal identifier. This could be
         *       highly concurrent since we can use a single global counter for
         *       the journal but we still have to reconcile inserts across
         *       concurrent transactions.
         * 
         * @todo The use of long[] identifiers for statements also means that
         *       the SPO and other statement indices are only locally ordered so
         *       they can not be used to perform a range scan without joining
         *       against the various term indices.
         */
        public long addTerm(Value t) {
            
            if( t instanceof URI ) {
                
                String uri = ((URI)t).getURI();
                
                return ndx_uri.insert(uri);
                
            } else if( t instanceof Literal ) {
                
                Literal lit = (Literal) t;
                
                if( lit.getLanguage() != null ) {
                    
                    throw new UnsupportedOperationException("literal has language tag");
                    
                }

                if( lit.getDatatype() != null ) {
                    
                    throw new UnsupportedOperationException("literal has data type");
                    
                }
                
                String label = lit.getLabel();
                
                return ndx_lit.insert(label);
                
            } else if( t instanceof BNode ) {
                
                throw new UnsupportedOperationException("bnode");
                
            } else {
                
                throw new AssertionError();
                
            }
            
        }
        
        /**
         * @todo restart safety requires that the individual indices are flushed
         *       to disk and their metadata records written and that we update
         *       the corresponding root in the root block with the new metadata
         *       record location and finally commit the journal.
         * 
         * @todo transactional isolation requires that we have isolation
         *       semantics (nested index, validation, and merging down) built
         *       into each index.
         */
        public void commit() {
            
            System.err.println("incremental commit");

            ndx_uri.commit();
            ndx_lit.commit();
            ndx_spo.commit();
            
        }

        /**
         * @todo write tests for the individual indices, restart safety,
         *       concurrent writers, and concurrent writes with concurrent
         *       query.
         */
        public void test_small() throws IOException {

//            // tiny
//            int nclass = 3;
//            int nproperty = 2;
//            int nliteral = 2;
//            int litsize = 100;

            // small.
      int nclass = 30;
      int nproperty = 20;
      int nliteral = 20;
      int litsize = 100;
      
            // moderate.
//      int nclass = 5000;
//      int nproperty = 20;
//      int nliteral = 30;
////      int nliteral = 0;
//      int litsize = 300;
      
    doTest( nclass, nproperty, nliteral, litsize );
    
        }

        public void setUp() throws Exception {
        
            Properties properties = getProperties();

            String file = properties.getProperty(Options.FILE);
            
            if( file != null ) {
                
                if(! new File(file).delete() ) {
                    
                    throw new RuntimeException("Could not delete file: "+file);
                    
                }
                
            }
            
            journal = new Journal(properties);

            ndx_spo = new SPOIndex(journal);
            ndx_uri = new StringIndex(journal);
            ndx_lit = new StringIndex(journal);
            
        }
        
        Journal journal;
        SPOIndex ndx_spo;
        StringIndex ndx_uri;
        StringIndex ndx_lit;

        public void tearDown() {

            journal.close();
            
        }
        
}
