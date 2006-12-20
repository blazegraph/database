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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
 * @todo The term indices need to use a distinct suffix code so that the
 *       identifiers assigned by each index are unique.
 * 
 * @todo Develop an {@link IValueSerializer} for use on leaves of a statement
 *       index. This can use an efficient data structure for immutable strings,
 *       e.g., some kind of persistent string table, trie, etc., to store the
 *       lexical components of each value in each statement and a compact
 *       representation of each statement in terms of those components.
 * 
 * @todo compute the MB/sec rate at which this test runs and compare it with the
 *       maximum transfer rate for the journal without the btree and the maximum
 *       transfer rate to disk. this will tell us the overhead of the btree
 *       implementation.
 * 
 * @todo Try a variant in which we have metadata linking statements and terms
 *       together. In this case we would have to go back to the terms and update
 *       them to have metadata about the statement. it is a bit circular since
 *       we can not create the statement until we have the terms and we can not
 *       add the metadata to the terms until we have the statement.
 * 
 * @todo Note that a very interesting solution for RDF places all data into a
 *       statement index and then use block compression techniques to remove
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
 * @todo What about BNodes? These need to get in here somewhere.... So does
 *       support for language tag literals and data type literals. XML (or other
 *       large value literals) will cause problems unless they are factored into
 *       large object references if we actually store values directly in the
 *       statement index.
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

            /*
             * Note: all buffer modes report in between 150 and 200k sustained
             * triples per second insert rate out to 10M triples on my dell
             * latitude 620 laptop. The transient mode is faster (~200k tps),
             * but not as much faster as I would have expected seeing as it is
             * not writing to disk while the other modes are. One thing worth
             * exploring is the tradeoff in the branching factor of the btree.
             * Since the disk and direct buffer modes currently write through to
             * disk with each write, we might see better performance with a
             * smaller branching factor in conjunction with deferring until a
             * larger IO can be performed, e.g., 32-64k chunks. I have done some
             * experimenting and there does seem to be better performance with a
             * higher branching factor (not 64, but 128, 196, or 256). Since the
             * btree data structure is slower for larger branching factors due
             * to memory overhead, I suspect the increased in performance is due
             * to larger IOs. A little more experimenting, and I see that
             * performance with the transient store increases with the branching
             * factor until at least 256 and possibly 384. I am now seeing ~
             * 170k tps for the disk and direct buffer modes and close to 200k
             * tps for the transient store with a branching factor of 256. The
             * increase in performance may be due to lower memory allocation
             * rates being less expense that increase moving around of data in
             * the nodes and leaves of the tree.
             */
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
//                    .toString());
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
//                    .toString());
            properties.setProperty(Options.SEGMENT, "0");
            properties.setProperty(Options.FILE, getName()+".jnl");
            properties.setProperty(Options.INITIAL_EXTENT,""+Bytes.megabyte*100);

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
                            DEFAULT_HARD_REF_QUEUE_CAPACITY,
                            DEFAULT_HARD_REF_QUEUE_SCAN),
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
                    new BTreeMetadata(BTree
                            .getTransitionalRawStore(store), metadataId),
                    new HardReferenceQueue<PO>( new DefaultEvictionListener(), DEFAULT_HARD_REF_QUEUE_CAPACITY, DEFAULT_HARD_REF_QUEUE_SCAN),
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
         * 
         * @todo convert to {@link ArrayType#LONG} with a stride of 3.
         */
        public static class KeySerializer implements IKeySerializer {

            static final IKeySerializer INSTANCE = new KeySerializer();
            
            public ArrayType getKeyType() {
                
                return ArrayType.OBJECT;
                
            }

            public int getSize(int n) {
                
                return (Bytes.SIZEOF_LONG * 3) * n;
                
            }

            public void getKeys(DataInputStream is, Object keys, int nkeys)
                    throws IOException {

                Object[] a = (Object[]) keys;

                for (int i = 0; i < nkeys; i++) {

                    long s = LongPacker.unpackLong(is);
                    long p = LongPacker.unpackLong(is);
                    long o = LongPacker.unpackLong(is);

                    a[i] = new SPO(s, p, o);

                }

            }

            public void putKeys(DataOutputStream os, Object keys, int nkeys)
                    throws IOException {

                Object[] a = (Object[]) keys;

                for (int i = 0; i < nkeys; i++) {

                    SPO key = (SPO) a[i];

                    LongPacker.packLong(os, key.s);
                    LongPacker.packLong(os, key.p);
                    LongPacker.packLong(os, key.o);

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

            public void getValues(DataInputStream is, Object[] values, int n) throws IOException {
                return;
            }

            public void putValues(DataOutputStream os, Object[] values, int n) throws IOException {
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
                            DEFAULT_HARD_REF_QUEUE_CAPACITY,
                            DEFAULT_HARD_REF_QUEUE_SCAN),
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
                    new BTreeMetadata(BTree
                            .getTransitionalRawStore(store), metadataId),
                    new HardReferenceQueue<PO>( new DefaultEvictionListener(), DEFAULT_HARD_REF_QUEUE_CAPACITY, DEFAULT_HARD_REF_QUEUE_SCAN),
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
            
            public ArrayType getKeyType() {
                
                return ArrayType.OBJECT;
                
            }

            /**
             * FIXME There is no fixed upper limit for URLs or strings in general,
             * therefore the btree may have to occasionally resize its buffer to
             * accomodate very long variable length keys.
             */
            public int getSize(int n) {
                
                return 4096*n;
                
            }

            public void getKeys(DataInputStream is, Object keys, int nkeys)
                    throws IOException {

                Object[] a = (Object[]) keys;

                for (int i = 0; i < nkeys; i++) {

                    a[i] = is.readUTF();

                }
                
            }

            public void putKeys(DataOutputStream os, Object keys, int nkeys)
                    throws IOException {

                if (nkeys == 0)
                    return;

                Object[] a = (Object[]) keys;

                for (int i = 0; i < nkeys; i++) {

                    os.writeUTF((String) a[i]);

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

            public void getValues(DataInputStream is, Object[] values, int n)
                    throws IOException {

                // NOP
                
            }

            public void putValues(DataOutputStream os, Object[] values, int n)
                    throws IOException {
                
                // NOP
                
            }
            
        }
        
    }
    
    /**
     * Large scale insert test.
     * 
     * @param args
     *            unused - just edit the code.
     * 
     * FIXME This needs to be worked through until the btrees are smoothly being
     * evicted onto the journal. Right now it appears to build up too much
     * overhead, presumably because we are defering all node evictions (only
     * leaves have an eviction queue).
     * 
     * FIXME The journal should smoothly be snapshot and perfect indices built
     * over time. However, very large RDF loads should probably use a bulk load
     * mechanism that is somewhat decoupled from the normal transactional
     * isolation mechanism. E.g., bulk load begins when isolation grows too
     * large and works by evicting perfect index segments onto the disk. Since
     * bulk loads can run for a long time, reconciling the bulk transaction to
     * existing data would be time consuming as well. There are probably a lot
     * of ways to cheat, including if we are using context we can insist that
     * bulk loads do not overlap with existing contexts.
     * 
     * @todo test with {@link RecordCompression} enabled for the {@link BTree}s.
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
    
        TestTripleStore test = new TestTripleStore("TestTripleStore");
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

                    if( index > 0 && index % 10000 == 0 ) {
                        
                        System.err.print( "." );

                        if( index % 100000 == 0 ) {

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
                
                URI uri = (URI)t;
                
                /*
                 * Note: The #1 cause of heap churn is URI.getURI() which
                 * appends the localName to the namespace.
                 * 
                 * FIXME Reduce heap churn here. One way is to simply use a
                 * String or char[] rather than the URIImpl class to model the
                 * URI. Another is to allow the URI into the index, but to
                 * serialize it as a char[] and to read it back into a variant
                 * URI implementation that does not break it down into namespace
                 * and localName.
                 * 
                 * @todo The SPO objects are another source of heap churn. This
                 * could be addressed by allowing the index to manage an array
                 * of primitive data types or a ByteBuffer so that we could
                 * handle the s:p:o elements of the key as if they were a
                 * primitive data type. This would mean more copying of data
                 * during insert and remove on a leaf, but less allocation.
                 * 
                 * @todo 1. String is char[] + String object.  1/2 the allocations
                 * if we use char[] and handle the comparison functions outselves.
                 * 
                 * @todo 2. key run length multiple for long[n] keys.  could also
                 * be used for fixed length char[] keys.  allows us to treat the
                 * key array as a single memory block with multiple complex items
                 * comprised of repetitions of the same primitive type.
                 * 
                 * @todo 3. override value factory to cache very small sets of
                 * URIs corresponding to rdf, rdfs, owl predicates to reduce the
                 * heap churn from very common URIs.
                 * 
                 * @todo 4. override value factory to produce a URI that uses a
                 * char[] and does more work when returning the localName or the
                 * namespace rather than when returning the URI.
                 */
                _term.setLength(0);
                _term.append(uri.getNamespace());
                _term.append(uri.getLocalName());
                
                return ndx_uri.insert(_term.toString());
                
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
        StringBuilder _term = new StringBuilder(4096);
        
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

            ndx_uri.write();
            ndx_lit.write();
            ndx_spo.write();
            
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

        /**
         * This tests nearly a million triples.
         * 
         * <pre>
         * Sustained insert rate: #statements=880000, elapsed=7422, stmts/sec=118566
         * </pre>
         */
        public void test_moderate() throws IOException {

            int nclass = 200; // @todo at 300 this will force the journal to be extended on commit.
            int nproperty = 20;
            int nliteral = 20;
            int litsize = 100;
      
            doTest( nclass, nproperty, nliteral, litsize );
            
//          // moderate.
//          int nclass = 5000;
//          int nproperty = 20;
//          int nliteral = 30;
////          int nliteral = 0;
//          int litsize = 300;

        }
        
        public void setUp() throws Exception {
        
            Properties properties = getProperties();

            String filename = properties.getProperty(Options.FILE);
            
            if( filename != null ) {
                
                File file = new File(filename);
                
                if(file.exists() && ! file.delete() ) {
                    
                    throw new RuntimeException("Could not delete file: "+file.getAbsoluteFile());
                    
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
