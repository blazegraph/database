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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.journal.Bytes;
import com.bigdata.journal.Journal;
import com.bigdata.objndx.Addr;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * A triple store based on bigdata.
 * 
 * @todo the only added cost for a quad store is the additional statement
 *       indices. There are only three more statement indices in a quad store.
 *       Since statement indices are so cheap, it is probably worth implementing
 *       them now, even if only as a configuration option.
 * 
 * @todo verify read after commit.
 * 
 * @todo add bulk data export (buffering statements and bulk resolving term
 *       identifiers).
 * 
 * @todo The use of long[] identifiers for statements also means that the SPO
 *       and other statement indices are only locally ordered so they can not be
 *       used to perform a range scan that is ordered in the terms without
 *       joining against the various term indices.
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions.
 * 
 * @todo support metadata about the statement, e.g., whether or not it is an
 *       inference.
 * 
 * @todo The more that we pack things down the slower we can expect this to run
 *       since IOs will be smaller ... until we begin buffering writes to larger
 *       IOs for the store. The way that we compensate for this is by increasing
 *       the branching factor for the indices, but that begins to run into
 *       overhead for moving keys during inserts on a node. Deferring writes
 *       from the direct bufferQueue on the journal onto the backing file should
 *       provide a nice performance boost. The writes could even be async since
 *       we only need to sync at a commit when the writer must assert that all
 *       bytes up to a given {@link Addr} have been written onto the store.
 * 
 * @todo The term indices need to use a distinct suffix code so that the
 *       identifiers assigned by each index are unique. Or just use one
 *       heterogenous reverse index to reverse any term identifier to a term and
 *       make sure that the term indices use distinct suffix codes so that there
 *       is never a cross-index collision.
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
public class TripleStore {
    
    public Logger log = Logger.getLogger(TripleStore.class);

    public Properties properties;

    public Journal journal;

    public RdfKeyBuilder keyBuilder;
    
    public StatementIndex ndx_spo;
    public StatementIndex ndx_pos;
    public StatementIndex ndx_osp;

    public TermIndex ndx_termId;

    public ReverseIndex ndx_idTerm;

    /**
     * When true the term, termId, and statement buffers are sorted before being
     * presented to the btrees. This is much faster. This field allows you to
     * turn off this behavior so that you can access just how much faster it
     * actually is.
     * 
     * Note: Sorting helps the btree operations have good locality.
     */
    final boolean sortBuffers = true;
    
    /**
     * When true, terms and statements are added to the triple store. When
     * false, they are not. This makes it possible to run the rio parser, bufferQueue
     * terms and statements, and optionally sort them without actually inserting
     * anything into the triple store.
     * 
     * Note in order for sort to profile when loading data is disabled terms are
     * assigned one up identifiers using {@link #nextFakeTermId}.
     */
    final boolean loadData = true;

    /**
     * Used to assign fake term identifiers so that sort has something to work
     * on with buffered data when {@link #loadData} is false.
     */
    private long nextFakeTermId = 1;
    
    /**
     * 
     */
    public TripleStore(Properties properties) throws IOException {

        this.properties = (Properties) properties.clone();

        journal = new Journal(properties);

        // setup key builder that handles unicode and primitive data types.
        KeyBuilder _keyBuilder = new KeyBuilder(createCollator(), Bytes.kilobyte32 * 4);
        
        // setup key builder for RDF Values and Statements.
        keyBuilder = new RdfKeyBuilder(_keyBuilder);

        // perfect statement indices.
        ndx_spo = new StatementIndex(journal,KeyOrder.SPO);
        ndx_pos = new StatementIndex(journal,KeyOrder.POS);
        ndx_osp = new StatementIndex(journal,KeyOrder.OSP);

        ndx_termId = new TermIndex(journal, (short) 1);
        
        ndx_idTerm = new ReverseIndex(journal);

    }

    /**
     * Create and return a new collator object responsible for encoding unicode
     * strings into sort keys.
     * 
     * @return A new collator object.
     * 
     * @todo define properties for configuring the collator.
     */
    public RuleBasedCollator createCollator() {
        
        // choose a collator for the default locale.
        RuleBasedCollator collator = (RuleBasedCollator) Collator
                .getInstance(Locale.getDefault());

        /*
         * Primary uses case folding and produces smaller sort strings.
         * 
         * Secondary does not fold case.
         * 
         * Tertiary is the default.
         * 
         * Identical is also allowed.
         * 
         * @todo handle case folding - currently the indices complain, e.g., for
         * wordnet that a term already exists with a given id "Yellow Pages" vs
         * "yellow pages". Clearly the logic to fold case needs to extend
         * further if it is to work.
         */
//        collator.setStrength(Collator.PRIMARY);
//        collator.setStrength(Collator.SECONDARY);

        return collator;
        
    }
    
    public Properties getProperties() {

        return properties;

    }

    /**
     * The #of triples in the store.
     */
    public int getStatementCount() {
        
        return ndx_spo.getEntryCount();
        
    }
    
    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api).
     */
    public void addStatement(Resource s, URI p, Value o) {

        long _s = addTerm(s);
        long _p = addTerm(p);
        long _o = addTerm(o);

        ndx_spo.insert(keyBuilder.statement2Key(_s, _p, _o),null);
        ndx_pos.insert(keyBuilder.statement2Key(_p, _o, _s),null);
        ndx_osp.insert(keyBuilder.statement2Key(_p, _s, _p),null);
        
    }

    /**
     * Return true if the statement exists in the store (non-batch API).
     */
    public boolean containsStatement(Resource s, URI p, Value o) {

        long _s, _p, _o;
        
        if( (_s = getTerm(s)) == 0L ) return false;
        if( (_p = getTerm(p)) == 0L ) return false;
        if( (_o = getTerm(o)) == 0L ) return false;
        
        return ndx_spo.contains(keyBuilder.statement2Key(_s, _p, _o));
        
    }

    /**
     * Adds the statements to each index (batch api).
     * <p> 
     * Note: this is not sorting by the generated keys so the sort order may not
     *       perfectly reflect the natural order of the index. however, i
     *       suspect that it simply creates a few partitions out of the natural
     *       index order based on the difference between signed and unsigned
     *       interpretations of the termIds when logically combined into a
     *       statement identifier.
     * 
     * @param stmts
     *            An array of statements
     */
    public void addStatements(_Statement[] stmts, int numStmts) {

        if( numStmts == 0 ) return;

        long begin = System.currentTimeMillis();
//        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.

        System.err.print("Writing " + numStmts + " statements...");

        if (ndx_spo != null) {

            if (sortBuffers) {

                long _begin = System.currentTimeMillis();
                
                Arrays.sort(stmts, 0, numStmts, SPOComparator.INSTANCE);
                
                sortTime += System.currentTimeMillis() - _begin;
                
            }

            if (loadData) {

                long _begin = System.currentTimeMillis();

                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    ndx_spo.insert(keyBuilder.statement2Key(stmt.s.termId,
                            stmt.p.termId, stmt.o.termId), null);
                    
                }

                insertTime += System.currentTimeMillis() - _begin;

            }

        }

        if (ndx_pos != null) {

            if (sortBuffers) {

                long _begin = System.currentTimeMillis();

                Arrays.sort(stmts, 0, numStmts, POSComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;

            }

            if (loadData) {

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    ndx_pos.insert(keyBuilder.statement2Key(stmt.p.termId,
                            stmt.o.termId, stmt.s.termId), null);

                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }

        }

        if (ndx_osp != null) {

            if (sortBuffers) {

                long _begin = System.currentTimeMillis();

                Arrays.sort(stmts, 0, numStmts, OSPComparator.INSTANCE);
             
                sortTime += System.currentTimeMillis() - _begin;

            }

            if (loadData) {

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    ndx_osp.insert(keyBuilder.statement2Key(stmt.o.termId,
                            stmt.s.termId, stmt.p.termId), null);

                }
                
                insertTime += System.currentTimeMillis() - _begin;

            }

        }

        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("in " + elapsed + "ms; sort=" + sortTime
                + "ms, keyGen+insert=" + insertTime + "ms");
        
    }

    /**
     * Generate the sort keys for the terms.
     * 
     * @param keyBuilder
     *            The object used to generate the sort keys - <em>this is not
     *            safe for concurrent writers</em>
     * @param terms
     *            The terms whose sort keys will be generated.
     * @param numTerms
     *            The #of terms in that array.
     * 
     * @see #createCollator()
     * @see KeyBuilder
     */
    public void generateSortKeys(RdfKeyBuilder keyBuilder, _Value[] terms, int numTerms) {
        
        for (int i = 0; i < numTerms; i++) {

            _Value term = terms[i];

            if (term.key == null) {

                term.key = keyBuilder.value2Key(term);

            }

        }

    }
    
    /**
     * Batch insert of terms into the database.
     * 
     * @param terms
     *            An array whose elements [0:nterms-1] will be inserted.
     * @param numTerms
     *            The #of terms to insert.
     * @param haveKeys
     *            True if the terms already have their sort keys.
     * @param sorted
     *            True if the terms are already sorted by their sort keys (in
     *            the correct order for a batch insert).
     * 
     * @exception IllegalArgumentException
     *                if <code>!haveKeys && sorted</code>.
     */
    public void insertTerms( _Value[] terms, int numTerms, boolean haveKeys, boolean sorted ) {

        if (numTerms == 0)
            return;

        if (!haveKeys && sorted)
            throw new IllegalArgumentException("sorted requires keys");
        
        long begin = System.currentTimeMillis();
        long keyGenTime = 0; // time to convert unicode terms to byte[] sort keys.
        long sortTime = 0; // time to sort terms by assigned byte[] keys.
        long insertTime = 0; // time to insert terms into the forward and reverse index.
        
        System.err.print("Writing "+numTerms+" terms ("+terms.getClass().getSimpleName()+")...");

        if(loadData) {

            /*
             * First make sure that each term has an assigned sort key.
             */
            if(!haveKeys) {

                long _begin = System.currentTimeMillis();
                
                generateSortKeys(keyBuilder, terms, numTerms);
                
                keyGenTime = System.currentTimeMillis() - _begin;

            }
            
            /* 
             * Sort terms by their assigned sort key.  This places them into
             * the natural order for the term:id index.
             */

            if (!sorted && sortBuffers) {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            {
            
                /*
                 * Lookup the term in the term:id index. if it is there then
                 * take its termId. Otherwise, insert the term into the term:id
                 * index which gives us its termId.
                 * 
                 * @todo modify to use the btree batch api.
                 */

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numTerms; i++) {

                    _Value term = terms[i];

                    term.termId = ndx_termId.add(term.key);
                    
                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        } else {
            
            // assign fake termIds.

            for( int i=0; i<numTerms; i++) {

                terms[i].termId = nextFakeTermId++;
                
            }
            
        }
        
        if (sortBuffers) {
            
            /*
             * Sort terms based on their assigned termId.
             * 
             * FIXME The termId should be converted to a byte[8] for this sort
             * order since that is how the termId is actually encoded when we
             * insert into the termId:term (reverse) index.  We could also 
             * fake this with a comparator that compared the termIds as if they
             * were _unsigned_ long integers.
             */

            long _begin = System.currentTimeMillis();

            Arrays.sort(terms, 0, numTerms, new Comparator<_Value>() {

                public int compare(_Value term1, _Value term2) {

                    long diff = term1.termId - term2.termId;

                    if (diff < 0)
                        return -1;
                    else if (diff > 0)
                        return 1;
                    else
                        return 0;

                }

            });

            sortTime += System.currentTimeMillis() - _begin;

        }
        
        if(loadData) {
        
            /*
             * add terms to the reverse index.  this is what we use to lookup
             * the RDF value by its termId when we need to serialize some data
             * as RDF/XML or the like.
             * 
             * @todo use batch api, insert iff not found (else sanity check for
             * equivilence). pre-convert the termId:long to a byte[] using the
             * key encoder so that we can batch this.
             */
            
            long _begin = System.currentTimeMillis();
            
            for (int i = 0; i < numTerms; i++) {

                ndx_idTerm.add(keyBuilder.id2key(terms[i].termId), terms[i]);

            }

            insertTime += System.currentTimeMillis() - _begin;

        }

        long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("in " + elapsed + "ms; keygen=" + keyGenTime
                + "ms, sort=" + sortTime + "ms, insert=" + insertTime + "ms");
        
    }
    
    /**
     * Add a term into the term:id index and the id:term index, returning the
     * assigned term identifier (non-batch API).
     * 
     * @param value
     *            The term.
     * 
     * @return The assigned term identifier.
     */
    public long addTerm(Value value) {

        // @todo avoid encoding if _Value and .key is set.
        
        // forward mapping assigns identifier.
        final long termId = ndx_termId.add(keyBuilder.value2Key(value));

        // reverse mapping from identifier to term (non-batch mode).
        ndx_idTerm.add(keyBuilder.id2key(termId), value);
        
        return termId;

    }

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            The value.
     * 
     * @return The pre-assigned termId -or- 0L iff the term is not known to the
     *         database.
     */
    public long getTerm(Value value) {

        return ndx_termId.get(keyBuilder.value2Key(value));

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

        ndx_termId.write();
        ndx_idTerm.write();
        ndx_spo.write();
        ndx_pos.write();
        ndx_osp.write();
        journal.commit();

        System.err.println("#termId="+ndx_termId.getEntryCount());
        System.err.println("#idTerm="+ndx_idTerm.getEntryCount());
        System.err.println("#spo="+ndx_spo.getEntryCount());
        System.err.println("#pos="+ndx_pos.getEntryCount());
        System.err.println("#osp="+ndx_osp.getEntryCount());
        
    }

    /**
     * Load a file into the triple store.
     * @param store
     * @param file
     * @throws IOException
     */
    public void loadData(File file ) throws IOException {

        final long begin = System.currentTimeMillis();
        
        long total_stmts = 0L;
        
        Reader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file)));

        IRioLoader loader = new PresortRioLoader( this );

        loader.addRioLoaderListener( new RioLoaderListener() {
            
            public void processingNotification( RioLoaderEvent e ) {
                
                log.info
                    ( e.getStatementsProcessed() + 
                      " stmts added in " + 
                      ((double)e.getTimeElapsed()) / 1000d +
                      " secs, rate= " + 
                      e.getInsertRate() 
                      );
                
            }
            
        });
        
        try {
            
            loader.loadRdfXml( reader );
            
            long nstmts = loader.getStatementsAdded();
            
            total_stmts += nstmts;
            
            log.info( nstmts + 
                      " stmts added in " + 
                      ((double)loader.getInsertTime()) / 1000d +
                      " secs, rate= " + 
                      loader.getInsertRate() 
                      );
            
        } catch ( Exception ex ) {
            
            ex.printStackTrace();
            
        } finally {
            
            reader.close();
            
        }
        
        long elapsed = System.currentTimeMillis() - begin;

        log
                .info(total_stmts
                        + " stmts added in "
                        + ((double) elapsed)
                        / 1000d
                        + " secs, rate= "
                        + ((long) (((double) total_stmts) / ((double) elapsed) * 1000d)));

    }
    
}
