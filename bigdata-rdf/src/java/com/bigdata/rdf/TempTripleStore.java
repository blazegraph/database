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
import java.util.Locale;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IEntryIterator;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.SPO;
import com.bigdata.rdf.model.OptimizedValueFactory.OSPComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.POSComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.model.OptimizedValueFactory._ValueSortKeyComparator;
import com.bigdata.rdf.rio.IRioLoader;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.RioLoaderEvent;
import com.bigdata.rdf.rio.RioLoaderListener;
import com.bigdata.rdf.serializers.RdfValueSerializer;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.bigdata.rdf.serializers.TermIdSerializer;
import com.bigdata.scaleup.PartitionedIndex;
import com.bigdata.scaleup.PartitionedJournal;
import com.bigdata.scaleup.SlaveJournal;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * @todo Refactor to use a delegation mechanism so that we can run with or
 *       without partitioned indices? (All you have to do now is change the
 *       class that is being extended from Journal to PartitionedJournal and
 *       handle some different initialization properties.)
 * 
 * @todo Play with the branching factor again.  Now that we are using overflow
 *       to evict data onto index segments we can use a higher branching factor
 *       and simply evict more often.  Is this worth it?  We might want a lower
 *       branching factor on the journal since we can not tell how large any
 *       given write will be and then use larger branching factors on the index
 *       segments.
 * 
 * @todo try loading some very large data sets; try Transient vs Disk vs Direct
 *       modes. If Transient has significantly better performance then it
 *       indicates that we are waiting on IO so introduce AIO support in the
 *       Journal and try Disk vs Direct with aio. Otherwise, consider
 *       refactoring the btree to have the values be variable length byte[]s
 *       with serialization in the client and other tuning focused on IO (the
 *       only questions with that approach are appropriate compression
 *       techniques and handling transparently timestamps as part of the value
 *       when using an isolated btree in a transaction).
 * 
 * @todo the only added cost for a quad store is the additional statement
 *       indices. There are only three more statement indices in a quad store.
 *       Since statement indices are so cheap, it is probably worth implementing
 *       them now, even if only as a configuration option.
 * 
 * @todo verify read after commit (restart safe) for large data sets and test
 *       re-load rate for a data set and verify that no new statements are
 *       added.
 * 
 * @todo add bulk data export (buffering statements and bulk resolving term
 *       identifiers).
 * 
 * @todo The use of long[] identifiers for statements also means that the SPO
 *       and other statement indices are only locally ordered so they can not be
 *       used to perform a range scan that is ordered in the terms without
 *       joining against the various term indices and then sorting the outputs.
 * 
 * @todo possibly save frequently seen terms in each batch for the next batch in
 *       order to reduce unicode conversions.
 * 
 * @todo support metadata about the statement, e.g., whether or not it is an
 *       inference.
 * 
 * @todo compute the MB/sec rate at which the store can load data and compare it
 *       with the maximum transfer rate for the journal without the btree and
 *       the maximum transfer rate to disk. this will tell us the overhead of
 *       the btree implementation.
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
 *       bit code the repeated substrings found within the key in each leaf.
 *       This way the serialized key size reflects only the #of distinctions.
 * 
 * @todo I've been thinking about rdfs stores in the light of the work on
 *       bigdata. Transactional isolation for rdf is really quite simple. Since
 *       lexicons (uri, literal or bnode indices) do not (really) support
 *       deletion, the only acts are asserting term and asserting and retracting
 *       statements. since assertion terms can lead to write-write conflicts,
 *       which must be resolved and can cascade into the statement indices since
 *       the statement key depends directly on the assigned term identifiers. a
 *       statement always merges with an existing statement, inserts never cause
 *       conflicts. Hence the only possible write-write conflict for the
 *       statement indices is a write-delete conflict. quads do not really make
 *       this more complex (or expensive) since merges only occur when there is
 *       a context match. however entailments can cause twists depending on how
 *       they are realized.
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TempTripleStore extends TemporaryStore {
    
    static transient public Logger log = Logger.getLogger(TempTripleStore.class);

    public RdfKeyBuilder keyBuilder;

    /*
     * Note: You MUST NOT retain hard references to these indices across
     * operations since they may be discarded and re-loaded.
     */
    private IIndex ndx_spo;
    private IIndex ndx_pos;
    private IIndex ndx_osp;

    final String name_spo = "spo";
    final String name_pos = "pos";
    final String name_osp = "osp";

    /**
     * Returns and creates iff necessary a scalable restart safe index for RDF
     * {@link _Statement statements}.
     * @param name The name of the index.
     * @return The index.
     * 
     * @see #name_spo
     * @see #name_pos
     * @see #name_osp
     */
    protected IIndex getStatementIndex(String name) {

        IIndex ndx = getIndex(name);

        if (ndx == null) {

            ndx = registerIndex(name, new BTree(this,
                    BTree.DEFAULT_BRANCHING_FACTOR,
                    StatementSerializer.INSTANCE));

        }

        return ndx;

    }

    public IIndex getSPOIndex() {

        if(ndx_spo!=null) return ndx_spo;

        return ndx_spo = getStatementIndex(name_spo);
        
    }
    
    public IIndex getPOSIndex() {

        if(ndx_pos!=null) return ndx_pos;

        return ndx_pos = getStatementIndex(name_pos);

    }
    
    public IIndex getOSPIndex() {
        
        if(ndx_osp!=null) return ndx_osp;

        return ndx_osp = getStatementIndex(name_osp);

    }

    /**
     * Create or re-open a triple store.
     * 
     * @todo initialize the locale for the {@link KeyBuilder} from properties or
     *       use the default locale if none is specified. The locale should be a
     *       restart safe property since it effects the sort order of the
     *       term:id index.
     */
    public TempTripleStore() {

        super();

        // setup key builder that handles unicode and primitive data types.
        KeyBuilder _keyBuilder = new KeyBuilder(createCollator(), Bytes.kilobyte32 * 4);
        
        // setup key builder for RDF Values and Statements.
        keyBuilder = new RdfKeyBuilder(_keyBuilder);

    }

    /**
     * Create and return a new collator object responsible for encoding unicode
     * strings into sort keys.
     * 
     * @return A new collator object.
     * 
     * @todo define properties for configuring the collator.
     */
    private RuleBasedCollator createCollator() {
        
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
    
    /**
     * The #of triples in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api).
     */
    public void addStatement(long _s, long _p, long _o) {

        getSPOIndex().insert(keyBuilder.statement2Key(_s, _p, _o),null);
        getPOSIndex().insert(keyBuilder.statement2Key(_p, _o, _s),null);
        getOSPIndex().insert(keyBuilder.statement2Key(_p, _s, _p),null);
        
    }

    /**
     * Return true if the statement exists in the store (non-batch API).
     */
    public boolean containsStatement(long _s, long _p, long _o) {

        return getSPOIndex().contains(keyBuilder.statement2Key(_s, _p, _o));
        
    }

    /**
     * Writes out some usage details on System.err.
     */
    public void usage() {

        usage("spo", getSPOIndex());
        usage("pos", getPOSIndex());
        usage("osp", getOSPIndex());
        
    }
    
    public void dump() {
        
        IIndex ndx_spo = getSPOIndex();
        
        IEntryIterator it = ndx_spo.rangeIterator(null, null);
        
        while( it.hasNext() ) {
            
            it.next();
            SPO spo = new SPO(KeyOrder.SPO, keyBuilder, it.getKey());
            
            System.err.println(spo.s + ", " + spo.p + ", " + spo.o);
            
            
        }
    }

    private void usage(String name,IIndex ndx) {
        
        if (ndx instanceof BTree) {

            BTree btree = (BTree) ndx;
            
            final int nentries = btree.getEntryCount();
            final int height = btree.getHeight();
            final int nleaves = btree.getLeafCount();
            final int nnodes = btree.getNodeCount();
            final int ndistinctOnQueue = btree.getNumDistinctOnQueue();
            final int queueCapacity = btree.getHardReferenceQueueCapacity();

            System.err.println(name + ": #entries=" + nentries + ", height="
                    + height + ", #nodes=" + nnodes + ", #leaves=" + nleaves
                    + ", #(nodes+leaves)=" + (nnodes + nleaves)
                    + ", #distinctOnQueue=" + ndistinctOnQueue
                    + ", queueCapacity=" + queueCapacity);
        } else {

            // Note: this is only an estimate if the index is a view.
            final int nentries = ndx.rangeCount(null, null);

            System.err.println(name+": #entries(est)="+nentries);
            
        }
        
    }
    
}
