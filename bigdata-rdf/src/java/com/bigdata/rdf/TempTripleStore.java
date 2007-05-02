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

import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.OSPComparator;
import com.bigdata.rdf.inf.POSComparator;
import com.bigdata.rdf.inf.SPO;
import com.bigdata.rdf.inf.SPOComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Statement;
import com.bigdata.rdf.serializers.StatementSerializer;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * A temporary triple store based on the <em>bigdata</em> architecture.  Data
 * is buffered in memory but will overflow to disk for large stores.
 * 
 * @todo refactor to use a delegate pattern so that we can share code with
 *       {@link TripleStore} while deriving from a different base class.
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
                    UUID.randomUUID(),
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
     * @todo initialize the locale for the {@link UnicodeKeyBuilder} from properties or
     *       use the default locale if none is specified. The locale should be a
     *       restart safe property since it effects the sort order of the
     *       term:id index.
     */
    public TempTripleStore() {

        super();

        // setup key builder that handles unicode and primitive data types.
        IKeyBuilder _keyBuilder = new UnicodeKeyBuilder(createCollator(), Bytes.kilobyte32 * 4);
        
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
     * Copies the entailments from the array into the {@link TempTripleStore}.
     * 
     * @param stmts
     *            The source statements.
     * 
     * @param n
     *            The #of statements in the buffer.
     */
    public void addStatements(SPO[] stmts, int n ) {
        
        // deal with the SPO index
        IIndex spo = getSPOIndex();
        Arrays.sort(stmts,0,n,SPOComparator.INSTANCE);
        for ( int i = 0; i < n; i++ ) {
            byte[] key = keyBuilder.statement2Key
                ( stmts[i].s, stmts[i].p, stmts[i].o
                  );
            if ( !spo.contains(key) ) {
                spo.insert(key, null);
            }
        }

        // deal with the POS index
        IIndex pos = getPOSIndex();
        Arrays.sort(stmts,0,n,POSComparator.INSTANCE);
        for ( int i = 0; i < n; i++ ) {
            byte[] key = keyBuilder.statement2Key
                ( stmts[i].p, stmts[i].o, stmts[i].s
                  );
            if ( !pos.contains(key) ) {
                pos.insert(key, null);
            }
        }

        // deal with the OSP index
        IIndex osp = getOSPIndex();
        Arrays.sort(stmts,0,n,OSPComparator.INSTANCE);
        for ( int i = 0; i < n; i++ ) {
            byte[] key = keyBuilder.statement2Key
                ( stmts[i].o, stmts[i].s, stmts[i].p
                  );
            if ( !osp.contains(key) ) {
                osp.insert(key, null);
            }
        }
        
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
