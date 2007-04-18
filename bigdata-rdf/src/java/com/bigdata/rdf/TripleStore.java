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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.Parser;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;
import org.openrdf.vocabulary.XmlSchema;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
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
import com.bigdata.scaleup.PartitionedIndexView;
import com.bigdata.scaleup.SlaveJournal;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * A triple store based on the <em>bigdata</em> architecture.
 * 
 * @todo verify that re-loading the same data does not cause index writes.
 * 
 * @todo Refactor to support transactions and concurrent load/query and test
 *       same.
 *       <p>
 *       Conflicts arise in the bigdata-RDF store when concurrent transactions
 *       attempt to define the same term. The problem arises because on index is
 *       used to map the term to an unique identifier and another to map the
 *       identifiers back to terms. Further, the statement indices use term
 *       identifiers directly in their keys. Therefore, resolving concurrent
 *       definition of the same term requires that we either do NOT isolate the
 *       writes on the term indices (which is probably an acceptable strategy)
 *       or that we let the application order the pass over the isolated indices
 *       and give the conflict resolver access to the {@link Tx} so that it can
 *       update the dependent indices if a conflict is discovered on the terms
 *       index.
 *       <p>
 *       The simplest approach appears to be NOT isolating the terms and ids
 *       indices. As long as the logic resides at the index, e.g., a lambda
 *       expression/method, to assign the identifier and create the entry in the
 *       ids index we can get buy with less isolation. If concurrent processes
 *       attempt to define the same term, then one or the other will wind up
 *       executing first (writes on indices are single threaded) and the result
 *       will be coherent as long as the write is committed before the ids are
 *       returned to the application. It simply does not matter which process
 *       defines the term since all that we care about is atomic, consistent,
 *       and durable. This is a case where group commit would work well (updates
 *       are blocked together on the server automatically to improve
 *       throughput).
 *       <p>
 *       Concurrent assertions of the same statement cause write-write
 *       conflicts, but they are trivially resolved -- we simply ignore the
 *       write-write conflict since both transactions agree on the statement
 *       data. Unlike the term indices, isolation is important for statements
 *       since we want to guarentee that a set of statements either is or is not
 *       asserted atomically. (With the terms index, we could care less as long
 *       as the indices are coherent.)
 *       <p>
 *       The only concern with the statement indices occurs when one transaction
 *       asserts a statement and a concurrent transaction deletes a statement. I
 *       need to go back and think this one through some more and figure out
 *       whether or not we need to abort a transaction in this case.
 * 
 * @todo bnodes do not need to be store in the terms or ids indices if we
 *       presume that an unknown identifier is a bnode. however, we still need
 *       to ensure that bnode identifiers are distinct or the same when and
 *       where appropriate, so we need to assign identifiers to bnodes in a
 *       restart-safe manner even if we "forget" the term-id mapping.
 *       
 * @todo modify the term identifier assignment mechanism to be compatible with
 *       the scale-out index partitions (32-bit unique within index partition
 *       identified plus a restart-safe counter for each index partition). 
 * 
 * @todo Refactor to use a delegation mechanism so that we can run with or
 *       without partitioned indices? (All you have to do now is change the
 *       class that is being extended from Journal to MasterJournal and handle
 *       some different initialization properties.)
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
 * @todo The use of term identifiers for statements means that the SPO and other
 *       statement indices are only locally ordered and that they can not be
 *       used to perform a range scan that is ordered in the terms without
 *       joining against the term index and/or sorting the outputs.
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
 * @todo Bit code the repeated substrings found within the key in each leaf.
 *       This way the serialized key size reflects only the #of distinctions.
 * 
 * @todo Very large bulk inserts outside of a transactional context while
 *       handling concurrent transactions. Reconcile as of the commit time of
 *       the bulk insert and you get to do that using efficient compacting
 *       sort-merges of "perfect" bulk index segments. The architecture would
 *       perform well on concurrent apstars style document loading as well as
 *       what we might normally consider a bulk load (a few hundred megabytes of
 *       data) within the normal transaction mechanisms, but if you needed to
 *       ingest uniprot you would want to use a different technique :-) outside
 *       of the normal transactional isolation mechanisms.
 *       <P>
 *       I'm not sure what the right solution is for entailments, e.g., truth
 *       maintenance vs eager closure. Either way, you would definitely want to
 *       avoid tuple at a time processing and batch things up so as to minimize
 *       the #of index tests that you had to do. So, handling entailments and
 *       efficient joins for high-level query languages would be the two places
 *       for more thought.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TripleStore extends /*Master*/Journal {
    
    /**
     * The logger for the {@link TripleStore} (shadows the logger for the
     * journal).
     */
    static transient public Logger log = Logger.getLogger(TripleStore.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /*
     * Declare indices for root addresses for the different indices maintained
     * by the store.
     */
    static public transient final int ROOT_COUNTER = ICommitRecord.FIRST_USER_ROOT;
    
    public RdfKeyBuilder keyBuilder;

    /*
     * Note: You MUST NOT retain hard references to these indices across
     * operations since they may be discarded and re-loaded.
     */
    private IIndex ndx_termId;
    private IIndex ndx_idTerm;
    private IIndex ndx_spo;
    private IIndex ndx_pos;
    private IIndex ndx_osp;
    private AutoIncCounter counter;

    final String name_termId = "termId";
    final String name_idTerm = "idTerm";

    final String name_spo = "spo";
    final String name_pos = "pos";
    final String name_osp = "osp";

    /*
     * Note: access to the indices through these methods is required to support
     * partitioned indices since the latter introduce an indirection to: (a)
     * periodically replace a mutable btree with a fused view of the btree on
     * the journal and one or more index segments; (b) periodically split or
     * join partitions of the index based on the size on disk of that partition.
     * 
     * At this time is is valid to hold onto a reference during a given load or
     * query operation but not across commits (more accurately, not across
     * overflow() events). Eventually we will have to put a client api into
     * place that hides the routing of btree api operations to the journals and
     * segments for each index partition.
     */
    public IIndex getTermIdIndex() {

        if(ndx_termId!=null) return ndx_termId;
        
        IIndex ndx = getIndex(name_termId);
        
        if(ndx==null) {
            
            ndx_termId = ndx = registerIndex(name_termId, new BTree(this,
                    BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                    TermIdSerializer.INSTANCE));
            
        }
        
        return ndx;
        
    }
    
    public IIndex getIdTermIndex() {

        if(ndx_idTerm!=null) return ndx_idTerm;
        
        IIndex ndx = getIndex(name_idTerm);

        if (ndx == null) {

            ndx_idTerm = ndx = registerIndex(name_idTerm,
                    new BTree(this, BTree.DEFAULT_BRANCHING_FACTOR,
                            UUID.randomUUID(),
                            RdfValueSerializer.INSTANCE));

        }

        return ndx;

    }

    /**
     * Returns and creates iff necessary a scalable restart safe index for RDF
     * {@link _Statement statements}.
     * 
     * @param name
     *            The name of the index.
     *
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
     * The restart-safe counter used to assign term identifiers.
     */
    public AutoIncCounter getCounter() {

        if (counter == null) {

            long addr;

            if ((addr = getRootAddr(ROOT_COUNTER)) == 0L) {

                // Note: first termId is ONE (1). Zero is reserved.
                counter = new AutoIncCounter(this, 1L);

            } else {

                counter = AutoIncCounter.read(this, addr);

            }

            setCommitter(ROOT_COUNTER, counter);

        }

        return counter;
        
    }
    
    /**
     * Create or re-open a triple store.
     * 
     * @todo initialize the locale for the {@link KeyBuilder} from properties or
     *       use the default locale if none is specified. The locale should be a
     *       restart safe property since it effects the sort order of the
     *       term:id index.
     */
    public TripleStore(Properties properties) throws IOException {

        super(properties);

        // setup key builder that handles unicode and primitive data types.
        KeyBuilder _keyBuilder = new KeyBuilder(createCollator(), Bytes.kilobyte32 * 4);
        
        // setup key builder for RDF Values and Statements.
        keyBuilder = new RdfKeyBuilder(_keyBuilder);

        // setup namespace mapping for serialization utility methods.
        addNamespace(RDF.NAMESPACE, "rdf");
        addNamespace(RDFS.NAMESPACE, "rdfs");
        addNamespace(OWL.NAMESPACE, "owl");
        addNamespace(XmlSchema.NAMESPACE, "xsd");

    }

    /**
     * Extends the default behavior to create/re-load the indices defined by the
     * {@link TripleStore} and to cache hard references to those indices.
     * 
     * FIXME setupCommitters and discardCommitters are not invoked when we are
     * using a {@link PartitionedIndexView}.  I need to refactor the apis so that
     * these methods do not appear on {@link PartitionedIndexView} or that they are
     * correctly invoked by the {@link SlaveJournal}.  I expect that they are
     * not required on {@link IJournal} but only on {@link Journal}.
     */
    public void setupCommitters() {

        super.setupCommitters();
       
        assert counter == null;

        /*
         * This is here as a work around so that this counter gets initialized
         * whether or not we are extending Journal vs MasterJournal.
         */
        getCounter();
        
    }
    
    /**
     * Extends the default behavior to discard hard references to the indices
     * defined by the {@link TripleStore}.
     */
    public void discardCommitters() {
        
        super.discardCommitters();

        counter = null;
        
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
    
    /**
     * The #of triples in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getStatementCount() {
        
        return getSPOIndex().rangeCount(null,null);
        
    }
    
    /**
     * The #of terms in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getTermCount() {
        
        return getTermIdIndex().rangeCount(null,null);
        
    }
    
    /**
     * The #of URIs in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getURICount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(RdfKeyBuilder.CODE_URI).getKey();
        byte[] toKey = keyBuilder.keyBuilder.reset().append(RdfKeyBuilder.CODE_LIT).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    /**
     * The #of Literals in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getLiteralCount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(RdfKeyBuilder.CODE_LIT).getKey();
        byte[] toKey = keyBuilder.keyBuilder.reset().append(RdfKeyBuilder.CODE_BND).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    /**
     * The #of BNodes in the store.
     * <p>
     * This may be an estimate when using partitioned indices.
     */
    public int getBNodeCount() {
        
        byte[] fromKey = keyBuilder.keyBuilder.reset().append(RdfKeyBuilder.CODE_BND).getKey();
        byte[] toKey = keyBuilder.keyBuilder.reset().append((byte)(RdfKeyBuilder.CODE_BND+1)).getKey();
        
        return getTermIdIndex().rangeCount(fromKey,toKey);
        
    }
    
    /**
     * Add a single statement by lookup and/or insert into the various indices
     * (non-batch api).
     */
    public void addStatement(Resource s, URI p, Value o) {

        long _s = addTerm(s);
        long _p = addTerm(p);
        long _o = addTerm(o);

        getSPOIndex().insert(keyBuilder.statement2Key(_s, _p, _o),null);
        getPOSIndex().insert(keyBuilder.statement2Key(_p, _o, _s),null);
        getOSPIndex().insert(keyBuilder.statement2Key(_o, _s, _p),null);
        
    }
    
    /*
     * @todo move this serialization stuff into a utility class.
     */
    
    // namespace to prefix.
    private final Map<String, String> uriToPrefix = new HashMap<String, String>();
    
    /**
     * Defines a transient mapping from a URI to a namespace prefix that will be
     * used for that URI by {@link #toString()}.
     * 
     * @param namespace
     * 
     * @param prefix
     */
    protected void addNamespace(String namespace, String prefix) {
    
        uriToPrefix.put(namespace, prefix);

    }

    /**
     * Externalizes a statement using an appreviated syntax.
     */
    public String toString( long s, long p, long o ) {
        
        IIndex ndx = getIdTermIndex();
        
        Resource s1 = (Resource) ndx.lookup(keyBuilder.id2key(s));
         
        URI p1 = (URI) ndx.lookup(keyBuilder.id2key(p));
         
        Value o1 = (Value) ndx.lookup(keyBuilder.id2key(o));
         
        return ("< " + (s1 instanceof URI ? abbrev((URI) s1) : s1) + ", "
                + abbrev(p1) + ", "
                + (o1 instanceof URI ? abbrev((URI) o1) : o1) + " >");
        
    }
    
    /**
     * Substitutes in well know namespaces (rdf, rdfs, etc).
     */
    private String abbrev( URI uri ) {
        
        String uriString = uri.getURI();
        
//        final int index = uriString.lastIndexOf('#');
//        
//        if(index==-1) return uriString;
//
//        final String namespace = uriString.substring(0, index);
        
        final String namespace = uri.getNamespace();
        
        final String prefix = uriToPrefix.get(namespace);
        
        if(prefix != null) {
            
            return prefix+":"+uri.getLocalName();
            
        } else return uriString;
        
    }

    /**
     * Utility method dumps the statements in the store onto {@link System#err}
     * using the SPO index (subject order).
     */
    public void dumpStore() {

        final int nstmts = getStatementCount();
        
        System.err.println("#statements="+nstmts);
        
        IEntryIterator itr = getSPOIndex().rangeIterator(null, null);

        int i = 0;
        
        while (itr.hasNext()) {

            itr.next();
            
            i++;
            
            SPO spo = new SPO(KeyOrder.SPO,keyBuilder,itr.getKey());

            System.err.println("#" + i + "\t" + toString(spo.s, spo.p, spo.o));
            
        }
        
    }
    
    /*
     * 
     */
    
    /**
     * Return true if the statement exists in the store (non-batch API).
     * 
     * @param s
     *            Optional subject.
     * @param p
     *            Optional predicate.
     * @param o
     *            Optional object.
     */
    public boolean containsStatement(Resource s, URI p, Value o) {

        long _s, _p, _o;

        _s = (s == null ? NULL : getTermId(s));
        _p = (p == null ? NULL : getTermId(p));
        _o = (o == null ? NULL : getTermId(o));

        /*
         * If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        if (_s == NULL && s != null)
            return false;
        if (_p == NULL && p != null)
            return false;
        if (_o == NULL && o != null)
            return false;
        
        /*
         * if all bound, then a slight optimization.
         */
        if (_s != NULL && _p != NULL && _o != NULL) {

            return getSPOIndex().contains(keyBuilder.statement2Key(_s, _p, _o));
            
        }
        
        /*
         * Choose the access path and test to see if any statements would be
         * visited for that triple pattern.
         */
        return rangeQuery(_s,_p,_o).hasNext();
        
    }

    /**
     * Return a range query iterator that will visit the statements matching the
     * triple pattern using the best access path given the triple pattern.
     * 
     * @param s
     *            An optional term identifier for the subject role or
     *            {@link #NULL}.
     * @param p
     *            An optional term identifier for the predicate role or
     *            {@link #NULL}.
     * @param o
     *            An optional term identifier for the object role or
     *            {@link #NULL}.
     * 
     * @return The range query iterator.
     * 
     * @todo write tests.
     */
    public IEntryIterator rangeQuery(long s, long p, long o) {

        if (s != NULL && p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, o);

            byte[] toKey = keyBuilder.statement2Key(s, p, o + 1);

            return getSPOIndex().rangeIterator(fromKey, toKey);

        } else if (s != NULL && p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, NULL);

            byte[] toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            return getSPOIndex().rangeIterator(fromKey, toKey);

        } else if (s != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, s, NULL);

            byte[] toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            return getOSPIndex().rangeIterator(fromKey, toKey);

        } else if (p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, o, NULL);

            byte[] toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            return getPOSIndex().rangeIterator(fromKey, toKey);

        } else if (s != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            return getSPOIndex().rangeIterator(fromKey, toKey);

        } else if (p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            return getPOSIndex().rangeIterator(fromKey, toKey);

        } else if (o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            return getOSPIndex().rangeIterator(fromKey, toKey);

        } else {

            return getSPOIndex().rangeIterator(null, null);

        }

    }
    
    /**
     * Return the #of statements matching the triple pattern using the best
     * access path given the triple pattern (the count will be approximate if
     * partitioned indices are being used).
     * 
     * @param s
     *            An optional term identifier for the subject role or
     *            {@link #NULL}.
     * @param p
     *            An optional term identifier for the predicate role or
     *            {@link #NULL}.
     * @param o
     *            An optional term identifier for the object role or
     *            {@link #NULL}.
     * 
     * @return The range count.
     * 
     * @todo write tests.
     */
    public int rangeCount(long s, long p, long o) {

        if (s != NULL && p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, o);

            byte[] toKey = keyBuilder.statement2Key(s, p, o + 1);

            return getSPOIndex().rangeCount(fromKey, toKey);

        } else if (s != NULL && p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, p, NULL);

            byte[] toKey = keyBuilder.statement2Key(s, p + 1, NULL);

            return getSPOIndex().rangeCount(fromKey, toKey);

        } else if (s != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, s, NULL);

            byte[] toKey = keyBuilder.statement2Key(o, s + 1, NULL);

            return getOSPIndex().rangeCount(fromKey, toKey);

        } else if (p != NULL && o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, o, NULL);

            byte[] toKey = keyBuilder.statement2Key(p, o + 1, NULL);

            return getPOSIndex().rangeCount(fromKey, toKey);

        } else if (s != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(s, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(s + 1, NULL, NULL);

            return getSPOIndex().rangeCount(fromKey, toKey);

        } else if (p != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(p, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(p + 1, NULL, NULL);

            return getPOSIndex().rangeCount(fromKey, toKey);

        } else if (o != NULL) {

            byte[] fromKey = keyBuilder.statement2Key(o, NULL, NULL);

            byte[] toKey = keyBuilder.statement2Key(o + 1, NULL, NULL);

            return getOSPIndex().rangeCount(fromKey, toKey);

        } else {

            return getSPOIndex().rangeCount(null, null);

        }

    }
    
    /**
     * Removes statements matching the triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     * 
     * @return The #of statements removed.
     * 
     * @todo write tests.
     */
    public int removeStatements(Resource s,URI p,Value o) {

        /*
         * convert our object types to internal identifiers.
         */
        long _s, _p, _o;

        _s = (s == null ? NULL : getTermId(s));
        _p = (p == null ? NULL : getTermId(p));
        _o = (o == null ? NULL : getTermId(o));

        /*
         * If a value was specified and it is not in the terms index then the
         * statement can not exist in the KB.
         */
        if (_s == NULL && s != null) {

            return 0;
            
        }
        
        if (_p == NULL && p != null) {
        
            return 0;
            
        }
        
        if (_o == NULL && o != null) {
            
            return 0;
            
        }
    
        return removeStatements(_s,_p,_o);
        
    }
    
    /**
     * Remove statements matching the triple pattern.
     * <p>
     * Since the indices do not support modification with concurrent traversal
     * the statements are materialized before they are deleted.
     * 
     * @param _s
     * @param _p
     * @param _o
     * 
     * @return The #of statements removed.
     * 
     * @todo the {@link #keyBuilder} is being used, which means that this is NOT
     *       thread safe.
     * 
     * @todo this is not using the batch btree api.
     * 
     * @todo write tests.
     */
    public int removeStatements(long _s, long _p, long _o) {
    
        /*
         * if all bound, then a slight optimization.
         */
        if (_s != NULL && _p != NULL && _o != NULL) {

            byte[] key = keyBuilder.statement2Key(_s, _p, _o);

            if (getSPOIndex().contains(key)) {

                getSPOIndex().remove(key);
                getPOSIndex().remove(keyBuilder.statement2Key(_p, _o, _s));
                getOSPIndex().remove(keyBuilder.statement2Key(_o, _s, _p));
                
                return 1;
                
            } else {
                
                return 0;
                
            }
            
        }
        
        /*
         * Choose the access path, count the #of statements that match the
         * triple pattern, and the materalize those statements (since traversal
         * with concurrent modification is not supported).
         */
        
        KeyOrder keyOrder = KeyOrder.getKeyOrder(_s, _p, _o);
        
        // #of matching statements.
        int rangeCount = rangeCount(_s, _p, _o);
        
        SPO[] stmts = new SPO[rangeCount];

        // materialize the matching statements.
        {
            IEntryIterator itr1 = rangeQuery(_s, _p, _o);

            int i = 0;

            while (itr1.hasNext()) {

                itr1.next();

                stmts[i++] = new SPO(keyOrder, keyBuilder, itr1.getKey());

            }

            assert i == rangeCount;
        }

        /*
         * Remove the statements from each of the access paths.
         */
        {

            {
                IIndex ndx = getSPOIndex();

                // Place statements in SPO order.
                Arrays.sort(stmts, com.bigdata.rdf.inf.SPOComparator.INSTANCE);

                // remove statements from SPO index.
                for (int i = 0; i < stmts.length; i++) {

                    SPO spo = stmts[i];

                    ndx.remove(keyBuilder.statement2Key(spo.s, spo.p, spo.o));

                }
            }

            {

                IIndex ndx = getPOSIndex();
                // Place statements in POS order.
                Arrays.sort(stmts, com.bigdata.rdf.inf.POSComparator.INSTANCE);

                // Remove statements from POS index.
                for (int i = 0; i < stmts.length; i++) {

                    SPO spo = stmts[i];

                    ndx.remove(keyBuilder.statement2Key(spo.p, spo.o, spo.s));

                }
                
            }

            {

                IIndex ndx = getOSPIndex();

                // Place statements in OSP order.
                Arrays.sort(stmts, com.bigdata.rdf.inf.OSPComparator.INSTANCE);

                // Remove statements from OSP index.
                for (int i = 0; i < stmts.length; i++) {

                    SPO spo = stmts[i];

                    ndx.remove(keyBuilder.statement2Key(spo.o, spo.s, spo.p));

                }

            }
            
        }

        return rangeCount;

    }

    /**
     * Value used for a "NULL" term identifier.
     * 
     * @todo use this throughout rather than "0" since the value should really
     *       be an <em>unsigned long</em>).
     */
    public static final long NULL = 0L;

    /**
     * Adds the statements to each index (batch api).
     * <p>
     * Note: this is not sorting by the generated keys so the sort order may not
     * perfectly reflect the natural order of the index. however, i suspect that
     * it simply creates a few partitions out of the natural index order based
     * on the difference between signed and unsigned interpretations of the
     * termIds when logically combined into a statement identifier.
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
        
        final long elapsed_SPO;
        final long elapsed_POS;
        final long elapsed_OSP;

        System.err.print("Writing " + numStmts + " statements...");

        { // SPO

            final long beginIndex = System.currentTimeMillis();
            
            IIndex ndx_spo = getSPOIndex();

            { // sort

                long _begin = System.currentTimeMillis();
                
                Arrays.sort(stmts, 0, numStmts, SPOComparator.INSTANCE);
                
                sortTime += System.currentTimeMillis() - _begin;
                
            }
            
            { // load

                long _begin = System.currentTimeMillis();

                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    ndx_spo.insert(keyBuilder.statement2Key(stmt.s.termId, stmt.p.termId,
                            stmt.o.termId), null);
                    
                }

                insertTime += System.currentTimeMillis() - _begin;

            }

            elapsed_SPO = System.currentTimeMillis() - beginIndex;
            
        }

        { // POS

            final long beginIndex = System.currentTimeMillis();
            
            IIndex ndx_pos = getPOSIndex();

            { // sort

                long _begin = System.currentTimeMillis();

                Arrays.sort(stmts, 0, numStmts, POSComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;

            }

            { // load

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];
                    
                    ndx_pos.insert(keyBuilder.statement2Key(stmt.p.termId,
                            stmt.o.termId, stmt.s.termId), null);

                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }

            elapsed_POS = System.currentTimeMillis() - beginIndex;

        }

        { // OSP

            final long beginIndex = System.currentTimeMillis();
            
            IIndex ndx_osp = getOSPIndex();

            { // sort

                long _begin = System.currentTimeMillis();

                Arrays.sort(stmts, 0, numStmts, OSPComparator.INSTANCE);
             
                sortTime += System.currentTimeMillis() - _begin;

            }

            { // load

                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numStmts; i++) {

                    final _Statement stmt = stmts[i];

                    ndx_osp.insert(keyBuilder.statement2Key(stmt.o.termId, stmt.s.termId,
                            stmt.p.termId), null);

                }
                
                insertTime += System.currentTimeMillis() - _begin;

            }

            elapsed_OSP = System.currentTimeMillis() - beginIndex;

        }

        long elapsed = System.currentTimeMillis() - begin;

        System.err.println("in " + elapsed + "ms; sort=" + sortTime
                + "ms, keyGen+insert=" + insertTime + "ms; spo=" + elapsed_SPO
                + "ms, pos=" + elapsed_POS + "ms, osp=" + elapsed_OSP + "ms");
        
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

        {

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

            if (!sorted ) {
            
                long _begin = System.currentTimeMillis();
                
                Arrays.sort(terms, 0, numTerms, _ValueSortKeyComparator.INSTANCE);

                sortTime += System.currentTimeMillis() - _begin;
                
            }

            {
            
                /*
                 * Lookup the term in the term:id index. If it is there then
                 * take its termId and mark it as 'known' so that we can avoid
                 * testing the reverse index. Otherwise, insert the term into
                 * the term:id index which gives us its termId.
                 * 
                 * @todo use batch api?
                 */

                IIndex termId = getTermIdIndex();
                AutoIncCounter counter = getCounter();
                
                long _begin = System.currentTimeMillis();
                
                for (int i = 0; i < numTerms; i++) {

                    _Value term = terms[i];

                    if (!term.known) {

                        //assert term.termId==0L; FIXME uncomment this and figure out why this assertion is failing.
                        assert term.key != null;

                        // Lookup in the forward index.
                        Long tmp = (Long)termId.lookup(term.key);
                        
                        if(tmp == null) { // not found.

                            // assign termId.
                            term.termId = counter.nextId();
                        
                            // insert into index.
                            if(termId.insert(term.key, Long.valueOf(term.termId))!=null) {
                                
                                throw new AssertionError();
                                
                            }
                            
                        } else { // found.
                        
                            term.termId = tmp.longValue();
                            
                            term.known = true;
                        
                        }
                        
                    } else assert term.termId != 0L;
                    
                }

                insertTime += System.currentTimeMillis() - _begin;
                
            }
            
        }
        
        {
            
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

            Arrays.sort(terms, 0, numTerms, TermIdComparator.INSTANCE);

            sortTime += System.currentTimeMillis() - _begin;

        }
        
        {
        
            /*
             * Add unknown terms to the reverse index, which is the index that
             * we use to lookup the RDF value by its termId to serialize some
             * data as RDF/XML or the like.
             * 
             * Note: We only insert terms that were reported as "not found" when
             * we inserted them into the forward mapping. This reduces the #of
             * index tests that we perform.
             * 
             * @todo use batch api?
             */
            
            IIndex idTerm = getIdTermIndex();
            
            long _begin = System.currentTimeMillis();
            
            for (int i = 0; i < numTerms; i++) {

                _Value term = terms[i];
                
                assert term.termId != 0L;
                
                if (!term.known) {
                    
                    final byte[] idKey = keyBuilder.id2key(term.termId);

                    if(idTerm.insert(idKey, term) != null) {

                        // term was already in this index.
                        
                        throw new AssertionError();
                        
                    }
                    
                    term.known = true; // now in the fwd and rev indices.
                    
                }

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

        final _Value val = (_Value) value;
        
        if(val.known) {
            
            assert val.termId !=0L;
            
            return val.termId;

        }

        /*
         * The forward mapping assigns the identifier.
         * 
         * Note: this code tests for existance based on the foward mapping so
         * that we can avoid the use of the reverse mapping if we know that the
         * term exists.
         */

        final IIndex termId = getTermIdIndex();
        final IIndex idTerm = getIdTermIndex();

        // formulate key from the RDF value.
        final byte[] termKey = keyBuilder.value2Key(val);

        // test the forward index.
        Long tmp = (Long) termId.lookup(termKey);

        if (tmp != null) {

            /*
             * the term was found on the forward lookup, so we are done.
             */

            val.termId = tmp.longValue();
            
        } else {
            
            /*
             * the term was not in the database.
             */

            // assign the next term identifier from the persistent counter.
            val.termId = getCounter().nextId();
            
            // insert into the forward mapping.
            if(termId.insert(termKey, Long.valueOf(val.termId))!=null) throw new AssertionError();

            /*
             * Insert into the reverse mapping from identifier to term.
             */

            final byte[] idKey = keyBuilder.id2key(val.termId);

            if (idTerm.insert(idKey, val) != null) {
    
                throw new AssertionError();
                        
            }

        }

        val.known = true;
        
        return val.termId;

    }

    /**
     * Return the RDF {@link Value} given a term identifier (non-batch api).
     *
     * @return the RDF value or <code>null</code> if there is no term with that
     * identifier in the index. 
     */
    public _Value getTerm(long id) {

        return (_Value)getIdTermIndex().lookup(keyBuilder.id2key(id));

    }

    /**
     * Return the pre-assigned termId for the value (non-batch API).
     * 
     * @param value
     *            The value.
     * 
     * @return The pre-assigned termId -or- 0L iff the term is not known to the
     *         database.
     * 
     * @todo cache some well-known values?  E.g., this defined by the InferenceEngine.
     */
    public long getTermId(Value value) {

        _Value val = (_Value) value;
        
        if( val.termId != 0l ) return val.termId; 

        Long id = (Long)getTermIdIndex().lookup(keyBuilder.value2Key(value));
        
        if( id == null ) return 0L;

        val.termId = id.longValue();

        return val.termId;

    }
    
    public long commit() {

        final long begin = System.currentTimeMillis();

        final long commitTime = super.commit();

        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("commit: commit latency="+elapsed+"ms");

        usage();
        
        return commitTime;
        
    }

//    /**
//     * FIXME The logic here should be triggered IFF the journal actually
//     * overflows (that is, iff a new journal is opened and the old journal
//     * becomes read-only).
//     * <p>
//     */
//    public boolean overflow() {
//
//        // invoke the base behavior on the super class.
//        boolean ret = super.overflow();
//        
//        return ret;
//        
//    }

    /**
     * @todo The commit is required to make the new counter restart safe by
     *       placing an address for it into its root slot.
     *       <p>
     *       rather than having multiple commits during overflow, we should
     *       create a method to which control is handed before and after the
     *       overflow event processing in the base class which provides the
     *       opportunity for such maintenance events. we could then just setup
     *       the new counter and let the overflow handle the commit.
     * 
     */
    protected Object willOverflow() {
        
        // the current state.
        final long counter = getCounter().getCounter();
        
        // clear hard references to named indices.
        ndx_termId = null;
        ndx_idTerm = null;
        ndx_spo = null;
        ndx_pos = null;
        ndx_osp = null;
        this.counter = null;

        return counter;

    }

    protected void didOverflow(Object state) {
        
        final long counter = (Long)state;
        
        // create a new counter that will be persisted on the new slave journal.
        this.counter = new AutoIncCounter(this,counter);
        
        // setup the counter as a committer on the new slave journal.
        setCommitter( ROOT_COUNTER, this.counter );
        
        commit();
        
    }
    
    /**
     * Writes out some usage details on System.err.
     */
    public void usage() {

        usage("termId", getTermIdIndex());
        usage("idTerm", getIdTermIndex());
        usage("spo", getSPOIndex());
        usage("pos", getPOSIndex());
        usage("osp", getOSPIndex());
        
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

    /**
     * Used to report statistics when loading data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class LoadStats {

        public long toldTriples;
        public long totalTime;
        public long loadTime;
        public long commitTime;
        
        public long triplesPerSecond() {
            
            return ((long)( ((double)toldTriples) / ((double)totalTime) * 1000d ));
            
        }
        
        /**
         * Human readable representation.
         */
        public String toString() {

            return toldTriples+" stmts added in " + 
                    ((double)loadTime) / 1000d +
                    " secs, rate= " + 
                    triplesPerSecond()+
                    ", commitLatency="+
                    commitTime+"ms" 
                    ;

        }
        
    }
    
//  /**
//   * Load a file into the triple store.
//   *
//   * @param file The file.
//   * 
//   * @param baseURI The baseURI or "" if none is known.
//   * 
//   * @throws IOException
//   */
//  public void loadData(File file, String baseURI ) throws IOException {
//      
//      loadData(file,baseURI,true);
//      
//  }

    /**
     * Load data into the triple store.
     * 
     * @param file
     *            The file.
     * @param baseURI
     *            The baseURI or <code>""</code> if none is known.
     * @param rdfFormat
     *            The RDF interchange syntax to be parsed.
     * @param verifyData
     *            Controls the {@link Parser#setVerifyData(boolean)} option.
     * @param commit
     *            A {@link #commit()} will be performed IFF true.
     * 
     * @return Statistics about the data load operation.
     * 
     * @throws IOException
     *             if there is a problem when parsing the data.
     */
    public LoadStats loadData(File file, String baseURI, RDFFormat rdfFormat,
            boolean verifyData, boolean commit) throws IOException {

        final long begin = System.currentTimeMillis();
        
        LoadStats stats = new LoadStats();
        
        log.debug( "loading: " + file.getAbsolutePath() );
        
        IRioLoader loader = new PresortRioLoader( this, rdfFormat, verifyData );

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
        
        /*
         * @todo change to use correct Parser method depending on Reader vs
         * InputStream (SAX Source).  Changing this means updating all of
         * the parser implementations, not just the PresortRioLoader.
         */
        Reader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file)));
        
        try {
            
            loader.loadRdf( reader, baseURI );
            
            long nstmts = loader.getStatementsAdded();
            
            stats.toldTriples += nstmts;
            
            stats.loadTime = System.currentTimeMillis() - begin;
            
            // commit the data.
            if(commit) {
                
                long beginCommit = System.currentTimeMillis();
                
                commit();

                stats.commitTime = System.currentTimeMillis() - beginCommit;

                log.info("commit: latency="+stats.commitTime+"ms");
                
            }
            
            stats.totalTime = System.currentTimeMillis() - begin;
            
            log.info( nstmts + 
                    " stmts added in " + 
                    ((double)loader.getInsertTime()) / 1000d +
                    " secs, rate= " + 
                    loader.getInsertRate()
                    );

            return stats;
            
        } catch ( Exception ex ) {
            
            throw new RuntimeException("While loading: "+file, ex);
            
        } finally {
            
            reader.close();
            
        }
        
//        long elapsed = System.currentTimeMillis() - begin;
//
//        log
//                .info(total_stmts
//                        + " stmts added in "
//                        + ((double) elapsed)
//                        / 1000d
//                        + " secs, rate= "
//                        + ((long) (((double) total_stmts) / ((double) elapsed) * 1000d)));

    }
    
}
