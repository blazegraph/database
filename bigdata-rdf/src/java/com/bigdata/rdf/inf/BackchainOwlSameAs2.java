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
/*
 * Created on Nov 9, 2007
 */

package com.bigdata.rdf.inf;

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.NOPSerializer;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.EmptySPOIterator;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Backchains {@link RuleOwlSameAs2} given triple pattern (s p o).
 * <p>
 * Approach:
 * 
 * <p>
 * Stage 1: we deliver all results from the source iterator collecting up the
 * set of distinct subjects in a {@link BTree} backed by a
 * {@link TemporaryRawStore} and updating a {@link BloomFilter} whose key is the
 * statement's fully bound term identifiers in SPO order.
 * <p>
 * Stage 2: we do an ordered read on the {@link BTree}. for each distinct
 * subject <code>s</code>, we issue a query <code>(s owl:sameAs y)</code>,
 * collecting up the set <code>Y</code>, where <code>y != s</code> of
 * resources that are the same as that subject. (We do not actually need to
 * collect that set - its members can be visited incrementally.)
 * <p>
 * Stage 3: For each <code>y</code> in <code>Y</code> we obtain an iterator
 * reading on <code>(y p o)</code> where <code>p</code> and <code>o</code>
 * are the predicate and object specified to the constructor and MAY be unbound.
 * For each statement visited by that iterator we test the bloom filter. If it
 * reports NO then this statement does NOT duplicate a statement visited in
 * Stage 1 and we deliver it via {@link #next()}. If it reports YES then this
 * statement MAY duplicate a statement delivered in Stage 1 and we add it into
 * an {@link ISPOBuffer}.
 * <p>
 * When the {@link ISPOBuffer} overflows we sort it and perform an ordered
 * existence test on the database. If the statement is found in the database
 * then it is discarded from the buffer. The remaining statements in the buffer
 * are then delivered via {@link #next()}.
 * <p>
 * Note: This assumes that backchaining can only discover duplicates of
 * statements delivered by the source iterator and NOT duplicates of statements
 * discovered by backchaining itself. If this is NOT true then you need to add
 * each backchained statement to the {@link BloomFilter} as well and we will
 * also need to explicitly track every single statement that we backchain so
 * that we can filter duplicates.
 * <p>
 * Note: This assumes that the reflexive ({@link RuleOwlSameAs1}) and
 * transitive ({@link RuleOwlSameAs1b}) closure of <code>owl:sameAs</code>
 * is maintained on the database.
 * 
 * @todo are there simplifications if <code>s</code> is bound that are worth
 *       pursuing?
 * 
 * @todo an alterative to all this logic is to dump everything into an SPO index
 *       on a temporary store and then stream the results from that index once
 *       the source iterator and the entailments are exhausted.
 * 
 * @todo write high-level test reading triple patterns from a database using
 *       this and other backchaining iterators and verify that each chunk is
 *       correctly sorted. it is not critical that the sequences of chunks are
 *       fully ordered, but the chunks themselves should be fully ordered (and
 *       should not contain any duplicates).
 * 
 * @todo decide how to nest (x type resource) and owl:sameAs backward chainers.
 *       if there is an explicit (x type resource) and (y owl:sameAs x) in the
 *       kb then running the (x type resource) backward chainer over the
 *       owl:sameAs backward chainer could fool the former based on the tests
 *       that it performs against the kb for duplicates.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BackchainOwlSameAs2 implements ISPOIterator {

    protected static final Logger log = Logger.getLogger(BackchainOwlSameAs2.class);
    
    private final ISPOIterator src;
    private final long p, o;
    private final AbstractTripleStore db;
    private final long owlSameAs;
    private final KeyOrder keyOrder;
    
    /**
     * Used to minimize index tests required in order to avoid duplicates in the
     * visited {@link SPO}s.
     */
    private BloomFilter bloomFilter;

//    private ISPOBuffer buffer;
    
    /**
     * Initally <code>false</code> and set to <code>true</code> when the
     * {@link #src} iterator runs dry.
     */
    private boolean sourceExhausted = false;
    
    private boolean open = true;

    private final TemporaryRawStore tempStore = new TemporaryRawStore();
    
    /**
     * Keys are Long integers corresponding to the subject identifer for
     * statements visted by the source iterator. Values are not used.
     */
    private final BTree subjects = new BTree(tempStore,
            BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
            NOPSerializer.INSTANCE);

    /**
     * Used to build keys for the {@link #subjects} index.
     */
    private final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
    
    /**
     * Allocated once we start to scan the {@link #subjects}.
     */
    private IEntryIterator subjectIterator = null;

    /**
     * Initially <code>false</code> and set to <code>true</code> when the
     * {@link #subjectIterator} runs dry.
     */
    private boolean subjectsExhausted = false;

    /**
     * Note: We do not save the original subject. Instead we bind this.s to each
     * distinct subject that we visit during backchaining. If s was originally
     * bound then there will be only a single distinct subject, but that is a
     * special case.
     */
    private long s;
    
    /**
     * Iterator initialized for each distinct subject using the triple pattern
     * <code>(s owl:sameAs ?)</code> - the bindings on the object position are
     * the distinct values in the set <code>Y</code> of resources that are the
     * same as the subject for which the iterator was constructed.
     * <p>
     * Note: This reference is cleared to <code>null</code> each time the
     * iterator is exhausted.
     */
    private ISPOIterator sameAsSubjectIterator;
    
    /**
     * An iterator reading property-values that apply to the current subject ({@link #s})
     * by virtue of owl:sameAs.
     * <p>
     * Note: When we deliver these entailments we need to bind the subject as
     * {@link #s} NOT as the value used to create the access path for this
     * iterator.
     */
    private ISPOIterator propertyValueIterator;

    /**
     * This is set each time by {@link #nextChunk()} and inspected by
     * {@link #nextChunk(KeyOrder)} in order to decide whether the chunk needs
     * to be sorted.
     */
    private KeyOrder chunkKeyOrder = null; 

    /**
     * The next SPO to be returned by {@link #next()}.
     */
    private SPO next = null;
    
    /**
     * The last {@link SPO} visited by {@link #next()}.
     */
    private SPO current = null;

    /**
     * Obtain an iterator that will backchain {@link RuleOwlSameAs2}.
     * 
     * @param src
     * @param s
     * @param p
     * @param o
     * @param db
     * @param owlSameAs
     * @return
     * 
     * @todo what other special cases are there?
     */
    public static ISPOIterator newIterator(ISPOIterator src, long s, long p, long o,
            AbstractTripleStore db, final long owlSameAs) {
        
        if(p == owlSameAs) {
            
            return new EmptySPOIterator(src.getKeyOrder());
            
        }

        return new BackchainOwlSameAs2(src,s,p,o,db,owlSameAs);
        
    }
    
    /**
     * Constructor used when the predicate is NOT <code>owl:sameAs</code>
     * 
     * @param src
     * @param s
     * @param p
     * @param o
     * @param db
     * @param owlSameAs
     */
    private BackchainOwlSameAs2(ISPOIterator src, long s, long p, long o,
            AbstractTripleStore db, final long owlSameAs) {

        if (src == null)
            throw new IllegalArgumentException();
        
        if (db == null)
            throw new IllegalArgumentException();
        
        this.src = src;

        /*
         * Note: We do not save the original subject. Instead we bind this.s to
         * each distinct subject that we visit during backchaining. If s was
         * originally bound then there will be only a single distinct subject,
         * but that is a special case.
         */
        
        this.p = p;
        
        this.o = o;
        
        this.db = db;
        
        this.owlSameAs = owlSameAs;

        this.keyOrder = src.getKeyOrder(); // MAY be null.
        
        /*
         * Estimate the #of elements that will be visited by the iterator.
         */
        int rangeCount = db.getAccessPath(s,p,o).rangeCount();
        
        /*
         * Allocate the bloomFilter filter to be high accuracy based on that estimate.
         */
        this.bloomFilter = new BloomFilter(rangeCount);
        
        log.info("bloomFilter: #of expected elements=" + rangeCount
                + ", #of hash functions=" + bloomFilter.d);

//        /*
//         * Allocate the SPOBuffer to be no larger than that estimate.
//         */
//        {
//
//            final int capacity = Math.min(rangeCount,10000);
//            
//            this.buffer = new AbstractSPOBuffer(db,null/*filter*/,capacity) {
//
//                public int flush() {
//                    
//                    throw new UnsupportedOperationException();
//
//                }
//                
//            };
//            
//        }        
        
    }

    public KeyOrder getKeyOrder() {
        
        return keyOrder;
        
    }

    public boolean hasNext() {

        if(!open) return false;
        
        if(!sourceExhausted) {
            
            if(src.hasNext()) {
                
                return true;
                
            }
            
            sourceExhausted = true;
            
            // first time allocation for the subjects iterator.

            log.info("Starting read on subjects: size="
                        + subjects.getEntryCount());

            subjectIterator = subjects.rangeIterator(null, null);

            if(!nextSubject()) {
                
                return false;
                
            }
            
        }

        // still reading subjects.
        while (!subjectsExhausted) {

            // @todo close propertyValueIterator.
            // @todo close sameAsSubjectIterator.
            
            // another property-value for the current subject [this.s].
            if (propertyValueIterator != null && propertyValueIterator.hasNext()) {

                /*
                 * A property-value is something that we can return directly to
                 * the caller so return true - we know that we can return
                 * another statement.
                 */

                SPO tmp = propertyValueIterator.next();
                
                next = new SPO(s,tmp.p,tmp.o,StatementEnum.Inferred);
                
                if(!testBloomFilter(tmp)) {
                    
                    /*
                     * If the bloom filter says that it has never seen this
                     * statement then we can believe it. In this case we
                     * definately have a statement that was not in the source
                     * iterator and we can return it to the caller.
                     */
                    
                    return true;
                    
                }
                
                /*
                 * Otherwise the statement MAY have been delivered.
                 * 
                 * Note: This does an index test to decide whether or not the
                 * statement is actually in the database and hence would have
                 * been delivered by the source iterator. This assumes that the
                 * reflexive and transitive closure of owl:sameAs is maintained
                 * on the database!
                 * 
                 * @todo Rather than do an immediate index test to decide
                 * whether or not the statement is in the index buffer the
                 * statement and process the buffer when it overflows. We can
                 * then sort the buffer and do an ordered contains() test on the
                 * SPO index.
                 */
                
                if(!db.hasStatement(tmp.s, tmp.p, tmp.o)) {

                    /*
                     * Since the statement is not in the database we know that
                     * it was not delivered by the source iterator.
                     */
                    
                    return true;
                    
                }
                
                /*
                 * Skip this entailment - the source iterator will have
                 * already delivered it.
                 */
                
                continue;
                
            }
            
            // reading resources sameAs the current subject.
            while (sameAsSubjectIterator != null && sameAsSubjectIterator.hasNext()) {

                if(!nextSubject()) return false;
                
                if (!sameAsSubjectIterator.hasNext()) {
                 
                    sameAsSubjectIterator.close();

                    break;
                    
                }

                /*
                 * This is a resource that is the same as the current subject.
                 */
                long o = sameAsSubjectIterator.next().o;

                /*
                 * An iterator reading properties that apply to the current
                 * subject by virtue of owl:sameAs.
                 * 
                 * Note: When we deliver these entailments we need to bind the
                 * subject as [this.s] NOT as the value used to create the
                 * access path for this iterator.
                 */
                propertyValueIterator = db.getAccessPath(o, p, this.o)
                        .iterator();

                // fall through
                
            }

            // fall through

        }
                
        return false;
        
    }

    // next subject please!
    private boolean nextSubject() {

        while (true) {

            if (!subjectIterator.hasNext()) {

                // no more subjects to be read.

                subjectsExhausted = true;

                log.info("Subjects exhausted");

                return false;

            } else {

                // next subject please.
                subjectIterator.next();

                // get the key and decode it to the subject term identifier.
                byte[] key = subjectIterator.getKey();

                // bind the current subject
                this.s = KeyBuilder.decodeLong(key, 0);

                while (true) {

                    // create iterator reading on (s owl:sameAs ?).
                    sameAsSubjectIterator = db
                            .getAccessPath(s, owlSameAs, NULL).iterator();

                    if (!sameAsSubjectIterator.hasNext()) {

                        sameAsSubjectIterator.close();

                    }

                    log.info("Starting read on things sameAs " + s);

                    return true;

                }

            }

        }

    }
    
    public SPO next() {
        
        if(!hasNext()) {
            
            throw new NoSuchElementException();
            
        }

        if(!sourceExhausted) {
            
            SPO spo = src.next();
            
            noticeSourceStatement(spo);
            
            return spo;
            
        }

        /*
         * [next] is set by one step lookhead in hasNext().
         */

        assert next != null;
        
        current = next;
        
        return next;
        
    }

    /**
     * Adds the statement to the bloomFilter filter and to the set of subjects that we
     * will have to process during backward chaining.
     * 
     * @param spo
     *            The statement.
     */
    private void noticeSourceStatement(SPO spo) {
        
        // add to the subjects index.
        subjects.insert(keyBuilder.reset().append(spo.s).getKey(),null);
        
        // add to the bloomFilter filter.
        bloomFilter.add(new long[] { spo.s, spo.p, spo.o });
        
    }

    /**
     * Report the answer from the {@link #bloomFilter} for the given statement.
     * <p>
     * Note: A <code>false</code> return is certain - the statement was never
     * added to the bloom filter.
     * <p>
     * Note: A <code>true</code> return could be a false position (the
     * statement MIGHT NOT have added to the bloom filter).
     * 
     * @param spo
     *            The statement.
     *            
     * @return The response from the bloom filter.
     */
    private boolean testBloomFilter(SPO spo) {
        
        return bloomFilter.contains(new long[] { spo.s, spo.p, spo.o });
        
    }
    
    /**
     * Note: This method preserves the {@link KeyOrder} of the source iterator
     * iff it is reported by {@link ISPOIterator#getKeyOrder()}. Otherwise
     * chunks read from the source iterator will be in whatever order that
     * iterator is using while chunks containing backchained entailments will be
     * in no particular order.
     * <p>
     * Note: The backchained entailments will always begin on a chunk boundary.
     */
    public SPO[] nextChunk() {

        final int chunkSize = 10000;
        
        if (!hasNext())
            throw new NoSuchElementException();
        
        if(!sourceExhausted) {
            
            /*
             * Return a chunk from the source iterator.
             * 
             * Note: The chunk will be in the order used by the source iterator.
             * If the source iterator does not report that order then
             * [chunkKeyOrder] will be null.
             */
            
            chunkKeyOrder = keyOrder;

            SPO[] s = new SPO[chunkSize];

            int n = 0;
            
            while(src.hasNext() && n < chunkSize ) {
                
                s[n++] = src.next();
                
            }
            
            SPO[] stmts = new SPO[n];
            
            // copy so that stmts[] is dense.
            System.arraycopy(s, 0, stmts, 0, n);
            
            return stmts;
            
        }

        /*
         * Create a "chunk" of entailments.
         * 
         * Note: This chunk will NOT be fully ordered unless it is sorted. In
         * fact, it is not fully in any order. We delivery property-values for
         * the current subject in SPO order, but the subjects that are visited
         * are NOT themselves in order - they are being delivered through the
         * subjectIterator and the sameAsSubjectIterator.
         */
        
        SPO[] s = new SPO[chunkSize];
        
        int n = 0;
        
        while(hasNext() && n < chunkSize ) {
            
            s[n++] = next();
            
        }
        
        final SPO[] stmts;
        
        if(n==chunkSize) {

            // make it dense.
            
            stmts = new SPO[n];
            
            System.arraycopy(s, 0, stmts, 0, n);
            
         
        } else {
            
            // perfect fit.
            
            stmts = s;
            
        }
                        
        if (keyOrder != null && keyOrder != KeyOrder.POS) {

            /*
             * Sort into the same order as the source iterator.
             * 
             * Note: We have to sort explicitly since we are scanning the POS
             * index
             */

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        /*
         * The chunk will be unordered so we will have to sort it.
         */
        
        chunkKeyOrder = null;
        
        return stmts;
        
    }

    public SPO[] nextChunk(KeyOrder keyOrder) {
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] stmts = nextChunk();
        
        if (chunkKeyOrder != this.keyOrder) {

            // sort into the required order.

            Arrays.sort(stmts, 0, stmts.length, keyOrder.getComparator());

        }

        return stmts;
        
    }

    public void close() {

        if(!open) return;
        
        open = false;

        src.close();
        
        subjects.close();
        
        tempStore.closeAndDelete();

        if (sameAsSubjectIterator != null) {

            sameAsSubjectIterator.close();
            
        }

        if (propertyValueIterator != null) {
            
            propertyValueIterator.close();
            
        }
        
//        buffer = null;
        
        bloomFilter = null;
        
    }

    /**
     * Delegates the current statement to the source iterator unless it is an
     * entailment generated by backchaining. When the current statement was
     * generated by backchaining within this iterator then the request is simply
     * ignored (since requests to delete entailments are meaningless).
     */
    public void remove() {

        if(!sourceExhausted) {
           
            src.remove();
            
        }
        
    }
    
}
