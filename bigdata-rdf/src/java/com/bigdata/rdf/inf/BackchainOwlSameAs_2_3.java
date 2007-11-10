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
import com.bigdata.rdf.spo.EmptySPOIterator;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.KeyOrder;

/**
 * Computes the entailments for {@link RuleOwlSameAs2} and
 * {@link RuleOwlSameAs3} at query time given triple pattern (s p o).
 * <p>
 * An instance of each of these rules is created and specialized by (a) binding
 * the triple pattern on the rule; and (b) adding the constraint
 * <code>p != owl:sameAs</code>. As the results from the source iterator are
 * streamed through, we record the set of distinct subjects and the set of
 * distinct objects, each on their own {@link BTree}. Once the source iterator
 * is exhausted, we forward chain the specialization of {@link RuleOwlSameAs2}
 * for each distinct subject, continuing to note the distinct objects, and make
 * the results available via {@link #next()}. Once the results from that source
 * are exhausted, we forward chain the specialization of {@link RuleOwlSameAs3}
 * for each distinct object and make the results available via {@link #next()}.
 * <p>
 * Note: This assumes that the (x rdf:type rdfs:Resource) are generated
 * dynamically and filter those entailments from its output. The
 * {@link BackchainTypeResourceIterator} is then used to wrap this iterator
 * while this iterator is used to wrap the triple pattern query against the
 * database.
 * 
 * @todo if <code>s</code> is bound then there will be only a single distinct
 *       subject. likewise if <code>o</code> is bound then there will be only
 *       a single distinct object.
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
public class BackchainOwlSameAs_2_3 implements ISPOIterator {

    protected static final Logger log = Logger.getLogger(BackchainOwlSameAs_2_3.class);
    
    private final ISPOIterator src;
    private final long p, o;
    private final AbstractTripleStore db;
    private final long owlSameAs;
    private final KeyOrder keyOrder;
    
//    private ISPOBuffer buffer;
    
    /**
     * Initally <code>false</code> and set to <code>true</code> when the
     * {@link #src} iterator runs dry.
     */
    private boolean sourceExhausted = false;
    
    private boolean open = true;

    /**
     * The {@link BTree}s write on this store.
     */
    private final TemporaryRawStore tempStore = new TemporaryRawStore();
    
    /**
     * Keys are Long integers corresponding to the distinct subject identifers for
     * statements visted by the source iterator. Values are not used.
     */
    private final BTree subjects = new BTree(tempStore,
            BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
            NOPSerializer.INSTANCE);

    /**
     * Keys are Long integers corresponding to the distinct object identifers
     * for statements visted by the source iterator. Values are not used.
     */
    private final BTree objects = new BTree(tempStore,
            BTree.DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
            NOPSerializer.INSTANCE);

    /**
     * Used to build keys for the {@link #subjects} and {@link #objects}
     * indices.
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
     * Allocated once we start to scan the {@link #subjects}.
     */
    private IEntryIterator objectIterator = null;

    /**
     * Initially <code>false</code> and set to <code>true</code> when the
     * {@link #objectIterator} runs dry.
     */
    private boolean objectsExhausted = false;

    /**
     * Note: We do not save the original subject. Instead we bind this.s to each
     * distinct subject that we visit during backchaining. If s was originally
     * bound then there will be only a single distinct subject, but that is a
     * special case.
     */
    private long s;
    
    /**
     * This is set each time by {@link #nextChunk()} and inspected by
     * {@link #nextChunk(KeyOrder)} in order to decide whether the chunk needs
     * to be sorted.
     */
    private KeyOrder chunkKeyOrder = null; 
    
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

        return new BackchainOwlSameAs_2_3(src,s,p,o,db,owlSameAs);
        
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
    private BackchainOwlSameAs_2_3(ISPOIterator src, long s, long p, long o,
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
                        
        }
        
        if(!subjectsExhausted) {
            
            // start the subjects iterator.

            log.info("Starting read on subjects: size="
                        + subjects.getEntryCount());

            subjectIterator = subjects.rangeIterator(null, null);

            if(subjectIterator.hasNext()) {
               
                return true;
                
            }

            subjectsExhausted = true;
            
        }

        if(!objectsExhausted) {
            
            // start the objects iterator.

            log.info("Starting read on objects: size="
                        + objects.getEntryCount());

            objectIterator = objects.rangeIterator(null, null);

            if(objectIterator.hasNext()) {
               
                return true;
                
            }

            objectsExhausted = true;
            
        }

        return false;
        
    }

    public SPO next() {
        
        if(!hasNext()) {
            
            throw new NoSuchElementException();
            
        }

        if(!sourceExhausted) {
            
            SPO spo = src.next();
            
            addSubject(spo);

            addObject(spo);
            
            return spo;
            
        }

        // @todo really if !owlSameAs2Exhausted
        if(!subjectsExhausted) {
            
            throw new UnsupportedOperationException();
            
        }

        // @todo really if !owlSameAs3Exhausted
        if(!objectsExhausted) {
            
            throw new UnsupportedOperationException();
            
        }
        
        // Should not reach this point.
        throw new AssertionError();
        
    }

    /** add to the {@link #subjects} index. */
    private void addSubject(SPO spo) {
        
        subjects.insert(keyBuilder.reset().append(spo.s).getKey(),null);
        
    }
    
    /** add to the {@link #objects} index. */
    private void addObject(SPO spo) {
            
        objects.insert(keyBuilder.reset().append(spo.o).getKey(),null);
        
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

        objects.close();
        
        tempStore.closeAndDelete();
        
//        buffer = null;
        
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
