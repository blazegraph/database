package com.bigdata.rdf.inf;

import java.util.Arrays;

import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * <p>
 * A justification for a {@link StatementEnum#Inferred} statement. The head is
 * the entailed statement. The tail of the justification is one or more triple
 * pattern(s). Consider <code>rdf1</code>
 * </p>
 * 
 * <pre>
 *               (?u ?a ?y) -&gt; (?a rdf:type rdf:Property)
 * </pre>
 * 
 * <p>
 * Then the triple pattern for the tail is:
 * <p>
 * 
 * <pre>
 *           (0 ?a 0)
 * </pre>
 * 
 * <p>
 * where 0 reprents a {@link IRawTripleStore#NULL} term identifier.
 * </p>
 * <p>
 * So a justification chain for <code>rdf1</code> would be:
 * </p>
 * 
 * <pre>
 *           head := [?a rdf:type rdf:Property]
 *           
 *           tail := [0 ?a 0]
 * </pre>
 * 
 * <p>
 * In fact, the total bindings for the rule are represented as a long[] with the
 * head occupying the 1st N positions in that array and the bindings for the
 * tail appearing thereafter in the declared order of the predicates in the
 * tail.
 * </p>
 * 
 * <p>
 * When a {@link StatementEnum#Explicit} statement is to be retracted from the
 * database we need to determined whether or not there exists a <strong>grounded</strong>
 * justification for that statement (same head). For each justification for that
 * statement we consider the tail. If there exists either an explicit statement
 * that satisifies the triple pattern for the tail or if there exists an
 * inference that satisifies the triple pattern for the tail and the inference
 * can be proven to be grounded by recursive examination of its justifications,
 * then the head is still valid and is converted from an explicit statement into
 * an inference.
 * </p>
 * <p>
 * This looks more or less like: <strong>Find all statements matching the
 * pattern. If any are explicit, then that part of the tail is grounded. If none
 * are explicit, then chase the justification recursively. Only retract a
 * justification when it can no longer be grounded.</strong>
 * </p>
 * 
 * <p>
 * The concept of grounded vs ungrounded justifications is described in <a
 * href="http://www.openrdf.org/doc/papers/inferencing.pdf"> Inferencing and
 * Truth Maintenance in RDF Schema : Exploring a naive practical approach</a>
 * by Jeen Broekstra and Arjohn Kampman.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Justification implements Comparable<Justification> {

    /**
     * The #of term identifiers in a statement.
     */
    private static final transient int N = IRawTripleStore.N;
    
    /**
     * From the ctor, but not persisted.
     */
    public final transient Rule rule;
    
    /**
     * Term identifiers for the head and bindings.
     * <p>
     * Divide the length by the #of terms in a statement #N and subtract one to
     * find the #of bindings for the tail. The first N entries are the head. The
     * rest are the tail.
     * <p>
     * Note: A term identifier MAY be {@link IRawTripleStore#NULL} to indicate a
     * wildcard.
     */
    final long[] ids;
    
    /**
     * Construct an entailment for an {@link StatementEnum#Inferred} statement.
     * 
     * @param rule
     *            The rule that licensed the entailment (this is only used for
     *            debugging).
     * @param head
     *            The entailment licensed by the rule and the bindings.
     * @param bindings
     *            The bindings for that rule that licensed the entailment.
     */
    public Justification(Rule rule, SPO head, SPO[] bindings) {

        assert rule != null;
        assert head != null;
        assert bindings != null;

        this.rule = rule;
        
        ids = new long[ (1 + bindings.length ) * N];

        int i = 0;
        
        ids[i++] = head.s;
        ids[i++] = head.p;
        ids[i++] = head.o;
        
        for( SPO spo : bindings ) {
            
            ids[i++] = spo.s;
            ids[i++] = spo.p;
            ids[i++] = spo.o;
            
        }
    
    }
    
    /**
     * Returns the head as an {@link SPO}.
     * <p>
     * Note: The {@link StatementEnum} associated with the head is actually
     * unknown, but it is marked as {@link StatementEnum#Inferred} in the
     * returned object. In order to discover the {@link StatementEnum} for the
     * head you MUST either already know it (this is not uncommon) or you MUST
     * read one of the statement indices.
     * 
     * @return
     */
    public SPO getHead() {

        return new SPO(ids[0], ids[1], ids[2], StatementEnum.Inferred);
        
    }
    
    /**
     * Returns the tail as an {@link SPO}[].
     * <p>
     * Note: The {@link StatementEnum} associated triple patterns in the tail is
     * actually unknown, but it is marked as {@link StatementEnum#Inferred} in
     * the returned object. In fact, since the tail consists of triple patterns
     * and not necessarily fully bound triples, the concept of a
     * {@link StatementEnum} is not even defined.
     * 
     * @return
     */
    public SPO[] getTail() {
        
        // #of triple patterns in the tail.
        final int m = (ids.length / N) - 1;

        SPO[] tail = new SPO[m];
        
        // for each triple pattern in the tail.
        int j = N;
        for(int i=0; i<m; i++, j+=N) {
        
            tail[i] = new SPO(ids[j], ids[j + 1], ids[j + 2],
                    StatementEnum.Inferred);
            
        }

        return tail;
        
    }
    
    /**
     * Construct a justification directly from the term identifier bindings for
     * the tail.
     * 
     * @param rule
     *            The rule (used for debugging only).
     * 
     * @param head
     *            The entailment licensed by the rule and the bindings.
     * 
     * @param bindings
     *            The term identifiers for the bindings under which the rule
     *            justified the entailments. A binding MAY be
     *            {@link IRawTripleStore#NULL} in which case it MUST be interpreted
     *            as a wildcard.
     */
    public Justification(Rule rule, SPO head, long[] bindings) {

        assert rule != null;
        assert head != null;
        assert bindings != null;
        
        // verify enough bindings for one or more triple patterns.
        assert bindings.length % N == 0;
        
        this.rule = rule;
        
        // #of triple patterns in the tail.
        final int m = bindings.length / N;

        // allocate enough for the head and the tail.
        ids = new long[(1 + m) * N];
        
        int i = 0;
        
        ids[i++] = head.s;
        ids[i++] = head.p;
        ids[i++] = head.o;
        
        for(long id : bindings) {
            
            ids[i++] = id;
            
        }
        
    }
    
    /**
     * Deserialize a justification from an index key.
     * 
     * @param key
     *            The key.
     */
    public Justification(byte[] key) {
        
        this.rule = null; // Not persisted.
        
        // verify key is even multiple of (N*sizeof(long)).
        assert key.length % (N * Bytes.SIZEOF_LONG) == 0;

        // #of term identifiers in the key.
        final int m = key.length / Bytes.SIZEOF_LONG;

        ids = new long[m];
        
        for(int i=0; i<m; i++) {

            ids[i] = KeyBuilder.decodeLong(key, i*8);
            
        }
        
    }

    /**
     * Serialize a justification as an index key. The key length is a function
     * of the #of bindings in the justification.
     * 
     * @param keyBuilder
     *            A key builder.
     * 
     * @return The key.
     */
    public byte[] getKey(KeyBuilder keyBuilder) {

        keyBuilder.reset();

        for(int i=0; i<ids.length; i++) {
            
            keyBuilder.append(ids[i]);
            
        }
        
        return keyBuilder.getKey();
        
    }
    
    public boolean equals(Justification o) {
        
        // Note: ignores transient [rule].
        
        if(this==o) return true;
        
        return Arrays.equals(ids, o.ids);
        
    }
    
    /**
     * Places the justifications into an ordering that clusters them based on
     * the entailment is being justified.
     */
    public int compareTo(Justification o) {

        // the length of the longer ids[].
        
        final int len = ids.length > o.ids.length ? ids.length : o.ids.length;
        
        // compare both arrays until a difference emerges or one is exhausted.
        
        for(int i=0; i<len; i++) {
            
            if(i>=ids.length) {
                
                // shorter with common prefix is ordered first.
                
                return -1;
                
            } else if(i>=o.ids.length) {
                
                // shorter with common prefix is ordered first.
                
                return 1;
                
            }
            
            /*
             * Both arrays have data for this index.
             * 
             * Note: logic avoids possible overflow of [long] by not computing the
             * difference between two longs.
             */

            int ret = ids[i] < o.ids[i] ? -1 : ids[i] > o.ids[i] ? 1 : 0;

            if (ret != 0)
                return ret;
            
        }

        // identical values and identical lengths.

        assert ids.length == o.ids.length;
        
        return 0;
        
    }

    public String toString() {
        
        return toString(null);
        
    }

    public String toString(AbstractTripleStore db) {

        StringBuilder sb = new StringBuilder();

        if (rule != null) {

            sb.append(rule.getClass().getSimpleName());

            sb.append("\n");

        }

        // tail
        {

            // #of triple patterns in the tail.
            final int m = (ids.length / N) - 1;

            for( int i=0; i<m; i++) {

                sb.append("   (");

                for( int j=0; j<N; j++ ) {
                    
                    long id = ids[i*N+N+j];
                    
                    sb.append((db == null ? "" + id : db.toString(id)));

                    if (j+1 < N)
                        sb.append(", ");

                }

                sb.append(")");

                if (i + 1 < m) {

                    sb.append(", \n");
                    
                }
                
            }
            
            sb.append("\n   -> ");
            
        }
        
        // head
        {

            sb.append("(");

            for (int i = 0; i < N; i++) {

                long id = ids[i];
                
                sb.append((db == null ? "" + id : db.toString(id)));

                if (i+1 < N)
                    sb.append(", ");

            }
            sb.append(")");
            
        }

        return sb.toString();

    }

    /**
     * Return true iff a grounded justification chain exists for the statement.
     * 
     * @param head
     *            The statement (fully bound).
     * 
     * @return True iff the statement is entailed by a grounded justification
     *         chain in the database.
     */
    public static boolean isGrounded(AbstractTripleStore db, SPO head) {
     
        assert head.isFullyBound();
        
        return isGrounded(db,head,false);
        
    }
        
    /**
     * 
     * @param db
     * @param head
     *            A triple pattern. When invoked on a statement during truth
     *            maintenance this will be fully bound. However, during
     *            recursive processing triple patterns may be encountered in the
     *            tail of {@link Justification}s that are not fully bound. In
     *            such cases we test for any statement matching the triple
     *            pattern that can be proven to be grounded.
     * @param testHead
     * @return
     * 
     * @todo if we decide that we have only grounded justifications using the
     *       fast closure method then this should be rewritten to be
     *       non-recursive and it will be MUCH faster.
     */
    private static boolean isGrounded(AbstractTripleStore db, SPO head, boolean testHead) {

        if(testHead) {
            
            /*
             * Scan the statement indices for the head. This covers both the
             * case when it is fully bound (since we need to know whether or not
             * it is explicit) and the case when it has unbound positions (where
             * we need to scan them and see if any matching statements in the
             * database are explicit).
             */
            
            ISPOIterator itr = db.getAccessPath(head.s,head.p,head.o).iterator();
            
            while(itr.hasNext()) {
                
                SPO spo = itr.next();
                
                if (spo.type == StatementEnum.Explicit) {

                    // grounded for this triple pattern.
                    
                    return true;
                    
                } else {
                    
                    if(isGrounded(db,spo,false)) {
                    
                        // recursively grounded somewhere.
                        
                        return true;
                        
                    } else {
                        
                        // next spo.
                        
                    }
                    
                }
                
            }
            
        }

        if(head.isFullyBound()) {

            /*
             * Examine all justifications for the statement. If any of them are
             * grounded then the statement is still entailed by the database.
             */
            
            JustificationIterator itr = new JustificationIterator(db,head);
            
            while(itr.hasNext()) {
                
                /*
                 * For each justification we consider the bindings. The first N are
                 * just the statement that was proven. The remaining bindings are
                 * M-1 triple patterns of N elements each.
                 */
                
                Justification jst = itr.next();
                
                SPO[] tail = jst.getTail();
                
                /*
                 * if all in tail are explicit in the statement indices, then done.
                 * 
                 * since tail is triple patterns, we have to scan those patterns for
                 * the first explicit statement matched.
                 * 
                 * if none in tail are explicit, then we can recurse. we could also
                 * scan the rest of the justifications for something that was easily
                 * proven to be explicit. it is a depth vs breadth 1st issue.
                 * 
                 * this is definately going to be expensive in a distributed store
                 * since it is all random RPCs.
                 */
                
                boolean ok = true;
                
                for( SPO t : tail ) {
                    
                    if(!isGrounded(db, t, true/*testHead*/)) {
                        
                        ok = false;
                        
                        break;
                        
                    }
                    
                }

                if(ok) {
                    
                    return true;
                    
                }
                
            } // next justification.

        } // head.isFullyBound()
            
        return false;
        
    }
    
}
