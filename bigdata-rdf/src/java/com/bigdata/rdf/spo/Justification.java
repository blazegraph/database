package com.bigdata.rdf.spo;

import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.inf.Rule;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.store.ITripleStore;

/**
 * <p>
 * A justification for a {@link StatementEnum#Inferred} statement. The head is
 * the entailed statement. The tail of the justification is one or more triple
 * pattern(s). Consider <code>rdf1</code>
 * </p>
 * 
 * <pre>
 *        (?u ?a ?y) -&gt; (?a rdf:type rdf:Property)
 * </pre>
 * 
 * <p>
 * Then the triple pattern for the tail is:
 * <p>
 * 
 * <pre>
 *    (0 ?a 0)
 * </pre>
 * 
 * <p>
 * where 0 reprents a {@link ITripleStore#NULL} term identifier.
 * </p>
 * <p>
 * So a justification chain for <code>rdf1</code> would be:
 * </p>
 * 
 * <pre>
 *    head := [?a rdf:type rdf:Property]
 *    
 *    tail := [0 ?a 0]
 * </pre>
 * 
 * <p>
 * This means we need to do an existence test on the triple pattern during TM.
 * If an element of the tail is still proven, regardless of whether it is fully
 * bound or has wildcard "0"s, then the head is still valid. This looks more or
 * less like: <strong>Find all matching the pattern. If any are explicit, then
 * that part of the tail is grounded. If none are explicit, then chase the
 * justification recursively. Only retract a justification when it can no longer
 * be grounded.</strong>
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo document the concept of grounded vs ungrounded justifications per the
 *       Sesame paper on TM.
 * 
 * @todo write a method to chase a proof chain for a given head and determine
 *       whether it is grounded or ungrounded.
 * 
 * @todo write unit test for key (de-)serialization for the justifications.
 */
public class Justification implements Comparable<Justification> {

    /**
     * The #of term identifiers in a statement.
     */
    private static final transient int N = ITripleStore.N;
    
    /**
     * From the ctor, but not persisted.
     */
    private final transient Rule rule;
    
    /**
     * Term identifiers for the head and bindings.
     * <p>
     * Divide the length by the #of terms in a statement #N and subtract one to
     * find the #of bindings for the tail. The first N entries are the head. The
     * rest are the tail.
     * <p>
     * Note: A term identifier MAY be {@link ITripleStore#NULL} to indicate a
     * wildcard.
     */
    private final long[] ids;
    
//    private final SPO head;
//    private final SPO[] bindings;

//    /**
//     * Note: This is not being persisted at present. It could be saved as the
//     * value in the justification index, but that would absorb a lot of space
//     * and we really do not need the information for anything other than
//     * debugging.
//     */
//    private final Rule rule;

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
     *            {@link ITripleStore#NULL} in which case it MUST be interpreted
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
        
        int i = 0;
        
        for(int j=0; j<m; j++) {

            for(int k=0; k<N; k++) {
              
                ids[i] = KeyBuilder.decodeLong(key, i*8);
                
            }
            
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
    
    /**
     * FIXME write unit tests.
     * <P>
     * Check for overflow of long.
     * <p>
     * Check when this is a longer or shorter array than the other where the two
     * justifications have the same data in common up to the end of whichever is
     * the shorter array.
     */
    public int compareTo(Justification o) {

        // the length of the longer ids[].
        
        final int len = ids.length > o.ids.length ? ids.length : o.ids.length;
        
        // compare both arrays until a difference emerges or one is exhausted.
        
        for(int i=0; i<len; i++) {
            
            if(i>ids.length) {
                
                // shorter with common prefix is ordered first.
                
                return -1;
                
            } else if(i>o.ids.length) {
                
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

    public String toString(ITripleStore db) {

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

}
