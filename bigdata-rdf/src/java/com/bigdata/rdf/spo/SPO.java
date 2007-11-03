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
package com.bigdata.rdf.spo;

import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.util.KeyOrder;
import com.bigdata.rdf.util.RdfKeyBuilder;

/**
 * Represents a triple.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPO {
    
    private transient static final long NULL = IRawTripleStore.NULL;
    
    /**
     * @see RdfKeyBuilder#CODE_STMT
     * @see RdfKeyBuilder#CODE_PRED
     * @see RdfKeyBuilder#CODE_RULE
     */
    public final byte code;
    public final long s;
    public final long p;
    public final long o;
    public final StatementEnum type;
    
    /**
     * Construct a statement.
     * <p>
     * Note: When the statement is {@link StatementEnum#Inferred} you MUST also
     * construct the appropriate {@link Justification}.
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     *            The statement type.
     */
    public SPO(long s, long p, long o, StatementEnum type) {
        assert type != null;
        this.code = RdfKeyBuilder.CODE_STMT;
        this.s = s;
        this.p = p;
        this.o = o;
        this.type = type;
    }

    /**
     * Construct a triple from the sort key.
     * 
     * @param keyOrder
     *            Indicates the permutation of the subject, predicate and object
     *            used by the key.
     * 
     * @param key
     *            The key from the index entry.
     * 
     * @param val
     *            The value from the index entry.
     * 
     * @see RdfKeyBuilder#key2Statement(byte[], long[])
     */
    public SPO(KeyOrder keyOrder, byte[] key, Object val) {
        
        assert keyOrder != null;
        assert key != null;
        assert val != null;
        
        long[] ids = new long[3];
        
        code = RdfKeyBuilder.key2Statement(key, ids); 
        
        switch (keyOrder) {

        case SPO:
            s = ids[0];
            p = ids[1];
            o = ids[2];

            break;
        case POS:
            p = ids[0];
            o = ids[1];
            s = ids[2];

            break;
        case OSP:
            o = ids[0];
            s = ids[1];
            p = ids[2];

            break;

        default:

            throw new UnsupportedOperationException();

        }

        type = StatementEnum.deserialize((byte[])val); 
        
    }
    
//    private int hashCode = 0;
//    
//    /**
//     * @todo validate the manner in which we are combining the hash codes for
//     *       the individual components of the triple (each component uses the
//     *       same hash code algorithm as {@link Long#hashCode()}).
//     */
//    public int hashCode() {
//        
//        if(hashCode==0) {
//
//            // compute and cache.
//            hashCode = (int) ((s ^ (s >>> 32)) | (p ^ (p >>> 32)) | (o ^ (o >>> 32))); 
//            
//        }
//        
//        return hashCode;
//        
//    }
    
    /**
     * Imposes s:p:o ordering based on termIds.
     * <p>
     * Note: By design, this does NOT differentiate between statements with the
     * different {@link StatementEnum} values.
     */
    public int compareTo(Object other) {

        if (other == this) {

            return 0;

        }

        final SPO stmt1 = this;
        
        final SPO stmt2 = (SPO) other;
        
        /*
         * Note: logic avoids possible overflow of [long] by not computing the
         * difference between two longs.
         */
        int ret;

        ret = stmt1.code - stmt2.code;

        if (ret == 0) {

            ret = stmt1.s < stmt2.s ? -1 : stmt1.s > stmt2.s ? 1 : 0;

            if (ret == 0) {

                ret = stmt1.p < stmt2.p ? -1 : stmt1.p > stmt2.p ? 1 : 0;

                if (ret == 0) {

                    ret = stmt1.o < stmt2.o ? -1 : stmt1.o > stmt2.o ? 1 : 0;

                }

            }

        }

        return ret;

    }
    
    /**
     * True iff the statements are the same object or if they have the code, the
     * same term identifiers assigned for the subject, predicate and object
     * positions, and either the same {@link StatementEnum} or <code>null</code>
     * for the {@link StatementEnum}.
     */
    public boolean equals(SPO stmt2) {

        if (stmt2 == this)
            return true;

        return this.code == stmt2.code && //
                this.s == stmt2.s && //
                this.p == stmt2.p && //
                this.o == stmt2.o && //
                this.type == stmt2.type
                ;

    }

    /**
     * Return a representation of the statement using the term identifiers (the
     * identifers are NOT resolved to terms).
     * 
     * @see ITripleStore#toString(long, long, long)
     */
    public String toString() {
        
        return (""+s+","+p+","+o)+(type==null?"":" : "+type);
        
    }

    /**
     * Resolves the term identifiers to terms against the store and returns a
     * representation of the statement using
     * {@link ITripleStore#toString(long, long, long)}.
     * 
     * @param store The store.
     * 
     * @return The externalized representation of the statement. 
     */
    public String toString(AbstractTripleStore store) {
        
        return store.toString(s,p,o)+(type==null?"":" : "+type);
        
    }

    /**
     * Return true iff all position (s,p,o) are non-{@link #NULL}.
     * <p>
     * Note: {@link SPO} are sometimes used to represent triple patterns. E.g.,
     * in the tail of a {@link Justification}. This method will return
     * <code>true</code> if the "triple pattern" is fully bound and
     * <code>false</code> if there are any unbound positions.
     */
    public boolean isFullyBound() {
    
        return s != NULL && p != NULL && o != NULL;

    }
    
}
