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
package com.bigdata.rdf.spo;

import java.util.Arrays;

import org.openrdf.model.Statement;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.relation.rule.IConstant;

/**
 * Represents a triple.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPO implements ISPO, Comparable<SPO> {
    
    /** The term identifier for the subject position. */
    public final long s;

    /** The term identifier for the predicate position. */
    public final long p;
    
    /** The term identifier for the object position. */
    public final long o;
    
    final public long s() {
        return s;
    }

    final public long p() {
        return p;
    }

    final public long o() {
        return o;
    }

    /**
     * Statement type (inferred, explicit, or axiom).
     */
    private StatementEnum type;
    
    /**
     * Statement identifier (optional).
     * <p>
     * Note: this is not final since we have to set it lazily when adding an
     * {@link SPO} to the database. However, when reading {@link SPO}s from the
     * database the statement identifier is aleady on hand and is set
     * immediately.
     */
    private long sid = NULL;

    /**
     * Set the statement identifier.
     * 
     * @param id
     *            The statement identifier.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</i> is {@link #NULL}.
     * @throws IllegalStateException
     *             if the statement identifier is already set.
     */
    public final void setStatementIdentifier(long id) {

        if (id == NULL)
            throw new IllegalArgumentException();

        if (this.sid != NULL && id != this.sid)
            throw new IllegalStateException(
                    "Different statement identifier already defined: "
                            + toString() + ", new=" + id);

        if( type != StatementEnum.Explicit)  {

            // Only allowed for explicit statements.
            throw new IllegalStateException();
            
        }
        
        if (!AbstractTripleStore.isStatement(id))
            throw new IllegalArgumentException("Not a statement identifier: "
                    + toString(id));
            
        this.sid = id;
        
    }
    
    public final long getStatementIdentifier() {

        if (sid == NULL)
            throw new IllegalStateException("No statement identifier: "+toString());

        return sid;
        
    }
    
    final public boolean hasStatementIdentifier() {
        
        return sid != NULL;
        
    }
    
    public void setOverride(boolean override) {

        this.override = override;
        
    }

    public boolean isOverride() {
        
        return override;
        
    }

    private transient boolean override = false;

    /**
     * Construct a statement whose type is NOT known.
     * <p>
     * Note: This is primarily used when you want to discover the
     * type of the statement.
     * 
     * @see AbstractTripleStore#bulkCompleteStatements(ISPOIterator)
     */
    public SPO(long s, long p, long o) {
        
        this.s = s;
        this.p = p;
        this.o = o;
        this.type = null;
        
    }
    
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

        if (type == null)
            throw new IllegalArgumentException();
        
        this.s = s;
        this.p = p;
        this.o = o;
        
        this.type = type;
        
    }

    /**
     * Constructor used when you know the {s,p,o} and have done a lookup in the
     * index to determine whether or not the statement exists, its
     * {@link StatementEnum} type, and its statement identifier (if assigned).
     * 
     * @param s
     * @param p
     * @param o
     * @param val
     */
    public SPO(long s, long p, long o, byte[] val) {

        this.s = s;
        this.p = p;
        this.o = o;
        
        decodeValue(val);
        
    }
    
    /**
     * Variant to create an {@link SPO} from constants (used by the unit tests).
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    public SPO(IConstant<Long> s, IConstant<Long> p, IConstant<Long> o,
            StatementEnum type) {

        this(s.get(), p.get(), o.get(), type);

    }

    /**
     * Construct an {@link SPO} from {@link BigdataValue}s.
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    public SPO(BigdataResource s, BigdataURI p, BigdataValue o,
            StatementEnum type) {
        
        this(s.getTermId(), p.getTermId(), o.getTermId(), type);
        
    }
    
    /**
     * Construct an {@link SPO} from a {@link BigdataStatement}. The term
     * identifiers and statement type information available on the
     * {@link BigdataStatement} will be used to initialize the {@link SPO}.
     * 
     * @param stmt The statement.
     */
    public SPO(BigdataStatement stmt) {
        
        this(   stmt.getSubject().getTermId(),//
                stmt.getPredicate().getTermId(),
                stmt.getObject().getTermId(),
                stmt.getStatementType()
                );
        
        final BigdataResource c = stmt.getContext();
        
        if (c != null && c.getTermId() != NULL) {

            setStatementIdentifier(c.getTermId());
            
        }
        
    }
    
    /**
     * Sets the statement type and optionally the statement identifier by
     * decoding the value associated with the key one of the statement indices.
     * 
     * @param val
     *            The value associated with the key one of the statement
     *            indices.
     */
    public void decodeValue(byte[] val) {
        
        final byte code = val[0];
        
        setStatementType( StatementEnum.decode( code ) ); 
        
        if (val.length == 1 + 8) {

            /*
             * The value buffer appears to contain a statement identifier, so we
             * read it.
             */
            
            setStatementIdentifier(new ByteArrayBuffer(1, val.length, val)
                    .getLong(1));
        
            assert AbstractTripleStore.isStatement(sid) : "Not a statement identifier: "
                    + toString(sid);

            assert type == StatementEnum.Explicit : "statement identifier for non-explicit statement : "
                    + toString();

            assert sid != NULL : "statement identifier is NULL for explicit statement: "
                    + toString();

        }
        
    }

    public byte[] serializeValue(ByteArrayBuffer buf) {

        return serializeValue(buf, isOverride(), type, sid);
        
    }
    
    /**
     * Return the byte[] that would be written into a statement index for this
     * {@link SPO}, including the optional {@link StatementEnum#MASK_OVERRIDE}
     * bit. If the statement identifier is non-{@link #NULL} then it will be
     * included in the returned byte[].
     * 
     * @param buf
     *            A buffer supplied by the caller. The buffer will be reset
     *            before the value is written on the buffer.
     * 
     * @param override
     *            <code>true</code> iff you want the
     *            {@link StatementEnum#MASK_OVERRIDE} bit set (this is only set
     *            when serializing values for a remote procedure that will write
     *            on the index, it is never set in the index itself).
     * @param type
     *            The {@link StatementEnum}.
     * @param sid
     *            The statement identifier iff this is
     *            {@link StatementEnum#Explicit} statement AND statement
     *            identifiers are enabled and otherwise {@link #NULL}.
     * 
     * @return The value that would be written into a statement index for this
     *         {@link SPO}.
     */
    static public byte[] serializeValue(ByteArrayBuffer buf, boolean override, StatementEnum type, long sid) {

        buf.reset();
        
        // optionally set the override bit on the value.
        final byte b = (byte) (override ? (type.code() | StatementEnum.MASK_OVERRIDE)
                : type.code());
        
        buf.putByte( b );
        
        if (sid != NULL) {
            
            assert type == StatementEnum.Explicit : "Statement identifier not allowed: type="
                    + type;
            
            buf.putLong(sid);
            
        }
        
        return buf.toByteArray();

    }
    
    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as {@link StatementEnum#Explicit}. 
     */
    public final boolean isExplicit() {
        
        return type == StatementEnum.Explicit;
        
    }
    
    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as {@link StatementEnum#Inferred}. 
     */
    public final boolean isInferred() {
        
        return type == StatementEnum.Inferred;
        
    }
    
    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as {@link StatementEnum#Axiom}. 
     */
    public final boolean isAxiom() {
        
        return type == StatementEnum.Axiom;
        
    }
    
    private int hashCode = 0;

    /**
     * Hash code for the SPO per {@link Arrays#hashCode(long[])}.
     */
    public int hashCode() {

        if (hashCode == 0) {

            // compute and cache.

            long[] a = new long[]{s,p,o};
            
            int result = 1;
            
            for (long element : a) {
            
                int elementHash = (int) (element ^ (element >>> 32));
                
                result = 31 * result + elementHash;
                
            }

        }

        return hashCode;

    }

    /**
     * Imposes s:p:o ordering based on termIds.
     * <p>
     * Note: By design, this does NOT differentiate between statements with the
     * different {@link StatementEnum} values.
     */
    public int compareTo(SPO stmt2) {

        if (stmt2 == this) {

            return 0;

        }

        final SPO stmt1 = this;
        
        /*
         * Note: logic avoids possible overflow of [long] by not computing the
         * difference between two longs.
         */
//        int ret = stmt1.code - stmt2.code;

//        if (ret == 0) {

            int ret = stmt1.s < stmt2.s ? -1 : stmt1.s > stmt2.s ? 1 : 0;

            if (ret == 0) {

                ret = stmt1.p < stmt2.p ? -1 : stmt1.p > stmt2.p ? 1 : 0;

                if (ret == 0) {

                    ret = stmt1.o < stmt2.o ? -1 : stmt1.o > stmt2.o ? 1 : 0;

                }

            }

//        }

        return ret;

    }

    public boolean equals(Object o) {
        
        if (this == o)
            return true;

        return equals((ISPO) o);
        
    }
    
    /**
     * True iff the {@link ISPO}s are the same object or if the same term
     * identifiers are assigned for the subject, predicate and object positions,
     * and the same {@link StatementEnum} are the same.
     * <p>
     * Note: This is NOT the same test as
     * {@link BigdataStatementImpl#equals(Object)} since the latter is
     * implemented per the {@link Statement} interface.
     */
    public boolean equals(ISPO stmt2) {

        if (stmt2 == this)
            return true;

        return
                this.s == stmt2.s() && //
                this.p == stmt2.p() && //
                this.o == stmt2.o() && //
                this.type == stmt2.getStatementType()
                ;
        
        // @todo statementId comparison?

    }

    /**
     * Return a representation of the statement using the term identifiers (the
     * identifers are NOT resolved to terms).
     * 
     * @see ITripleStore#toString(long, long, long)
     */
    public String toString() {

        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o))
                + (type == null ? "" : " : " + type
                        + (sid == NULL ? "" : ", sid=" + sid)) + " >";

    }

    /**
     * Represents the term identifier together with its type (literal, bnode, uri,
     * or statement identifier).
     * 
     * @param id
     *            The term identifier.
     * @return
     */
    private String toString(long id) {

        if (id == NULL)
            return "NULL";

        if (AbstractTripleStore.isLiteral(id))
            return id + "L";

        if (AbstractTripleStore.isURI(id))
            return id + "U";

        if (AbstractTripleStore.isBNode(id))
            return id + "B";

        if (AbstractTripleStore.isStatement(id))
            return id + "S";

        throw new AssertionError("id="+id);
        
    }

    /**
     * Resolves the term identifiers to terms against the store and returns a
     * representation of the statement using
     * {@link IRawTripleStore#toString(long, long, long)}.
     * 
     * @param store
     *            The store (optional). When non-<code>null</code> the store
     *            will be used to resolve term identifiers to terms.
     * 
     * @return The externalized representation of the statement.
     */
    public String toString(IRawTripleStore store) {
        
        if (store != null) {

            String t = null;
            
            if (type != null) {
                switch(type) {
                case Explicit    : t = "Explicit    "; break;
                case Inferred    : t = "Inferred    "; break;
                case Axiom       : t = "Axiom       "; break;
                case Backchained : t = "Backchained "; break;
                default: throw new AssertionError();
                }
            } else {
                t = "Unknown     ";
            }
            
            // Note: the statement [id] is not stored in the reverse lexicon.
            final String idStr = (sid==NULL?"":" : sid="+sid);
            
            return t +" : " + store.toString(s, p, o) + idStr;
            
        } else {
            
            return toString();
            
        }
        
    }

    final public boolean isFullyBound() {
    
        return s != NULL && p != NULL && o != NULL;

    }

    final public StatementEnum getStatementType() {

//        if (type == null)
//            throw new IllegalStateException();
        
        return type;

    }

    /**
     * Set the statement type.
     * 
     * @param type
     *            The statement type.
     * 
     * @throws IllegalStateException
     *             if the statement type is already set to a different value.
     */
    final public void setStatementType(StatementEnum type) {
        
        if(this.type != null && this.type != type) {
            
            throw new IllegalStateException();
            
        }
        
        this.type = type;
        
    }
    
    /**
     * Return <code>true</code> iff the statement type is known.
     * 
     * <code>true</code> iff the statement type is known for this statement.
     */
    final public boolean hasStatementType() {
        
        return type != null;
        
    }

}
