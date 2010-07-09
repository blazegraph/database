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

import org.openrdf.model.Statement;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtil;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataStatementImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariableOrConstant;

/**
 * Represents a triple, triple+SID, or quad. When used to represent a triple,
 * the statement identifier MAY be set on the triple after the object has been
 * instantiated. When used to represent a quad, the context position SHOULD be
 * treated as immutable and {@link #setStatementIdentifier(IV)} will reject
 * arguments if they can not be validated as statement identifiers (based on
 * their bit pattern).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPO implements ISPO {
    
    /** The internal value for the subject position. */
    public final IV s;

    /** The internal value for the predicate position. */
    public final IV p;
    
    /** The internal value for the object position. */
    public final IV o;

    /**
     * The context position or statement identifier (optional).
     * <p>
     * Note: this is not final since, for SIDs mode, we have to set it lazily
     * when adding an {@link SPO} to the database.
     */
    private IV c = null;

    /**
     * Statement type (inferred, explicit, or axiom).
     */
    private StatementEnum type;

    /**
     * Override flag used for downgrading statements during truth maintenance.
     */
    private transient boolean override = false;
    
    private transient boolean modified = false;

    final public IV get(final int index) {
        switch(index) {
        case 0: return s;
        case 1: return p;
        case 2: return o;
        case 3: return c;
        default: throw new IllegalArgumentException();
        }
    }
    
    final public IV s() {
        return s;
    }

    final public IV p() {
        return p;
    }

    final public IV o() {
        return o;
    }

    final public IV c() {
        return c;
    }
    
    /**
     * Set the statement identifier.
     * 
     * @param iv
     *            The statement identifier.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</i> is {@link #NULL}.
     * @throws IllegalStateException
     *             if the statement identifier is already set.
     */
    public final void setStatementIdentifier(final IV iv) {

        if (iv == null)
            throw new IllegalArgumentException();

        if (!iv.isStatement())
            throw new IllegalArgumentException("Not a statement identifier: "
                    + toString(iv));

        if (type != StatementEnum.Explicit) {

            // Only allowed for explicit statements.
            throw new IllegalStateException();

        }

        if (c != null && !IVUtil.equals(iv, c))
            throw new IllegalStateException(
                    "Different statement identifier already defined: "
                            + toString() + ", new=" + iv);

        this.c = iv;

    }

    public final IV getStatementIdentifier() {

        if (c == null)
            throw new IllegalStateException("No statement identifier: "
                    + toString());

        return c;

    }

    final public boolean hasStatementIdentifier() {
        
        return c != null && c.isStatement();
        
    }
    
    public void setOverride(final boolean override) {

        this.override = override;
        
    }

    public boolean isOverride() {
        
        return override;
        
    }

    /**
     * Triple constructor for a statement whose type is NOT known.
     * <p>
     * Note: This is primarily used when you want to discover the
     * type of the statement.
     * 
     * @see AbstractTripleStore#bulkCompleteStatements(ISPOIterator)
     */
    public SPO(final IV s, final IV p, final IV o) {
        
        this.s = s;
        this.p = p;
        this.o = o;
        this.type = null;
        
    }

    /**
     * Quads constructor.
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     */
    public SPO(final IV s, final IV p, final IV o, final IV c) {

        this.s = s;
        this.p = p;
        this.o = o;
        this.c = c;
        this.type = null;

    }

    /**
     * Construct a triple.
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
    public SPO(final IV s, final IV p, final IV o, StatementEnum type) {

        if (type == null)
            throw new IllegalArgumentException();
        
        this.s = s;
        this.p = p;
        this.o = o;
        
        this.type = type;
        
    }
    
    /**
     * Quads constructor with {@link StatementEnum}.
     * @param s
     * @param p
     * @param o
     * @param c
     * @param type
     */
    public SPO(final IV s, final IV p, final IV o, final IV c,
            final StatementEnum type) {

        if (type == null)
            throw new IllegalArgumentException();
        
        this.s = s;
        this.p = p;
        this.o = o;
        this.c = c;
        
        this.type = type;
        
    }

//    /**
//     * Constructor used when you know the {s,p,o} and have done a lookup in the
//     * index to determine whether or not the statement exists, its
//     * {@link StatementEnum} type, and its statement identifier (if assigned).
//     * 
//     * @param s
//     * @param p
//     * @param o
//     * @param val
//     */
//    public SPO(final long s, final long p, final long o, final byte[] val) {
//
//        this.s = s;
//        this.p = p;
//        this.o = o;
//        
//        decodeValue(this, val);
//        
//    }

    /**
     * Variant to create an {@link SPO} from constants (used by the unit tests).
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    public SPO(final IConstant<IV> s, final IConstant<IV> p,
            final IConstant<IV> o, final StatementEnum type) {

        this(s.get(), p.get(), o.get(), type);

    }

    /**
     * Variant to create an SPO from a predicate - the {@link StatementEnum} and
     * statement identifier are not specified. This may be used as a convenience
     * to extract the {s, p, o, c} from an {@link IPredicate} or from an
     * {@link IAccessPath} when the predicate is not known to be an
     * {@link SPOPredicate} or the {@link IAccessPath} is not known to be an
     * {@link SPOAccessPath}.
     * 
     * @param predicate
     *            The predicate.
     */
    @SuppressWarnings("unchecked")
    public SPO(final IPredicate<ISPO> predicate) {
        
        {

            final IVariableOrConstant<IV> t = predicate.get(0);

            s = t.isVar() ? null : t.get();

        }

        {

            final IVariableOrConstant<IV> t = predicate.get(1);

            p = t.isVar() ? null : t.get();

        }

        {

            final IVariableOrConstant<IV> t = predicate.get(2);

            o = t.isVar() ? null : t.get();

        }

        if (predicate.arity() >= 4) {

            final IVariableOrConstant<IV> t = predicate.get(3);

            c = t.isVar() ? null : t.get();

        } else {

            c = null;
            
        }

    }

    /**
     * Construct a triple from {@link BigdataValue}s and the specified statement
     * type.
     * 
     * @param s
     * @param p
     * @param o
     * @param type
     */
    public SPO(final BigdataResource s, final BigdataURI p,
            final BigdataValue o, final StatementEnum type) {

        this(s.getIV(), p.getIV(), o.getIV(), type);

    }

    /**
     * Construct a triple/quad from a {@link BigdataStatement}. The term
     * identifiers and statement type information available on the
     * {@link BigdataStatement} will be used to initialize the {@link SPO}.
     * 
     * @param stmt
     *            The statement.
     */
    public SPO(final BigdataStatement stmt) {
        
        this(   stmt.s(),//
                stmt.p(),//
                stmt.o(),//
                stmt.c(),//
                stmt.getStatementType()//
                );
        
//        final BigdataResource c = stmt.getContext();
//        
//        if (c != null && c.getTermId() != NULL) {
//
//            setStatementIdentifier(c.getTermId());
//            
//        }
        
    }

    /**
     * Sets the statement type and optionally the statement identifier by
     * decoding the value associated with the key one of the statement indices.
     * 
     * @param val
     *            The value associated with the key one of the statement
     *            indices.
     * 
     * @return The <i>spo</i>.
     */
    public static ISPO decodeValue(final ISPO spo, final byte[] val) {
        
        final byte code = val[0];

        final StatementEnum type = StatementEnum.decode(code);

        spo.setStatementType(type);

        if (val.length == 1 + 8) {

            /*
             * The value buffer appears to contain a statement identifier, so we
             * read it.
             */
            
            final long sid = new ByteArrayBuffer(1, val.length, val).getLong(1);
            
            assert sid != TermId.NULL : "statement identifier is NULL for explicit statement: "
                + spo.toString();

            assert VTE.isStatement(sid) : "Not a statement identifier: "
                    + sid;

            assert type == StatementEnum.Explicit : "statement identifier for non-explicit statement : "
                    + spo.toString();

            spo.setStatementIdentifier(new TermId(sid));
            
        }

        return spo;
        
    }

    public byte[] serializeValue(final ByteArrayBuffer buf) {

        return serializeValue(buf, isOverride(), type, c);
        
    }

    /**
     * Return the byte[] that would be written into a statement index for this
     * {@link SPO}, including the optional {@link StatementEnum#MASK_OVERRIDE}
     * bit. If the statement identifier is non-null then it will be
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
     * @param c
     *            The term identifier associated with the context position. This
     *            will be included in the returned byte[] value IFF
     *            {@link AbstractTripleStore#isStatement(IV)} returns
     *            <code>true</code> for <i>c</i> AND the <i>type</i> is
     *            {@link StatementEnum#Explicit}.
     * 
     * @return The value that would be written into a statement index for this
     *         {@link SPO}.
     */
    static public byte[] serializeValue(final ByteArrayBuffer buf,
            final boolean override, final StatementEnum type, final IV c) {

        buf.reset();

        // optionally set the override bit on the value.
        final byte b = (byte) (override ? (type.code() | StatementEnum.MASK_OVERRIDE)
                : type.code());

        buf.putByte(b);

        if (type == StatementEnum.Explicit
                && c != null && c.isStatement()) {

            buf.putLong(c.getTermId());

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
     * Hash code for the (s,p,o) per Sesame's {@link Statement#hashCode()}. It
     * DOES NOT consider the context position.
     */
    public int hashCode() {

        if (hashCode == 0) {

            final int s = this.s.hashCode();
            
            final int p = this.p.hashCode();
            
            final int o = this.o.hashCode();
            
            // Note: historical behavior was (s,p,o) based hash.
            hashCode = 961 * ((int) (s ^ (s >>> 32))) + 31
                    * ((int) (p ^ (p >>> 32))) + ((int) (o ^ (o >>> 32)));

        }

        return hashCode;

    }

    public boolean equals(final Object o) {
        
        if (this == o)
            return true;

        if (o == null)
            return false;
        
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
    public boolean equals(final ISPO stmt2) {

        if (stmt2 == this)
            return true;

        return
                IVUtil.equals(this.s, stmt2.s()) && //
                IVUtil.equals(this.p, stmt2.p()) && //
                IVUtil.equals(this.o, stmt2.o()) && //
                this.type == stmt2.getStatementType()
                ;

    }

    /**
     * Return a representation of the statement using the term identifiers (the
     * identifiers are NOT resolved to terms).
     * 
     * @see ITripleStore#toString(IV, IV, IV)
     */
    public String toString() {

        return ("< " + toString(s) + ", " + toString(p) + ", " + toString(o))
                + (c == null ? "" : ", " + toString(c))
                + (type == null ? "" : " : " + type
                        + (override ? ", override" : ""))
                + (modified ? ", modified" : "") + " >";

    }

    /**
     * Represents the term identifier together with its type (literal, bnode,
     * uri, or statement identifier).
     * 
     * @param iv
     *            The term identifier.
     * @return
     */
    public static String toString(final IV iv) {

        if (iv == null)
            return IRawTripleStore.NULLSTR;

        return iv.toString();
        
    }

    /**
     * Resolves the term identifiers to terms against the store and returns a
     * representation of the statement using
     * {@link IRawTripleStore#toString(IV, IV, IV)}.
     * 
     * @param store
     *            The store (optional). When non-<code>null</code> the store
     *            will be used to resolve term identifiers to terms.
     * 
     * @return The externalized representation of the statement.
     */
    public String toString(final IRawTripleStore store) {
        
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

            return t + (modified ? "(*)" : "") + " : "
                    + store.toString(s, p, o, c);

        } else {
            
            return toString();
            
        }
        
    }

    final public boolean isFullyBound() {
    
        return s != null && p != null && o != null;

    }

    final public StatementEnum getStatementType() {

        return type;

    }

    final public void setStatementType(final StatementEnum type) {
        
        if(this.type != null && this.type != type) {
            
            throw new IllegalStateException("newValue="+type+", spo="+this);
            
        }
        
        this.type = type;
        
    }
    
    final public boolean hasStatementType() {
        
        return type != null;
        
    }

    public boolean isModified() {
        
        return modified;
        
    }

    public void setModified(final boolean modified) {

        this.modified = modified;

    }

}
