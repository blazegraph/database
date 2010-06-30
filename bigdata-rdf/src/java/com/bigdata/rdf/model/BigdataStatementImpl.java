/*

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
package com.bigdata.rdf.model;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Implementation reveals whether a statement is explicit, inferred, or an axiom
 * and the internal term identifiers for the subject, predicate, object, the
 * context bound on that statement (when present). When statement identifiers
 * are enabled, the context position (if bound) will be a blank node that
 * represents the statement having that subject, predicate, and object and its
 * term identifier, when assigned, will report <code>true</code> for
 * {@link AbstractTripleStore#isStatement(IV)}. When used to model a quad, the
 * 4th position will be a {@link BigdataValue} but its term identifier will
 * report <code>false</code> for {@link AbstractTripleStore#isStatement(IV)}.
 * <p>
 * Note: The ctors are intentionally protected. Use the
 * {@link BigdataValueFactory} to create instances of this class - it will
 * ensure that term identifiers are propagated iff the backing lexicon is the
 * same.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataStatementImpl implements BigdataStatement {

    /**
     * 
     */
    private static final long serialVersionUID = 6739949195958368365L;

    protected final BigdataResource s;
    protected final BigdataURI p;
    protected final BigdataValue o;
    protected final BigdataResource c;
    private StatementEnum type;
    private transient boolean override = false;
    private transient boolean modified = false;
    
    /**
     * Used by {@link BigdataValueFactory}
     */
    public BigdataStatementImpl(final BigdataResource subject,
            final BigdataURI predicate, final BigdataValue object,
            final BigdataResource context, final StatementEnum type) {

        if (subject == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (object == null)
            throw new IllegalArgumentException();
        
        // Note: context MAY be null
        
        // Note: type MAY be null.
        
        this.s = subject;

        this.p = predicate;
        
        this.o = object;
        
        this.c = context;

        this.type = type;
        
    }

    final public BigdataResource getSubject() {

        return s;
        
    }
    
    final public BigdataURI getPredicate() {

        return p;
        
    }

    final public BigdataValue getObject() {
     
        return o;
        
    }

    final public BigdataResource getContext() {
        
        return c;
        
    }
    
    final public boolean hasStatementType() {
        
        return type != null;
        
    }

    final public StatementEnum getStatementType() {
        
        return type;
        
    }
 
    final public void setStatementType(final StatementEnum type) {
        
        if (type == null) {
        
            throw new IllegalArgumentException();
            
        }
        
        if (this.type != null && type != this.type) {
        
            throw new IllegalStateException();
            
        }
        
        this.type = type;
        
    }

    final public boolean isAxiom() {
        
        return StatementEnum.Axiom == type;
        
    }
    
    final public boolean isInferred() {
        
        return StatementEnum.Inferred == type;
        
    }
        
    final public boolean isExplicit() {
        
        return StatementEnum.Explicit == type;
        
    }

    public boolean equals(final Object o) {
    
        return equals((Statement)o);
        
    }

    /**
     * Note: implementation per {@link Statement} interface, which specifies
     * that only the (s,p,o) positions are to be considered.
     */
    public boolean equals(final Statement stmt) {

        return s.equals(stmt.getSubject()) && //
               p.equals(stmt.getPredicate()) && //
               o.equals(stmt.getObject())//
        ;
        
    }

    /**
     * Note: implementation per Statement interface, which does not consider the
     * context position.
     */
    final public int hashCode() {
        
        if (hash == 0) {

            hash = 961 * s.hashCode() + 31 * p.hashCode() + o.hashCode();

        }
        
        return hash;
    
    }
    private int hash = 0;
    
    public String toString() {
        
        return "<" + s + ", " + p + ", " + o + (c == null ? "" : ", " + c)
                + ">" + (type == null ? "" : " : " + type)
                + (modified ? " : modified" : "");

    }

    final public IV s() {

        return s.getIV();
        
    }

    final public IV p() {
        
        return p.getIV();
        
    }

    final public IV o() {
        
        return o.getIV();
        
    }
    
    final public IV c() {

        if (c == null)
            return NULL;
        
        return c.getIV();
        
    }

    public IV get(final int index) {

        switch (index) {
        case 0:
            return s.getIV();
        case 1:
            return p.getIV();
        case 2:
            return o.getIV();
        case 3: // 4th position MAY be unbound.
            return (c == null) ? NULL : c.getIV();
        default:
            throw new IllegalArgumentException();
        }

    }
    
    final public boolean isFullyBound() {
        
        return s() != NULL && p() != NULL && o() != NULL;

    }

    public final void setStatementIdentifier(final IV sid) {

        if (sid == NULL)
            throw new IllegalArgumentException();

        if (!AbstractTripleStore.isStatement(sid))
            throw new IllegalArgumentException("Not a statement identifier: "
                    + sid);

        if (type != StatementEnum.Explicit) {

            // Only allowed for explicit statements.
            throw new IllegalStateException();

        }

        if (c != null && c.getIV() != sid)
            throw new IllegalStateException(
                    "Different statement identifier already defined: "
                            + toString() + ", new=" + sid);

        c.setIV(sid);

    }

    public final IV getStatementIdentifier() {

        if (!hasStatementIdentifier())
            throw new IllegalStateException("No statement identifier: "
                    + toString());

        return c.getIV();

    }
    
    final public boolean hasStatementIdentifier() {
        
        return c != null && AbstractTripleStore.isStatement(c.getIV());
        
    }

    public final boolean isOverride() {
        
        return override;
        
    }

    public final void setOverride(final boolean override) {
        
        this.override = override;
        
    }

    public byte[] serializeValue(final ByteArrayBuffer buf) {

        return SPO.serializeValue(buf, override, type, c != null ? c
                .getIV() : NULL);

    }

    /**
     * Note: this implementation is equivalent to {@link #toString()} since the
     * {@link Value}s are already resolved.
     */
    public String toString(final IRawTripleStore storeIsIgnored) {
        
        return toString();
        
    }
    
    public boolean isModified() {
        
        return modified;
        
    }

    public void setModified(final boolean modified) {

        this.modified = modified;

    }

}
