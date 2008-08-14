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
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Implementation reveals whether a statement is explicit, inferred, or an axiom
 * and the internal term identifiers for the subject, predicate, object, the
 * context bound on that statement (when present). When statement identifiers
 * are enabled, the context position (if bound) will be a blank node that
 * represents the statement having that subject, predicate, and object.
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

    private final BigdataResource s;
    private final BigdataURI p;
    private final BigdataValue o;
    private final BigdataResource c;
    private StatementEnum type;
    private transient boolean override = false;
    
    /**
     * Used by {@link BigdataValueFactory}
     */
    protected BigdataStatementImpl(BigdataResource subject, BigdataURI predicate,
            BigdataValue object, BigdataResource context, StatementEnum type) {

        if (subject == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (object == null)
            throw new IllegalArgumentException();
        
//        if (type == null)
//            throw new IllegalArgumentException();
        
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
 
    final public void setStatementType(StatementEnum type) {
        
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

    public boolean equals(Object o) {
    
        return equals((Statement)o);
        
    }

    /**
     * Note: implementation per {@link Statement} interface.
     */
    final public boolean equals(Statement stmt) {

        return s.equals(stmt.getSubject()) && //
               p.equals(stmt.getPredicate()) && //
               o.equals(stmt.getObject())//
        ;
        
    }
    
    /*
     * Note: implementation per Statement interface.
     */
    final public int hashCode() {
        
        return 961 * s.hashCode() + 31 * p.hashCode() + o.hashCode();
    
    }
    
    public String toString() {
        
        return "<"+s+", "+p+", "+o+(c==null?"":", "+c)+">"+(type==null?"":" : "+type);
        
    }

    final public long s() {

        return s.getTermId();
        
    }

    final public long p() {
        
        return p.getTermId();
        
    }

    final public long o() {
        
        return o.getTermId();
        
    }
    
    final public boolean isFullyBound() {
        
        return s() != NULL && p() != NULL && o() != NULL;

    }

    public final void setStatementIdentifier(final long sid) {

        if (sid == NULL)
            throw new IllegalArgumentException();

        if (c == null && c.getTermId() != sid)
            throw new IllegalStateException(
                    "Different statement identifier already defined: "
                            + toString() + ", new=" + sid);

        if( type != StatementEnum.Explicit)  {

            // Only allowed for explicit statements.
            throw new IllegalStateException();
            
        }

        if (!AbstractTripleStore.isStatement(sid))
            throw new IllegalArgumentException("Not a statement identifier: "
                    + sid);

        c.setTermId(sid);

    }

    public final long getStatementIdentifier() {

        if (c == null || c.getTermId() == NULL)
            throw new IllegalStateException("No statement identifier: "
                    + toString());

        return c.getTermId();

    }
    
    final public boolean hasStatementIdentifier() {
        
        return c != null && c.getTermId() != NULL;
        
    }

    public final boolean isOverride() {
        
        return override;
        
    }

    public final void setOverride(boolean override) {
        
        this.override = override;
        
    }
    
    public byte[] serializeValue(ByteArrayBuffer buf) {

        return SPO.serializeValue(buf, isOverride(), type,
                (hasStatementIdentifier() ? getStatementIdentifier() : NULL));
        
    }

    /**
     * Note: this implementation is equivilent to {@link #toString()} since the
     * {@link Value}s are already resolved.
     */
    public String toString(IRawTripleStore storeIsIgnored) {
        
        return toString();
        
    }
    
}
