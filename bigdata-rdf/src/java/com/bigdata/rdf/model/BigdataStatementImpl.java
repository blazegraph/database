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

/**
 * Implementation reveals whether a statement is explicit, inferred, or an axiom
 * and the internal term identifiers for the subject, predicate, object and,
 * when present, the context bound on that statement.
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
    
    /**
     * Assumes that the context is <code>null</code>.
     * 
     * @param subject
     * @param predicate
     * @param object
     * @param type
     */
    public BigdataStatementImpl(BigdataResource subject, BigdataURI predicate,
            BigdataValue object, StatementEnum type) {

        this(subject, predicate, object, null, type);
        
    }

    /**
     * Constructor accepting an optional context.
     * 
     * @param subject
     * @param predicate
     * @param object
     * @param context
     *            (optional)
     * @param type
     *            (optional).
     */
    public BigdataStatementImpl(BigdataResource subject, BigdataURI predicate,
            BigdataValue object, BigdataResource context, StatementEnum type) {

        if (subject == null)
            throw new IllegalArgumentException();
        if (predicate == null)
            throw new IllegalArgumentException();
        if (object==null) throw new IllegalArgumentException();
        
//        if (type == null)
//            throw new IllegalArgumentException();
        
        this.s = subject;

        this.p = predicate;
        
        this.o = object;
        
        this.c = context;

        this.type = type;
        
    }

    public BigdataResource getSubject() {

        return s;
        
    }
    
    public BigdataURI getPredicate() {

        return p;
        
    }

    public BigdataValue getObject() {
     
        return o;
        
    }

    public BigdataResource getContext() {
        
        return c;
        
    }
    
    public StatementEnum getStatementType() {
        
        return type;
        
    }
 
    public void setStatementType(StatementEnum type) {
        
        if (type == null) {
        
            throw new IllegalArgumentException();
            
        }
        
        if (this.type != null && type != this.type) {
        
            throw new IllegalStateException();
            
        }
        
        this.type = type;
        
    }

    public boolean isAxiom() {
        
        return StatementEnum.Axiom == type;
        
    }
    
    public boolean isInferred() {
        
        return StatementEnum.Inferred == type;
        
    }
        
    public boolean isExplicit() {
        
        return StatementEnum.Explicit == type;
        
    }

    public boolean equals(Object o) {
    
        return equals((Statement)o);
        
    }

    /*
     * Note: implementation per Statement interface.
     */
    public boolean equals(Statement stmt) {

        return s.equals(stmt.getSubject()) && //
               p.equals(stmt.getPredicate()) && //
               o.equals(stmt.getObject())//
        ;
        
    }
    
    /*
     * Note: implementation per Statement interface.
     */
    public int hashCode() {
        
        return 961 * s.hashCode() + 31 * p.hashCode() + o.hashCode();
    
    }
    
    public String toString() {
        
        return "<"+s+", "+p+", "+o+(c==null?"":", "+c)+">"+(type==null?"":" : "+type);
        
    }

}
