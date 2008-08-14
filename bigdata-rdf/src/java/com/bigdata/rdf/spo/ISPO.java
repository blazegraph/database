/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 19, 2008
 */

package com.bigdata.rdf.spo;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * A interface representing an RDF triple where the slots are 64-bit
 * <code>long</code> term identifiers assigned by a lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISPO {

    /**
     * The constant that indicates an unassigned term identifier.
     * 
     * @see IRawTripleStore#NULL
     */
    long NULL = IRawTripleStore.NULL;
    
    /** The term identifier for the subject position -or- {@link #NULL}. */
    long s();

    /** The term identifier for the predicate position -or- {@link #NULL}. */
    long p();

    /** The term identifier for the object position -or- {@link #NULL}. */
    long o();

    /**
     * Return true iff all position (s,p,o) are non-{@link #NULL}.
     * <p>
     * Note: {@link SPO}s are sometimes used to represent triple patterns,
     * e.g., in the tail of a {@link Justification}. This method will return
     * <code>true</code> if the "triple pattern" is fully bound and
     * <code>false</code> if there are any unbound positions.
     * <p>
     * Note: {@link BigdataStatement}s are not fully bound when they are
     * instantiated during parsing until their term identifiers have been
     * resolved against a database's lexicon.
     */
    boolean isFullyBound();

    /**
     * Whether the statement is {@link StatementEnum#Explicit},
     * {@link StatementEnum#Inferred} or an {@link StatementEnum#Axiom}.
     * 
     * @return The {@link StatementEnum} type -or- <code>null</code> if the
     *         statement type has not been specified.
     */
    StatementEnum getStatementType();
    
    /**
     * Set the statement type for this statement.
     * 
     * @param type
     *            The statement type.
     * 
     * @throws IllegalArgumentException
     *             if <i>type</i> is <code>null</code>.
     * @throws IllegalStateException
     *             if the statement type is already set to a different non-<code>null</code>
     *             value.
     */
    void setStatementType(StatementEnum type);

    /**
     * Return <code>true</code> iff the statement type is known.
     * 
     * <code>true</code> iff the statement type is known for this statement.
     */
    boolean hasStatementType();
    
    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as {@link StatementEnum#Explicit}. 
     */
    boolean isExplicit();
    
    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as {@link StatementEnum#Inferred}. 
     */
    boolean isInferred();
    
    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as {@link StatementEnum#Axiom}. 
     */
    boolean isAxiom();
    
    /**
     * Set the statement identifier.
     * 
     * @param sid
     *            The statement identifier.
     * 
     * @throws IllegalArgumentException
     *             if <i>sid</i> is {@link #NULL}.
     * @throws IllegalStateException
     *             if the statement identifier is already set.
     */
    void setStatementIdentifier(final long sid);
    
    /**
     * The statement identifier (optional). Statement identifiers are a unique
     * per-triple identifier assigned when a statement is first asserted against
     * the database and are are defined iff
     * {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} was specified.
     * 
     * @throws IllegalStateException
     *             unless a statement identifier is assigned to this
     *             {@link ISPO}.
     */
    long getStatementIdentifier();
    
    /**
     * <code>true</code> iff this {@link ISPO} has an assigned statement
     * identifier.
     */
    boolean hasStatementIdentifier();

    /**
     * Set the override flag.
     * 
     * @param override
     *            the new value.
     */
    public void setOverride(boolean override);

    /**
     * When <code>true</code> the statement will be written onto the database
     * with exactly its current {@link #getStatementType()} (default
     * <code>false</code>).
     * <p>
     * Note: This feature is used by {@link TruthMaintenance} when we need to
     * downgrade an {@link ISPO} from "Explicit" to "Inferred". Normally, a
     * statement is automatically upgraded from "Inferred" to "Explicit" so
     * without {@link #setOverride(boolean)} you could not downgrade the
     * {@link StatementEnum} in the database without first deleting the
     * statement (which would also delete its justifications).
     */
    public boolean isOverride();
    
    /**
     * Return the byte[] that would be written into a statement index for this
     * {@link ISPO}, including the optional {@link StatementEnum#MASK_OVERRIDE}
     * bit. If the {@link #getStatementIdentifier()} is non-{@link #NULL} then
     * it will be included in the returned byte[].
     * 
     * @param buf
     *            A buffer supplied by the caller. The buffer will be reset
     *            before the value is written on the buffer.
     * 
     * @return The value that would be written into a statement index for this
     *         {@link ISPO}.
     */
    public byte[] serializeValue(ByteArrayBuffer buf);
    
    /**
     * Method may be used to externalize the {@link BigdataValue}s in the
     * {@link ISPO}.
     * 
     * @param db
     *            The database whose lexicon will be used.
     */
    public String toString(IRawTripleStore db);
    
}
