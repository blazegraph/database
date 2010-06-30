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

import org.openrdf.model.Value;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * A interface representing an RDF triple, an RDF triple with a statement
 * identifier, or an RDF quad. The slots are 64-bit <code>long</code> term
 * identifiers assigned by a lexicon. The 4th position is either unused
 * (triples), the statement identifier (triples with the provenance mode
 * enabled), or the context/named graph position of a quad. This interface
 * treats all four positions as "data" and requires the caller to be aware of
 * the database mode (triples, triples+SIDs, or quads). The (s,p,o) of the
 * interface are immutable. The (c) position is mutable because its value is not
 * knowable until after the other three values have been bound in the
 * triples+SIDs database mode. When using this interface for a quads mode
 * database, the context position SHOULD be set by the appropriate constructor
 * and NOT modified thereafter.
 * <p>
 * Two additional data are carried by this interface for use with inference and
 * truth maintenance. First, this interface may also carry an indication of
 * whether the triple/quad is an explicit statement, an inferred statement or an
 * axiom. Second, the {@link #isOverride()} flag is used during truth
 * maintenance when an explicit statement is retracted and we need to downgrade
 * the statement in the database to an inference because it is still provable by
 * other statements in the knowledge base.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISPO {

    /**
     * The term identifier for the subject position (slot 0) -or- {@link #NULL}.
     */
    IV s();

    /**
     * The term identifier for the predicate position (slot 1) -or-
     * {@link #NULL}.
     */
    IV p();

    /** The term identifier for the object position (slot 2) -or- {@link #NULL}. */
    IV o();

    /**
     * The term identifier for the SID/context position (slot 3) -or-
     * {@link #NULL}. The semantics of the returned value depend on the database
     * mode. For triples, it is unused. For triples+SIDs, it is the statement
     * identifier as assigned by the lexicon. For quads, it is the context (aka
     * named graph) and {@link #NULL} iff the context was not bound.
     * 
     * @see AbstractTripleStore.Options#STATEMENT_IDENTIFIERS
     * @see AbstractTripleStore.Options#QUADS
     */
    IV c();

    /**
     * Return the s,p,o, or c value corresponding to the given index.
     * 
     * @param index
     *            The legal values are: s=0, p=1, o=2, c=3.
     */
    IV get(int index);

    /**
     * Return true iff all position (s,p,o) are non-{@link #NULL}.
     * <p>
     * Note: {@link SPO}s are sometimes used to represent triple patterns, e.g.,
     * in the tail of a {@link Justification}. This method will return
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
     *             if the statement type is already set to a different non-
     *             <code>null</code> value.
     */
    void setStatementType(StatementEnum type);

    /**
     * Return <code>true</code> iff the statement type is known.
     * 
     * <code>true</code> iff the statement type is known for this statement.
     */
    boolean hasStatementType();

    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as
     * {@link StatementEnum#Explicit}.
     */
    boolean isExplicit();

    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as
     * {@link StatementEnum#Inferred}.
     */
    boolean isInferred();

    /**
     * Return <code>true</code> IFF the {@link SPO} is marked as
     * {@link StatementEnum#Axiom}.
     */
    boolean isAxiom();

    /**
     * Set the statement identifier. This sets the 4th position of the quad, but
     * some constraints are imposed on its argument.
     * 
     * @param sid
     *            The statement identifier.
     * 
     * @throws IllegalArgumentException
     *             if <i>sid</i> is {@link #NULL}.
     * @throws IllegalStateException
     *             if the statement identifier is already set.
     */
    void setStatementIdentifier(final IV sid);

    /**
     * The statement identifier (optional). This has nearly identical semantics
     * to {@link #c()}, but will throw an exception if the 4th position is not
     * bound.
     * <p>
     * Statement identifiers are a unique per-triple identifier assigned when a
     * statement is first asserted against the database and are are defined iff
     * {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS} was specified.
     * 
     * @throws IllegalStateException
     *             unless a statement identifier is assigned to this
     *             {@link ISPO}.
     */
    IV getStatementIdentifier();

    /**
     * <code>true</code> IFF {@link AbstractTripleStore#isStatement(IV)}
     * returns <code>true</code> for {@link #c()}.
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
     * Set a transient flag indicating whether or not the persistent state of
     * the statement was modified when it was last written onto the database.
     * Modification can indicate that the statement was inserted, retracted, or
     * had its associated {@link StatementEnum} in the database updated.
     */
    public void setModified(boolean modified);

    /**
     * Return the state of the transient <i>modified</i> flag. This flag
     * indicates whether or not the persistent state of the statement was
     * modified when it was written onto the database. Modification can indicate
     * that the statement was inserted, retracted, or had its associated
     * {@link StatementEnum} in the database updated.
     * 
     * @todo This flag is set by
     *       {@link SPORelation#insert(ISPO[], int, IElementFilter)} and
     *       {@link SPORelation#delete(IChunkedOrderedIterator)}. The state of
     *       this flag generally DOES NOT survive across higher level APIs such
     *       as the {@link StatementBuffer} because they formulate new
     *       {@link ISPO} objects which are distinct from the caller's
     *       {@link ISPO} objects.
     *       <p>
     *       In order to use take advantage of this information right now, you
     *       should batch resolve the RDF values to their term identifiers,
     *       construct the appropriate {@link SPO}[], and invoke the
     *       corresponding method on {@link SPORelation}.
     *       <p>
     *       Because this information is set at a low-level it can not currently
     *       be used in combination with truth maintenance mechanisms.
     */
    public boolean isModified();

    /**
     * Return the byte[] that would be written into a statement index for this
     * {@link ISPO}, including the optional {@link StatementEnum#MASK_OVERRIDE}
     * bit. If the {@link #hasStatementIdentifier()} would return
     * <code>true</code>, then the SID will be included in the returned byte[].
     * Note that {@link #hasStatementIdentifier()} is defined in terms of the
     * bit pattern of the SID identifiers and therefore will be
     * <code>true</code> ONLY for a statement identifier and NOT for an RDF
     * {@link Value} identifier.
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
