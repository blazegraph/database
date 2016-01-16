/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

/**
 * Typesafe enumeration for SPARQL Graph Update and Graph Management operations.
 * 
 * @see http://www.w3.org/TR/sparql11-update/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum UpdateType {

    /*
     * Graph Management.
     */
    /**
     * This operation creates a graph in the Graph Store (this operation is a
     * NOP for bigdata).
     * 
     * <pre>
     * CREATE ( SILENT )? GRAPH IRIref
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#create
     * @see CreateGraph
     */
    Create(false/* graphUpdate */), //
    /**
     * The DROP operation removes the specified graph(s) from the Graph Store.
     * 
     * <pre>
     * DROP  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
     * </pre>
     * 
     * Note: Bigdata does not support empty graphs, so {@link #Clear} and
     * {@link #Drop} have the same semantics.
     * 
     * @see http://www.w3.org/TR/sparql11-update/#drop
     * @see DropGraph
     */
    Drop(false/* graphUpdate */), //
    /**
     * The COPY operation is a shortcut for inserting all data from an input
     * graph into a destination graph. Data from the input graph is not
     * affected, but data from the destination graph, if any, is removed before
     * insertion.
     * 
     * <pre>
     * COPY ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT )
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#copy
     * @see CopyGraph
     */
    Copy(false/* graphUpdate */), //
    /**
     * The MOVE operation is a shortcut for moving all data from an input graph
     * into a destination graph. The input graph is removed after insertion and
     * data from the destination graph, if any, is removed before insertion.
     * 
     * <pre>
     * MOVE (SILENT)? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT)
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#move
     * @see MoveGraph
     */
    Move(false/* graphUpdate */), //
    /**
     * Graph management operation inserts all data from one graph into another.
     * Data from the source graph is not effected. Data already present in the
     * target graph is not effected.
     * 
     * <pre>
     * ADD ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT)
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#add
     * @see AddGraph
     */
    Add(false/* graphUpdate */), //
    /*
     * Graph Update.
     */
    /**
     * The INSERT DATA operation adds some triples, given inline in the request,
     * into the Graph Store:
     * 
     * <pre>
     * INSERT DATA  QuadData
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#insertData
     * @see InsertData
     */
    InsertData(false/* graphUpdate */, true/* dataOnly */), //
    /**
     * The DELETE DATA operation removes some triples, given inline in the
     * request, if the respective graphs in the Graph Store contain those:
     * 
     * <pre>
     * DELETE DATA  QuadData
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#deleteData
     * @see DeleteData
     */
    DeleteData(false/* graphUpdate */, true/* dataOnly */), //
    /**
     * The DELETE/INSERT operation can be used to remove or add triples from/to
     * the Graph Store based on bindings for a query pattern specified in a
     * WHERE clause:
     * 
     * <pre>
     * ( WITH IRIref )?
     * ( ( DeleteClause InsertClause? ) | InsertClause )
     * ( USING ( NAMED )? IRIref )*
     * WHERE GroupGraphPattern
     * </pre>
     * 
     * The DeleteClause and InsertClause forms can be broken down as follows:
     * 
     * <pre>
     * DeleteClause ::= DELETE  QuadPattern 
     * InsertClause ::= INSERT  QuadPattern
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#deleteInsert
     * @see DeleteInsertGraph
     */
    DeleteInsert(false/* graphUpdate */), //
    /**
     * The LOAD operation reads an RDF document from a IRI and inserts its
     * triples into the specified graph in the Graph Store.
     * 
     * <pre>
     * LOAD ( SILENT )? IRIref_from ( INTO GRAPH IRIref_to )?
     * </pre>
     * 
     * @see http://www.w3.org/TR/sparql11-update/#load
     * @see LoadGraph
     */
    Load(false/* graphUpdate */), //
    /**
     * The CLEAR operation removes all the triples in the specified graph(s) in
     * the Graph Store.
     * 
     * <pre>
     * CLEAR  ( SILENT )? (GRAPH IRIref | DEFAULT | NAMED | ALL )
     * </pre>
     * 
     * Note: Bigdata does not support empty graphs, so {@link #Clear} and
     * {@link #Drop} have the same semantics.
     * 
     * @see http://www.w3.org/TR/sparql11-update/#clear
     * @see ClearGraph
     */
    Clear(false/* graphUpdate */), 
    
    /**
     * The DROP ENTAILMENTS operation drop the entailments.
     * This is only required if you have removed some statements from the database. 
     */
    DropEntailments(false/* graphUpdate */),
    
    /**
     * The CREATE ENTAILMENTS operation compute the entailments 
     * using an efficient "database-at-once" closure operation. 
     */
    CreateEntailments(false/* graphUpdate */), 
    
    /**
     * The DISABLE ENTAILMENTS operation can be used 
     * to disable incremental truth maintenance.
     */
    DisableEntailments(false/* graphUpdate */),
    
    /**
     * The ENABLE ENTAILMENTS operation can be used
     * to enable incremental truth maintenance.
     */
    EnableEntailments(false/* graphUpdate */);

    private UpdateType(final boolean graphUpdate) {

        this(graphUpdate, false/* dataOnly */);

    }

    private UpdateType(final boolean graphUpdate, final boolean dataOnly) {

        this.graphUpdate = graphUpdate;

        this.dataOnly = dataOnly;

    }

    private final boolean graphUpdate;

    private final boolean dataOnly;

    /**
     * Return <code>true</code> iff this is a graph update operation.
     */
    public boolean isGraphUpdate() {
        return graphUpdate;
    }

    /**
     * Return <code>true</code> iff this is a graph management operation.
     */
    public boolean isGraphManagement() {
        return !graphUpdate;
    }

    /**
     * Return <code>true</code> iff this is a data only graph update operation.
     */
    public boolean isDataOnly() {

        return graphUpdate && dataOnly;

    }

}
