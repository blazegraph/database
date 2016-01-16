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
 * Created on Aug 28, 2012
 */
package com.bigdata.rdf.sparql.ast;


/**
 * Type-safe enumeration of the different ways in which we can evaluate a
 * DESCRIBE query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578"> Concise
 *      Bounded Description </a>
 * 
 *      TODO This enumeration does not allow for inverse functional constraints.
 *      That extension can help with foaf data.
 */
public enum DescribeModeEnum {

    /**
     * The DESCRIBE is only the attributes and forward links.
     */
    ForwardOneStep(true/* forward */, false/* reverse */,
            false/* reifiedStatements */, false/* recursive */),
    /**
     * The DESCRIBE is the symmetric one-step neighborhood of the resource
     * (attributes, forward links, and backward links).
     */
    SymmetricOneStep(true/* forward */, true/* reverse */,
            false/* reifiedStatements */, false/* recursive */),
    /**
     * Concise Bounded Description.
     * 
     * <pre>
     *     Given a particular node (the starting node) in a particular RDF
     *     graph (the source graph), a subgraph of that particular graph,
     *     taken to comprise a concise bounded description of the resource
     *     denoted by the starting node, can be identified as follows:
     * 
     *         1. Include in the subgraph all statements in the source graph
     *            where the subject of the statement is the starting node;
     *         
     *         2. Recursively, for all statements identified in the subgraph
     *            thus far having a blank node object, include in the subgraph
     *            all statements in the source graph where the subject of the
     *            statement is the blank node in question and which are not
     *            already included in the subgraph.
     *         
     *         3. Recursively, for all statements included in the subgraph
     *            thus far, for all reifications of each statement in the
     *            source graph, include the concise bounded description
     *            beginning from the rdf:Statement node of each reification.
     * 
     *     This results in a subgraph where the object nodes are either URI
     *     references, literals, or blank nodes not serving as the subject
     *     of any statement [not] in the [sub]graph.
     * </pre>
     * 
     * The text is square brackets is my own (presumed) correction to the
     * definition of Concise Bounded Description and reflects the actual
     * behavior of this mode.
     * 
     * @see <a href="http://www.w3.org/Submission/CBD/"> CBD - Concise Bounded
     *      Description </a>
     */
    CBD(true/* forward */, false/* reverse */, true/* reifiedStatements */,
            true/* recursive */),
    /**
     * Symmetric Concise Bounded Description (SCBD).
     * 
     * <pre>
     * Specifically, given a particular node (the starting node) in a particular
     * RDF graph (the source graph), a subgraph of that particular graph, taken
     * to comprise a symmetric concise bounded description of the resource
     * denoted by the starting node, can be identified as follows:
     * 
     * 1. Include in the subgraph all statements in the source graph where the
     * object of the statement is the starting node;
     * 
     * 2. Recursively, for all statements identified in the subgraph thus far
     * having a blank node subject not equal to the starting node, include in
     * the subgraph all statements in the source graph where the object of the
     * statement is the blank node in question and which are not already
     * included in the subgraph.
     * 
     * 3. Recursively, for all statements included in the subgraph thus far, for
     * all reifications of each statement in the source graph, include the
     * symmetric concise bounded description beginning from the rdf:Statement
     * node of each reification.
     * 
     * 4. Include in the subgraph the concise bounded description beginning from
     * the starting node.
     * 
     * This produces a subgraph that includes a concise bounded description,
     * given the same starting point, but in addition, includes all inbound arc
     * paths, up to but not beyond a URIref subject node.
     * </pre>
     * 
     * Note: Our evaluation actually proceeds in both the forward and reverse
     * direction at the same time. However, this is an efficiency matter and has
     * no effect on the semantics of the description.
     */
    SCBD(true/* forward */, true/* reverse */, true/* reifiedStatements */,
            true/* recursive */);
//    /**
//     * Concise Bounded Description without Reification.
//     * <p>
//     * This is the same as {@link #CBD} except that the source graph is NOT
//     * probed for reifications of statements in the subgraph thus far (step 3).
//     * This option is useful when reified statement models are not common in the
//     * data and it avoids the overhead of testing for a reified statement model
//     * for each statement in the subgraph.
//     */
//    CBDNR(true/* forward */, false/* reverse */, false/* reifiedStatements */,
//            true/* recursive */),
//    /**
//     * Symmetric Concise Bounded Description without Reification.
//     * <p>
//     * This is the same as {@link #SCBD} except that the source graph is NOT
//     * probed for reifications of statements in the subgraph thus far (step 3).
//     * This option is useful when reified statement models are not common in the
//     * data and it avoids the overhead of testing for a reified statement model
//     * for each statement in the subgraph.
//     */
//    SCBDNR(true/* forward */, true/* reverse */, false/* reifiedStatements */,
//            true/* recursive */);

    private final boolean forward;
    private final boolean reverse;
    private final boolean reifiedStatements;
    private final boolean recursive;

    private DescribeModeEnum(final boolean forward, final boolean reverse,
            final boolean reifiedStatements, final boolean recursive) {
        this.forward = forward;
        this.reverse = reverse;
        this.reifiedStatements = reifiedStatements;
        this.recursive = recursive;
    }

    /**
     * Return <code>true</code> if the description includes the non-link
     * attributes and forward links.
     */
    public boolean isForward() {
        return forward;
    }

    /**
     * Return <code>true</code> if the description includes the reverse links.
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Return <code>true</code> if the description includes the description of
     * reified statements found in the description.
     */
    public boolean isReifiedStatements() {
        return reifiedStatements;
    }

    /**
     * Return <code>true</code> if the description includes a recursive
     * expansion.
     */
    public boolean isRecursive() {
        return recursive;
    }
}
