/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Query hints are identified applied to AST nodes based on the specified scope
 * and the location within the AST in which they are found. Query hints
 * recognized by this optimizer have the form:
 * 
 * <pre>
 * scopeURL propertyURL value
 * </pre>
 * 
 * Where <i>scope</i> is any of the {@link QueryHintScope}s; and <br/>
 * Where <i>propertyURL</i> identifies the query hint;<br/>
 * Where <i>value</i> is a literal.
 * <p>
 * Once recognized, query hints are removed from the AST. All query hints are
 * declared internally using interfaces. It is an error if the specified query
 * hint can not be identified via reflection.
 * <p>
 * For example:
 * 
 * <pre>
 * PREFIX hint: <http://www.bigdata.com/queryHints#>
 * ...
 * {
 *    # query hint binds for this join group.
 *    hint:group hint:com.bigdata.bop.PipelineOp.maxParallel 10
 *    
 *    ...
 *    
 *    # query hint binds for the next basic graph pattern in this join group.
 *    hint:bgp hint:com.bigdata.relation.accesspath.IBuffer.chunkCapacity 100
 *    
 *    ?x rdf:type foaf:Person .
 * }
 * </pre>
 * 
 * @see QueryHints
 * @see QueryHintScope
 * 
 *      TODO It should be possible to specify query hints which effect both
 *      query evaluation (pipeline ops) and query compilation.
 * 
 *      TODO We should only apply a query hint to a node for which it has
 *      semantics. For example, {@link QueryHints#QUERYID} only has semantics
 *      for a {@link QueryRoot}.
 * 
 *      TODO Review the existing optimizers to make sure that they propagate
 *      query hints as well as other annotations when creating a new node. I
 *      think that the {@link ASTSparql11SubqueryOptimizer} probably fails to do
 *      this when it extracts a {@link NamedSubqueryRoot}.
 * 
 *      TODO The SPARQL parser should recognize query hints when they appear
 *      within a SPARQL comment (a line whose first non-whitespace character is
 *      "#"). That would let people run the same SPARQL queries against
 *      non-bigdata end points. This would require a look ahead mechanism in the
 *      parser for the comment lines to decide whether or not they were query
 *      hints. Perhaps we could use something like "#bigdata#" at the start of
 *      such comment lines? That token would simply be stripped and the rest of
 *      the apparent "comment line" would be interpreted as a basic statement
 *      pattern which *should* be a query hint.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTQueryHintOptimizer implements IASTOptimizer {

    private static final Logger log = Logger.getLogger(ASTQueryHintOptimizer.class);
    
    @SuppressWarnings("unchecked")
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        if (context.queryHints != null && !context.queryHints.isEmpty()) {

            // Apply any given query hints globally.
            applyGlobalQueryHints(queryRoot, context.queryHints);
            
        }
        
        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    processGroup(context, queryRoot, namedSubquery.getWhereClause());

                }

            }

        }
        
        // Now process the main where clause.
        processGroup(context, queryRoot, queryRoot.getWhereClause());

        return queryNode;

    }

    /**
     * Note: We only need to do this for those AST nodes which can interpret
     * query hints. Right now, this is just the statement pattern nodes. If that
     * changes, then we need to annotate more of the AST Nodes and this method
     * will need to be modified.
     * 
     * @param op
     * @return
     */
    private boolean isNodeAcceptingQueryHints(final BOp op) {
        if (op instanceof StatementPatternNode) {
            return true;
        }
        return false;
    }
    
    /**
     * Applies the global query hints to each node in the query.
     * 
     * @param queryRoot
     * @param queryHints
     */
    private void applyGlobalQueryHints(final QueryRoot queryRoot,
            final Properties queryHints) {

        if (queryHints == null || queryHints.isEmpty())
            return;
        
        validateQueryHints(queryHints);
        
        final Iterator<BOp> itr = BOpUtility
                .preOrderIteratorWithAnnotations(queryRoot);

        // Note: working around a ConcurrentModificationException.
        final List<ASTBase> list = new LinkedList<ASTBase>();
        
        while (itr.hasNext()) {

            final BOp op = itr.next();

            if (!isNodeAcceptingQueryHints(op))
                continue;

            /*
             * Set a properties object on each AST node. The properties object
             * for each AST node will use the global query hints for its
             * defaults so it can be overridden selectively.
             */

            final ASTBase t = (ASTBase) op;

            list.add(t);
            
        }
        
        for(ASTBase t : list) {
            
            if (t.getProperty(ASTBase.Annotations.QUERY_HINTS) != null) {
                /*
                 * There should not be any query hints applied yet.
                 */
                throw new AssertionError("Query hints are already present: "
                        + t);
            }

            t.setProperty(ASTBase.Annotations.QUERY_HINTS, new Properties(
                    queryHints));
            
        }
        
    }

    /**
     * Validate the global query hints.
     * 
     * @param queryHints
     */
    private void validateQueryHints(Properties queryHints) {

        final Enumeration<?> e = queryHints.propertyNames();

        while (e.hasMoreElements()) {

            final String name = (String) e.nextElement();

            final String value = queryHints.getProperty(name);

            validateQueryHint(name, value);
            
        }
        
    }

    /**
     * Recursively process a join group, applying any query hints found and
     * removing them from the join group.
     * 
     * @param context
     * @param queryRoot
     *            The {@link QueryRoot}.
     * @param group
     *            The join group.
     */
    @SuppressWarnings("unchecked")
    private void processGroup(final AST2BOpContext context, final QueryRoot queryRoot, 
            final GraphPatternGroup<IGroupMemberNode> group) {

        if(group == null)
            return;
        
        /*
         * Note: The loop needs to be carefully written as query hints will be
         * removed after they are processed. This can either be done on a hint
         * by hint basis or after we have processed all hints in the group. It
         * is easier to make the latter robust, so this uses a second pass over
         * each group in which a query hint was identified to remove the query
         * hints once they have been interpreted.
         */
        
        /*
         * Identify and interpret query hints.
         */
        // #of query hints found in this group.
        int nfound = 0;
        {

            final int arity = group.arity();
            
            // The last statement pattern node which was not a query hint. 
            StatementPatternNode lastSP = null;

            for (int i = 0; i < arity; i++) {
            
                final IGroupMemberNode child = (IGroupMemberNode) group.get(i);

                /*
                 * Recursion.
                 */
                if(child instanceof GraphPatternGroup<?>) {
                    processGroup(context, queryRoot,
                            (GraphPatternGroup<IGroupMemberNode>) child);
                } else if (child instanceof SubqueryRoot) {
                    processGroup(context, queryRoot,
                            ((SubqueryRoot) child).getWhereClause());
                } else if (child instanceof ServiceNode) {
                    processGroup(context, queryRoot,
                            ((ServiceNode) child).getGroupNode());
                }
                
                if (!(child instanceof StatementPatternNode))
                    continue;

                final StatementPatternNode sp = (StatementPatternNode) child;

                if(!isQueryHint(sp)) {
                    lastSP = sp;
                    continue;
                }

                applyQueryHint(context, queryRoot, group, lastSP, sp);
                
                nfound++;

            }
            
        }

        /*
         * Remove the query hints from this group.
         */
        if (nfound > 0) {
            
            for (int i = 0; i < group.arity(); ) {

                final IGroupMemberNode child = (IGroupMemberNode) group.get(i);

                if (!(child instanceof StatementPatternNode))
                    continue;

                final StatementPatternNode sp = (StatementPatternNode) child;

                if (isQueryHint(sp)) {
                    // Remove and redo the loop.
                    group.removeArg(sp);
                    continue;
                }
                
                // Next child.
                i++;
                
            }

        }
        
    }

    private boolean isQueryHint(final StatementPatternNode sp) {
        
        if(!(sp.p() instanceof ConstantNode))
            return false;
        
        final BigdataValue p = ((ConstantNode) sp.p()).getValue();
        
        if(!(p instanceof URI))
            return false;
        
        final BigdataURI u = (BigdataURI)p;
        
        return u.getNamespace().equals(QueryHints.NAMESPACE);

    }
    
    /**
     * Extract and return the {@link QueryHintScope}.
     * 
     * @param t
     *            The subject position of the query hint
     *            {@link StatementPatternNode}.
     * 
     * @return The {@link QueryHintScope}.
     * 
     * @throws RuntimeException
     *             if something goes wrong.
     * @throws IllegalArgumentException
     *             if something goes wrong.
     */
    private QueryHintScope getScope(final TermNode t) {

        if(!(t instanceof ConstantNode))
            throw new RuntimeException(
                    "Subject position of query hint must be a constant.");

        final BigdataValue v = ((ConstantNode) t).getValue();

        if (!(v instanceof BigdataURI))
            throw new RuntimeException("Query hint scope is not a URI.");

        final BigdataURI u = (BigdataURI) v;

        return QueryHintScope.valueOf(u);

    }

    /**
     * Extract, validate, and return the name of the query hint property.
     * 
     * @param t
     *            The predicate position of the query hint
     *            {@link StatementPatternNode}.
     * 
     * @return The name of the query hint property.
     */
    private String getName(final TermNode t) {

        if (!(t instanceof ConstantNode)) {
            throw new RuntimeException(
                    "Predicate position of query hint must be a constant.");
        }

        final BigdataValue v = ((ConstantNode) t).getValue();

        if (!(v instanceof BigdataURI))
            throw new RuntimeException("Predicate position of query hint is not a URI.");

        final BigdataURI u = (BigdataURI) v;

        final String name = u.getLocalName();
        
        return name;
        
    }
    
    /**
     * Validates the query hint.
     * 
     * @param name
     *            The name of the query hint.
     * @param value
     *            The value for the query hint.
     * 
     * @throws RuntimeException
     *             if the query hint could not be validated.
     * 
     *             TODO Query hints often have a base name which is NOT the name
     *             of the declaring interface. Therefore, maybe the best way to
     *             control all of this is to have a separate registry for
     *             declaring query hints. Query hints could then be verified
     *             against that registry. This would also let us control what
     *             people are able to override.
     */
    private void validateQueryHint(final String name, final String value) {

//        final int pos = name.lastIndexOf('.');
//
//        if (pos == -1)
//            throw new RuntimeException("Badly formed query hint name: " + name);
//
//        // The name of the class or interface which declares that query hint.
//        final String className = name
//                .substring(0/* beginIndex */, pos/* endIndex */);
//
//        // The name of the field on the class or interface for the query hint.
//        final String fieldName = name.substring(pos + 1);
//
//        final Class<?> cls;
//        try {
//            cls = Class.forName(className);
//        } catch (ClassNotFoundException e) {
//            log.error(e);
//            return;
//        }
//
//        final Field f;
//        try {
//            f = cls.getField(fieldName);
//        } catch (SecurityException e) {
//            log.error(e);
//        } catch (NoSuchFieldException e) {
//            log.error(e);
//        }
        
    }

    /**
     * Return the value for the query hint.
     * 
     * @param t
     * @return
     */
    private String getValue(final TermNode t) {

        if (!(t instanceof ConstantNode)) {
            throw new RuntimeException(
                    "Object position of query hint must be a constant.");
        }

        final BigdataValue v = ((ConstantNode) t).getValue();

        if (!(v instanceof Literal))
            throw new RuntimeException(
                    "Object position of query hint is not a Literal.");

        final Literal lit = (Literal) v;
        
        return lit.stringValue();
        
    }
    
    /**
     * @param context
     * @param queryRoot
     * @param group
     * @param lastSP
     * @param s The subject position of the query hint.
     * @param o The object position of the query hint.
     */
    private void applyQueryHint(//
            final AST2BOpContext context,//
            final QueryRoot queryRoot,//
            final GraphPatternGroup<IGroupMemberNode> group,//
            final StatementPatternNode lastSP, // MAY be null IFF scope != BGP
            final StatementPatternNode hint //
            ) {
        
        if(context == null)
            throw new IllegalArgumentException();
        if(queryRoot == null)
            throw new IllegalArgumentException();
        if(group == null)
            throw new IllegalArgumentException();
        if(hint == null)
            throw new IllegalArgumentException();

        final QueryHintScope scope = getScope(hint.s());

        final String name = getName(hint.p());
        
        final String value = getValue(hint.o());
        
        validateQueryHint(name, value);
        
        switch (scope) {
        case Query: {
            applyToQuery(context,queryRoot,name,value);
            break;
        }
        case SubQuery: {
            applyToSubQuery(group, name, value);
            break;
        }
        case Group: {
            applyToGroup(group,name,value);
            break;
        }
        case GroupAndSubGroups: {
            applyToGroupAndSubGroups(group,name,value);
            break;
        }
        case BGP: {
            if (scope == QueryHintScope.BGP && lastSP == null) {
                throw new RuntimeException(
                        "Query hint with BGP scope must follow the statement pattern to which it will bind.");
            }
            // Apply to just the last SP.
            ((ASTBase) lastSP).setQueryHint(name, value);
            break;
        }
        default:
            throw new UnsupportedOperationException("Unknown scope: " + scope);
        }

    }

    /**
     * @param group
     * @param name
     * @param value
     */
    private void applyToSubQuery(GraphPatternGroup<IGroupMemberNode> group,
            String name, String value) {
        
        /*
         * Find the top-level parent group for the given group.
         */
        GraphPatternGroup<IGroupMemberNode> parent = group;
        
        while(parent != null) {

            group = parent;

            parent = parent.getParentGraphPatternGroup();
            
        }
        
        // Apply to all child groups of that top-level group.
        applyToGroupAndSubGroups(group, name, value);
        
    }

    /**
     * Applies the query hint to the entire query.
     * 
     * @param queryRoot
     * @param name
     * @param value
     */
    private void applyToQuery(final AST2BOpContext context,
            final QueryRoot queryRoot, final String name, String value) {

        if (context.queryHints == null) {
            /*
             * Also stuff the query hint on the global context for things which
             * look there.
             */
            context.queryHints.setProperty(name, value);
        }
        
        final Iterator<BOp> itr = BOpUtility
                .preOrderIteratorWithAnnotations(queryRoot);

        // Note: working around a ConcurrentModificationException.
        final List<ASTBase> list = new LinkedList<ASTBase>();
        
        while (itr.hasNext()) {

            final BOp op = itr.next();

            if (!isNodeAcceptingQueryHints(op))
                continue;

            /*
             * Set a properties object on each AST node. The properties object
             * for each AST node will use the global query hints for its
             * defaults so it can be overridden selectively.
             */

            final ASTBase t = (ASTBase) op;

            list.add(t);
            
        }
        
        for(ASTBase t : list) {

            t.setQueryHint(name, value);
            
        }
        
    }

    /**
     * Apply the query hint to the group and, recursively, to any sub-groups.
     * 
     * @param group
     * @param name
     * @param value
     */
    @SuppressWarnings("unchecked")
    private void applyToGroupAndSubGroups(
            GraphPatternGroup<IGroupMemberNode> group, String name, String value) {

        for (IGroupMemberNode child : group) {

            ((ASTBase) child).setQueryHint(name, value);

            if (child instanceof GraphPatternGroup<?>) {

                applyToGroupAndSubGroups(
                        (GraphPatternGroup<IGroupMemberNode>) child, name,
                        value);

            }

        }

    }

    /**
     * Apply the query hint to the group.
     * 
     * @param group
     * @param name
     * @param value
     */
    private void applyToGroup(GraphPatternGroup<IGroupMemberNode> group,
            String name, String value) {

        for (IGroupMemberNode child : group) {

            ((ASTBase) child).setQueryHint(name, value);

        }

    }

}
