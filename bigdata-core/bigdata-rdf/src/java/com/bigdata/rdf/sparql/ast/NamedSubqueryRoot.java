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
package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.optimizers.ASTNamedSubqueryOptimizer;

/**
 * A subquery with a named solution set which can be referenced from other parts
 * of the query.
 * 
 * @see NamedSubqueryInclude
 */
public class NamedSubqueryRoot extends SubqueryBase implements
        INamedSolutionSet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends INamedSolutionSet.Annotations {

        /**
         * The {@link String}[] of the named solution sets on which this named
         * subquery has a dependency. This is computed by the
         * {@link ASTNamedSubqueryOptimizer}.
         */
        String DEPENDS_ON = "dependsOn";
        
        /**
         * A {@link VarNode}[] specifying the join variables that will be used
         * when the named result set is join with the query. The join variables
         * MUST be bound for a solution to join.
         * <p>
         * Note: This can be different for each context in the query in which a
         * given named result set is included. When there are different join
         * variables for different INCLUDEs, then we need to build a hash index
         * for each set of join variable context that will be consumed within
         * the query.
         * <p>
         * Note: If no join variables are specified, then the join will consider
         * the N x M cross product, filtering for solutions which join. This is
         * very expensive. Whenever possible you should identify one or more
         * variables which must be bound for the join and specify those as the
         * join variables.
         */
        String JOIN_VARS = "joinVars";

        /**
         * The set of variables which are known to have been materialized once
         * this named subquery is evaluated. This is set when the
         * {@link NamedSubqueryRoot} is evaluated and then referenced by the
         * {@link NamedSubqueryInclude}.
         */
        String DONE_SET = "doneSet";

    }
    
    /**
     * Deep copy constructor.
     */
    public NamedSubqueryRoot(final NamedSubqueryRoot queryBase) {
    
        super(queryBase);
        
    }
    
    /**
     * Shallow copy constructor.
     */
    public NamedSubqueryRoot(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
    }

    /**
     * 
     * @param queryType
     * @param name
     *            The name of the subquery result set.
     */
    public NamedSubqueryRoot(final QueryType queryType, final String name) {

        super(queryType);

        setName(name);

    }

    @Override
    public String getName() {

        return (String) getProperty(Annotations.NAMED_SET);
        
    }

    @Override
    public void setName(final String name) {

        if (name == null)
            throw new IllegalArgumentException();
        
        setProperty(Annotations.NAMED_SET, name);
        
    }

    /**
     * The join variables to be used when the named result set is included into
     * the query.
     */
    public VarNode[] getJoinVars() {

        return (VarNode[]) getProperty(Annotations.JOIN_VARS);

    }

    /**
     * Set the join variables.
     * 
     * @param joinVars
     *            The join variables.
     */
    public void setJoinVars(final VarNode[] joinVars) {

        setProperty(Annotations.JOIN_VARS, joinVars);

    }

    /**
     * Return the set of named solution sets on which this named subquery
     * depends.
     * <p>
     * Note: This is currently set by the {@link ASTNamedSubqueryOptimizer}.
     * However, it could also be computed dynamically by scanning for
     * {@link NamedSubqueryInclude}s within the WHERE clause of the named
     * subquery.
     * 
     * @see Annotations#DEPENDS_ON
     */
    public final String[] getDependsOn() {
        
        return (String[]) getRequiredProperty(Annotations.DEPENDS_ON);
        
    }

    public final void setDependsOn(final String[] dependsOn) {
        
        setProperty(Annotations.DEPENDS_ON, dependsOn);
        
    }
    
    @Override
    public String toString(int indent) {

        final StringBuilder sb = new StringBuilder();
        
        sb.append("\n");
        
        sb.append(indent(indent));

        sb.append("WITH {");

        sb.append(super.toString(indent+1));
        
        sb.append("\n");
        
        sb.append(indent(indent));

        sb.append("} AS ").append(getName());

        final VarNode[] joinVars = getJoinVars();

        if (joinVars != null) {

            sb.append(" JOIN ON (");

            boolean first = true;

            for (VarNode var : joinVars) {

                if (!first)
                    sb.append(",");

                sb.append(var);

                first = false;

            }

            sb.append(")");

        }
        
        final String[] dependsOn = (String[]) getProperty(Annotations.DEPENDS_ON);

        if(dependsOn != null) {

            sb.append(" DEPENDS ON (");

            boolean first = true;

            for (String s : dependsOn) {

                if (!first)
                    sb.append(",");

                sb.append(s);

                first = false;

            }

            sb.append(")");

        }
        
        return sb.toString();

    }

}
