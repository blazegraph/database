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
 * Created on Feb 29, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;

/**
 * The solutions declared by a BINDINGS clause.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BindingsClause extends GroupMemberNodeBase<BindingsClause> 
        implements IBindingProducerNode, IJoinNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public interface Annotations extends ASTBase.Annotations, 
            IJoinNode.Annotations {

        /**
         * The ordered set of declared variables for which there MIGHT be a
         * binding in any given solution.
         */
        String DECLARED_VARS = "declaredVars";

        /**
         * The binding sets.
         */
        String BINDING_SETS = "bindingSets";

    }
    
    /**
     * Deep copy constructor.
     * @param bindings
     */
    public BindingsClause(final BindingsClause bindings) {
        
        super(bindings);
        
    }

    public BindingsClause(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
    }

    /**
     * 
     * @param declaredVars
     *            The ordered set of declared variables.
     * @param bindingSets
     *            The set of solutions.
     */
    public BindingsClause(final LinkedHashSet<IVariable<?>> declaredVars,
            final List<IBindingSet> bindingSets) {

        super(NOARGS, new HashMap<String, Object>(2));

        if (declaredVars == null)
            throw new IllegalArgumentException();

        if (bindingSets == null)
            throw new IllegalArgumentException();

        setDeclaredVariables(declaredVars);

        setBindingSets(bindingSets);
                
    }

    /**
     * Return the #of declared variables.
     */
    public final int getDeclaredVariableCount() {
        
        return getDeclaredVariables().size();
        
    }
    
    /**
     * Return the ordered set of declared variables for the BINDINGS clause. The
     * declared variables MIGHT have a binding in any given solution, but there
     * is no guarantee that any given variable is ever bound within a solution.
     */
    @SuppressWarnings("unchecked")
    public final LinkedHashSet<IVariable<?>> getDeclaredVariables() {

        return (LinkedHashSet<IVariable<?>>) getProperty(Annotations.DECLARED_VARS);

    }

    public final void setDeclaredVariables(
            final LinkedHashSet<IVariable<?>> declaredVars) {

        setProperty(Annotations.DECLARED_VARS, declaredVars);

    }

    /**
     * Return the #of binding sets.
     */
    public final int getBindingSetsCount() {

        final List<IBindingSet> bindingSets = getBindingSets();

        if (bindingSets == null)
            return 0;

        return bindingSets.size();

    }

    /**
     * The binding sets -or- <code>null</code>.
     */
    @SuppressWarnings("unchecked")
    public final List<IBindingSet> getBindingSets() {

        return (List<IBindingSet>) getProperty(Annotations.BINDING_SETS);

    }

    public final void setBindingSets(final List<IBindingSet> bindingSets) {

        setProperty(Annotations.BINDING_SETS, bindingSets);

    }

    @Override
    public String toString(final int indent) {

        final LinkedHashSet<IVariable<?>> declaredVars = getDeclaredVariables();
        
        final List<IBindingSet> bindingSets = getBindingSets();
        
        final String s = indent(indent);

        final String s1 = indent(indent + 1);

        final StringBuilder sb = new StringBuilder();

        sb.append("\n");
        
        sb.append(s);
        
        sb.append("BindingsClause");

        for(IVariable<?> var : declaredVars) {
            
            sb.append(" ?");
            
            sb.append(var.getName());
            
        }

        sb.append("\n");

        sb.append(s);

        sb.append("{");
        
        if (bindingSets.size() <= 10) {
        	
	        for(IBindingSet bset : bindingSets) {
	        
	            sb.append("\n");
	            
	            sb.append(s1);
	            
	            sb.append(bset.toString());
	        
	        }
	        
        } else {
        	
            sb.append("\n");
            
            sb.append(s1);
            
            sb.append("[ count=" + bindingSets.size() + " ]");
        	
        }
        
        sb.append("\n");

        sb.append(s);

        sb.append("}");

        return sb.toString();
        
    }
    
    @Override
    final public List<FilterNode> getAttachedJoinFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    @Override
    final public void setAttachedJoinFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }

    @Override
    public boolean isOptional() {
        return false;
    }

    @Override
    public boolean isMinus() {
        return false;
    }

    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
       return new HashSet<IVariable<?>>();
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
       return new HashSet<IVariable<?>>();
    }    
}
