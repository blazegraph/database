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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ModifiableBOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;

/**
 * Base class for AST group nodes.
 */
public abstract class GroupNodeBase<E extends IGroupMemberNode> extends
        GroupMemberNodeBase<E> implements IGroupNode<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations,
            IJoinNode.Annotations {

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public GroupNodeBase(final GroupNodeBase<E> op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public GroupNodeBase(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }
    
//    /**
//     * Note: Uses the default for the "optional" annotation.
//     */
    protected GroupNodeBase() {
        
    }

//	protected GroupNodeBase(final boolean optional) {
//		
//		setOptional( optional );
//		
//    }

	@SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<E> iterator() {
		
		return (Iterator) argIterator();
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<E> getChildren() {
		
		return (List) args();
		
	}

//	/**
//	 * {@inheritDoc}
//	 * <p>
//	 * Force the maintenance of the parent reference on the children. 
//	 */
//	@Override
//	protected void mutation() {
//        super.mutation();
//        final int arity = arity();
//        for (int i = 0; i < arity; i++) {
//            final BOp child = get(i);
//            ((E) child).setParent((IGroupNode<IGroupMemberNode>) this);
//        }
//    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to set the parent reference on the child.
     */
    @SuppressWarnings("unchecked")
    @Override
    public ModifiableBOpBase setArg(final int index, final BOp newArg) {
        
        super.setArg(index, newArg);
        
        ((E) newArg).setParent((IGroupNode<IGroupMemberNode>) this);

        return this;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to set the parent reference on the child
     */
    @SuppressWarnings("unchecked")
    @Override
    public void addArg(final BOp newArg) {

        super.addArg(newArg);

        ((E) newArg).setParent((IGroupNode<IGroupMemberNode>) this);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to set the parent reference on the child
     */
    @SuppressWarnings("unchecked")
    @Override
    public void addArg(final int index, final BOp newArg) {

        super.addArg(index, newArg);

        ((E) newArg).setParent((IGroupNode<IGroupMemberNode>) this);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to clear the parent reference on the child.
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean removeArg(final BOp child) {

        if (super.removeArg(child)) {
        
            ((E) child).setParent(null);
            
            return true;
            
        }

        return false;
        
    }

    @Override
    public IGroupNode<E> addChild(final E child) {
		
        addArg((BOp) child);
		
//        assert child.getParent() == this;
        
		return this;
		
	}
	
    @Override
    public IGroupNode<E> removeChild(final E child) {

        removeArg((BOp) child);

//        assert child.getParent() == null;

        return this;

	}

    @Override
    public boolean isEmpty() {
        
        return arity() == 0;
        
    }
    
    @Override
	public int size() {
		
		return arity();
		
	}
	
    final public List<FilterNode> getAttachedJoinFilters() {

        @SuppressWarnings("unchecked")
        final List<FilterNode> filters = (List<FilterNode>) getProperty(Annotations.FILTERS);

        if (filters == null) {

            return Collections.emptyList();

        }

        return Collections.unmodifiableList(filters);

    }

    final public void setAttachedJoinFilters(final List<FilterNode> filters) {

        setProperty(Annotations.FILTERS, filters);

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to also clone the children and then set the parent reference
     * on the cloned children.
     */
    @Override
    public GroupNodeBase<E> clone() {

        @SuppressWarnings("unchecked")
        final GroupNodeBase<E> tmp = (GroupNodeBase<E>) super.clone();

        final int size = size();

        for (int i = 0; i < size; i++) {

            IGroupMemberNode aChild = (IGroupMemberNode) tmp.get(i);

            aChild = (IGroupMemberNode) ((ASTBase) aChild).clone();

            tmp.setArg(i, (ASTBase) aChild);

        }

        return tmp;

    }

    /**
     * Simple but robust version of to-String 
     */
    @Override
    public String toString(final int indent) {
        
        final String s = indent(indent);
        
        final StringBuilder sb = new StringBuilder();

        sb.append("\n").append(s).append(getClass().getSimpleName());

        if (this instanceof IJoinNode) {

            final IJoinNode joinNode = (IJoinNode) this;

            if (joinNode.isOptional())
                sb.append(" [optional]");

            if (joinNode.isMinus())
                sb.append(" [minus]");

        }

        if (this instanceof JoinGroupNode) {

            final JoinGroupNode joinGroup = (JoinGroupNode) this;

            if (joinGroup.getContext() != null) {

                sb.append(" [context=" + joinGroup.getContext() + "]");

            }

            if (joinGroup.getProperty(JoinGroupNode.Annotations.OPTIMIZER) != null) {

                // Show non-default value.
                sb.append(" [" + JoinGroupNode.Annotations.OPTIMIZER + "="
                        + joinGroup.getQueryOptimizer() + "]");
                
            }

        }
        
        if (this instanceof GraphPatternGroup) {

            final GraphPatternGroup<?> t = (GraphPatternGroup<?>) this;

            final IVariable<?>[] joinVars = t.getJoinVars();

            if (joinVars != null)
                sb.append(" [joinVars=" + Arrays.toString(joinVars) + "]");

            final IVariable<?>[] projectInVars = t.getProjectInVars();

            if (projectInVars != null)
                sb.append(" [projectInVars=" + Arrays.toString(projectInVars) + "]");

        }

        sb.append(" {");

        for (IQueryNode n : this) {

            if(n instanceof AssignmentNode) {
                /*
                 * Note: Otherwise no newline before an AssignmentNode since
                 * also used in a ProjectionNode.
                 */
                sb.append("\n");
            }
            
            sb.append(n.toString(indent + 1));

            if (((IGroupMemberNode) n).getParent() != this) {
                sb.append(" : ERROR : parent not [this]");
                throw new RuntimeException("Parent not this: child=" + n
                        + ", this=" + toShortString() + ", but parent="
                        + ((IGroupMemberNode) n).getParent());
            }

        }

        sb.append("\n").append(s).append("}");

        if (this instanceof GraphPatternGroup) {

            final IVariable<?>[] joinVars = ((GraphPatternGroup<?>) this)
                    .getJoinVars();

            if (joinVars != null && joinVars.length > 0) {

                sb.append(" JOIN ON (");

                boolean first = true;

                for (IVariable<?> var : joinVars) {

                    if (!first)
                        sb.append(",");

                    sb.append(var);

                    first = false;

                }

                sb.append(")");

            }
            
        }

        final List<FilterNode> filters = getAttachedJoinFilters();
        if(!filters.isEmpty()) {
            for (FilterNode filter : filters) {
                sb.append(filter.toString(indent + 1));
            }
        }
        
        if (getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY) != null) {
            sb.append(" AST2BOpBase.estimatedCardinality=");
            sb.append(getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY).toString());
        }

        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
        
        return sb.toString();

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to set the parent reference on the new child and clear the
     * parent reference on the old child.
     */
    @SuppressWarnings("unchecked")
    @Override
    public int replaceWith(final BOp oldChild, final BOp newChild) {

        final int i = super.replaceWith(oldChild, newChild);

        if (i > 0) {

            if (((E) oldChild).getParent() == this) {
                ((E) oldChild).setParent(null);
            }

            ((E) newChild).setParent((IGroupNode<IGroupMemberNode>) this);

        }

        return i;
    	
    }

}
