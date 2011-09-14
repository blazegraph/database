package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;

/**
 * A special kind of group {@link IGroupNode} that represents the sparql union
 * operator.
 * <p>
 * Note: This node only accepts {@link JoinGroupNode}s as children.
 */
public class UnionNode extends GraphPatternGroup<JoinGroupNode> {
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger.getLogger(UnionNode.class);
	
    /**
     * Required deep copy constructor.
     */
    public UnionNode(UnionNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public UnionNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
	 * Construct a non-optional union.
	 */
	public UnionNode() {
		
		super(false/*optional*/);
		
	}

	public UnionNode(final boolean optional) {
		
		super(optional);
		
	}

    @Override
    public UnionNode addChild(final JoinGroupNode child) {

        // can only add non-optional join groups as children to union
        if (!(child instanceof JoinGroupNode)) {

            throw new IllegalArgumentException("UnionNode only permits "
                    + JoinGroupNode.class.getSimpleName()
                    + " children, but child=" + child);

        }

        final JoinGroupNode group = (JoinGroupNode) child;

        // can only add non-optional join groups as children to union
        if (group.isOptional()) {

            log.warn("optional tag on child will be ignored");

        }

        return (UnionNode) super.addChild(child);

    }

    @Deprecated
    @Override
    public Set<IVariable<?>> getIncomingBindings(final Set<IVariable<?>> vars) {

        final GraphPatternGroup<IGroupMemberNode> parent = getParentGraphPatternGroup();

        if (parent == null)
            return vars;

        return parent.getIncomingBindings(vars);

    }

    @Deprecated
    @Override
    public Set<IVariable<?>> getDefinatelyProducedBindings(
            final Set<IVariable<?>> vars, final boolean recursive) {

        if (!recursive || isOptional()) {

            // Nothing to contribute
            return vars;
            
        }

        /*
         * Collect all definitely produced bindings from each of the children.
         */
        final Set<IVariable<?>> all = new LinkedHashSet<IVariable<?>>();

        final List<Set<IVariable<?>>> perChildSets = new LinkedList<Set<IVariable<?>>>();

        for (JoinGroupNode child : this) {

            final Set<IVariable<?>> childSet = new LinkedHashSet<IVariable<?>>();
            
            perChildSets.add(childSet);

            all.addAll(child.getDefinatelyProducedBindings(childSet, recursive));

        }

        /*
         * Now retain only those bindings which are definitely produced by each
         * child of the union.
         */
        for(Set<IVariable<?>> childSet : perChildSets) {
            
            all.retainAll(childSet);
            
        }
        
        // These are the variables which are definitely bound by the union.
        vars.addAll(all);
        
        return vars;

    }

    @Deprecated
    @Override
    public Set<IVariable<?>> getMaybeProducedBindings(
            final Set<IVariable<?>> vars, final boolean recursive) {

        if (!recursive) {

            // Nothing to contribute.
            return vars;

        }

        /*
         * Collect all "maybe" bindings from each of the children.
         */
        for (JoinGroupNode child : this) {

            child.getDefinatelyProducedBindings(vars, recursive);

        }

        return vars;

    }

}
