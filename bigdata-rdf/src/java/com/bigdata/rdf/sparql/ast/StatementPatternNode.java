package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;

/**
 * A node in the AST representing a statement pattern.
 * 
 * TODO This should inherit the context dynamically from the parent rather than
 * requiring the context to be specified explicitly.
 */
public class StatementPatternNode extends GroupMemberNodeBase {

    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations {

        String SCOPE = "scope";
    
    }
    
//	private final TermNode s, p, o, c;
//	
//    private final Scope scope;

    /**
     * A triple pattern.
     * 
     * @param s
     * @param p
     * @param o
     */
    public StatementPatternNode(final TermNode s, final TermNode p,
            final TermNode o) {

        this(s, p, o, null/* context */, null/* scope */);

    }

    /**
     * A quad pattern.
     * 
     * @param s
     * @param p
     * @param o
     * @param c
     * @param scope
     *            Either {@link Scope#DEFAULT_CONTEXTS} or
     *            {@link Scope#NAMED_CONTEXTS}.
     */
	@SuppressWarnings("unchecked")
    public StatementPatternNode(
			final TermNode s, final TermNode p, final TermNode o, 
			final TermNode c, final Scope scope) {

        super(new BOp[] { s, p, o, c }, scope == null ? null/* anns */: NV
                .asMap(new NV(Annotations.SCOPE, scope)));

		if (s == null || p == null || o == null) {
		
		    throw new IllegalArgumentException();
		    
		}

//		this.s = s;
//		this.p = p;
//		this.o = o;
//		this.c = c;
//		this.scope = scope;
		
	}
	
	public TermNode s() {
		return (TermNode) get(0);
	}

	public TermNode p() {
	    return (TermNode) get(1);
	}

	public TermNode o() {
	    return (TermNode) get(2);
	}

	public TermNode c() {
	    return (TermNode) get(3);
	}
	
    public Scope getScope() {

        return (Scope) getProperty(Annotations.SCOPE);
        
    }

    /**
     * Return the variables used by the predicate - i.e. what this node will
     * attempt to bind when run.
     * <p>
     * Note: Not cached since this is part of an immutable data structure.
     */
	public Set<IVariable<?>> getProducedBindings() {

//	    if(producedBindings == null) {
	        
	        final Set<IVariable<?>> producedBindings = new LinkedHashSet<IVariable<?>>();

            final TermNode s = s();
            final TermNode p = p();
            final TermNode o = o();
            final TermNode c = c();
            
	        if (s instanceof VarNode) {
	            producedBindings.add(((VarNode) s).getValueExpression());
	        }
	        
	        if (p instanceof VarNode) {
	            producedBindings.add(((VarNode) p).getValueExpression());
	        }
	        
	        if (o instanceof VarNode) {
	            producedBindings.add(((VarNode) o).getValueExpression());
	        }
	        
	        if (c != null && c instanceof VarNode) {
	            producedBindings.add(((VarNode) c).getValueExpression());
	        }
	        
//	    }
	    
	    return producedBindings;
	    
	}

//	/**
//     * Lazily computed.  Not serialized.
//     */
//    private transient Set<IVariable<?>> producedBindings;
	
	public String toString(final int indent) {
		
	    final StringBuilder sb = new StringBuilder(indent(indent));

		sb.append("sp(");
		sb.append(s()).append(", ");
		sb.append(p()).append(", ");
		sb.append(o());
		
        final TermNode c = c();
        if (c != null) {
            sb.append(", ").append(c);
        }
		
		final Scope scope = getScope();
		if (scope != null) {
			sb.append(", ").append(scope);
		}
		
		sb.append(")");
		
		return sb.toString();
		
	}

//    public boolean equals(final Object obj) {
//
//        if (this == obj)
//            return true;
//
//        if (!(obj instanceof StatementPatternNode))
//            return false;
//
//        final StatementPatternNode t = (StatementPatternNode) obj;
//
//        if (!s.equals(t.s))
//            return false;
//        if (!p.equals(t.p))
//            return false;
//        if (!o.equals(t.o))
//            return false;
//        if (c == null) {
//            if (t.c != null)
//                return false;
//        } else {
//            if (!c.equals(t.c))
//                return false;
//        }
//        if (scope == null) {
//            if (t.scope != null)
//                return false;
//        } else {
//            if (!scope.equals(t.scope))
//                return false;
//        }
//
//        return true;
//
//    }

}
