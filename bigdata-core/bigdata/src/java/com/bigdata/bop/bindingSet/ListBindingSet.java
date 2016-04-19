package com.bigdata.bop.bindingSet;

import com.bigdata.bop.Constant;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>An {@link IBindingSet} based on a {@link LinkedList}. Since {@link Var}s may
 * be compared using <code>==</code> this should be faster than a hash map for
 * most operations unless the binding set has a large number of entries.
 * </p><p>
 * Note: {@link #push()} and {@link #pop(boolean)} are implemented by making a
 * copy of the current symbol table with distinct {@link Map.Entry} objects. If
 * the symbol table is saved when it is {@link #pop(boolean) popped), then it
 * simply replaces the pre-existing symbol table which was uncovered when it
 * was popped off of the stack.  This design has several advantages, including:
 * <ul>
 * <li>Methods such as {@link #get(IVariable)}, {@link #set(IVariable, IConstant)},
 * and {@link #size()} can be written solely in terms of the current symbol table.</li>
 * <li>{@link #clear(IVariable)} removes the {@link Map.Entry} from the
 * current symbol table rather than introducing <code>null</code> values or
 * delete markers.</li>
 * </ul>
 * </p>
 * The only down side to this approach is that the serialized representation of
 * the {@link IBindingSet} is more complex.  However, java default serialization
 * will do a good job by providing back references for the object graph.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: HashBindingSet.java 5123 2011-09-03 10:48:24Z thompsonbry $
 */
public class ListBindingSet implements IBindingSet {

	private static final long serialVersionUID = 1L;

	/**
	 * A (var,val) entry.
	 */
	private static class E implements Map.Entry<IVariable<?>, IConstant<?>>, Serializable {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private final IVariable<?> var;
		
		private IConstant<?> val;

		E(final IVariable<?> var, final IConstant<?> val) {
			this.var = var;
			this.val = val;
		}

		public IVariable<?> getKey() {
			return var;
		}

		public IConstant<?> getValue() {
			return val;
		}

		public IConstant<?> setValue(final IConstant<?> value) {
			if (value == null) {
				// Null bindings are not permitted.
				throw new IllegalArgumentException();
			}
			final IConstant<?> tmp = this.val;
			this.val = value;
            return tmp;
        }

        public String toString() {
            return var + "=" + val;
        }
    };

//	/**
//	 * The stack of symbol tables. Each symbol table is a mapping from an
//	 * {@link IVariable} onto its non-<code>null</code> bound {@link IConstant}.
//	 * The stack is initialized with an empty symbol table. Symbol tables may be
//	 * pushed onto the stack or popped off of the stack, but the stack MAY NOT
//	 * become empty.
//	 */
//	private final LightStack<List<E>> stack;
	private final List<E> current;

	/**
	 * Return the symbol table on the top of the stack.
	 */
	final private List<E> current() {

	    return current;
//		return stack.peek();
		
	}

//    public void push(final IVariable[] vars) {
//
//        // Create a new symbol table.
//        final List<E> tmp = new LinkedList<E>();
//
//        /*
//         * For each variable which is to be projected into the new symbol table,
//         * look up its binding. If it is bound, then copy that binding into the
//         * new symbol table.
//         */
//        for (IVariable<?> var : vars) {
//
//		    final IConstant<?> val = get(var);
//		    
//            if (val != null)
//                tmp.add(new E(var, val));
//
//		}
//
//		// Push the new symbol table onto the stack.
//        stack.push(tmp);
//
//        hash = 0;
//        
//	}
//
//	public void pop(final IVariable[] vars) {
//
//		if (stack.size() < 2) {
//			/*
//			 * The stack may never become empty. Therefore there must be at
//			 * least two symbol tables on the stack for a pop() request.
//			 */
//			throw new IllegalStateException();
//		}
//		
//		// Pop the symbol table off of the top of the stack.
//        final List<E> old = stack.pop();
//
//        /*
//         * Copy the current binding for any variable that was projected by the
//         * subquery.
//         * 
//         * Note: This does not enforce consistency between the existing binding
//         * (if any) for a variable on the nested symbol table and the binding
//         * from the symbol table which is being popped off of the stack. It is
//         * the responsibility of the JOINs, BIND, etc. to verify that bindings
//         * are consistent when they are made. If the operators are written
//         * correctly then a variable which was bound on entry to a subquery will
//         * have the same binding on exit and there will be no need to verify
//         * that the value, when bound, is consistent.
//         */
//        for (E e : old) { // for each binding from the top of the stack.
//
//            for (IVariable<?> v : vars) { // for each projected variable.
//
//                if (e.var == v) { // if the binding was projected
//                    
//                    set(e.var, e.val); // then copy to revealed symbol table.
//
//                    break;
//                    
//                }
//                
//            }
//
//        }
//
//        hash = 0;
//        
//	}

    /**
     * Create an empty binding set.
     */
//    * <p>
//    * We generally want the binding set stack to be pretty shallow. We only
//    * push/pop around a subquery. Simple queries have a depth of 1. A stack
//    * depth of 3 is typical of a complex query with nested subqueries. Deeper
//    * nesting is uncommon. Since we create a LOT of {@link IBindingSet}s, the
//    * initial capacity of the associated stack is important.
	public ListBindingSet() {

//	    /*
//	     * Start with a capacity of ONE (1) and expand only as required.
//	     */
//		stack = new LightStack<List<E>>(1/*initialCapacity*/);
//		
//		stack.push(new LinkedList<E>());
	    
	    current = new LinkedList<E>();

	}
	
    /**
	 * Alternative constructor.
	 * 
	 * @param vars
	 *            A copy is made of the data.
	 * @param vals
	 *            A copy is made of the data.
	 */
    @SuppressWarnings("rawtypes")
    public ListBindingSet(final IVariable[] vars, final IConstant[] vals) {

        this();

        if (vars == null)
            throw new IllegalArgumentException();

        if (vals == null)
            throw new IllegalArgumentException();

        if (vars.length != vals.length)
            throw new IllegalArgumentException();

        for (int i = 0; i < vars.length; i++) {

            set(vars[i], vals[i]);

        }
        
    }

    /**
	 * Copy constructor (used by clone, copy).
	 * 
	 * @param src
	 *            The source to be copied.
	 * @param variablesToKeep
	 *            The variables to be retained for the symbol table on the top
	 *            of the stack (optional).
	 */
	@SuppressWarnings("rawtypes")
    protected ListBindingSet(final ListBindingSet src,
			final IVariable[] variablesToKeep) {

//        final int stackSize = src.stack.size();
//
//		stack = new LightStack<List<E>>(stackSize);
//
//		int depth = 1;
//		
//		for (List<E> srcLst : src.stack) {
//
//			/*
//			 * Copy the source bindings.
//			 * 
//			 * Note: If a restriction exists on the variables to be copied, then
//			 * it is applied onto the the top level of the stack. If the symbol
//			 * table is saved when it is pop()'d, then the modified bindings
//			 * will replace the parent symbol table on the stack.
//			 */
//			final List<E> tmp = copy(srcLst,
//					depth == stackSize ? variablesToKeep : null);
//
//			// Push onto the stack.
//			stack.push(tmp);
//
//		}

        current = copy(src.current, variablesToKeep);

	}

       
       private ListBindingSet(List<E> contents) {
           current = contents;
       }
        
        
	/**
	 * Return a copy of the source list. The copy will use new {@link E}s to
	 * represent the bindings so changes to the copy will not effect the source.
	 * 
	 * @param src
	 *            The source list.
	 * @param variablesToKeep
	 *            When non-<code>null</code>, only the bindings for the
	 *            variables listed in this array will copied.
	 * 
	 * @return The copy.
	 */
	@SuppressWarnings("rawtypes")
    private List<E> copy(final List<E> src, final IVariable[] variablesToKeep) {

		final List<E> dst = new LinkedList<E>();

		final Iterator<E> itr = src.iterator();

		while (itr.hasNext()) {

			final E e = itr.next();

			boolean keep = true;

			if (variablesToKeep != null) {
			
				keep = false;
				
				for (IVariable<?> x : variablesToKeep) {
				
					if (x == e.var) {
					
						keep = true;
						
						break;
						
					}
					
				}
				
			}

			if (keep)
				dst.add(new E(e.var, e.val));

        }

		return dst;
		
	}
        
        
        
    /**
     * Return a copy of the source list minus entries assigning error values
     * (Constant.errorValue() or its copies). The copy will use new
     * {@link E}s to represent the bindings so changes to the copy will not
     * effect the source.
     *
     * @param src The source list.
     * @param variablesToKeep When non-<code>null</code>, only the bindings for
     * the variables listed in this array will copied.
     *
     * @return The copy.
     */
    @SuppressWarnings("rawtypes")
    private List<E> copyMinusErrors(final List<E> src, final IVariable[] variablesToKeep) {

        final List<E> dst = new LinkedList<E>();

        final Iterator<E> itr = src.iterator();

        while (itr.hasNext()) {

            final E e = itr.next();

            if (e.val == Constant.errorValue())
                continue;
            
            boolean keep = true;

            if (variablesToKeep != null) {

                keep = false;

                for (IVariable<?> x : variablesToKeep) {

                    if (x == e.var) {

                        keep = true;

                        break;

                    }

                }

            }

            if (keep) {
                dst.add(new E(e.var, e.val));
            }

        } // while (itr.hasNext())

        return dst;

    } // copyMinusErrors(final List<E> src, final IVariable[] variablesToKeep)


        
        
        
        
        
        
        
        
        
        

	public ListBindingSet clone() {

		return new ListBindingSet(this, null /* variablesToKeep */);

	}

	@SuppressWarnings("rawtypes")
    public IBindingSet copy(final IVariable[] variablesToKeep) {
		
		return new ListBindingSet(this/*src*/, variablesToKeep);
		
	}

    @Override
    @SuppressWarnings("rawtypes")
    public final IBindingSet copyMinusErrors(final IVariable[] variablesToKeep) {
        return new ListBindingSet(copyMinusErrors(this.current,
                variablesToKeep));
    }
    
    /** 
     * @return true if this IBindingSet contains an assignment of an error value
     */
    @Override
    public final boolean containsErrorValues() {        
        for (E e : this.current) {
            
            if (e.val == Constant.errorValue())
                return true;            
        }
        return false;
    }
    
    
    @SuppressWarnings("rawtypes")
    public void clear(final IVariable var) {

		if (var == null)
			throw new IllegalArgumentException();
		
    	final List<E> cur = current();
    	
    	for(E e : cur) {

    		if(e.var == var) {
    		
    			cur.remove(e);

    	        // clear the hash code.
    	        hash = 0;

    			return;
    			
    		}
    		
    	}
    	
	}

	public void clearAll() {

		current().clear();
		
        // clear the hash code.
        hash = 0;

	}

	@SuppressWarnings("rawtypes")
    public IConstant get(final IVariable var) {

		if (var == null)
			throw new IllegalArgumentException();
		
		final List<E> cur = current();
    	
    	for(E e : cur) {

    		if(e.var == var) {
    		
    			return e.val;
    			
    		}
    		
    	}
    	
    	return null;
    	
	}

	@SuppressWarnings("rawtypes")
    public boolean isBound(IVariable var) {
	
		if (var == null)
			throw new IllegalArgumentException();
		
		final List<E> cur = current();
    	
    	for(E e : cur) {

    		if(e.var == var) {
    		
    			return true;
    			
    		}
    		
    	}
    	
    	return false;
    	
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Iterator<Map.Entry<IVariable, IConstant>> iterator() {
		
//		return (Iterator<Map.Entry<IVariable, IConstant>>) ((List) Collections
//				.unmodifiableList(current())).iterator();
        return (Iterator<Map.Entry<IVariable, IConstant>>) ((List) current())
                .iterator();
	}

	@SuppressWarnings("rawtypes")
    public void set(final IVariable var, final IConstant val) {
		
		if (var == null)
			throw new IllegalArgumentException();

		if (val == null)
			throw new IllegalArgumentException();

		final List<E> cur = current();

		for (E e : cur) {

			if (e.var == var) {

				e.val = val;

		        // clear the hash code.
		        hash = 0;

				return;

			}

		}

		cur.add(new E(var, val));

        // clear the hash code.
        hash = 0;

	}

	public boolean isEmpty() {
	    
	    return current().isEmpty();
	    
	}
	
	public int size() {

		return current().size();

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Iterator<IVariable> vars() {
		return (Iterator<IVariable>) new Striterator(Collections
				.unmodifiableList(current()).iterator())
				.addFilter(new Resolver() {
					private static final long serialVersionUID = 1L;

					@Override
					protected Object resolve(Object obj) {
						return ((E) obj).var;
					}
				});
	}

    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("{ ");

        int i = 0;
        
		final Iterator<E> itr = current().iterator();

        while (itr.hasNext()) {

            if (i > 0)
                sb.append(", ");

            final E entry = itr.next();
            
            sb.append(entry.getKey());

            sb.append("=");

            sb.append(entry.getValue());

            i++;

        }

        sb.append(" }");

        return sb.toString();

    }

    public boolean equals(final Object t) {
        
        if (this == t)
            return true;
        
        if(!(t instanceof IBindingSet))
            return false;
        
        final IBindingSet o = (IBindingSet) t;

        if (size() != o.size())
            return false;
        
        final Iterator<E> itr = current().iterator();
        
        while(itr.hasNext()) {

            final E entry = itr.next();
            
            final IVariable<?> var = entry.getKey();
            
            final IConstant<?> val = entry.getValue();
            
//            if (!o.isBound(vars[i]))
//                return false;
            final IConstant<?> o_val = o.get ( var ) ;
            if (null == o_val || !val.equals(o_val))
                return false;
            
        }
        
        return true;
        
    }

    public int hashCode() {

        if (hash == 0) {

            int result = 0;

            final List<E> cur = current();
            
            for(E e : cur) {

                if (e.val == null)
                    continue;

                result ^= e.val.hashCode();

            }

            hash = result;

        }

        return hash;

    }
    
    /**
     * Note: The hash code MUST be reset by any mutation!
     */
    private int hash;

}
