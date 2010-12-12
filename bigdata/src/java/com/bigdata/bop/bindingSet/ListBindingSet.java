package com.bigdata.bop.bindingSet;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

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
 * @version $Id: HashBindingSet.java 3836 2010-10-22 11:59:15Z thompsonbry $
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
	};

	/**
	 * The stack of symbol tables. Each symbol table is a mapping from an
	 * {@link IVariable} onto its non-<code>null</code> bound {@link IConstant}.
	 * The stack is initialized with an empty symbol table. Symbol tables may be
	 * pushed onto the stack or popped off of the stack, but the stack MAY NOT
	 * become empty.
	 */
	private final Stack<List<E>> stack;

	/**
	 * Return the symbol table on the top of the stack.
	 */
	private List<E> current() {

		return stack.peek();
		
	}
	
	public void push() {

		// The current symbol table.
		final List<E> cur = current();

		// Create a new symbol table.
		final List<E> tmp = new LinkedList<E>();

		// Push the new symbol table onto the stack.
		stack.push(tmp);

		/*
		 * Make a copy of each entry in the symbol table which was on the top of
		 * the stack when we entered this method, inserting the entries into the
		 * new symbol table as we go. This avoids side effects of mutation on
		 * the nested symbol tables and also ensures that we do not need to read
		 * through to the nested symbol tables when answering a query about the
		 * current symbol table. The only down side of this is that naive
		 * serialization is that much less compact.
		 */
		for (E e : cur) {

			tmp.add(new E(e.var, e.val));

		}
		
	}

	public void pop(final boolean save) {

		if (stack.size() < 2) {
			/*
			 * The stack may never become empty. Therefore there must be at
			 * least two symbol tables on the stack for a pop() request.
			 */
			throw new IllegalArgumentException();
		}
		
		// Pop the symbol table off of the top of the stack.
		final List<E> old = stack.pop();

		if (save) {

			// discard the current symbol table.
			stack.pop();
			
			// replacing it with the symbol table which we popped off the stack.
			stack.push(old);

		} else {
			
	        // clear the hash code.
	        hash = 0;

		}
		
	}

	/**
	 * Create an empty binding set. 
	 */
	public ListBindingSet() {

		stack = new Stack<List<E>>();
		
		stack.push(new LinkedList<E>());

	}
	
    /**
     * Package private constructor used by the unit tests.
     * @param vars
     * @param vals
     */
    ListBindingSet(final IVariable[] vars, final IConstant[] vals) {

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
	protected ListBindingSet(final ListBindingSet src,
			final IVariable[] variablesToKeep) {

		stack = new Stack<List<E>>();

		final int stackSize = src.stack.size();

		int depth = 1;
		
		for (List<E> srcLst : src.stack) {

			/*
			 * Copy the source bindings.
			 * 
			 * Note: If a restriction exists on the variables to be copied, then
			 * it is applied onto the the top level of the stack. If the symbol
			 * table is saved when it is pop()'d, then the modified bindings
			 * will replace the parent symbol table on the stack.
			 */
			final List<E> tmp = copy(srcLst,
					depth == stackSize ? variablesToKeep : null);

			// Push onto the stack.
			stack.push(tmp);

		}

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

	public ListBindingSet clone() {

		return new ListBindingSet(this, null /* variablesToKeep */);

	}

	public IBindingSet copy(final IVariable[] variablesToKeep) {
		
		return new ListBindingSet(this/*src*/, variablesToKeep);
		
	}

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

	@SuppressWarnings("unchecked")
	public Iterator<Map.Entry<IVariable, IConstant>> iterator() {
		
		return (Iterator<Map.Entry<IVariable, IConstant>>) ((List) Collections
				.unmodifiableList(current())).iterator();
		
	}

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

	public int size() {

		return current().size();

	}

	@SuppressWarnings("unchecked")
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
    private int hash;

}
