/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 20, 2008
 */

package com.bigdata.bop.bindingSet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

/**
 * An {@link IBindingSet} backed by an dense array (no gaps). This
 * implementation is more efficient for fixed or small N (N LTE ~20). It simples
 * scans the array looking for the variable using references tests for equality.
 * Since the #of variables is generally known in advance this can be faster and
 * lighter than {@link HashBindingSet} for most applications.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ArrayBindingSet implements IBindingSet {

    private static final long serialVersionUID = -6468905602211956490L;
    
//    private static final Logger log = Logger.getLogger(ArrayBindingSet.class);

    /**
     * A symbol table implemented by two correlated arrays.
     */
	private static class ST implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * A dense array of the bound variables.
		 */
		private final IVariable[] vars;

		/**
		 * A dense array of the values bound to the variables (correlated with
		 * {@link #vars}).
		 */
		private final IConstant[] vals;

		/**
		 * The #of entries in the arrays which have defined values.
		 */
	    private int nbound = 0;

		private ST(final int nbound,final IVariable[] vars, final IConstant[] vals) {
			this.nbound = nbound;
			this.vars = vars;
			this.vals = vals;
		}

	    public IConstant get(final IVariable var) {

	        if (var == null)
	            throw new IllegalArgumentException();

	        for (int i = 0; i < nbound; i++) {

	            if (vars[i] == var) {
	        
	                return vals[i];
	                
	            }
	            
	        }
	        
	        return null;
	        
	    }

		void set(final IVariable var, final IConstant val) {

			if (var == null)
				throw new IllegalArgumentException();

			if (val == null)
				throw new IllegalArgumentException();

			for (int i = 0; i < nbound; i++) {

				if (vars[i] == var) {

					vals[i] = val;

					return;

				}

			}

			vars[nbound] = var;

			vals[nbound] = val;

			nbound++;

		}

		void clearAll() {

			for (int i = nbound - 1; nbound > 0; i--, nbound--) {

				vars[i] = null;

				vals[i] = null;

			}

			assert nbound == 0;

		}

		/**
		 * Since the array is dense (no gaps), {@link #clear(IVariable)}
		 * requires that we copy down any remaining elements in the array by one
		 * position.
		 * 
		 * @return <code>true</code> if the data structure was modified by the
		 *         operation.
		 */
	    boolean clear(final IVariable var) {

	        if (var == null)
	            throw new IllegalArgumentException();

	        for (int i = 0; i < nbound; i++) {

	            if (vars[i] == var) {

	                final int nremaining = nbound-(i+1);
	                
	                if (nremaining >= 0) {
	                    
	                    // Copy down to close up the gap!
	                    System.arraycopy(vars, i+1, vars, i, nremaining);

	                    System.arraycopy(vals, i+1, vals, i, nremaining);
	                    
	                } else {

	                    // Just clear the reference.
	                    
	                    vars[i] = null;

	                    vals[i] = null;

	                }
	                
	                nbound--;

	                return true;

	            }

	        }
	        
	        return false;

	    }
	    
	}
    
	/**
	 * The stack of symbol tables. Each symbol table is a mapping from an
	 * {@link IVariable} onto its non-<code>null</code> bound {@link IConstant}.
	 * The stack is initialized with an empty symbol table. Symbol tables may be
	 * pushed onto the stack or popped off of the stack, but the stack MAY NOT
	 * become empty.
	 */
//	private final Stack<ST> stack;
	private final ST current;

	/**
	 * Return the symbol table on the top of the stack.
	 */
	private ST current() {

	    return current;
//		return stack.peek();
		
	}
	
//	public void push() {
//
//		// The current symbol table.
//		final ST cur = current();
//
//		// Create a new symbol table.
//		final ST tmp = new ST(cur.nbound, cur.vars.clone(), cur.vals.clone());
//
//		// Push the new symbol table onto the stack.
//		stack.push(tmp);
//
//	}
//
//	public void pop(final boolean save) {
//
//		if (stack.size() < 2) {
//			/*
//			 * The stack may never become empty. Therefore there must be at
//			 * least two symbol tables on the stack for a pop() request.
//			 */
//			throw new IllegalArgumentException();
//		}
//		
//		// Pop the symbol table off of the top of the stack.
//		final ST old = stack.pop();
//
//		if (save) {
//
//			// discard the current symbol table.
//			stack.pop();
//			
//			// replacing it with the symbol table which we popped off the stack.
//			stack.push(old);
//
//		} else {
//			
//	        // clear the hash code.
//	        hash = 0;
//
//		}
//		
//	}

    /**
	 * Copy constructor (used by clone, copy).
	 * 
	 * @param src
	 *            The source to be copied.
	 * @param variablesToKeep
	 *            The variables to be retained for the symbol table on the top
	 *            of the stack (optional).
	 */
	protected ArrayBindingSet(final ArrayBindingSet src,
			final IVariable[] variablesToKeep) {

//		stack = new Stack<ST>();
//
//		final int stackSize = src.stack.size();
//
//		int depth = 1;
//		
//		for (ST srcLst : src.stack) {
//
//			/*
//			 * Copy the source bindings.
//			 * 
//			 * Note: If a restriction exists on the variables to be copied, then
//			 * it is applied onto the the top level of the stack. If the symbol
//			 * table is saved when it is pop()'d, then the modified bindings
//			 * will replace the parent symbol table on the stack.
//			 */
//			final ST tmp = copy(srcLst,
//					depth == stackSize ? variablesToKeep : null);
//
//			// Push onto the stack.
//			stack.push(tmp);
//
//		}

        current = copy(src.current, variablesToKeep);
		
	}

	/**
	 * Return a copy of the source list.
	 * 
	 * @param src
	 *            The source list.
	 * @param variablesToKeep
	 *            When non-<code>null</code>, only the bindings for the
	 *            variables listed in this array will copied.
	 * 
	 * @return The copy.
	 */
	private ST copy(final ST src, final IVariable[] variablesToKeep) {

		if (variablesToKeep == null) {

			return new ST(src.nbound, src.vars, src.vals);

		}
		
		final ST dst = new ST(0/* nbound */, new IVariable[src.vars.length],
				new IConstant[src.vals.length]);

        // bitflag for the old binding set
        final boolean[] keep = new boolean[src.nbound];
        
        // for each var in the old binding set, see if we need to keep it
        for (int i = 0; i < src.nbound; i++) {
            
            final IVariable v = src.vars[i];

            keep[i] = false;
            for (IVariable k : variablesToKeep) {
                if (v == k) {
                    keep[i] = true;
                    break;
                }
            }
            
        }
        
        // fill in the new binding set based on the keep bitflag
        for (int i = 0; i < src.nbound; i++) {
            if (keep[i]) {
                dst.vars[dst.nbound] = src.vars[i];
                dst.vals[dst.nbound] = src.vals[i];
                dst.nbound++;
            }
        }
        
//		final Iterator<E> itr = src.iterator();
//
//		while (itr.hasNext()) {
//
//			final E e = itr.next();
//
//			boolean keep = true;
//
//			if (variablesToKeep != null) {
//			
//				keep = false;
//				
//				for (IVariable<?> x : variablesToKeep) {
//				
//					if (x == e.var) {
//					
//						keep = true;
//						
//						break;
//						
//					}
//					
//				}
//				
//			}
//
//			if (keep)
//				dst.add(new E(e.var, e.val));
//
//        }

		return dst;
		
	}
    
//    public ArrayBindingSet XXcopy(final IVariable[] variablesToKeep) {
//
//        // bitflag for the old binding set
//        final boolean[] keep = new boolean[nbound];
//        
//        // for each var in the old binding set, see if we need to keep it
//        for (int i = 0; i < nbound; i++) {
//            
//            final IVariable v = vars[i];
//
//            keep[i] = false;
//            for (IVariable k : variablesToKeep) {
//                if (v == k) {
//                    keep[i] = true;
//                    break;
//                }
//            }
//            
//        }
//        
//        // allocate the new vars
//        final IVariable[] newVars = new IVariable[vars.length];
//        
//        // allocate the new vals
//        final IConstant[] newVals = new IConstant[vals.length];
//        
//        // fill in the new binding set based on the keep bitflag
//        int newbound = 0;
//        for (int i = 0; i < nbound; i++) {
//            if (keep[i]) {
//                newVars[newbound] = vars[i];
//                newVals[newbound] = vals[i];
//                newbound++;
//            }
//        }
//        
//        ArrayBindingSet bs = new ArrayBindingSet(newVars, newVals);
//        bs.nbound = newbound;
//        
//        return bs;
//        
//    }

    public ArrayBindingSet clone() {

		return new ArrayBindingSet(this, null/* variablesToKeep */);
        
    }

    public ArrayBindingSet copy(final IVariable[] variablesToKeep) {

    	return new ArrayBindingSet(this, variablesToKeep);
    	
	}

    /**
     * Initialized with the given bindings (assumes for efficiency that all
     * elements of bound arrays are non-<code>null</code> and that no
     * variables are duplicated).
     * 
     * @param vars
     *            The variables.
     * @param vals
     *            Their bound values.
     */
    public ArrayBindingSet(final IVariable[] vars, final IConstant[] vals) {

        if (vars == null)
            throw new IllegalArgumentException();

        if (vals == null)
            throw new IllegalArgumentException();

        if(vars.length != vals.length)
            throw new IllegalArgumentException();

//        stack = new Stack<ST>();
//        
//		stack.push(new ST(vars.length, vars, vals));
        
        current = new ST(vars.length, vars, vals);

    }

    /**
     * Initialized with the given capacity.
     * 
     * @param capacity
     *            The capacity.
     * 
     * @throws IllegalArgumentException
     *             if the <i>capacity</i> is negative.
     */
    public ArrayBindingSet(final int capacity) {

		if (capacity < 0)
			throw new IllegalArgumentException();

//		stack = new Stack<ST>();
//
//		stack.push(new ST(0/* nbound */, new IVariable[capacity],
//				new IConstant[capacity]));

        current = new ST(0/* nbound */, new IVariable[capacity],
                new IConstant[capacity]);
		
    }

    /**
     * {@inheritDoc}
     * <p>
     * Iterator does not support either removal or concurrent modification of 
     * the binding set.
     */
     public Iterator<IVariable> vars() {

		return Collections.unmodifiableList(Arrays.asList(current().vars))
				.iterator();

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Iterator does not support either removal or concurrent modification of 
     * the binding set.
     */
    public Iterator<Map.Entry<IVariable,IConstant>> iterator() {
       
        return new BindingSetIterator();
        
    }
    
    private class BindingSetIterator implements Iterator<Map.Entry<IVariable,IConstant>> {

        private int i = 0;
        
        private ST cur = current();
        
        public boolean hasNext() {

            return i < cur.nbound;
            
        }

        public Entry<IVariable, IConstant> next() {

            // the index whose bindings are being returned.
            final int index = i++;

            return new Map.Entry<IVariable, IConstant>() {

                public IVariable getKey() {
                    
                    return cur.vars[index];
                    
                }

                public IConstant getValue() {

                    return cur.vals[index];
                    
                }

                public IConstant setValue(IConstant value) {

                    if (value == null)
                        throw new IllegalArgumentException();
                    
                    final IConstant t = cur.vals[index];
                    
                    cur.vals[index] = value;
                    
                    return t;
                    
                }

            };

        }

        public void remove() {

            throw new UnsupportedOperationException();
            
        }
        
    }
    
    public int size() {
        
        return current().nbound;
        
    }
    
    public void clearAll() {

    	current().clearAll();
    	
        // clear the hash code.
        hash = 0;
        
    }

	public void clear(final IVariable var) {

		if (current().clear(var)) {

			// clear the hash code.
			hash = 0;

		}

	}

    public IConstant get(final IVariable var) {

    	return current().get(var);
        
    }

    public boolean isBound(final IVariable var) {
        
        return get(var) != null;
        
    }

    public void set(final IVariable var, final IConstant val) {
        
		current().set(var, val);
        
        // clear the hash code.
        hash = 0;
        
    }

    public String toString() {
        
    	final ST cur = current();
    	
		final StringBuilder sb = new StringBuilder();

		sb.append("{");

		for (int i = 0; i < cur.nbound; i++) {

			if (i > 0)
				sb.append(", ");

			sb.append(cur.vars[i]);

			sb.append("=");

			sb.append(cur.vals[i]);

        }
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    public boolean equals(final Object t) {
        
        if (this == t)
            return true;
        
        if(!(t instanceof IBindingSet))
            return false;
        
        final IBindingSet o = (IBindingSet)t;

        final ST cur = current();

        if (cur.nbound != o.size())
            return false;
        
        for(int i=0; i<cur.nbound; i++) {
            
            final IConstant<?> o_val = o.get ( cur.vars [ i ] ) ;
            if ( null == o_val || !cur.vals[i].equals( o_val ))
                return false;
            
        }
        
        return true;
        
    }

    public int hashCode() {

        if (hash == 0) {

            int result = 0;

            final ST cur = current();
            
            for (int i = 0; i < cur.nbound; i++) {

                if (cur.vals[i] == null)
                    continue;

                result ^= cur.vals[i].hashCode();

            }

            hash = result;

        }
        return hash;

    }
    private int hash;

}
