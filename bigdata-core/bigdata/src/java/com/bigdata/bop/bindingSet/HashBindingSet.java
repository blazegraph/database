/*

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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop.bindingSet;

import com.bigdata.bop.Constant;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

/**
 * {@link IBindingSet} backed by a {@link LinkedHashMap}.
 * <p>
 * Note: A {@link LinkedHashMap} provides a fast iterator, which we use a bunch.
 * However, {@link IBindingSet}s are inherently unordered collections of
 * bindings so the order preservation aspect of the {@link LinkedHashMap} is not
 * relied upon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HashBindingSet implements IBindingSet {

    private static final long serialVersionUID = -2989802566387532422L;
    
//	/**
//  * Note: A {@link LinkedHashMap} provides a fast iterator, which we use a
//  * bunch.
//  */
// private final LinkedHashMap<IVariable, IConstant> map;

//	/**
//	 * The stack of symbol tables. Each symbol table is a mapping from an
//	 * {@link IVariable} onto its non-<code>null</code> bound {@link IConstant}.
//	 * The stack is initialized with an empty symbol table. Symbol tables may be
//	 * pushed onto the stack or popped off of the stack, but the stack MAY NOT
//	 * become empty.
//	 */
//	private final LightStack<LinkedHashMap<IVariable, IConstant>> stack;

    final private LinkedHashMap<IVariable,IConstant> current;
    
	/**
	 * Return the symbol table on the top of the stack.
	 */
	private LinkedHashMap<IVariable, IConstant> current() {

	    return current;
	    
//		return stack.peek();
		
	}

//    public void push(final IVariable[] vars) {
//
//        // Create a new symbol table.
//        final LinkedHashMap<IVariable, IConstant> tmp = new LinkedHashMap<IVariable, IConstant>(
//                current().size());
//
//        /*
//         * For each variable which is to be projected into the new symbol table,
//         * look up its binding. If it is bound, then copy that binding into the
//         * new symbol table.
//         */
//        for (IVariable<?> var : vars) {
//
//            final IConstant<?> val = get(var);
//            
//            if (val != null)
//                tmp.put(var, val);
//
//        }
//
//        // Push the new symbol table onto the stack.
//        stack.push(tmp);
//
//        hash = 0;
//
//    }
//
//    public void pop(final IVariable[] vars) {
//        
//        if (stack.size() < 2) {
//            /*
//             * The stack may never become empty. Therefore there must be at
//             * least two symbol tables on the stack for a pop() request.
//             */
//            throw new IllegalStateException();
//        }
//        
//        // Pop the symbol table off of the top of the stack.
//        final LinkedHashMap<IVariable, IConstant> old = stack.pop();
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
//        for (IVariable<?> var : vars) { // for each projected variable.
//
//            // The binding from the old symbol table.
//            final IConstant val = old.get(var);
//            
//            if (val != null) { // if the binding was projected
//
//                set(var, val); // then copy to revealed symbol table.
//
//            }
//
//        }
//
//        hash = 0;
//
//    }
	
//	public void push() {
//
//		// The current symbol table.
//		final LinkedHashMap<IVariable, IConstant> cur = current();
//
//		// Create a new symbol table.
//		final LinkedHashMap<IVariable, IConstant> tmp = new LinkedHashMap<IVariable, IConstant>(
//				cur.size());
//
//		// Push the new symbol table onto the stack.
//		stack.push(tmp);
//
//		/*
//		 * Make a copy of each entry in the symbol table which was on the top of
//		 * the stack when we entered this method, inserting the entries into the
//		 * new symbol table as we go. This avoids side effects of mutation on
//		 * the nested symbol tables and also ensures that we do not need to read
//		 * through to the nested symbol tables when answering a query about the
//		 * current symbol table. The only down side of this is that naive
//		 * serialization is that much less compact.
//		 */
//		for (Map.Entry<IVariable, IConstant> e : cur.entrySet()) {
//
//			tmp.put(e.getKey(), e.getValue());
//
//		}
//		
//	}

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
//		final LinkedHashMap<IVariable,IConstant> old = stack.pop();
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
     * New empty binding set.
     */
    public HashBindingSet() {

//        /*
//         * Start with a capacity of ONE (1) and expand only as required.
//         */
//		stack = new LightStack<LinkedHashMap<IVariable, IConstant>>(1);
//
//		stack.push(new LinkedHashMap<IVariable, IConstant>());
        
        current = new LinkedHashMap<IVariable, IConstant>();
        
    }

    /**
     * Copy constructor (used by clone, copy).
     * 
     * @param src
     */
    protected HashBindingSet(final HashBindingSet src, final IVariable[] variablesToKeep) {
        
//        final int stackSize = src.stack.size();
//
//		stack = new LightStack<LinkedHashMap<IVariable,IConstant>>(stackSize);
//
//		int depth = 1;
//		
//		for (LinkedHashMap<IVariable, IConstant> srcLst : src.stack) {
//
//			/*
//			 * Copy the source bindings.
//			 * 
//			 * Note: If a restriction exists on the variables to be copied, then
//			 * it is applied onto the the top level of the stack. If the symbol
//			 * table is saved when it is pop()'d, then the modified bindings
//			 * will replace the parent symbol table on the stack.
//			 */
//			final LinkedHashMap<IVariable,IConstant> tmp = copy(srcLst,
//					depth == stackSize ? variablesToKeep : null);
//
//			// Push onto the stack.
//			stack.push(tmp);
//
//		}

        current = copy(src.current, variablesToKeep);

	}
    
   
       private HashBindingSet(LinkedHashMap<IVariable,IConstant> contents) {
           current = contents;
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
	private LinkedHashMap<IVariable, IConstant> copy(
			final LinkedHashMap<IVariable, IConstant> src,
			final IVariable[] variablesToKeep) {

		final LinkedHashMap<IVariable, IConstant> dst = new LinkedHashMap<IVariable, IConstant>(
				variablesToKeep != null ? variablesToKeep.length : src.size());

		final Iterator<Map.Entry<IVariable, IConstant>> itr = src.entrySet()
				.iterator();

		while (itr.hasNext()) {

			final Map.Entry<IVariable, IConstant> e = itr.next();

			boolean keep = true;

			if (variablesToKeep != null) {
			
				keep = false;
				
				for (IVariable<?> x : variablesToKeep) {
				
					if (x == e.getKey()) {
					
						keep = true;
						
						break;
						
					}
					
				}
				
			}

			if (keep)
				dst.put(e.getKey(), e.getValue());

        }

		return dst;
		
	}
        
        
    /**
     * Return a copy of the source list minus entries assigning error values
     * (Constant.errorValue() or its copies). .
     *
     * @param src The source list.
     * @param variablesToKeep When non-<code>null</code>, only the bindings for
     * the variables listed in this array will copied.
     *
     * @return The copy.
     */
    private LinkedHashMap<IVariable, IConstant> copyMinusErrors(
            final LinkedHashMap<IVariable, IConstant> src,
            final IVariable[] variablesToKeep) {

        final LinkedHashMap<IVariable, IConstant> dst = new LinkedHashMap<IVariable, IConstant>(
                variablesToKeep != null ? variablesToKeep.length : src.size());

        final Iterator<Map.Entry<IVariable, IConstant>> itr = src.entrySet()
                .iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            if (e.getValue() == Constant.errorValue()) {
                continue;
            }

            boolean keep = true;

            if (variablesToKeep != null) {

                keep = false;

                for (IVariable<?> x : variablesToKeep) {

                    if (x == e.getKey()) {

                        keep = true;

                        break;

                    }

                }

            }

            if (keep) {
                dst.put(e.getKey(), e.getValue());
            }

        }

        return dst;


    } // copyMinusErrors(..)
        
        
        

    /**
     * Package private constructor used by the unit tests.
     * 
     * @param src
     */
    HashBindingSet(final IBindingSet src) {

    	this();
    	
        final Iterator<Map.Entry<IVariable, IConstant>> itr = src.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            set(e.getKey(), e.getValue());

        }

    }
    
    /**
     * Package private constructor used by the unit tests.
     * @param vars
     * @param vals
     */
    HashBindingSet(final IVariable[] vars, final IConstant[] vals) {

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
    
    public HashBindingSet clone() {
        
		return new HashBindingSet(this, null /* variablesToKeep */);
        
    }

	public HashBindingSet copy(final IVariable[] variablesToKeep) {

		return new HashBindingSet(this/* src */, variablesToKeep);

    }
               
        
    @Override
    public IBindingSet copyMinusErrors(final IVariable[] variablesToKeep) {
        return new HashBindingSet(copyMinusErrors(this.current,
                variablesToKeep));
        
    }

        
    /** 
     * @return true if this IBindingSet contains an assignment of an error value
     */
    @Override
    public final boolean containsErrorValues() {        
        for (IConstant val : this.current.values()) {  
            if (val == Constant.errorValue())
                return true;            
        }
        return false;
    }
        
    public boolean isBound(final IVariable var) {
     
        if (var == null)
            throw new IllegalArgumentException();
        
        return current().containsKey(var);
        
    }
    
    public IConstant get(final IVariable var) {
   
        if (var == null)
            throw new IllegalArgumentException();
        
        return current().get(var);
        
    }

    public void set(final IVariable var, final IConstant val) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();
        
        current().put(var,val);

        // clear the hash code.
        hash = 0;
        
    }

    public void clear(final IVariable var) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        current().remove(var);
        
        // clear the hash code.
        hash = 0;

    }
    
    public void clearAll() {
        
        current().clear();
    
        // clear the hash code.
        hash = 0;

    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("{ ");

        int i = 0;
        
        final Iterator<Map.Entry<IVariable, IConstant>> itr = current().entrySet()
                .iterator();

        while (itr.hasNext()) {

            if (i > 0)
                sb.append(", ");

            final Map.Entry<IVariable, IConstant> entry = itr.next();
            
            sb.append(entry.getKey());

            sb.append("=");

            sb.append(entry.getValue());

            i++;

        }

        sb.append(" }");

        return sb.toString();

    }

    public Iterator<Entry<IVariable, IConstant>> iterator() {

//        return Collections.unmodifiableMap(current()).entrySet().iterator();
        
        return current().entrySet().iterator();
        
    }

    public Iterator<IVariable> vars() {

        return Collections.unmodifiableSet(current().keySet()).iterator();
        
    }
    
    public boolean isEmpty() {
        
        return current().isEmpty();
        
    }
    
    public int size() {

        return current().size();
        
    }

    public boolean equals(final Object t) {
        
        if (this == t)
            return true;
        
        if(!(t instanceof IBindingSet))
            return false;
        
        final IBindingSet o = (IBindingSet) t;

        if (size() != o.size())
            return false;
        
        final Iterator<Map.Entry<IVariable,IConstant>> itr = current().entrySet().iterator();
        
        while(itr.hasNext()) {

            final Map.Entry<IVariable,IConstant> entry = itr.next();
            
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

            for(IConstant<?> c : current().values()) {

                if (c == null)
                    continue;

                result ^= c.hashCode();

            }

            hash = result;

        }
        return hash;

    }
    
    /**
     * Note: This hash code MUST be invalidate on mutation!
     */
    private int hash;

}
