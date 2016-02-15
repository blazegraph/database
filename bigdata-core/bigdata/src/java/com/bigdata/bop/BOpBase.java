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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Abstract base class for copy-on-write {@link BOp}s. The {@link BOpBase} class
 * is used for query evaluation operators. The copy-on-write contract provides a
 * safety margin during concurrent evaluation of query plans by ensuring that
 * all references are fully published.
 * <p>
 * Instances of this class are effectively immutable (mutation APIs always
 * return a deep copy of the operator to which the mutation has been applied),
 * {@link Serializable} to facilitate distributed computing, and
 * {@link Cloneable} to facilitate non-destructive tree rewrites.
 * <p>
 * <h2>Constructor patterns</h2>
 * <p>
 * {@link BOp}s should define the following public constructors
 * <dl>
 * <dt>
 * <code>public <i>Class</i>(BOp[] args, Map&lt;String,Object&gt; anns)</code></dt>
 * <dd>A shallow copy constructor. This is used when initializing a {@link BOp}
 * from the caller's data or when generated a query plan from Prolog. There are
 * some exceptions to this rule. For example, {@link Constant} does not define a
 * shallow copy constructor because that would not provide a means to set the
 * constant's value.</dd>
 * <dt><code>public <i>Class</i>(<i>Class</i> src)</code></dt>
 * <dd>A deep copy constructor. Mutation methods make a deep copy of the
 * {@link BOp}, apply the mutation to the copy, and then return the copy. This
 * is the "effectively immutable" contract. Again, there are some exceptions.
 * For example, {@link Var} provides a canonicalized mapping such that reference
 * tests may be used to determine if two {@link Var}s are the same. In order to
 * support that contract it overrides {@link Var#clone()}.</dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BOpBase extends CoreBaseBOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
	 * The argument values - <strong>direct access to this field is
	 * discouraged</strong> - the field is protected to support
	 * <em>mutation</em> APIs and should not be relied on for other purposes.
	 * <p>
	 * Note: This field is reported out as a {@link List} so we can make it
	 * thread safe and, if desired, immutable. However, it is internally a
	 * simple array. Subclasses can implement mutation operations which return
	 * deep copies in which the argument values have been modified using
	 * {@link #_set(int, BOp)}.
	 * <p>
	 * If we allowed mutation of the arguments (outside of the object creation
	 * pattern) then caching of the arguments (or annotations) by classes such
	 * as EQ will cause {@link #clone()} to fail because (a) it will do
	 * a field-by-field copy on the concrete implementation class; and (b) it
	 * will not consistently update the cached references. In order to "fix"
	 * this problem, any classes which cache arguments or annotations would have
	 * to explicitly overrides {@link #clone()} in order to set those fields
	 * based on the arguments on the cloned {@link BOpBase} class.
	 * <p>
	 * Note: This must be at least "effectively" final per the effectively
	 * immutable contract for {@link BOp}s.
	 */
    private final BOp[] args;

    /**
     * The operator annotations.
     * <p>
     * Note: This must be at least "effectively" final per the effectively
     * immutable contract for {@link BOp}s.
     */
    private final Map<String,Object> annotations;

    /**
     * Deep copy constructor (required).
     * <p>
     * Each {@link BOp} MUST implement a public copy constructor with the
     * signature:
     * 
     * <pre>
     * public Foo(Foo)
     * </pre>
     * 
     * This construct is invoked by {@link #clone()} using reflection and is
     * responsible for the deep copy semantics for the {@link BOp}.
     * <p>
     * The default implementation makes a deep copy of {@link #args()} and
     * {@link #annotations()} but DOES NOT perform field-by-field copying.
     * Subclasses may simply delegate the constructor to their super class
     * unless they have additional fields which need to be copied.
     * <p>
     * This design pattern was selected because it preserves the immutable
     * contract of the {@link BOp} which gives us our thread safety and
     * visibility guarantees. Since the deep copy is realized by the {@link BOp}
     * implementation classes, it is important that each class take
     * responsibility for the deep copy semantics of any fields it may declare.
     * 
     * @param op
     *            A deep copy will be made of this {@link BOp}.
     * 
     * @throws NullPointerException
     *             if the argument is <code>null</code>.
     */
    public BOpBase(final BOpBase op) {
        // Note: only shallow copy is required to achieve immutable semantics!
		if (op.args == BOp.NOARGS || op.args.length == 0) {
			// fast path for zero arity operators.
			args = BOp.NOARGS;
        } else {
        	args = Arrays.copyOf(op.args, op.args.length);
        }
        annotations = new LinkedHashMap<String, Object>(op.annotations);
    }

    /**
     * Shallow copy constructor (required).
     * 
     * @param args
     *            The arguments to the operator.
     * @param annotations
     *            The annotations for the operator (optional).
     */
    public BOpBase(final BOp[] args,
            final Map<String, Object> annotations) {

        if (args == null)
            throw new IllegalArgumentException();
        
        checkArgs(args);
        
        this.args = args;

		this.annotations = (annotations == null ? new LinkedHashMap<String, Object>(
				DEFAULT_INITIAL_CAPACITY)
				: annotations);

    }

    @Override
    final public Map<String, Object> annotations() {

        return Collections.unmodifiableMap(annotations);
    
    }

    @Override
    protected boolean annotationsEqual(final BOp o) {

        if (o instanceof BOpBase) {

            // Fast path when comparing two immutable bops.
            return annotationsEqual(annotations, ((BOpBase) o).annotations);

        }

        return super.annotationsEqual(annotations, o.annotations());

    }
    
    /**
     * A copy of the args[] array.
     */
    final protected BOp[] argsCopy() {
        
        final BOp[] tmp = new BOp[args.length];

        for (int i = 0; i < args.length; i++) {
        
            tmp[i] = args[i];
            
        }

        return tmp;
        
    }

    /**
     * A copy of the annotations.
     */
    final protected Map<String, Object> annotationsCopy() {
        
        return new LinkedHashMap<String, Object>(annotations);

    }

    /**
     * A reference to the actual annotations map object. This is used in some
     * hot spots to avoid creating a new annotations map when we know that the
     * annotations will not be modified (annotations are always set within the
     * context in which the {@link BOpBase} instance is created so we can know
     * this locally by inspection of the code).
     */
    final protected Map<String,Object> annotationsRef() {
        
        return annotations;
        
    }
    
    @Override
    public BOp get(final int index) {
        
        return args[index];
        
    }
    
    /**
     * Set the value of an operand.
     * <p>
     * Note: This is protected to facilitate copy-on-write patterns. It is not
     * public to prevent arbitrary changes to operators outside of methods which
     * clone the operator and return the modified version. This is part of the
     * effectively immutable contract for {@link BOp}s.
     * 
     * @param index
     *            The index.
     * @param op
     *            The operand.
     *            
     * @return The old value.
     */
    final protected void _set(final int index, final BOp op) {
        
        this.args[index] = op;
        
    }

    /**
     * Return a new {@link BOpBase} in which the child operand has been replaced
     * by the given expression.
     * 
     * @param index
     *            The index of the child expression to be replaced.
     * @param newArg
     *            The new child expression.
     * 
     * @return A copy of this {@link BOpBase} in which the child operand has
     *         been replaced.
     */
    public BOpBase setArg(final int index, final BOp newArg) {

        if (newArg == null)
            throw new IllegalArgumentException();

        final BOpBase tmp = (BOpBase) this.clone();

        tmp._set(index, newArg);

        return tmp;

    }

    /**
     * Effectively overwrites the specified argument with the provided value.<p>
     * WARNING: this method could break logic of the code, which relies on immutability of the arguments list.
     * It is introduced while fixing issues with deferred IV resolution and intended to be used only before IV resolution completed.
     * @see https://jira.blazegraph.com/browse/BLZG-1755 (Date literals in complex FILTER not properly resolved)
     * @param index
     *            The index of the child expression to be replaced.
     * @param newArg
     *            The new child expression.
     */
    public void __replaceArg(final int index, final BOp newArg) {
    	args[index] = newArg;
    }
    
    @Override
    public int arity() {
        
        return args.length;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This is much less efficient than {@link #argIterator()}.
     */
    @Override
    final public List<BOp> args() {

        return Collections.unmodifiableList(Arrays.asList(args));
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The iterator does not support removal. (This is more efficient than
     * #args()).
     */
    @Override
    final public Iterator<BOp> argIterator() {
    	
    	return new ArgIterator();
    	
    }

	/**
	 * An iterator visiting the arguments which does not support removal.
	 */
	private class ArgIterator implements Iterator<BOp> {

		private int i = 0;

		public boolean hasNext() {
			return i < args.length;
		}

		public BOp next() {
			if (!hasNext())
				throw new NoSuchElementException();
			return args[i++];
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

    // shallow copy
    @Override
    public BOp[] toArray() {

        final BOp[] a = new BOp[args.length];

        return Arrays.copyOf(args, args.length, a.getClass());

    }

    // shallow copy
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] a) {
        if (a.length < args.length)
            return (T[]) Arrays.copyOf(args, args.length, a.getClass());
        System.arraycopy(args, 0, a, 0, args.length);
        if (a.length > args.length)
            a[args.length] = null;
        return a;
    }

//    /**
//     * Deep copy of a {@link BOpBase}.
//     * 
//     * @return The deep copy.
//     */
//    public BOpBase deepCopy() {
//        
//        final BOpBase bop = (BOpBase) this.clone();
//
//        bop.args = deepCopy(bop.args);
//        
//        bop.annotations = deepCopy(bop.annotations);
//        
//        return bop;
//        
//    }
    
    /**
     * Deep copy the arguments.
     * <p>
     * Note: As long as we stick to the immutable semantics for bops, we can
     * just make a shallow copy of the arguments in the "copy" constructor and
     * then modify them within the specific operator constructor before
     * returning control to the caller. This would result in less heap churn.
     */
    static protected BOp[] deepCopy(final BOp[] a) {
		if (a == BOp.NOARGS || a.length == 0) {
			// fast path for zero arity operators.
			return BOp.NOARGS;
        }
        final BOp[] t = new BOp[a.length];
        for (int i = 0; i < a.length; i++) {
            t[i] = a[i] == null ? null : a[i].clone();
        }
        return t;
    }

    /**
     * Deep copy the annotations.
     * <p>
     * Note: This does not know how to deep copy annotations which are not
     * {@link BOp}s or immutable objects such as {@link String}s or
     * {@link Number}s. Such objects should not be used as annotations.
     * 
     * @todo When attaching large data sets to a query plan they should be
     *       attached using a light weight reference object which allows them to
     *       be demanded by a node so deep copy remains a light weight
     *       operation. This also has the advantage that the objects are
     *       materialized on a node only when they are needed, which keeps the
     *       query plan small. Examples would be sending a temporary graph
     *       containing an ontology or some conditional assertions with a query
     *       plan.
     */
    static protected Map<String, Object> deepCopy(final Map<String, Object> a) {
        if (a == BOp.NOANNS) {
            // Fast past for immutable, empty annotations.
            return a;
        }
        // allocate map.
        final Map<String, Object> t = new LinkedHashMap<String, Object>(a
                .size());
        // copy map's entries.
        final Iterator<Map.Entry<String, Object>> itr = a.entrySet().iterator();
        while (itr.hasNext()) {
            final Map.Entry<String, Object> e = itr.next();
            if (e.getValue() instanceof BOp) {
                // deep copy bop annotations.
                t.put(e.getKey(), ((BOp) e.getValue()).clone());
            } else {
                // shallow copy anything else.
                t.put(e.getKey(), e.getValue());
            }
        }
        // return the copy.
        return t;
    }

//    @SuppressWarnings("unchecked")
//    public <T> T getProperty(final String name, final T defaultValue) {
//
//        if (!annotations.containsKey(name))
//            return defaultValue;
//
//        final Object val = annotations.get(name);
//
//        if (defaultValue != null && val.getClass() != defaultValue.getClass()) {
//
//            /*
//             * Attempt to convert to the correct target type.
//             */
//            
//            if (defaultValue.getClass() == Integer.class) {
//                return (T) Integer.valueOf("" + val);
//            }
//            if (defaultValue.getClass() == Long.class) {
//                return (T) Long.valueOf("" + val);
//            }
//            if (defaultValue.getClass() == Float.class) {
//                return (T) Float.valueOf("" + val);
//            }
//            if (defaultValue.getClass() == Double.class) {
//                return (T) Double.valueOf("" + val);
//            }
//
//        }
//
//        return (T) val;
//
//    }

//    @SuppressWarnings("unchecked")
//    public <T> T getProperty(final String name) {
//
//        return (T) annotations.get(name);
//
//    }

    @Override
    public Object getProperty(final String name) {

        return annotations.get(name);

    }

//    public <T> T getRequiredProperty(final String name) {
//
//        @SuppressWarnings("unchecked")
//        final T tmp = (T) annotations.get(name);
//
//        if (tmp == null)
//            throw new IllegalArgumentException("Required property: " + name);
//
//        return tmp;
//        
//    }

//    public Object getRequiredProperty(final String name) {
//
//        final Object tmp = annotations.get(name);
//
//        if (tmp == null)
//			throw new IllegalStateException("Required property: " + name
//					+ " : " + this);
//
//        return tmp;
//        
//    }

    /**
     * Set an annotation.
     * <p>
     * Note: This is protected to facilitate copy-on-write patterns. It is not
     * public to prevent arbitrary changes to operators outside of methods which
     * clone the operator and return the modified version. This is part of the
     * effectively immutable contract for {@link BOp}s.
     * 
     * @param name
     *            The name.
     * @param value
     *            The value.
     * 
     * @return The old value.
     */
    protected Object _setProperty(final String name, final Object value) {
        
        return annotations.put(name,value);
        
    }
    
    /**
     * Clear an annotation.
     * <p>
     * Note: This is protected to facilitate copy-on-write patterns. It is not
     * public to prevent arbitrary changes to operators outside of methods which
     * clone the operator and return the modified version. This is part of the
     * effectively immutable contract for {@link BOp}s.
     * 
     * @param name
     *            The name.
     */
    protected void _clearProperty(final String name) {
        
        annotations.remove(name);
        
    }

    @Override
    public BOpBase setProperty(final String name, final Object value) {

        final BOpBase tmp = (BOpBase) this.clone();

        tmp._setProperty(name, value);

        return tmp;

    }

    /**
     * Conditionally sets the property.
     * 
     * @param name
     *            The name.
     * @param value
     *            The value.
     * 
     * @return A copy of this {@link BOp} on which the property has been set.
     * 
     * @throws IllegalStateException
     *             if the property is already set.
     */
    public BOpBase setUnboundProperty(final String name, final Object value) {

        final BOpBase tmp = (BOpBase) this.clone();

        if (tmp._setProperty(name, value) != null)
            throw new IllegalStateException("Already set: name=" + name
                    + ", value=" + value);

        return tmp;

    }
    
    /**
     * Clear the named annotation.
     * 
     * @param name
     *            The annotation.
     *            
     * @return A copy of this {@link BOp} in which the named annotation has been
     *         removed.
     */
    public BOpBase clearProperty(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        final BOpBase tmp = (BOpBase) this.clone();

        tmp._clearProperty(name);

        return tmp;

    }

    /**
     * Strips off the named annotations.
     * 
     * @param names
     *            The annotations to be removed.
     * 
     * @return A copy of this {@link BOp} in which the specified annotations do
     *         not appear.
     */
    public BOp clearAnnotations(final String[] names) {

        final BOpBase tmp = (BOpBase) this.clone();

        for(String name : names) {
            
            tmp._clearProperty(name);
            
        }

        return tmp;
        
    }

}
