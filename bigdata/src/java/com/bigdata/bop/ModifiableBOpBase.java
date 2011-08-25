/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.bigdata.bop.constraint.EQ;

/**
 * Abstract base class for mutable {@link BOp}s. Unlike {@link BOpBase}, this
 * class supports destructive mutation. This is the base class for the bigdata
 * AST nodes. ASTs are destructively rewritten by optimizers before they are
 * turned into a query plan.
 * <p>
 * <h2>Constructor patterns</h2>
 * <p>
 * {@link ModifiableBOpBase}s should define the following public constructors
 * <dl>
 * <dt>
 * <code>public <i>Class</i>(BOp[] args, Map&lt;String,Object&gt; anns)</code></dt>
 * <dd>A shallow copy constructor. This is used when initializing a {@link BOp}
 * from the caller's data or when generated a query plan from Prolog. There are
 * some exceptions to this rule. For example, {@link Constant} does not define a
 * shallow copy constructor because that would not provide a means to set the
 * constant's value.</dd>
 * <dt><code>public <i>Class</i>(<i>Class</i> src)</code></dt>
 * <dd>A copy constructor.</dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ModifiableBOpBase extends CoreBaseBOp {

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
     * as {@link EQ} will cause {@link #clone()} to fail because (a) it will do
     * a field-by-field copy on the concrete implementation class; and (b) it
     * will not consistently update the cached references. In order to "fix"
     * this problem, any classes which cache arguments or annotations would have
     * to explicitly overrides {@link #clone()} in order to set those fields
     * based on the arguments on the cloned {@link ModifiableBOpBase} class.
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
     * This construct is invoked by {@link #clone()} using reflection.
     * <p>
     * The default implementation makes a shallow copy of {@link #args()} and
     * {@link #annotations()} but DOES NOT perform field-by-field copying.
     * Subclasses may simply delegate the constructor to their super class
     * unless they have additional fields which need to be copied.
     * 
     * @param op
     *            A deep copy will be made of this {@link BOp}.
     * 
     * @throws NullPointerException
     *             if the argument is <code>null</code>.
     */
    public ModifiableBOpBase(final ModifiableBOpBase op) {
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
    public ModifiableBOpBase(final BOp[] args, final Map<String, Object> annotations) {

        if (args == null)
            throw new IllegalArgumentException();

        checkArgs(args);

        this.args = args;

        this.annotations = (annotations == null ? new LinkedHashMap<String, Object>(
                DEFAULT_INITIAL_CAPACITY) : annotations);

    }

    final public Map<String, Object> annotations() {

        return annotations;
    
    }
    
    public BOp get(final int index) {
        
        return args[index];
        
    }

    /**
     * Replace the value of the argument at the specified index (destructive
     * mutation).
     * 
     * @param index
     *            The index of the child expression to be replaced.
     * @param newArg
     *            The new child expression.
     * 
     * @return This {@link ModifiableBOpBase}.
     */
    public ModifiableBOpBase setArg(final int index, final BOp newArg) {

        if (newArg == null)
            throw new IllegalArgumentException();

        args[index] = newArg;

        return this;

    }

    public int arity() {
        
        return args.length;
        
    }

    /**
     * A mutable list backed by the {@link #args}[]. Elements of the list may be
     * modified, but the size of the list can not be changed.
     */
    final public List<BOp> args() {

        return Arrays.asList(args);
        
    }

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

//    @SuppressWarnings("unchecked")
//    public <T> T getProperty(final String name) {
//
//        return (T) annotations.get(name);
//
//    }

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

    /**
     * Set the named property value (destructive mutation).
     * 
     * @param name
     *            The name.
     * @param value
     *            The new value.
     *            
     * @return <i>this</i>.
     */
    public ModifiableBOpBase setProperty(final String name, final Object value) {

        annotations.put(name, value);

        return this;
        
    }

    /**
     * Conditionally set the named property iff it is not bound (destructive
     * mutation).
     * 
     * @param name
     *            The name.
     * @param value
     *            The value.
     * 
     * @return <i>this</i>.
     * 
     * @throws IllegalStateException
     *             if the property is already set.
     */
    public ModifiableBOpBase setUnboundProperty(final String name, final Object value) {

        final Object oldValue = getProperty(name);
        
        if (oldValue != null)
            throw new IllegalStateException("Already set: name=" + name
                    + ", value=" + oldValue);

        setProperty(name, value);

        return this;

    }

    /**
     * Clear the named annotation (destructive mutation).
     * 
     * @param name
     *            The annotation.
     *            
     * @return <i>this</i>.
     */
    public ModifiableBOpBase clearProperty(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        annotations.remove(name);
        
        return this;

    }
    
}
