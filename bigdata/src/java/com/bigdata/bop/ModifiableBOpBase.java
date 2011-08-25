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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * A mutable list containing the argument values (aka children) and never
     * <code>null</code>.
     */
    private final ArrayList<BOp> args;

    /**
     * A mutable map containing the operator annotations and never
     * <code>null</code>.
     */
    private final Map<String, Object> annotations;

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

        args = new ArrayList<BOp>(op.args);

        annotations = new LinkedHashMap<String, Object>(op.annotations);

    }

    /**
     * Shallow copy constructor (required).
     * 
     * @param args
     *            The arguments to the operator.
     * @param annotations
     *            The annotations for the operator (optional). When
     *            <code>null</code>, a mutable map with a default capacity of
     *            {@value CoreBaseBOp#DEFAULT_INITIAL_CAPACITY} is allocated. Do
     *            NOT specify an immutable map.
     */
    public ModifiableBOpBase(final BOp[] args,
            final Map<String, Object> annotations) {

        if (args == null)
            throw new IllegalArgumentException();

        checkArgs(args);

        this.args = new ArrayList<BOp>(args.length);

        for(BOp t : args)
            this.args.add(t);

        this.annotations = (annotations == null ? new LinkedHashMap<String, Object>(
                DEFAULT_INITIAL_CAPACITY) : annotations);

    }

    final public Map<String, Object> annotations() {

        return annotations;
    
    }
    
    public BOp get(final int index) {
        
        return args.get(index);
        
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

        args.set(index, newArg);

        return this;

    }

    /**
     * Add a new argument.
     * 
     * @param newArg
     *            The argument.
     * 
     * @return <i>this</i>.
     */
    public void addArg(final BOp newArg) {
        
        args.add(newArg);
        
    }

    /**
     * Add an argument iff it is not already present.
     * 
     * @param arg
     *            The argument.
     */
    public void addArgIfAbsent(final BOp arg) {
        
        if(!args.contains(arg))
            args.add(arg);
        
    }
    
    /**
     * Remove the 1st occurrence of the argument.
     * 
     * @param arg
     *            The argument.
     *            
     * @return <code>true</code> iff the argument was removed.
     */
    public boolean removeArg(final BOp arg) {
        
        return args.remove(arg);
        
    }
    
    public int arity() {
        
        return args.size();
        
    }

    /**
     * The list of arguments (aka children) of this node.
     */
    final public List<BOp> args() {

        return args;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This {@link Iterator} supports removal.
     */
    final public Iterator<BOp> argIterator() {
        
        return args.iterator();
        
    }

    // shallow copy
    public BOp[] toArray() {

        return args.toArray(new BOp[args.size()]);

    }

    // shallow copy
    public <T> T[] toArray(final T[] a) {

        return args.toArray(a);
        
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
