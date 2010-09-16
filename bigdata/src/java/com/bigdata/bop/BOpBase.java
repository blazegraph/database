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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.constraint.EQ;

/**
 * Abstract base class for {@link BOp}s.
 * <p>
 * <h2>Constructor patterns</h2>
 * <p>
 * {@link BOp}s should define the following public constructors
 * <dl>
 * <dt>{@link #AbstractBOp(BOp[], Map)}</dt>
 * <dd>A shallow copy constructor. This is used when initializing a {@link BOp}
 * from the caller's data or when generated a query plan from Prolog. There are
 * some exceptions to this rule. For example, {@link Constant} does not define a
 * shallow copy constructor because that would not provide a means to set the
 * constant's value.</dd>
 * <dt>{@link #AbstractBOp(BOp[])}</dt>
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
public class BOpBase implements BOp {

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
     * simple array and exposed to subclasses so they can implement mutation
     * operations which return deep copies in which the argument values have
     * been modified.
     * <p>
     * If we allow mutation of the arguments then caching of the arguments (or
     * annotations) by classes such as {@link EQ} will cause {@link #clone()} to
     * fail because (a) it will do a field-by-field copy on the concrete
     * implementation class; and (b) it will not consistently update the cached
     * references. In order to "fix" this problem, any classes which cache
     * arguments or annotations would have to explicitly overrides
     * {@link #clone()} in order to set those fields based on the arguments on
     * the cloned {@link BOpBase} class.
     */
    protected final BOp[] args;

    /**
     * The operator annotations.
     */
    protected final Map<String,Object> annotations;
    
    /**
     * Check the operator argument.
     * 
     * @param args
     *            The arguments.
     * 
     * @throws IllegalArgumentException
     *             if the arguments are not valid for the operator.
     */
    protected void checkArgs(final Object[] args) {

    }

    /**
     * Deep copy clone semantics for {@link #args} and {@link #annotations}.
     * <p>
     * {@inheritDoc}
     */
    public BOpBase clone() {
        final Class<? extends BOpBase> cls = getClass();
        final Constructor<? extends BOpBase> ctor;
        try {
            ctor = cls.getConstructor(new Class[] { cls });
            return ctor.newInstance(new Object[] { this });
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
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
        // deep copy the arguments.
        args = deepCopy(op.args);
        // deep copy the annotations.
        annotations = deepCopy(op.annotations);
    }
    
    /**
     * @param args
     *            The arguments to the operator.
     */
    public BOpBase(final BOp[] args) {
       
        this(args, null/* annotations */);
        
    }

    /**
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
        
        this.annotations = (annotations == null ? new LinkedHashMap<String, Object>()
                : annotations);
        
    }

    final public Map<String, Object> annotations() {

        return Collections.unmodifiableMap(annotations);
    
    }
    
    public BOp get(final int index) {
        
        return args[index];
        
    }
    
    public int arity() {
        
        return args.length;
        
    }

    final public List<BOp> args() {

        return Collections.unmodifiableList(Arrays.asList(args));
        
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

    /** deep copy the arguments. */
    static private BOp[] deepCopy(final BOp[] a) {
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
    static private Map<String,Object> deepCopy(final Map<String,Object> a) {
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

    @SuppressWarnings("unchecked")
    public <T> T getProperty(final String name, final T defaultValue) {

        if (!annotations.containsKey(name))
            return defaultValue;

        return (T) annotations.get(name);

    }

    @SuppressWarnings("unchecked")
    public <T> T getProperty(final String name) {

        return (T) annotations.get(name);

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

    public Object getRequiredProperty(final String name) {

        final Object tmp = annotations.get(name);

        if (tmp == null)
            throw new IllegalStateException("Required property: " + name);

        return tmp;
        
    }

    public int getId() {
        
        return (Integer) getRequiredProperty(Annotations.BOP_ID);
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("(");
        for (int i = 0; i < args.length; i++) {
            final BOp t = args[i];
            if (i > 0)
                sb.append(',');
            sb.append(t.getClass().getSimpleName());
        }
        sb.append(")[");
        final Integer id = (Integer) annotations.get(Annotations.BOP_ID);
        if (id != null)
            sb.append("Annotations.BOP_ID=" + id);
        sb.append("]");
        return sb.toString();

    }

    /**
     * The default implementation returns {@link BOpEvaluationContext#ANY} and
     * must be overridden by operators which have a different {@link BOpEvaluationContext}.
     * <p>
     * {@inheritDoc}
     */
    public BOpEvaluationContext getEvaluationContext() {
        
        return BOpEvaluationContext.ANY;
        
    }

    public final boolean isMutation() {

        return getProperty(Annotations.MUTATION, Annotations.DEFAULT_MUTATION);

    }

    public final long getTimestamp() {

		return (Long) getRequiredProperty(Annotations.TIMESTAMP);

    }

}
