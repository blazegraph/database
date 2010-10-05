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
import com.bigdata.btree.Tuple;

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
     * An empty array.
     */
    static protected final transient BOp[] NOARGS = new BOp[] {};

    /**
     * An empty immutable annotations map.
     * 
     * @todo This is for things like {@link Constant} and {@link Var} which are
     *       never annotated. However, i'm not sure if this is a good idea or
     *       not. A "copy on write" map might be better.
     */
    static protected final transient Map<String,Object> NOANNS = Collections.emptyMap();
    
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
     * Check the operator argument.
     * 
     * @param args
     *            The arguments.
     * 
     * @throws IllegalArgumentException
     *             if the arguments are not valid for the operator.
     */
    protected void checkArgs(final BOp[] args) {

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

//    /**
//     * Deserialization constructor (required).
//     */
//    public BOpBase() {
//        
//    }
    
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
        // deep copy the arguments.
        args = deepCopy(op.args);
        // deep copy the annotations.
        annotations = deepCopy(op.annotations);
    }

//    /**
//     * @param args
//     *            The arguments to the operator.
//     *            
//     * @deprecated Use the shallow copy constructor.
//     */
//    public BOpBase(final BOp[] args) {
//       
//        this(args, null/* annotations */);
//        
//    }

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
        
        this.annotations = (annotations == null ? new LinkedHashMap<String, Object>()
                : annotations);
        
    }

    final public Map<String, Object> annotations() {

        return Collections.unmodifiableMap(annotations);
    
    }
    
    public BOp get(final int index) {
        
        return args[index];
        
    }
    
    /**
     * Set the value of an operand.
     * <p>
     * Note: This protected to facilitate copy-on-write patterns. It is not
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
     * 
     * @todo thread safety and visibility....
     */
    final protected void set(final int index, final BOp op) {
        
        this.args[index] = op;
        
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
    static protected BOp[] deepCopy(final BOp[] a) {
        if (a == NOARGS) {
            // fast path for zero arity operators.
            return a;
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
        if (a == NOANNS) {
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

    @SuppressWarnings("unchecked")
    public <T> T getProperty(final String name, final T defaultValue) {

        if (!annotations.containsKey(name))
            return defaultValue;

        return (T) annotations.get(name);

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

    public Object getRequiredProperty(final String name) {

        final Object tmp = annotations.get(name);

        if (tmp == null)
            throw new IllegalStateException("Required property: " + name);

        return tmp;
        
    }

    /**
     * Set an annotation.
     * <p>
     * Note: This protected to facilitate copy-on-write patterns. It is not
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
     * Note: This protected to facilitate copy-on-write patterns. It is not
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

    /**
     * Unconditionally sets the property.
     * 
     * @param name
     *            The name.
     * @param value
     *            The value.
     *            
     * @return A copy of this {@link BOp} on which the property has been set.
     */
    public BOpBase setProperty(final String name, final Object value) {

        final BOpBase tmp = this.clone();

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

        final BOpBase tmp = this.clone();

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
     * @return A copy of this {@link BOp} in which the named annotation has been
     *         removed.
     */
    public BOpBase clearProperty(final String name) {

        if (name == null)
            throw new IllegalArgumentException();
        
        final BOpBase tmp = this.clone();

        tmp._clearProperty(name);
        
        return tmp;

    }
    
    /**
     * Strips off the named annotations.
     * 
     * @param names
     *            The annotations to be removed.
     *            
     * @return A copy of this {@link BOp} in which the specified annotations do not appear.
     */
    public BOp clearAnnotations(final String[] names) {

        final BOpBase tmp = this.clone();

        for(String name : names) {
            
            tmp._clearProperty(name);
            
        }

        return tmp;
        
    }

    public int getId() {
        
        return (Integer) getRequiredProperty(Annotations.BOP_ID);
        
    }
    
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
//        sb.append(super.toString());
        final Integer bopId = (Integer) getProperty(Annotations.BOP_ID);
        if (bopId != null) {
            sb.append("[" + bopId + "]");
        }
        sb.append("(");
        int nwritten = 0;
        for (BOp t : args) {
            if (nwritten > 0)
                sb.append(',');
            sb.append(t.getClass().getSimpleName());
            final Integer tid = (Integer) t.getProperty(Annotations.BOP_ID);
            if (tid != null) {
                sb.append("[" + tid + "]");
            }
            nwritten++;
        }
        sb.append(")");
        annotationsToString(sb);
        return sb.toString();

    }

    protected void annotationsToString(final StringBuilder sb) {
        final Map<String,Object> annotations = annotations();
        if (!annotations.isEmpty()) {
            sb.append("[");
            boolean first = true;
            for (Map.Entry<String, Object> e : annotations.entrySet()) {
                if (first)
                    sb.append(" ");
                else
                    sb.append(", ");
                if (e.getValue() != null && e.getValue().getClass().isArray()) {
                    sb.append(e.getKey() + "="
                            + Arrays.toString((Object[]) e.getValue()));
                } else if (e.getKey() == IPredicate.Annotations.FLAGS) {
                    sb.append(e.getKey() + "="
                            + Tuple.flagString((Integer) e.getValue()));
                } else {
                    sb.append(e.getKey() + "=" + e.getValue());
                }
                first = false;
            }
            sb.append("]");
        }
    }
    
    final public BOpEvaluationContext getEvaluationContext() {

        return getProperty(Annotations.EVALUATION_CONTEXT,
                Annotations.DEFAULT_EVALUATION_CONTEXT);

    }

    public final boolean isMutation() {

        return getProperty(Annotations.MUTATION, Annotations.DEFAULT_MUTATION);

    }

    public final long getTimestamp() {

		return (Long) getRequiredProperty(Annotations.TIMESTAMP);

    }

    /*
     * Note: I've played around with a few hash functions and senses of
     * equality. Predicate (before the bops were introduced) used to have a
     * hashCode() and equals() which was used to cache access paths, but that is
     * long gone. The problem with specifying a hashCode() and equals() method
     * for BOp/BOpBase/Predicate is that we wind up with duplicate bop
     * exceptions being reported by BOpUtility#getIndex(BOp).
     */
    
//    /**
//     * <code>true</code> if all arguments and annotations are the same.
//     */
//    public boolean equals(final Object other) {
//
//        if (this == other)
//            return true;
//
//        if (!(other instanceof BOp))
//            return false;
//
//        final BOp o = (BOp) other;
//
//        final int arity = arity();
//
//        if (arity != o.arity())
//            return false;
//
//        for (int i = 0; i < arity; i++) {
//
//            final BOp x = get(i);
//
//            final BOp y = o.get(i);
//
//            /*
//             *    X      Y
//             * same   same : continue (includes null == null);
//             * null  other : return false;
//             * !null other : if(!x.equals(y)) return false.
//             */
//            if (x != y || x == null || !(x.equals(y))) { 
////                    && (//
////                    (x != null && !(x.equals(y))) || //
////                    (y != null && !(y.equals(x))))//
////            ) {
//
//                return false;
//
//            }
//
//        }
//
//        return annotations.equals(o.annotations());
//
//    }
//
//    /**
//     * The hash code is based on the hash of the operands plus the optional
//     * {@link BOp.Annotations#BOP_ID}.  It is cached.
//     */
//    public int hashCode() {
//
//        int h = hash;
//
//        if (h == 0) {
//
//            final int n = arity();
//
//            for (int i = 0; i < n; i++) {
//
//                h = 31 * h + get(i).hashCode();
//
//            }
//
//            Integer id = (Integer) getProperty(Annotations.BOP_ID);
//
//            if (id != null)
//                h = 31 * h + id.intValue();
//
//            hash = h;
//
//        }
//
//        return h;
//
//    }
//
//    /**
//     * Caches the hash code.
//     */
//    private int hash = 0;

}
