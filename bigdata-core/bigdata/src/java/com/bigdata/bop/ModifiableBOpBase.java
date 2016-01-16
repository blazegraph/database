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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
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
    
    @Override
    protected boolean annotationsEqual(final BOp o) {

        if (o instanceof ModifiableBOpBase) {

            // Fast path when comparing two modifiable bops.
            return annotationsEqual(annotations,
                    ((ModifiableBOpBase) o).annotations);

        }

        return super.annotationsEqual(annotations, o.annotations());

    }

    public BOp get(final int index) {
        
        return args.get(index);
        
    }
    
    /**
     * Return the index of the bop in the args.  Returns -1 if bop is not
     * present in the args.
     */
    public int indexOf(final BOp bop) {
    	
    	return args.indexOf(bop);
    	
    }

    /**
     * Invoked automatically any time a mutation operation occurs. The default
     * implementation is a NOP.
     */
    protected void mutation() {
        // NOP
    }

    /**
     * Replace the arguments.
     * 
     * @param args
     *            The new arguments.
     * @return <i>this</i>.
     */
    public ModifiableBOpBase setArgs(final BOp[] args) {
        if (args == null)
            throw new IllegalArgumentException();
        this.args.clear(); // FIXME Override might not catch clear of parent ref.
        for (BOp arg : args) {
            addArg(arg);
        }
        mutation();
        return this;
    }
    
    /**
     * Replace the value of the argument at the specified index (core mutation
     * method).
     * 
     * @param index
     *            The index of the child expression to be replaced.
     * @param newArg
     *            The new child expression.
     * 
     * @return This {@link ModifiableBOpBase}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the argument is <i>this</i>.
     */
    public ModifiableBOpBase setArg(final int index, final BOp newArg) {

        if (newArg == null)
            throw new IllegalArgumentException();

        if(newArg == this)
            throw new IllegalArgumentException();

        args.set(index, newArg);

        mutation();
        
        return this;

    }

    /**
     * Add a new argument (core mutation method).
     * 
     * @param newArg
     *            The argument.
     * 
     * @return <i>this</i>.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the argument is <i>this</i>.
     */
    public void addArg(final BOp newArg) {
        
        if(newArg == null)
            throw new IllegalArgumentException();

        if(newArg == this)
            throw new IllegalArgumentException();
        
        args.add(newArg);
        
        mutation();
        
    }

    /**
     * Add a new argument (core mutation method) at the specified index.
     * 
     * @param index
     *            The index at which the child expression is to be inserted.
     * @param newArg
     *            The argument.
     * 
     * @return <i>this</i>.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the argument is <i>this</i>.
     */
    public void addArg(final int index, final BOp newArg) {
        
        if(newArg == null)
            throw new IllegalArgumentException();

        if(newArg == this)
            throw new IllegalArgumentException();
        
        args.add(index, newArg);
        
        mutation();
        
    }

    /**
     * Add an argument iff it is not already present.
     * 
     * @param arg
     *            The argument.
     *            
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the argument is <i>this</i>.
     */
    public void addArgIfAbsent(final BOp arg) {

        if(arg == null)
            throw new IllegalArgumentException();

        if(arg == this)
            throw new IllegalArgumentException();

        if(!args.contains(arg)) {

            addArg(arg);
         
        }
        
    }
    
    /**
     * Remove the 1st occurrence of the argument (core mutation method).
     * 
     * @param arg
     *            The argument.
     *            
     * @return <code>true</code> iff the argument was removed.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the argument is <i>this</i>.
     */
    public boolean removeArg(final BOp arg) {
        
        if (arg == null)
            throw new IllegalArgumentException();

        if (arg == this)
            throw new IllegalArgumentException();

        if (args.remove(arg)) {
         
            mutation();
            
            return true;
            
        }

        return false;
        
    }
    
    /**
     * Replace a child of a node with another reference (destructive
     * modification). All arguments which point to the oldChild will be replaced
     * by references to the newChild.
     * 
     * @param oldChild
     * @param newChild
     * 
     * @return The #of references which were replaced.
     */
    public int replaceWith(final BOp oldChild, final BOp newChild) {

        final ModifiableBOpBase p = this;

        final int arity = p.arity();

        int nmods = 0;

        for (int i = 0; i < arity; i++) {

            final BOp child = p.get(i);

            if (child == oldChild) {

                ((ModifiableBOpBase) p).setArg(i, newChild);

                nmods++;

            }

        }

        return nmods;

    }
    
    public int arity() {
        
        return args.size();
        
    }

    /**
     * An unmodifiable view of the list of arguments (aka children) of this
     * node.
     * <p>
     * Note: The view is not modifiable in order to preserve the contract that
     * {@link #mutation()} will be invoked if there is change in the state of
     * this {@link BOp}.
     */
    final public List<BOp> args() {

        return new NotifyingList<BOp>(this);
        
    }
    
    /**
     * Class provides notice on mutation events.
     */
    static private class NotifyingList<T> implements List<T> {
        
        private final ModifiableBOpBase bop;
        
        private final List<T> delegate;
        
        @SuppressWarnings("unchecked")
        NotifyingList(final ModifiableBOpBase bop) {
            this(bop, (List<T>) bop.args);
        }

        NotifyingList(final ModifiableBOpBase bop, final List<T> subList) {
            this.bop = bop;
            this.delegate = subList;
        }

        private boolean mutation(boolean modified) {
            if(modified) {
                bop.mutation();
            }
            return modified;
        }
        public int size() {
            return delegate.size();
        }
        public boolean isEmpty() {
            return delegate.isEmpty();
        }
        public boolean contains(Object o) {
            return delegate.contains(o);
        }

        public Iterator<T> iterator() {
            return new NotifyingListIterator<T>(bop, delegate.listIterator());
        }
        public Object[] toArray() {
            return delegate.toArray();
        }
        public <T1> T1[] toArray(T1[] a) {
            return delegate.toArray(a);
        }
        public boolean add(T e) {
            return mutation(delegate.add(e));
        }
        public boolean remove(Object o) {
            return mutation(delegate.remove(o));
        }
        public boolean containsAll(Collection<?> c) {
            return delegate.containsAll(c);
        }
        public boolean addAll(Collection<? extends T> c) {
            return mutation (delegate.addAll(c));
        }
        public boolean addAll(int index, Collection<? extends T> c) {
            return mutation(delegate.addAll(index, c));
        }
        public boolean removeAll(Collection<?> c) {
            return mutation(delegate.removeAll(c));
        }
        public boolean retainAll(Collection<?> c) {
            return mutation(delegate.retainAll(c));
        }
        public void clear() {
            delegate.clear();
            bop.mutation();
        }
        public boolean equals(Object o) {
            return delegate.equals(o);
        }
        public int hashCode() {
            return delegate.hashCode();
        }
        public T get(int index) {
            return delegate.get(index);
        }
        public T set(int index, T element) {
            final T ret = delegate.set(index, element);
            bop.mutation();
            return ret;
        }
        public void add(int index, T element) {
            delegate.add(index, element);
            bop.mutation();
        }
        public T remove(int index) {
            final T ret = delegate.remove(index);
            bop.mutation();
            return ret;
        }
        public int indexOf(Object o) {
            return delegate.indexOf(o);
        }
        public int lastIndexOf(Object o) {
            return delegate.lastIndexOf(o);
        }
        public ListIterator<T> listIterator() {
            return new NotifyingListIterator<T>(bop, delegate.listIterator());
        }
        public ListIterator<T> listIterator(int index) {
            return new NotifyingListIterator<T>(bop, delegate.listIterator(index));
        }

        public List<T> subList(int fromIndex, int toIndex) {
            return new NotifyingList<T>(bop, delegate.subList(fromIndex,
                    toIndex));
        }

    }

    /**
     * Iterator with mutation notify pattern.
     */
    private static class NotifyingListIterator<T> implements ListIterator<T> {

        private final ModifiableBOpBase bop;

        private final ListIterator<T> delegate;

        NotifyingListIterator(ModifiableBOpBase bop,
                ListIterator<T> listIterator) {
            this.bop = bop;
            this.delegate = listIterator;
        }

        public boolean hasNext() {
            return delegate.hasNext();
        }

        public T next() {
            return delegate.next();
        }

        public boolean hasPrevious() {
            return delegate.hasPrevious();
        }

        public T previous() {
            return delegate.previous();
        }

        public int nextIndex() {
            return delegate.nextIndex();
        }

        public int previousIndex() {
            return delegate.previousIndex();
        }

        public void remove() {
            delegate.remove();
            bop.mutation();
        }

        public void set(T e) {
            delegate.set(e);
            bop.mutation();
        }

        public void add(T e) {
            delegate.add(e);
            bop.mutation();
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This {@link Iterator} supports removal.
     */
    final public Iterator<BOp> argIterator() {
        
        return new NotifyingList<BOp>(this, args).iterator();

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
     * Copy all annotations from the caller's map.
     * 
     * @param anns
     *            The annotations to be copied.
     * 
     * @return <i>this</i>
     */
    public ModifiableBOpBase copyAll(final Map<String, Object> anns) {

        annotations.putAll(anns);

        return this;

    }

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

        if (value == null) {
            if (annotations.remove(name) != null) {
                mutation();
            }
        } else {
            annotations.put(name, value);
            mutation();
        }

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

        if (annotations.remove(name) != null) {

            mutation();
            
        }
        
        return this;

    }

    /**
     * Unconditionally set the {@link Annotations#BOP_ID}.
     * 
     * @param id
     *            The id.
     */
    public void setId(final int id) {

        setProperty(Annotations.BOP_ID, id);
        
    }
    
//    public ModifiableBOpBase deepCopy() {
//        
//        final ModifiableBOpBase bop = (ModifiableBOpBase) this.clone();
//
//        deepCopyArgs(bop.args);
//        
//        deepCopyAnnotations(bop.annotations);
//        
//        return bop;
//        
//    }
//    
//    /**
//     * Deep copy the arguments.
//     * <p>
//     * Note: As long as we stick to the immutable semantics for bops, we can
//     * just make a shallow copy of the arguments in the "copy" constructor and
//     * then modify them within the specific operator constructor before
//     * returning control to the caller. This would result in less heap churn.
//     */
//    private void deepCopyArgs() {
//        final ArrayList<BOp> tmp = new ArrayList<BOp>(args);
//        
//        if (a == BOp.NOARGS || a.length == 0) {
//            // fast path for zero arity operators.
//            return BOp.NOARGS;
//        }
//        final BOp[] t = new BOp[a.length];
//        for (int i = 0; i < a.length; i++) {
//            t[i] = a[i] == null ? null : a[i].clone();
//        }
//        return t;
//    }
//
//    /**
//     * Deep copy the annotations.
//     * <p>
//     * Note: This does not know how to deep copy annotations which are not
//     * {@link BOp}s or immutable objects such as {@link String}s or
//     * {@link Number}s. Such objects should not be used as annotations.
//     * 
//     * @todo When attaching large data sets to a query plan they should be
//     *       attached using a light weight reference object which allows them to
//     *       be demanded by a node so deep copy remains a light weight
//     *       operation. This also has the advantage that the objects are
//     *       materialized on a node only when they are needed, which keeps the
//     *       query plan small. Examples would be sending a temporary graph
//     *       containing an ontology or some conditional assertions with a query
//     *       plan.
//     */
//    static protected Map<String, Object> deepCopy(final Map<String, Object> a) {
//        if (a == BOp.NOANNS) {
//            // Fast past for immutable, empty annotations.
//            return a;
//        }
//        // allocate map.
//        final Map<String, Object> t = new LinkedHashMap<String, Object>(a
//                .size());
//        // copy map's entries.
//        final Iterator<Map.Entry<String, Object>> itr = a.entrySet().iterator();
//        while (itr.hasNext()) {
//            final Map.Entry<String, Object> e = itr.next();
//            if (e.getValue() instanceof BOp) {
//                // deep copy bop annotations.
//                t.put(e.getKey(), ((BOp) e.getValue()).clone());
//            } else {
//                // shallow copy anything else.
//                t.put(e.getKey(), e.getValue());
//            }
//        }
//        // return the copy.
//        return t;
//    }

}
