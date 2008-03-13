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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Sorter;
import cutthecrap.utils.striterators.Striterator;

/**
 * A set of counters arranged in a hierarchy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo provide for efficient serialization of a set of counters of
 *       interest. E.g., by sorted into order and using prefix compression or
 *       by using hamming compression on the components of the path name.
 */
public class CounterSet implements ICounterSet {

    static protected final Logger log = Logger.getLogger(CounterSet.class);

    private String path;
    private final String name;
    private CounterSet parent;
    private final Map<String,ICounterSet> children = new ConcurrentHashMap<String,ICounterSet>();
    private final Map<String,ICounter> counters = new ConcurrentHashMap<String,ICounter>(); 
    
    /**
     * Ctor for top-level.
     */
    public CounterSet() {

        this("");

    }

    /**
     * Ctor for a child {@link CounterSet}.
     */
    public CounterSet(String name) {

        this(name, null);

    }

    /**
     * Used to add a child.
     * 
     * @param name
     *            The name of the child.
     * @param parent
     *            The reference to the parent.
     */
    private CounterSet(String name, CounterSet parent) {

        if (name == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.parent = parent;
        
        this.path = (parent != null ? parent.computePath() : pathSeparator)
                + name;
        
    }
    
    private String computePath() {

        final ICounterSet[] a = getPathComponents(); 

        final StringBuilder sb = new StringBuilder();

        for(ICounterSet x : a) {
            
            sb.append(pathSeparator);
            
            sb.append(x.getName());
            
        }
        
        return sb.toString();

    }
    
    public boolean isLeaf() {
        
        return children.isEmpty();
        
    }

    public ICounterSet getRoot() {

        CounterSet t = this;

        while (t.parent != null) {

            t = t.parent;

        }

        return t;
        
    }
    
    public String getName() {
        
        return name;
        
    }

    /**
     * The ordered array of counter sets from the root.
     */
    public ICounterSet[] getPathComponents() {

        /*
         * Get the depth of this set of counters.
         */
        int depth = 0;
        {
            
            CounterSet t = this;

            while (t.parent != null) {

                t = t.parent;

                depth++;

            }
        
        }

        /*
         * Build the path.
         */
        final CounterSet[] a = new CounterSet[depth+1];
        
        {
            
            int index = a.length-1;

            CounterSet t = this;
            
            while (t.parent != null) {

                a[index--] = t;

                t = t.parent;

            }

            a[index] = t;
            
        }
        
        return a;
        
    }
    
    public String getPath() {

        return path;
        
    }
    
    /**
     * Add a set of counters as a child of this set of counters.
     * 
     * @param child
     *            The child counter set.
     * 
     * @throws IllegalArgumentException
     *             if <i>child</i> is <code>null</code>
     * @throws IllegalStateException
     *             if <i>child</i> is aleady attached to some
     *             {@link CounterSet}.
     * @throws IllegalStateException
     *             if there is already a child by the same name.
     */
    synchronized public CounterSet addCounterSet(CounterSet child) {

        if (child == null)
            throw new IllegalArgumentException();

        if (child.getParent() != null) {

            throw new IllegalStateException("child has parent");

        }

        if (children.containsKey(child.getName())) {

            throw new IllegalStateException("child by that name exists");
            
        }
        
        synchronized(child) {
            
            children.put(child.getName(), child);
            
            child.parent = this;
            
            /*
             * update the path on the child to reflect its location in the
             * hierarchy. the counters on the child use a dynamic path so that
             * is not a problem.
             */
            child.path = child.computePath();
            
        }
        
        return child;
        
    }

    synchronized public ICounter addCounter(String name, final IInstrument instrument) {
        
        if (name== null)
            throw new IllegalArgumentException();

        if (instrument == null)
            throw new IllegalArgumentException();
        
        if (counters.containsKey(name))
            throw new IllegalStateException();
        
        ICounter counter = new Counter(this,name) {
            
            public Object getValue() {
                
                return instrument.getValue();
                
            }
            
        };
        
        log.info("parent="+this+", name="+name);
        
        counters.put(name, counter);
        
        return counter;
        
    }
    
    /**
     * Visits counters belonging directly to this set of counters and
     * matching the optional filter.
     * <p>
     * Note: Since the filter does NOT have to be anchored at the root, the
     * only place we can apply a filter that is NOT anchored at the root is
     * when checking a fully qualified counter name.
     * 
     * @todo optimize for patterns that are anchored by filtering the child
     *       {@link ICounterSet}.
     */
    @SuppressWarnings("unchecked")
    public Iterator<ICounter> counterIterator(final Pattern filter) {
        
        IStriterator src = new Striterator(counters.values().iterator());
        
        if (filter != null) {

            src.addFilter(

            new Filter() {

                private static final long serialVersionUID = 1L;

                @Override
                protected boolean isValid(Object val) {

                    ICounter counter = (ICounter) val;

                    return filter.matcher(counter.getPath()).matches();

                }

            });

        }

        return src;
        
    }
    
    @SuppressWarnings("unchecked")
    public Iterator<ICounter> getCounters(final Pattern filter) {
     
        IStriterator src = ((IStriterator) postOrderIterator())
                .addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator expand(Object val) {
                
                CounterSet c = (CounterSet)val;
                
                return c.counterIterator(filter);
                
            }
            
        });
        
        return src;
        
    }
    
    /**
     * Iterator visits the directly attached {@link ICounterSet} children.
     */
    @SuppressWarnings("unchecked")
    public Iterator<ICounterSet> counterSetIterator() {

        /*
         * @todo consider using linked list or insertion sort rather than hash
         * map and runtime sort.
         */
        IStriterator itr = new Striterator(children.values().iterator())
                .addFilter(new Sorter(){

                    private static final long serialVersionUID = 1L;

                    @Override
                    public int compare(Object arg0, Object arg1) {
                        
                        return ((CounterSet)arg0).name.compareTo(((CounterSet)arg1).name);
                        
                    }
                    
                });
        
        return itr;
        
    }

    /**
     * Iterator visits children matching the option filter recursively
     * expanding each child with a post-order traversal of its children and
     * finally visits this node itself.
     */
    @SuppressWarnings("unchecked")
    public Iterator postOrderIterator() {

        /*
         * Iterator append this node to the iterator in the post-order
         * position.
         */

        return new Striterator(postOrderIterator1())
                .append(new SingleValueIterator(this));

    }

    /**
     * Visits the children (recursively) using post-order traversal, but
     * does NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    private Iterator<ICounterSet> postOrderIterator1() {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the post-order iterator.
         */

        return new Striterator(counterSetIterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(Object childObj) {

                /*
                 * A child of this node.
                 */

                CounterSet child = (CounterSet) childObj;

                if (!child.isLeaf()) {

                    /*
                     * The child has children.
                     */

                    Striterator itr = new Striterator(child
                            .postOrderIterator1());

                    // append this node in post-order position.
                    itr.append(new SingleValueIterator(child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

                    // visit the leaf itself.
                    return new SingleValueIterator(child);

                }
            }
        });

    }

    public ICounter getCounterByName(String name) {
        
        return counters.get(name);
        
    }

    public ICounterSet getParentByPath(String path) {
        
        if (path == null)
            throw new IllegalArgumentException();
        
        if(path.startsWith(pathSeparator) && parent != null) {
            
            return getRoot().getParentByPath(path);
            
        }
        
        final String[] a = path.split(pathSeparator);
        
        if(a.length==0) throw new IllegalArgumentException();
        
        ICounterSet t = this;
        
        for(int i=1; i<a.length-1; i++) {
        
            t = t.getCounterSetByName(a[i]);
            
            if(t == null) return null;
            
        }
        
        return t;
        
    }
    
    /**
     * Adds any necessary {@link CounterSet}s described in the path (ala
     * mkdirs).
     * 
     * @param path
     *            The path.
     * 
     * @return The {@link CounterSet} described by the path.
     */
    public CounterSet makePath(String path) {
        
        if (path == null)
            throw new IllegalArgumentException();
        
        if(path.startsWith(pathSeparator) && parent != null) {
            
            return ((CounterSet)getRoot()).makePath(path);
            
        }
        
        final String[] a = path.split(pathSeparator);
        
        if(a.length==0) throw new IllegalArgumentException();
        
        if (!a[1].equals(name)) {

            throw new IllegalArgumentException("Wrong root name: root=" + name
                    + ", but given root is: " + a[1]);

        }
        
        CounterSet parent = this;
        
        for (int i = 2; i < a.length; i++) {
        
            String name = a[i];
            
            CounterSet t = (CounterSet) parent.getCounterSetByName(name);
            
            if(t == null) {

                t = parent.addCounterSet(new CounterSet(name));
                
            }
            
            parent = t;
            
        }
        
        return parent;
        
    }
    
    public ICounter getCounterByPath(String path) {

        ICounterSet t = getParentByPath( path );
        
        if(t == null) return null;

        final String[] a = path.split(pathSeparator);
        
        return t.getCounterByName(a[a.length-1]);
        
    }
    
    synchronized public ICounter addCounterByPath(String path, final IInstrument instrument) {

        if(path==null) throw new IllegalArgumentException();
        
        final int indexOf = path.lastIndexOf(pathSeparator);
        
        if (indexOf == -1) {
            
            return ((CounterSet)getRoot()).addCounter(path, instrument);
            
        }
        
        final String name = path.substring(indexOf+1,path.length());
        
        final String ppath = path.substring(0,indexOf);
        
        CounterSet parent = (CounterSet)makePath(ppath);
        
        return parent.addCounter(name, instrument);
        
    }
    
    public ICounterSet getCounterSetByName(String name) {
        
        if (name == null)
            throw new IllegalArgumentException();
        
        return children.get(name);
        
    }
    
    public ICounterSet getParent() {
        
        return parent;
        
    }

    public boolean isRoot() {
        
        return parent == null;
        
    }
    
    public String toString() {
        
        return path;
        
    }
    
}
