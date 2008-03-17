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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Sorter;
import cutthecrap.utils.striterators.Striterator;

/**
 * A set of counters arranged in a hierarchy, much like a file system. Each node
 * has a name and a path. The name is a local and immutable label. The path is
 * the {separator, name} sequence reading down from the root to a given node.
 * The "root" is the top-most node in the hierarchy - it always has an empty
 * name and its path is <code>/</code>. The direct children of a root are
 * typically fully qualified host names. E.g., <code>/www.bigdata.com</code>.
 * <p>
 * Nodes are always created as children of an existing root. Once created, any
 * non-root node may be attached as a child of any other node, including a root
 * node, as long as cycles would not be formed. When a node is attached as a
 * child of another node, the path of the child and all of its children are
 * updated recursively. E.g., if <code>/Memory</code> is attached to
 * <code>/www.bigdata.com</code> then its path becomes
 * <code>/www.bigdata.com/Memory</code>.
 * <p>
 * Children are either {@link CounterSet}s or individual {@link Counter}s.
 * Counter sets and counters are declared in the namespace and their names must
 * be distinct.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo each counter value should carry a timestamp which should be part of any
 *       interchange format. this will let us know if a counter is not getting
 *       updated. e.g., a lastModifiedTime. milliseconds precision is fine here
 *       since the counters are not meant to support high resolution sampling.
 * 
 * @todo the syntax "." and ".." are not recognized.
 * 
 * @todo provide for efficient serialization of a set of counters of interest.
 *       E.g., by sorted into order and using prefix compression or by using
 *       hamming compression on the components of the path name.
 */
public class CounterSet implements ICounterSet {

    static protected final Logger log = Logger.getLogger(CounterSet.class);

//    private String pathx;
    private final String name;
    private CounterSet parent;
    private final Map<String,ICounterNode> children = new ConcurrentHashMap<String,ICounterNode>();
//    private final Map<String,ICounter> counters = new ConcurrentHashMap<String,ICounter>(); 
    
    /**
     * Ctor for a root node.
     */
    public CounterSet() {

        this("",null);

    }

//    /**
//     * Ctor for a child {@link CounterSet}.
//     */
//    private CounterSet(String name) {
//
//        this(name, null);
//
//    }

    /**
     * Used to add a child.
     * 
     * @param name
     *            The name of the child.
     */
    private CounterSet(String name,CounterSet parent) {

        if (name == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.parent = parent;
        
//        this.path = computePath();
        
    }
    
//    /**
//     * Updates the {@link #path} on this {@link CounterSet} and then recursively
//     * on all of its children.
//     */
//    private void updatePath() {
//
//        this.path = computePath();
//        
//        Iterator itr = children.values().iterator();
//        
//        while(itr.hasNext()) {
//            
//            CounterSet child = (CounterSet)itr.next();
//            
//            child.updatePath();
//            
//        }
//        
//    }
//    
//    private String computePath() {
//
//        if (parent == null || parent.isAbsoluteRoot()) {
//
//            return pathSeparator + name;
//            
//        }
//        
//        final ICounterSet[] a = getPathComponents(); 
//
//        final StringBuilder sb = new StringBuilder();
//
//        for(ICounterSet x : a) {
//            
//            sb.append(pathSeparator);
//            
//            sb.append(x.getName());
//            
//        }
//        
//        return sb.toString();
//
//    }
    
    public boolean isLeaf() {
        
        return children.isEmpty();
        
    }

    public CounterSet getRoot() {

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

        if (parent == null) {
         
            /*
             * Handles: "/", where this is an absolute root.
             */
            
            return pathSeparator;
            
        }

        if (parent.parent == null) {

            /*
             * Handles: "/foo".
             */
            
            return parent.getPath() + name;
            
        }

        /*
         * Handles "/foo/bar", etc.
         */

        return parent.getPath() + pathSeparator + name;
        
    }
    
    /**
     * Attaches a {@link CounterSet} as a child of this node. If <i>child</i>
     * is a root, then all children of the <i>child</i> are attached instead.
     * If a {@link CounterSet} already exists then its children are attached. If
     * a {@link Counter}s already exists then it is overwritten.
     * 
     * @param src
     *            The child counter set.
     * 
     * @throws IllegalArgumentException
     *             if <i>child</i> is <code>null</code>
     * @throws IllegalStateException
     *             if <i>child</i> is either this node or any parent of this
     *             node since a cycle would be formed.
     */
    synchronized public void attach(ICounterNode src) {
        
        // FIXME detect cycles.

        if(src.isRoot()) {
            
            /*
             * If the child is a root then we attach its children.
             */
            Iterator<ICounterNode> itr = ((CounterSet) src).children.values()
                    .iterator();

            while (itr.hasNext()) {

                ICounterNode child2 = itr.next();

                attach2(child2);

            }

        } else {

            attach2(src);
            
        }
        
    }

    @SuppressWarnings("unchecked")
    private void attach2(ICounterNode child) {
        
        if (child == null)
            throw new IllegalArgumentException();

        if (children.containsKey(child.getName())) {

            throw new IllegalStateException("child by that name exists");
            
        }
        
        synchronized(child) {

            final String name = child.getName();
            
            final CounterSet oldParent = (CounterSet)child.getParent();
            
            assert oldParent != null;
            
            if (oldParent.children.remove(name) == null) {
                
                throw new AssertionError();
                
            }
            
            if(child.isCounterSet()) {
                
                ((CounterSet)child).parent = this;
                
            } else {
                
                ((Counter)child).parent = this;
                
            }
            
            children.put(name, child);
            
//            /*
//             * update the path on the child (and recursively on its children) to
//             * reflect its location in the hierarchy. the counters on the child
//             * use a dynamic path so that is not a problem.
//             */
//            child.updatePath();
            
        }
        
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
        
        IStriterator src = new Striterator(children.values().iterator())
                .addTypeFilter(ICounter.class);
        
        if (filter != null) {

            src.addFilter(

            new Filter() {

                private static final long serialVersionUID = 1L;

                @Override
                protected boolean isValid(Object val) {

                    final ICounter counter = (ICounter) val;

                    final String path = counter.getPath();
                    
                    Matcher matcher = filter.matcher(path);
                    
                    boolean matched = matcher.matches();

                    return matched;
                    
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
                        
                        return ((ICounterNode)arg0).getName().compareTo(((ICounterNode)arg1).getName());
                        
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

        return new Striterator(counterSetIterator()).addTypeFilter(
                ICounterSet.class).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(Object childObj) {

                /*
                 * A child of this node.
                 */

                ICounterSet child = (ICounterSet) childObj;

//                if (child instanceof ICounterSet) {

                    /*
                     * The child has children.
                     */

                    Striterator itr = new Striterator(((CounterSet) child)
                            .postOrderIterator1());

                    // append this node in post-order position.
                    itr.append(new SingleValueIterator(child));

                    return itr;
//
//                } else {
//
//                    /*
//                     * The child is a leaf.
//                     */
//
//                    // visit the leaf itself.
//                    return new SingleValueIterator(child);

//                }
            }
        });

    }

//    public ICounterSet getCounterSetByName(String name) {
//        
//        if (name == null)
//            throw new IllegalArgumentException();
//        
//        ICounterNode node = children.get(name);
//        
//        if( node instanceof ICounterSet) {
//            
//            return (ICounterSet) node;
//            
//        }
//        
//        return null;
//        
//    }
    
    public ICounterNode getChild(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        return children.get(name);
        
    }

//    public ICounterSet getCounterSetByPath(String path) {
//
//        ICounterNode node = getPath(path);
//        
//        if(node instanceof ICounterSet) {
//            
//            return (ICounterSet)node;
//            
//        }
//        
//        return null;
//        
//    }
    
    public ICounterNode getPath(String path) {
       
        if (path == null) {

            throw new IllegalArgumentException();
            
        }
        
        if(path.length()==0) {
            
            throw new IllegalArgumentException();
            
        }

        if(path.equals(pathSeparator)) {
            
            // Handles: "/"
            
            return getRoot();
            
        }
        
        /*
         * Normalize to a path relative to the node on which we evaluate the
         * path. If the path is absolute, then we drop off the leading '/' and
         * evaluate against the root (so the path is now relative to the root).
         * Otherwise the path is already relative to this node and we evaluate
         * it here.
         */
        if (path.startsWith(pathSeparator)) {

            // drop off the leading '/'
            path = path.substring(1);

            // start at the root
            if (parent != null)
                return getRoot().getPath(path);

        }

        /*
         * Split path into node name components. The path is known to be
         * relative (see above) so there is never a leading '/'.
         */
        final String[] a = path.split(pathSeparator);
        
//        assert a.length > 0 : "path="+path;
//        
//        // empty path is this node.
//        if(a.length==0) return this;

        /*
         * This is a root and we are going to desend by name a node at a time.
         * a[0] is the name of the first path component to be matched.
         */

        ICounterNode t = this;

        // the remaining path components.
        for (int i = 0; i < a.length; i++) {
            
            final String name = a[i];
            
            t = t.getChild( name );
            
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
    synchronized public CounterSet makePath(String path) {
        
        if (path == null) {

            throw new IllegalArgumentException();
            
        }
        
        if(path.length()==0) {
            
            throw new IllegalArgumentException();
            
        }
        
        if (path.equals(pathSeparator)) {
         
            // Handles: "/"
            
            return getRoot();
            
        }
        
        /*
         * Normalize to a path relative to the node on which we evaluate the
         * path. If the path is absolute, then we drop off the leading '/' and
         * evaluate against the root (so the path is now relative to the root).
         * Otherwise the path is already relative to this node and we evaluate
         * it here.
         */
        if (path.startsWith(pathSeparator)) {

            // drop off the leading '/'
            path = path.substring(1);

            // start at the root
            if (parent != null)
                return getRoot().makePath(path);

        }
        
        final String[] a = path.split(pathSeparator);
        
//        assert a.length > 0 : "path="+path;
//        
//        // empty path is this node.
//        if(a.length==0) return this;
        
        CounterSet p = this;
        
        for (int i = 0; i < a.length; i++) {
        
            String name = a[i];
            
            ICounterNode t = p.children.get(name);

            if (t == null) {

                // node does not exist, so create it now.
                
                t = new CounterSet(name, p);

                p.children.put(name, t);

            } else if (t instanceof ICounter) {

                // path names a counter.
                
                throw new IllegalArgumentException("path identifies a counter");
                
            }

            p = (CounterSet) t;


        }
        
        return p;
        
    }
    
//    public ICounter getCounterByPath(String path) {
//
//        ICounterSet t = getCounterSetByPath( path );
//        
//        if(t == null) return null;
//
//        final String[] a = path.split(pathSeparator);
//        
//        return t.get(a[a.length-1]);
//        
//    }
    
    /**
     * Add a counter.
     * 
     * @param path
     *            The path of the counter (absolute or relative).
     * 
     * @param instrument
     *            The object that is used to take the measurements from which
     *            the counter's value will be determined.
     */
    synchronized public ICounter addCounter(String path, final IInstrument instrument) {

        if (path == null)
            throw new IllegalArgumentException();
        
        final int indexOf = path.lastIndexOf(pathSeparator);
        
        if (indexOf == -1) {
            
            return addCounter2(path, instrument);
            
        }
        
        final String name = path.substring(indexOf + 1, path.length());

        final String ppath = path.substring(0, indexOf);

        final CounterSet parent = (CounterSet) makePath(ppath);
        
        return parent.addCounter2(name, instrument);
        
    }
    
    @SuppressWarnings("unchecked")
    private ICounter addCounter2(String name,
            final IInstrument instrument) {

        if (name == null)
            throw new IllegalArgumentException();

        if (instrument == null)
            throw new IllegalArgumentException();
        
        if (children.containsKey(name)) {

            throw new IllegalStateException("Exists: path=" + getPath()
                    + ", name=" + name);
            
        }
        
        final ICounter counter = new Counter(this, name, instrument);
        
        log.info("parent="+getPath()+", name="+name);
        
        children.put(name, counter);
        
        return counter;
        
    }
    
    public ICounterSet getParent() {
        
        return parent;
        
    }

    public boolean isRoot() {
        
        return parent == null;
        
    }

    public String toString() {

        return toString(null/*filter*/);
        
    }
    
    public String toString(Pattern filter) {

        StringBuilder sb = new StringBuilder();
        
        Iterator<ICounter> itr = getCounters(filter);

        while (itr.hasNext()) {

            ICounter c = itr.next();

            sb.append("\n" + c.getPath() + "=" + c.getValue());

        }

        return sb.toString();
        
    }

    final public boolean isCounterSet() {
        
        return true;
        
    }

    final public boolean isCounter() {
        
        return false;
        
    }

}
