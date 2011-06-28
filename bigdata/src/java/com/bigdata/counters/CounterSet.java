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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

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
 * @todo The CounterSet should perhaps obtain a lock on the node(s) to be
 *       modified rather than the root for better concurrency.
 * 
 * @todo the syntax "." and ".." are not recognized.
 * 
 * @todo should declare the units and the counter description with the counter
 *       but only propagate the description once (alternatively, specify a
 *       counter description interface and pass that along). The more difficult
 *       question is how to limit the transfer of the full description. Perhaps
 *       by having the LBS query for it?
 */
public class CounterSet extends AbstractCounterSet implements ICounterSet {

    static protected final Logger log = Logger.getLogger(CounterSet.class);

//    private String pathx;
    private final Map<String,ICounterNode> children = new ConcurrentHashMap<String,ICounterNode>();
    
    /**
     * Ctor for a root node.
     */
    public CounterSet() {

        this("",null);

    }

    /**
     * Used to add a child.
     * 
     * @param name
     *            The name of the child.
     */
    private CounterSet(String name,CounterSet parent) {

        super(name,parent);
        
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

    /**
     * Attaches a {@link CounterSet} as a child of this node. If <i>child</i>
     * is a root, then all children of the <i>child</i> are attached instead.
     * If a {@link CounterSet} already exists then its children are attached. If
     * a {@link Counter}s already exists then it is overwritten. During
     * recursive attach if we encounter a node that already exists then just
     * copy its children. If there is a conflict (trying to copy a counter over
     * a counter set or visa-versa), then a warning is logged and we ignore the
     * conflicting node.
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
    synchronized public void attach(final ICounterNode src) {
        
        attach(src, false/* replace */);
        
    }

    synchronized public void attach(final ICounterNode src, final boolean replace) {
            
        // FIXME detect cycles.
        
        if(src.isRoot()) {
            
            /*
             * If the child is a root then we attach its children.
             */
            final Iterator<ICounterNode> itr = ((CounterSet) src).children
                    .values().iterator();

            while (itr.hasNext()) {

                attach2(itr.next(), replace);

            }

        } else {

            attach2(src, replace);
            
        }
        
    }

    @SuppressWarnings("unchecked")
    private void attach2(final ICounterNode src, final boolean replace) {

        if (src == null)
            throw new IllegalArgumentException();

        if (children.containsKey(src.getName())) {

            /*
             * There is an existing node with the same path as [src]. 
             */

            // the existing node with the same path as [src].
            final ICounterNode existingChild = getChild(src.getName());

            if (existingChild.isCounter() && !replace) {

                log.warn("Will not replace existing counter: "
                        + existingChild.getPath());
                
                return;
                
            } else if (src.isCounterSet()) {
                
                /*
                 * If the [src] is a counter set, then attach its children
                 * instead.
                 */

                final Iterator<ICounterNode> itr = ((CounterSet) src).children
                        .values().iterator();

                while (itr.hasNext()) {

                    ((CounterSet) existingChild).attach2(itr.next(), replace);

                }

                return;
                
            }
            
            // fall through.
            
        }

        /*
         * Attach or replace the counter.
         */
        synchronized(src) {

            final String name = src.getName();
            
            final CounterSet oldParent = (CounterSet) src.getParent();
            
            assert oldParent != null;
            
            if (oldParent.children.remove(name) == null) {
                
                throw new AssertionError();
                
            }
            
            if(src.isCounterSet()) {
                
                ((CounterSet)src).parent = this;
                
            } else {
                
                ((Counter)src).parent = this;
                
            }
            
            children.put(name, src);
            
//            /*
//             * update the path on the child (and recursively on its children) to
//             * reflect its location in the hierarchy. the counters on the child
//             * use a dynamic path so that is not a problem.
//             */
//            child.updatePath();
            
        }
        
    }

    /**
     * Detaches and returns the node having that path.
     * 
     * @param path
     *            The path.
     * @return The node -or- <code>null</code> if there is no node with that
     *         path.
     */
    synchronized public ICounterNode detach(String path) {
        
        final ICounterNode node = getPath(path);
        
        if(node != null && !node.isRoot() ) { 
            
            final CounterSet p = (CounterSet)node.getParent();
            
            p.children.remove(node.getName());
            
            if(node.isCounterSet()) {
                
                ((CounterSet)node).parent = null;
                
            } else {
                
                ((Counter)node).parent = null;
                
            }
            
        }
        
        return node;
        
    }
    
    /**
     * Visits direct child counters matching the optional filter.
     * <p>
     * Note: Since the filter does NOT have to be anchored at the root, the only
     * place we can apply a filter that is NOT anchored at the root is when
     * checking a fully qualified counter name.
     * 
     * @todo optimize for patterns that are anchored by filtering the child
     *       {@link ICounterSet}.
     */
    @SuppressWarnings("unchecked")
    public Iterator<ICounter> counterIterator(final Pattern filter) {
        
        final IStriterator src = new Striterator(directChildIterator(
                true/* sorted */, ICounter.class));

        if (filter != null) {

            src.addFilter(

            new Filter() {

                private static final long serialVersionUID = 1L;

                @Override
                public boolean isValid(Object val) {

                    final ICounter counter = (ICounter) val;

                    final String path = counter.getPath();
                    
                    final Matcher matcher = filter.matcher(path);
                    
                    final boolean matched = matcher.matches();

                    return matched;
                    
                }

            });

        }

        return src;
        
    }

    /**
     * All spanned nodes.
     * 
     * @param filter An optional filter.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public Iterator<ICounterNode> getNodes(final Pattern filter) {
     
        IStriterator src = ((IStriterator) postOrderIterator())
                .addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator expand(Object val) {
                
                CounterSet c = (CounterSet)val;
                
                return new Striterator(new SingleValueIterator(c)).append(c.counterIterator(filter));
                
            }
            
        });
        
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
     * Iterator visits all directly attached children.
     * 
     * @param sorted
     *            When <code>true</code> the children will be visited in order
     *            by their name.
     * 
     * @param type
     *            An optional type filter - specify either {@link ICounterSet}
     *            or {@link ICounter} you want to be visited by the iterator.
     *            When <code>null</code> all directly attached children
     *            (counters and counter sets) are visited.
     */
    public Iterator directChildIterator(boolean sorted,
            Class<? extends ICounterNode> type) {
        
        /*
         * Note: In order to avoid concurrent modification problems under
         * traversal I am currently creating a snapshot of the set of child
         * references and then using the sorterator over that stable snapshot.
         * 
         * @todo consider using linked list or insertion sort rather than hash
         * map and runtime sort.
         */
        final ICounterNode[] a;
        
        synchronized(this) {
            
            a = (ICounterNode[])children.values().toArray(new ICounterNode[]{});
            
        }
        
        final IStriterator itr = new Striterator( Arrays.asList(a).iterator() );
        
        if (type != null) {
            
            itr.addTypeFilter(type);
           
        }
        
        if (sorted) {

            itr.addFilter(new Sorter() {

                private static final long serialVersionUID = 1L;

                @Override
                public int compare(Object arg0, Object arg1) {

                    return ((ICounterNode) arg0).getName().compareTo(
                            ((ICounterNode) arg1).getName());

                }

            });
            
        }
        
        return itr;
        
    }
    
    /**
     * Iterator visits the directly attached {@link ICounterSet} children.
     */
    @SuppressWarnings("unchecked")
    public Iterator<ICounterSet> counterSetIterator() {

        return directChildIterator(true/*sorted*/,ICounterSet.class);
        
    }

    /**
     * Iterator visits {@link ICounterSet} children recursively expanding each
     * child with a post-order traversal of its children and finally visits this
     * node itself.
     */
    @SuppressWarnings("unchecked")
    public Iterator postOrderIterator() {

        /*
         * Appends this node to the iterator in the post-order position.
         */

        return new Striterator(postOrderIterator1())
                .append(new SingleValueIterator(this));

    }

    /**
     * Iterator visits this node recursively expanding each {@link ICounterSet}
     * child with a pre-order traversal of its children and finally visits this
     * node itself.
     */
    public Iterator preOrderIterator() {
        
        /*
         * Appends this node to the iterator in the pre-order position.
         */

        return new Striterator(new SingleValueIterator(this))
                .append(preOrderIterator1());
        
    }
    
    /**
     * Visits the {@link ICounterSet} children (recursively) using pre-order
     * traversal, but does NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    private Iterator<ICounterSet> preOrderIterator1() {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the pre-order iterator.
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

                final ICounterSet child = (ICounterSet) childObj;

                // append this node in pre-order position.
                final Striterator itr = new Striterator(
                        new SingleValueIterator(child));

                itr.append(((CounterSet) child).preOrderIterator1());

                return itr;
                
            }
        });

    }
    
    /**
     * Visits the {@link ICounterSet} children (recursively) using post-order
     * traversal, but does NOT visit this node.
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

                final ICounterSet child = (ICounterSet) childObj;

                final Striterator itr = new Striterator(((CounterSet) child)
                        .postOrderIterator1());

                // append this node in post-order position.
                itr.append(new SingleValueIterator(child));

                return itr;
                
            }
        });

    }
    
    public ICounterNode getChild(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        return children.get(name);
        
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
            
            return (CounterSet) getRoot();
            
        }
        
        if( path.contains("//")) {

            /*
             * Empty path names are not allowed.
             */
            
            throw new IllegalArgumentException(path);
            
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
            if (parent != null) {
                
                return (CounterSet)getRoot().makePath(path);
                
            }

        }
        
        final String[] a = path.split(pathSeparator);
        
        CounterSet p = this;
        
        for (int i = 0; i < a.length; i++) {
        
            String name = a[i];
            
            ICounterNode t = p.getChild(name);

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
    synchronized public ICounter addCounter(final String path,
            final IInstrument instrument) {

        if (path == null) {

            throw new IllegalArgumentException();

        }

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
    private ICounter addCounter2(final String name, final IInstrument instrument) {

        if (name == null)
            throw new IllegalArgumentException();

        if (instrument == null)
            throw new IllegalArgumentException();

        {

            final ICounterNode counter = children.get(name);

            if (counter != null) {

                if(counter instanceof ICounter ) {
                
                    // counter exists for that path.
                    log.error("Exists: path=" + getPath() + ", name=" + name);
                
                    // return existing counter for path @todo vs replace.
                    return (ICounter)counter;
                    
                } else {
                    
                    // a counter set exists for that path(not a counter).
                    throw new IllegalStateException("Node exists: path="
                            + getPath() + ", name=" + name);
                    
                }

            }

        }

//        if (children.containsKey(name)) {
//
//            throw new IllegalStateException("Exists: path=" + getPath()
//                    + ", name=" + name);
//            
//        }
        
        final ICounter counter = new Counter(this, name, instrument);
        
        if (log.isInfoEnabled())
            log.info("parent=" + getPath()+", name="+name);
        
        children.put(name, counter);
        
        return counter;
        
    }

    /**
     * Per {@link #asXML(OutputStream, String, Pattern)} but does not write out
     * the header declaring the encoding.
     * 
     * @param w
     *            The XML will be written on this object.
     * @param filter
     *            The optional filter.
     * 
     * @throws IOException
     */
    public void asXML(Writer w, Pattern filter) throws IOException {

        XMLUtility.INSTANCE.writeXML(this, w, filter);

    }

    public void readXML(final InputStream is,
            final IInstrumentFactory instrumentFactory, final Pattern filter)
            throws IOException, ParserConfigurationException, SAXException {

        XMLUtility.INSTANCE.readXML(this, is, instrumentFactory, filter);

    }

}
