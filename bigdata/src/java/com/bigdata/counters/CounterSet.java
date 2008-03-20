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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

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
 */
public class CounterSet implements ICounterSet {

    static protected final Logger log = Logger.getLogger(CounterSet.class);

//    private String pathx;
    private final String name;
    private CounterSet parent;
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
            if (parent != null)
                return getRoot().getPath(path);

        }

        /*
         * Split path into node name components. The path is known to be
         * relative (see above) so there is never a leading '/'.
         */
        final String[] a = path.split(pathSeparator);
        
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
            if (parent != null)
                return getRoot().makePath(path);

        }
        
        final String[] a = path.split(pathSeparator);
        
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

    /**
     * Uses a post-order iteration to visit the {@link CounterSet}s and for
     * each {@link CounterSet} writes the current value of each {@link Counter}.
     * <p>
     * A sample output is below.
     * <p>
     * <code>cs</code> is a {@link CounterSet} element and has a
     * <code>path</code> attribute which expresses the location of the counter
     * set within the hierarchy (counter set elements are not nested inside of
     * each other in the XML serialization). Only counter sets with counters are
     * emitted.
     * <p>
     * <code>c</code> is a {@link Counter} element and is nested inside of the
     * corresponding counter set. Each counter carries a <code>name</code>
     * attribute, a simple XML Schema Datatype, a timestamp (milliseconds since
     * the epoch per {@link System#currentTimeMillis()}, and has a counter
     * value which is the inner content of the <code>c</code> element.
     * 
     * <pre>
     *      &lt;?xml version=&quot;1.0&quot; encoding=&quot;UTF-8&quot;?&gt;
     *      &lt;counters xmlns:xs=&quot;http://www.w3.org/2001/XMLSchema&quot;&gt;
     *          &lt;cs path=&quot;/www.bigdata.com/cpu&quot;&gt;
     *              &lt;c name=&quot;availableProcessors&quot; type=&quot;xs:int&quot; time=&quot;1205928108602&quot;&gt;2&lt;/c&gt;
     *          &lt;/cs&gt;
     *          &lt;cs path=&quot;/www.bigdata.com/memory&quot;&gt;
     *              &lt;c name=&quot;maxMemory&quot; type=&quot;xs:long&quot; time=&quot;1205928108602&quot;&gt;517013504&lt;/c&gt;
     *          &lt;/cs&gt;
     *          &lt;cs path=&quot;/&quot;&gt;
     *              &lt;c name=&quot;elapsed&quot; type=&quot;xs:long&quot; time=&quot;1205928108602&quot;&gt;1205928108602&lt;/c&gt;
     *          &lt;/cs&gt;
     *           &lt;/counters&gt;
     * </pre>
     * 
     */
    public void asXML(OutputStream os, String encoding, Pattern filter) throws IOException {
        
        final Writer w = new OutputStreamWriter(os, encoding);
        
        w.write("<?xml version=\"1.0\" encoding=\""+encoding+"\" ?>");
        
        w.write("<counters");
        w.write(" xmlns:xs=\""+NAMESPACE_XSD+"\"");
        w.write("\n>");
        
        final Iterator itr = postOrderIterator();
        
        while(itr.hasNext()) {
            
            final CounterSet counterSet = (CounterSet)itr.next();
            
            final Iterator<ICounter> itr2 = counterSet.counterIterator(filter);

            if(!itr2.hasNext()) {
                
                /*
                 * do not emit counter sets that do not have directly attached
                 * counters.
                 */
                
                continue;
                
            }
            
            w.write("<cs");
            w.write(" path=\""+counterSet.getPath()+"\"");
            w.write("\n>");

            while(itr2.hasNext()) {
                
                final ICounter counter = itr2.next();
                
                final String name = counter.getName();
                
                final Object value = counter.getValue();
                
                final String type = getXSDType(value);
            
                final long time = counter.lastModified();
                
                if(time<=0L) {
                    
                    /*
                     * Zero and negative timestamps are generally an indicator
                     * that the counter value is not yet defined.
                     */
                    
                    log.info("Ignoring counter with invalid timestamp: name="
                                    + name
                                    + ", timestamp="
                                    + time
                                    + ", value="
                                    + value);

                    continue;
                    
                }
                
                w.write("<c");
                w.write(" name=\"" + name + "\"");
                w.write(" type=\"" + type + "\"");
                w.write(" time=\"" + time + "\"");
                w.write(">");
                
                // FIXME encode for XML.
                w.write("" + value);
                
                w.write("</c>");
                
            }

            w.write("</cs\n>");

        }
        
        w.write("</counters\n>");
        
        w.flush();
        
    }
    
    public void readXML(InputStream is, IInstrumentFactory instrumentFactory,
            Pattern filter) throws IOException, ParserConfigurationException, SAXException {

        if (is == null)
            throw new IllegalArgumentException();

        if (instrumentFactory == null)
            throw new IllegalArgumentException();
        
        final SAXParser p;
        {
            
            SAXParserFactory f = SAXParserFactory.newInstance();
        
            f.setNamespaceAware(true);
            
            p = f.newSAXParser();
            
        }
        
        MyHandler handler = new MyHandler(this, instrumentFactory, filter);
        
        p.parse(is, handler /*@todo set validating and pass in systemId*/);
        
    }
    
    /**
     * Helper class for SAX based parse of counter XML.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class MyHandler extends DefaultHandler {
        
        /** Note: inner class so named with '$' vs '.' */
        protected final Logger log = Logger.getLogger(MyHandler.class);
        
        private final CounterSet root;
        
        private final IInstrumentFactory instrumentFactory;

        private final Pattern filter;
        
        public MyHandler(CounterSet root, IInstrumentFactory instrumentFactory,
                Pattern filter) {

            if (root == null)
                throw new IllegalArgumentException();

            if (instrumentFactory == null)
                throw new IllegalArgumentException();

            this.root = root;
            
            this.instrumentFactory = instrumentFactory;
            
            this.filter = filter;
            
        }
        
        /**
         * Set each time we enter a <code>cs</code> element.
         */
        private String path;
        
//        /**
//         * Set each time we enter a <code>c</code> element. The value will be
//         * <code>null</code> if there is no node with the same path as the
//         * described counter (the {@link #path} plus the counter
//         * <code>name</code> attribute), a {@link CounterSet} if the path
//         * describes a {@link CounterSet} rather than a {@link Counter}, or a
//         * {@link Counter} if there is a pre-existing counter for that path.
//         */
//        private ICounterNode node;

        /**
         * The value of the <code>name</code> attribute from the last
         * <code>c</code> element.
         */
        private String name;
        
        /**
         * The value of the <code>time</code> attribute from the last
         * <code>c</code> element.
         */
        private long time;
        
        /**
         * The value of the <code>type</code> attribute from the last
         * <code>c</code> element.
         */
        private String type;
        
        /** qualified name for the <code>cs</code> element. */
        private final String cs = "cs"; 

        /** qualified name for the <code>c</code> element. */
        private final String c = "c"; 
        
        /** buffers the cdata content inside of each element. */
        private StringBuilder cdata = new StringBuilder();
        
        public void startElement(String uri, String localName, String qName,
                Attributes attributes) throws SAXException {

            log.info("uri=" + uri + ",localName=" + localName + ", qName="
                    + qName);

            if(qName.equals(cs)) {
                
                path = attributes.getValue("path");
                
                log.info("path="+path);
                
            } else if(qName.equals(c)) {
                
                name = attributes.getValue("name");

                type = attributes.getValue("type");
                
                log.info("name="+name+", type="+type+", time="+attributes.getValue("time"));
                
                time = Long.parseLong(attributes.getValue("time"));

            }
            
        }

        public void characters(char[] ch, int start, int length)
                throws SAXException {

            cdata.append(ch, start, length);

        }
        
        public void endElement(String uri, String localName, String qName)
                throws SAXException {

            try {

                if(!qName.equals(c)) return;

                final String localType = type.substring(type.lastIndexOf("#")+1);
                
                final Class typ;
                
                if(localType.equals(xsd_int)||localType.equals(xsd_long)) {
                    
                    typ = Long.class;
                    
                } else if(localType.equals(xsd_float)||localType.equals(xsd_double)) {
                    
                    typ = Double.class;
                    
                } else {
                    
                    typ = String.class;
                    
                }

                final ICounter counter;

                // iff there is an existing node for that path.
                final ICounterNode node;
                
                // atomic makePath + counter create iff necessary.
                synchronized (root) {

                    node = root
                            .getPath(path + ICounterSet.pathSeparator + name);

                    if (node == null) {

                        final IInstrument inst = instrumentFactory
                                .newInstance(typ);

                        counter = root.makePath(path).addCounter(name, inst);

                    } else if (node.isCounter()) {

                        counter = (ICounter) node;

                    } else {

                        log.error("Can not load counter: path=" + path
                                + ", name=" + name
                                + " : existing counter set with same name");

                        return;

                    }
                    
                }
                
                final String text = cdata.toString();

                try {

                    if (typ == Long.class) {

                        counter.setValue(Long.parseLong(text), time);

                    } else if (typ == Double.class) {

                        counter.setValue(Double.parseDouble(text), time);

                    } else {

                        counter.setValue(text, time);

                    }
                } catch (Exception ex) {
                    
                    log.warn("Could not set counter value: path=" + path
                            + ", name=" + name + " : " + ex, ex);
                    
                }

            } finally {

                // clear any buffered data.
                cdata.setLength(0);
                
            }
            
        }
        
    }

    private static final transient String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema";
    
    /** assuming xs == http://www.w3.org/2001/XMLSchema */
    private static final transient String xsd = "xs:";
    private static final transient String xsd_anyType = xsd+"anyType";
    private static final transient String xsd_long    = xsd+"long";
    private static final transient String xsd_int     = xsd+"int";
    private static final transient String xsd_double  = xsd+"double";
    private static final transient String xsd_float   = xsd+"float";
    private static final transient String xsd_string  = xsd+"string";
    private static final transient String xsd_boolean = xsd+"boolean";

    /**
     * Return the XML datatype for an {@link ICounter}'s value.
     * 
     * @param value
     *            The current counter value.
     * 
     * @return The corresponding XML datatype -or- "xsd:anyType" if no more
     *         specific datatype could be determined.
     */
    private String getXSDType(Object value) {
        
        if (value == null)
            return xsd_anyType;

        Class c = value.getClass();
        
        if (c.equals(Long.class)) 
            
            return xsd_long;

        else if (c.equals(Integer.class))
            
            return xsd_int;
        
        else if (c.equals(Double.class))
        
            return xsd_double;
        
        else if (c.equals(Float.class))
            
            return xsd_float;
        
        else if (c.equals(String.class))
            
            return xsd_string;
        
        else if (c.equals(Boolean.class))
            
            return xsd_boolean;
        
        else
            
            return xsd_anyType;

    }

}
