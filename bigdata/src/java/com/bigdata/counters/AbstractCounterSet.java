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
 * Created on May 1, 2008
 */

package com.bigdata.counters;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractCounterSet implements ICounterSet {

    protected final String name;
    protected AbstractCounterSet parent;

    protected AbstractCounterSet(String name,CounterSet parent) {
        
        if (name == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.parent = parent;

    }
    
    public AbstractCounterSet getRoot() {

        AbstractCounterSet t = this;

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
            
            AbstractCounterSet t = this;

            while (t.parent != null) {

                t = t.parent;

                depth++;

            }
        
        }

        /*
         * Build the path.
         */
        final AbstractCounterSet[] a = new CounterSet[depth+1];
        
        {
            
            int index = a.length-1;

            AbstractCounterSet t = this;
            
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
    
    public int getDepth() {
        
        int depth = 0;
        
        ICounterNode t = this;
        
        while(!t.isRoot()) {
            
            t = t.getParent();
            
            depth++;
            
        }
        
        return depth;
        
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
     */
    public void asXML(final OutputStream os, final String encoding,
            final Pattern filter) throws IOException {
        
        final Writer w = new OutputStreamWriter(os, encoding);

        asXML(w, encoding, filter);
        
    }

    /**
     * Alternative, but you are still required to specify the character set
     * encoding in use by the writer.
     * 
     * @param w
     *            The writer.
     * @param encoding
     *            The character set encoding used by that writer.
     * @param filter
     *            An optional filter.
     *            
     * @throws IOException
     */
    public void asXML(final Writer w, final String encoding,
            final Pattern filter) throws IOException {
        
        w.write("<?xml version=\"1.0\" encoding=\""+encoding+"\" ?>");

        asXML(w, filter);
        
    }
    
    public String asXML(final Pattern filter) {

        final StringWriter w = new StringWriter();

        try {

            asXML(w, filter);

        } catch (IOException ex) {

            throw new RuntimeException("Unexpected exception: " + ex, ex);

        }

        return w.toString();

    }

}
