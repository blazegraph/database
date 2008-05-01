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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Iterator;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

/**
 * Provides lazy evaluation of a {@link CounterSet}. This is useful when the
 * set of structure of the counter set can not be determined in advance. For
 * example, when the counter set will enumerate resources which are dynamically
 * created and destroyed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class LazyEvaluationCounterSet implements ICounterSet {

    private final long refreshDelay;
    private long lastUpdateMillis = 0L;
    private CounterSet delegate = null;
    
    public LazyEvaluationCounterSet() {
    
        this(1000/*ms*/);
        
    }
    
    public LazyEvaluationCounterSet(long refreshDelay) {

        if (refreshDelay <= 0L)
            throw new IllegalArgumentException();
        
        this.refreshDelay = refreshDelay;
        
    }
        
    /**
     * Return the delegate. If at least one second has elapsed then offer an
     * opportunity to refresh the delegate {@link CounterSet} using
     * {@link #refreshCounterSet()}.
     * 
     * @return the delegate
     */
    synchronized private CounterSet getDelegate() {
    
        final long now = System.currentTimeMillis();
        
        final long elapsed = now - lastUpdateMillis;
        
        if (delegate == null || elapsed >= refreshDelay) {
            
            delegate = refreshCounterSet();
            
        }
        
        return delegate;
        
    }
    
    /**
     * This is invoked to give the implementation an opportunity to replace the
     * current {@link CounterSet} with a new {@link CounterSet}.
     * <p>
     * Note: this method will not be invoked more than once per second (elapsed
     * wall time) in order to avoid re-generation of the {@link CounterSet}
     * during traversal, XML serialization, etc. During that period the previous
     * counters will simply be reused.
     * 
     * @return
     */
    abstract protected CounterSet refreshCounterSet();
    
    /*
     * Note: Direct modification is not supported. Modification is accomplished
     * by returning a new CounterSet via #getCounters().
     */

    public ICounter addCounter(String path, IInstrument instrument) {
        throw new UnsupportedOperationException();
    }

    public CounterSet makePath(String path) {
        throw new UnsupportedOperationException();
    }

    public void attach(ICounterNode src) {
        throw new UnsupportedOperationException();
    }

    public ICounterNode detach(String path) {
        throw new UnsupportedOperationException();
    }

    /*
     * Methods that are delegated to the last evaluated result.
     */
    public void asXML(OutputStream os, String encoding, Pattern filter) throws IOException {
        getDelegate().asXML(os, encoding, filter);
    }

    public String asXML(Pattern filter) {
        return getDelegate().asXML(filter);
    }

    public void asXML(Writer w, Pattern filter) throws IOException {
        getDelegate().asXML(w, filter);
    }

    public Iterator<ICounter> counterIterator(Pattern filter) {
        return getDelegate().counterIterator(filter);
    }

    public Iterator<ICounterSet> counterSetIterator() {
        return getDelegate().counterSetIterator();
    }

    public Iterator directChildIterator(boolean sorted, Class<? extends ICounterNode> type) {
        return getDelegate().directChildIterator(sorted, type);
    }

    public boolean equals(Object obj) {
        return getDelegate().equals(obj);
    }

    public ICounterNode getChild(String name) {
        return getDelegate().getChild(name);
    }

    public Iterator<ICounter> getCounters(Pattern filter) {
        return getDelegate().getCounters(filter);
    }

    public int getDepth() {
        return getDelegate().getDepth();
    }

    public String getName() {
        return getDelegate().getName();
    }

    public Iterator<ICounterNode> getNodes(Pattern filter) {
        return getDelegate().getNodes(filter);
    }

    public ICounterSet getParent() {
        return getDelegate().getParent();
    }

    public String getPath() {
        return getDelegate().getPath();
    }

    public ICounterNode getPath(String path) {
        return getDelegate().getPath(path);
    }

    public ICounterSet[] getPathComponents() {
        return getDelegate().getPathComponents();
    }

    public AbstractCounterSet getRoot() {
        return getDelegate().getRoot();
    }

    public int hashCode() {
        return getDelegate().hashCode();
    }

    public boolean isCounter() {
        return getDelegate().isCounter();
    }

    public boolean isCounterSet() {
        return getDelegate().isCounterSet();
    }

    public boolean isLeaf() {
        return getDelegate().isLeaf();
    }

    public boolean isRoot() {
        return getDelegate().isRoot();
    }

    public Iterator postOrderIterator() {
        return getDelegate().postOrderIterator();
    }

    public Iterator preOrderIterator() {
        return getDelegate().preOrderIterator();
    }

    public void readXML(InputStream is, IInstrumentFactory instrumentFactory, Pattern filter) throws IOException, ParserConfigurationException, SAXException {
        getDelegate().readXML(is, instrumentFactory, filter);
    }

    public String toString() {
        return getDelegate().toString();
    }

    public String toString(Pattern filter) {
        return getDelegate().toString(filter);
    }

}
