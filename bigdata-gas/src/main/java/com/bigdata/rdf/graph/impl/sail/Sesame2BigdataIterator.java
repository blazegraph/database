/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
/*
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.graph.impl.sail;

import java.util.NoSuchElementException;

import info.aduna.iteration.CloseableIteration;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Class aligns a Sesame 2 {@link CloseableIteration} with a bigdata
 * {@link ICloseableIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Bigdata2SesameIteration.java 2265 2009-10-26 12:51:06Z
 *          thompsonbry $
 * @param <T>
 *            The generic type of the visited elements.
 * @param <E>
 *            The generic type of the exceptions thrown by the Sesame 2
 *            {@link CloseableIteration}.
 */
/*
 * Note: This is a clone of the same-named class in the bigdata-rdf module. The
 * clone exists to have it under the Apache 2 license without going through a
 * large relayering of the dependencies.
 */
class Sesame2BigdataIterator<T, E extends Exception> implements
        ICloseableIterator<T> {

    private final CloseableIteration<? extends T,E> src;
    
    private volatile boolean open = true;
    
    public Sesame2BigdataIterator(final CloseableIteration<? extends T,E> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public void close() {

        if (open) {
            open = false;
            try {
                src.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
    }

    public boolean hasNext() {

        try {

            if (open && src.hasNext())
                return true;

            close();
            
            return false;
            
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    public T next() {

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            return src.next();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    public void remove() {

        if(!open)
            throw new IllegalStateException();

        try {
            src.remove();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }

}
