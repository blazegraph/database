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
 * Created on Apr 10, 2012
 */

package com.bigdata.bop.rdf.join;

import java.util.NoSuchElementException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.lexicon.LexiconRelation;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Iterator pattern for chunked materialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see ChunkedMaterializationOp
 */
public class ChunkedMaterializationIterator implements
        ICloseableIterator<IBindingSet[]> {

    private final IVariable<?>[] required;

    private final LexiconRelation lex;

    private final boolean materializeInlineIVs;

    private final ICloseableIterator<IBindingSet[]> src;

    private boolean open = false;

    /**
     * 
     * @param vars
     *            The variables to be materialized (required; must not be an
     *            empty array).
     * @param lex
     *            The {@link LexiconRelation}.
     * @param materializeInlineIVs
     *            When <code>true</code>, inline IVs will also be materialized.
     * @param src
     *            The source iterator.
     */
    public ChunkedMaterializationIterator(//
            final IVariable<?>[] vars,//
            final LexiconRelation lex,//
            final boolean materializeInlineIVs,//
            final ICloseableIterator<IBindingSet[]> src) {

        if (vars == null)
            throw new IllegalArgumentException();

        if (vars != null && vars.length == 0)
            throw new IllegalArgumentException();

        if (lex == null)
            throw new IllegalArgumentException();
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.required = vars;

        this.lex = lex;
        
        this.materializeInlineIVs = materializeInlineIVs;
        
        this.src = src;

    }

    @Override
    public void close() {
     
        if (open) {
        
            open = false;
            
            src.close();
            
        }
        
    }

    @Override
    public boolean hasNext() {

        if (open && !src.hasNext()) {

            close();

            return false;

        }

        return open;
        
    }

    @Override
    public IBindingSet[] next() {

        if (!hasNext())
            throw new NoSuchElementException();

        final IBindingSet[] chunkIn = src.next();

        final IBindingSet[] chunkOut =
            ChunkedMaterializationOp.resolveChunk(
                required, lex, chunkIn, materializeInlineIVs);

        return chunkOut;        

    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
