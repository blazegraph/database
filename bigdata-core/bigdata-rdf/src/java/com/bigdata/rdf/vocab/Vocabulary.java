/*

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
 * Created on Aug 26, 2008
 */

package com.bigdata.rdf.vocab;

import java.util.Iterator;

import org.openrdf.model.Value;

import com.bigdata.bop.IConstant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Interface for a pre-defined vocabulary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface Vocabulary {

    /**
     * The namespace of the owning {@link LexiconRelation}.
     * @return
     */
    public String getNamespace();
    
    /**
     * The #of defined {@link Value}s.
     * 
     * @throws IllegalStateException
     *             if the values have not been defined.
     */
    int size();

    /**
     * The {@link Value}s in an arbitrary order.
     * 
     * @throws IllegalStateException
     *             if the values have not been defined.
     */
    Iterator<? extends Value> values();

    /**
     * The term identifier for the pre-defined {@link Value}.
     * 
     * @param value
     *            The value.
     *            
     * @return The {@link IV} for that {@link Value} -or- <code>null</code> if
     *         the {@link Value} was not defined by this {@link Vocabulary}.
     */
    /*
     * Note: Prior to the TERMS_REFACTOR_BRANCH this would throw an exception
     * if the Value was not declared by the Vocabulary.
     */
    public IV get(Value value);
    
    /**
     * Returns the {@link IConstant} for the {@link Value}.
     * 
     * @param value
     *            The value.
     *            
     * @return The {@link IConstant}.
     * 
     * @throws IllegalArgumentException
     *             if that {@link Value} is not defined for this vocabulary.
     */
    public IConstant<IV> getConstant(Value value);

    /**
     * Reverse lookup of an {@link IV} defined by this vocabulary.
     * 
     * @param iv
     *            The {@link IV}.
     * 
     * @return The {@link BigdataValue} -or- <code>null</code> if the {@link IV}
     *         was not defined by the vocabulary.
     * 
     * @since TERMS_REFACTOR_BRANCH
     */
    public Value asValue(IV iv);

}
