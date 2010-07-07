/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 16, 2008
 */

package com.bigdata.rdf.model;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtil;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class BigdataValueImpl implements BigdataValue {

    private transient BigdataValueFactory valueFactory;

    private IV iv;

    public final BigdataValueFactory getValueFactory() {
        
        return valueFactory;
        
    }
    
    /**
     * 
     * @param valueFactory
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if a different {@link BigdataValueFactoryImpl} has already been
     *             set.
     */
    public final void setValueFactory(final BigdataValueFactory valueFactory) {

        if (valueFactory == null)
            throw new IllegalArgumentException();

        if (this.valueFactory != null && this.valueFactory != valueFactory)
            throw new IllegalStateException();

        this.valueFactory = valueFactory;
        
    }
    
    /**
     * @param valueFactory
     *            The value factory that created this object (optional).
     * @param iv
     *            The internal value (optional).
     */
    protected BigdataValueImpl(final BigdataValueFactory valueFactory,
            final IV iv) {
        
//        if (valueFactory == null)
//            throw new IllegalArgumentException();
        
        this.valueFactory = valueFactory;
        
        this.iv = iv;
        
    }

    final public void clearInternalValue() {

        iv = null;
        
    }

    final public IV getIV() {

        return iv;
        
    }

    final public void setIV(final IV iv) {

        if (iv == null) {

            throw new IllegalArgumentException(
                    "Can not set termId to null: term=" + this);

        }

        if (this.iv != null && !IVUtil.equals(this.iv, iv)) {

            throw new IllegalStateException("termId already assigned: old="
                    + this.iv + ", new=" + iv);

        }
        
        this.iv = iv;
        
    }

//    /**
//     * Imposes a total ordering over {@link BigdataValue}s. The different
//     * classes of {@link BigdataValue} are ordered as follows:
//     * 
//     * <ol>
//     * <li>URI, by its externalized form</li>
//     * <li>plain literals, by their data</li>
//     * <li>language code literals, by language code and then their data.</li>
//     * <li>data type literals</li>
//     * <li>blank nodes</li>
//     * </ol>
//     * 
//     * which is the same ordering imposed by {@link ITermIndexCodes}s. Within
//     * each class, the data are ordered as indicated, which is the same order
//     * that is imposed by the {@link Term2IdTupleSerializer}.
//     */
//    public int compareTo(BigdataValue o) {
//        
//        return 0;
//        
//    }
    
}
