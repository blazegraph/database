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

import java.io.IOException;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * Abstract base class for {@link BigdataValue} implementations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class BigdataValueImpl implements BigdataValue {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3114316856174115308L;

	private volatile transient BigdataValueFactory valueFactory;

    private volatile IV iv;

    public final BigdataValueFactory getValueFactory() {
        
        return valueFactory;
        
    }
    
//    Note: unused.
//    /**
//     * 
//     * @param valueFactory
//     * 
//     * @throws IllegalArgumentException
//     *             if the argument is <code>null</code>.
//     * @throws IllegalStateException
//     *             if a different {@link BigdataValueFactoryImpl} has already been
//     *             set.
//     */
//    public final void setValueFactory(final BigdataValueFactory valueFactory) {
//
//        if (valueFactory == null)
//            throw new IllegalArgumentException();
//
//        if (this.valueFactory != null && this.valueFactory != valueFactory)
//            throw new IllegalStateException();
//
//        this.valueFactory = valueFactory;
//        
//    }
    
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

        if (this.iv != null && !IVUtility.equals(this.iv, iv)) {

            throw new IllegalStateException("termId already assigned: old="
                    + this.iv + ", new=" + iv);

        }
        
        this.iv = iv;
        
    }

    /**
     * Extends the serialization format to include the namespace of the lexicon
     * so we can recover the {@link BigdataValueFactory} singleton reference for
     * that namespace when the value is deserialized.
     */
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {

		out.defaultWriteObject();
		
		out.writeUTF(((BigdataValueFactoryImpl) valueFactory).getNamespace());
		
	}

	/**
	 * Imposes the canonicalizing mapping on the non-Serializable
	 * BigdataValueFactory during object de-serialization.
	 */
	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {

		in.defaultReadObject();

		final String namespace = in.readUTF();
		
		valueFactory = BigdataValueFactoryImpl.getInstance(namespace);
		
	}

}
