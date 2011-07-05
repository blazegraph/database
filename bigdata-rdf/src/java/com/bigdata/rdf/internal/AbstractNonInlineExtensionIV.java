/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 29, 2011
 */

package com.bigdata.rdf.internal;

import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for non-inline {@link IV}s which use the extension bit
 * and distinguish themselves by an {@link #getExtensionByte() extension byte}
 * following the flags byte.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractNonInlineExtensionIV<V extends BigdataValue, T>
        extends AbstractNonInlineIV<V, T> {

    private static final long serialVersionUID = 1L;
    
    protected AbstractNonInlineExtensionIV(final byte flags) {

        super(flags);

    }

    /**
     * @param vte
     * @param dte
     */
    public AbstractNonInlineExtensionIV(VTE vte, DTE dte) {

        super(vte, true/* extension */, dte);

    }

    /**
     * Return the extension byte for this type of non-inline IV.
     * 
     * @see INonInlineExtensionCodes
     */
    abstract public byte getExtensionByte();
    
}
