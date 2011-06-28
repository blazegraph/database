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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop;

/**
 * A constant.
 * <p>
 * Note: {@link IConstant} does not implement {@link Comparable} for two
 * reasons:
 * <ol>
 * <li>{@link Constant}s wrapping different data types are not comparable. Rigid
 * schema data models such as SQL do not have this problem since columns have a
 * single data type, but schema flexible object models and RDF both have runtime
 * determination of the data type.</li>
 * <li>The specifics of the ordering to be imposed are generally determined by a
 * high level query language (SPARQL, XQUERY, SQL, etc). Thus even if this
 * interface was {@link Comparable}, SORT operators generally must provide their
 * own ordering semantics.</li>
 * </ol>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IConstant<E> extends IVariableOrConstant<E> {

    /**
     * The hash code of the value that would be returned by
     * {@link IVariableOrConstant#get()}
     */
    int hashCode();
    
}
