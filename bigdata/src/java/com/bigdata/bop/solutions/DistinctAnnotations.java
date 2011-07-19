/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jul 19, 2010
 */
package com.bigdata.bop.solutions;

/**
 * Annotations for DISTINCT operators.
 * 
 * @author thompsonbry@users.sourceforge.net
 */
public interface DistinctAnnotations {

	/**
	 * The variables on which the distinct constraint will be imposed.
	 * Binding sets with distinct values for the specified variables will be
	 * passed on.
	 */
	String VARIABLES = DistinctAnnotations.class.getName() + ".variables";

}
