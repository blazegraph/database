/*

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
package com.bigdata.rdf.internal.constraints;

import java.util.Set;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;


/**
 * Some {@link IVValueExpression} need materialized terms to perform their
 * evaluation. Those that do can implement this interface, and specify which
 * terms they need materialized.
 */
public interface INeedsMaterialization {

	Set<IVariable<IV>> getTermsToMaterialize();

}
