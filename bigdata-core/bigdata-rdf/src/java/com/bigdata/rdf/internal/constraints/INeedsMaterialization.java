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
package com.bigdata.rdf.internal.constraints;

/**
 * Some {@link IVValueExpression} need materialized terms to perform their
 * evaluation. Those that do can implement this interface, and specify which
 * terms they need materialized.
 */
public interface INeedsMaterialization {

	public enum Requirement {
		
		/**
		 * Always needs materialization.
		 */
		ALWAYS,
		
		/**
		 * Only needs materialization if inline evaluation fails.
		 */
		SOMETIMES,
		
		/**
		 * Never needs materialization.
		 */
		NEVER
	};
	
	/**
	 * Does the bop always need materialized variables, or can it sometimes
	 * operate on inline terms without materialization?  If sometimes, we'll
	 * run it before the materialization pipeline steps in an effort to avoid
	 * unnecessary materialization overhead. If it fails to evaluate for a 
	 * particular solution, then it will be run again after the materialization
	 * steps for that solution.
	 */
	Requirement getRequirement(); 

}
