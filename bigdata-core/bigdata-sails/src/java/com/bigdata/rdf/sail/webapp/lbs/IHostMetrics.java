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
package com.bigdata.rdf.sail.webapp.lbs;

/**
 * Generic interface exposes an abstract model of the performance metrics
 * for a given host.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHostMetrics {

//    /**
//     * Return a textual representation of the named metric.
//     * 
//     * @param name
//     *            The metric.
//     *            
//     * @return The textual representation of that metric -or- <code>null</code>
//     *         if there is no such metric available.
//     */
//    String getText(String name);

    /**
     * Return a numeric representation of the named metric.
     * 
     * @param name
     *            The metric.
     * 
     * @return The numeric representation of that metric -or- <code>null</code>
     *         if there is no such metric available.
     * 
     * @throws IllegalArgumentException
     *             if the <code>name</code> is <code>null</code>.
     */
    Number getNumeric(String name);

    /**
     * Return a numeric representation of the named metric.
     * 
     * @param name
     *            The metric.
     * @param defaultValue
     *            The default value.
     * 
     * @return The numeric representation of that metric -or- the
     *         <code>defaultValue</code> if there is no such metric available.
     * @throws IllegalArgumentException
     *             if the <code>name</code> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <code>defaultValue</code> is <code>null</code>.
     */
    <T extends Number> T getNumeric(String name, T defaultValue);

    /**
     * Return the names of all known metrics.
     */
    String[] getMetricNames();

}
