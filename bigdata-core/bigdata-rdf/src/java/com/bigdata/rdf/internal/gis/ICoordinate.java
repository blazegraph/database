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
package com.bigdata.rdf.internal.gis;

/**
 * Interface for a coordinate (latitude, longitude) on a sphere. There are
 * several ways in which a coordinate may be expressed:
 * 
 * <pre>
 *     Degrees, Minutes and Seconds
 *    
 *     DDD� MM' SS.S&quot;
 *     32� 18' 23.1&quot; N 122� 36' 52.5&quot; W
 *    
 *    
 *     Degrees and Decimal Minutes
 *    
 *     DDD� MM.MMM'
 *     32� 18.385' N 122� 36.875' W
 *     
 *    
 *     Decimal Degrees
 *    
 *     DDD.DDDDD�
 *     32.30642� N 122.61458� W
 *     or +32.30642, -122.61458
 *    
 *     These are the basics, but they can be listed many different ways.
 *    
 *     Here are a few examples:
 *    
 *     32� 18' 23.1&quot; N 122� 36' 52.5&quot; W
 *     
 *     32 18 23.1N 122 36 52.5 W
 *    
 *     32 18 23.1N/122 36 52.5W
 *    
 *     32:18:23N/122:36:52W
 *    
 *     321823N/1223652W (zeros would need to go in front of single digits and two zeros
 *     in front of the longitude degrees because it�s range is up to 180 � the latitude
 *     range is only up to 90)
 *    
 *     3218N/12236W
 * </pre>
 * 
 * @see http://en.wikipedia.org/wiki/Geographic_coordinate_system
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICoordinate {
    /**
     * Return true if two coordinates are exactly the same.
     * <p>
     * Note: Coordinates MUST be expressed in the same system before they may be
     * compared. E.g., Degrees, Minutes and Seconds or Degrees and Decimal
     * Minutes or Decimal Degrees.
     * 
     * @param o
     *            Another coordinate.
     */
    public boolean equals(ICoordinate o);

    /**
     * Typesafe enumeration for units in which distances may be expressed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static enum UNITS {
        Meters, Kilometers, Feet, Miles, NauticalMiles;
    };

    /**
     * Computes the distance to the specified coordinate and returns that
     * distance in the specified units.
     * 
     * @param o
     *            Another coordinate.
     * @param units
     *            The units in which the distance will be reported.
     * 
     * @return The distance in the specified units.
     */
    public double distance(ICoordinate o, UNITS units);

    /**
     * Convert to degrees, minutes and (tenths of) seconds.
     * 
     * @return The coordinate expressed as degrees, minutes and (tenths of)
     *         seconds.
     */
    public CoordinateDMS toDMS();

    /**
     * Convert to degrees and decimal minutes.
     * 
     * @return The coordinate expressed as degrees and decimal minutes.
     */
    public CoordinateDDM toDDM();

    /**
     * Convert to decimal degrees.
     * 
     * @return The coordinate expressed as decimal degrees.
     */
    public CoordinateDD toDD();
}
