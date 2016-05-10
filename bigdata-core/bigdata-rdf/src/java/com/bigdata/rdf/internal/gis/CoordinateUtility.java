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

import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;

/**
 * Utility class for operations on {@link ICoordinate}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see http://en.wikipedia.org/wiki/Geographic_coordinate_system
 * @see http://en.wikipedia.org/wiki/Earth_radius
 * @see http://barelybad.com/north_of_canada_map.htm
 */
public class CoordinateUtility {
    /**
     * 
     */
    public CoordinateUtility() {
        super();
    }

    /**
     * The #of meters per second of latitude at sea level <code>30.82</code>
     * (this is the same regardless of the degrees north/south).
     */
    public static double metersPerSecondOfLatitudeAtSeaLevel = 30.82;

    /**
     * The #of meters per minute of latitude at sea level (this is the same
     * regardless of the degrees north/south).
     */
    public static double metersPerMinuteOfLatitudeAtSeaLevel = metersPerSecondOfLatitudeAtSeaLevel * 60;

    /**
     * The #of meters per degree of latitude at sea level (this is the same
     * regardless of the degrees north/south).
     */
    public static double metersPerDegreeOfLatitudeAtSeaLevel = metersPerSecondOfLatitudeAtSeaLevel * 60 * 60;

    /**
     * The #of meters per second of longitude at sea level on the equator. The
     * #of meters per second of longitude decreases as the angle increases until
     * it becomes zero (0) at the poles (at the poles the longitudinal radius of
     * a sphere is always zero).
     */
    public static double metersPerSecondOfLongitudeAtSeaLevelAtEquator = 30.92;

    /**
     * The average radius of the Earth (meters). This does not account for
     * flattening of the Earth.
     */
    static final double averageRadius = 6367449d;

    /** The equatorial radius of the Earth (meters). */
    static final double equatorialRadius = 6378137d;

    /** The polar radius of the Earth (meters). */
    static final double polarRadius = 6356752.3d;

    /** {@link #equatorialRadius} to the 4th power. */
    static final double equatorialRadius4 = Math.pow(equatorialRadius, 4);

    /** {@link #polarRadius} to the 4th power. */
    static final double polarRadius4 = Math.pow(polarRadius, 4);

    /** {@link Math#PI} / 180 degrees. */
    static final double _pi_div_180 = Math.PI / 180.d;

    /** 180 degrees / {@link Math#PI}. */
    static final double _180_div_pi = 180.d / Math.PI;

    /**
     * The real width of a longitudinal degree on a given latitude (this
     * accounts for the flattening of the Earth).
     * 
     * @param degreesNorth
     *            The latitude (north/south).
     * 
     * @return The real width of a degree of longitude at sea level at the given
     *         latitude.
     * 
     * @see #approxMetersPerDegreeOfLongitudeAtSeaLevel(double)
     */
    public static double realMetersPerDegreeOfLongitudeAtSeaLevel(
            double degreesNorth) {
        assertDegreeLatitude(degreesNorth);
        // convert angle to radians.
        final double radians = toRadians(degreesNorth);
        assert radians <= Math.PI / 2d;
        assert radians >= -Math.PI / 2d;
        final double cos = Math.cos(radians);
        final double sin = Math.sin(radians);
        final double nom = (equatorialRadius4 * cos * cos)
                + (polarRadius4 * sin * sin);
        final double denom = Math.pow(equatorialRadius * cos, 2d)
                + Math.pow(polarRadius * sin, 2d);
        /*
         * This is the radius (in meters) at a given latitude.
         */
        final double radiusAtLatitude = Math.sqrt(nom / denom);
        assert radiusAtLatitude <= equatorialRadius;
        assert radiusAtLatitude >= polarRadius;
        /*
         * Compute meters per degree using the estimated radius of the earth
         * (adjusted for flattening) at the given latitude (now in radians).
         */
        final double lengthOfArc = _pi_div_180 * Math.cos(radians)
                * radiusAtLatitude;
        // assert lengthOfArc <= metersPerSecondOfLongitudeAtSeaLevelAtEquator;
        return lengthOfArc;
    }
    
    public static CoordinateDD boundingBoxSouthWest(
       final CoordinateDD start, double distance, UNITS units) {
       
       double distanceAsMeters = unitsToMeters(distance, units);

       // compute numbers of degrees to travel to the top
       final double deltaNorthSouth = 
          distanceAsMeters/metersPerDegreeOfLatitudeAtSeaLevel;
       
       // compute numbers of degrees to travel to the left
       final Double currentLat = start.northSouth;
       
       final Double deltaEastWest = (1 / (111320 * Math.cos(currentLat/360*2*Math.PI))) * distanceAsMeters;

       Double lon = start.eastWest - deltaEastWest;
       
       /** 
        * over case where we go "beyond" the -180 degrees border. If the search range
        * is smaller than 360 (=everything), we just convert a value such as -190 into
        * (180 - 10) = 170. Our search range should then later be from [170;180].
        */
       if (deltaEastWest<360) {
           if (lon<=-180) { 
               lon = 180 + (lon%180);
           }
       }
       
       final CoordinateDD ret = new CoordinateDD(
             start.northSouth - deltaNorthSouth, lon, true);
       
       return ret;
    }
    
    public static CoordinateDD boundingBoxNorthEast(
          final CoordinateDD start, double distance, UNITS units) {
          
          double distanceAsMeters = unitsToMeters(distance, units);

          // compute numbers of degrees to travel to the left
          final double deltaNorthSouth = 
             distanceAsMeters/metersPerDegreeOfLatitudeAtSeaLevel;
          
          // compute numbers of degrees to travel to the top

          final Double currentLat = start.northSouth;
          final Double deltaEastWest = (1 / (111320 * Math.cos(currentLat/360*2*Math.PI))) * distanceAsMeters;

          Double lon = start.eastWest + deltaEastWest;
          
          /** 
           * over case where we go "beyond" the 180 degrees border. If the search range
           * is smaller than 360 (=everything), we just convert a value such as 190 into
           * (=180 + 10) =-170 and start search from there.
           * Our search range should then later be from [-180;-170].
           */
          if (deltaEastWest<360) {          
              if (lon>=180) {
                  lon = -180 + (lon%180);
              }
          }
          
          final CoordinateDD ret = new CoordinateDD(
                start.northSouth + deltaNorthSouth, lon, true);
          
          return ret;

       }
    

    public static void assertDegreeLatitude(double d) {
        if (d <= -90d || d > 90d)
            throw new IllegalArgumentException("" + d + " is not in [90:-90)");
    }

    public static void assertDegreeLongitude(double d) {
        if (d <= -180d || d > 180d)
            throw new IllegalArgumentException("" + d + " is not in [180:-180)");
    }

    // /**
    // * The real width of a longitudinal degree on a given latitude.
    // *
    // * @param secondsNorth
    // * The latitude (north/south).
    // *
    // * @return
    // */
    // public static double metersPerSecondOfLongitudeAtSeaLevel(double
    // secondsNorth) {
    //        
    // return realMetersPerDegreeOfLongitudeAtSeaLevel(secondsNorth*3600.);
    //        
    // }
    /**
     * The approximate width of a longitudinal degree on a given latitude.
     * <p>
     * Note: This routine is faster than
     * {@link #realMetersPerDegreeOfLongitudeAtSeaLevel(double)} but does not
     * account for the flattening of the Earth.
     * 
     * @param degreesNorth
     *            The latitude (north/south).
     * 
     * @return The approximate width of a longitudial degree at that latitude.
     * 
     * @see #realMetersPerDegreeOfLongitudeAtSeaLevel(double)
     */
    public static double approxMetersPerDegreeOfLongitudeAtSeaLevel(
            double degreesNorth) {
        assertDegreeLatitude(degreesNorth);
        // convert angle to radians.
        final double radians = toRadians(degreesNorth);
        /*
         * Compute meters per degree using the average radius of the earth and
         * the given latitude (now in radians).
         */
        final double lengthOfArc = _pi_div_180 * Math.cos(radians)
                * averageRadius;
        return lengthOfArc;
    }

    // /**
    // * Return the angle converted to the transverse graticule that is used by
    // * trig functions (shifted 90 degrees).
    // *
    // * @param degreesNorth The latitude in degrees north/south.
    // *
    // * @return The angle in the transverse graticule.
    // */
    // public static double toTransverseGraticule(double degreesNorth) {
    //        
    // double angle = degreesNorth + 90;
    //        
    // if(angle >= 180d) {
    //            
    // angle -= 180d;
    //            
    // }
    //        
    // return angle;
    //        
    // }
    /**
     * Convert degrees to radians.
     * 
     * @param degrees
     *            The angle in degrees.
     * 
     * @return The angle in radians.
     */
    public static double toRadians(double degrees) {
        final double radians = _pi_div_180 * degrees;
        return radians;
    }

    /**
     * Convert radians to degrees.
     * 
     * @param radians
     *            The angle in radians.
     * 
     * @return The angle in degrees.
     */
    public static double toDegrees(double radians) {
        final double degrees = _180_div_pi * radians;
        return degrees;
    }
    
    
    /**
     * Computes the distance between two coordinates.
     * 
     * @param p1 coordinate one
     * @param p2 coordinate two
     * @param units desired return unit
     * @return
     */
    public static double distance(CoordinateDD p1, CoordinateDD p2, UNITS units) {
       
       final double latP1 = p1.northSouth;
       final double latP2 = p2.northSouth;
       final double lonP1 = p1.eastWest;
       final double lonP2 = p2.eastWest;
       
       // BLZG-1897: distance calculation fails for identical points
       if (latP1==latP2 && lonP1==lonP2)
           return 0;
       
       double distRad = 
          Math.acos(
             Math.sin(toRadians(latP1)) * Math.sin(toRadians(latP2)) + 
             Math.cos(toRadians(latP1)) * Math.cos(toRadians(latP2)) * Math.cos(toRadians(lonP1 - lonP2)));

       final double distAsDegree = toDegrees(distRad);

       return metersToUnits(distAsDegree * 60 * 1.1515 * 1609.344, units);
    }
    
    
    /**
     * Computes the distance between two coordinates in meters.
     * 
     * @param latP1 latitude of point #1
     * @param latP2 latitude of point #2
     * @param lonP1 latitude of point #1
     * @param lonP1 latitude of point #2
     * @param units desired return unit
     * @return
     */
    public static double distanceInMeters(
       final double latP1, final double latP2, final double lonP1, final double lonP2) {
       
       double distRad = 
          Math.acos(
             Math.sin(toRadians(latP1)) * Math.sin(toRadians(latP2)) + 
             Math.cos(toRadians(latP1)) * Math.cos(toRadians(latP2)) * Math.cos(toRadians(lonP1 - lonP2)));

       final double distAsDegree = toDegrees(distRad);

       return distAsDegree * 60 * 1.1515 * 1609.344;
    }



    /**
     * Convert meters to the desired units.
     * 
     * @param meters
     *            The #of meters.
     * @param units
     *            The target units.
     * 
     * @return The converted distance.
     */
    public static double metersToUnits(double meters, UNITS units) {
        switch (units) {
        case Feet:
            return meters * 3.2808399d;
        case Miles:
            return meters / 1609.344d;
        case Meters:
            return meters;
        case Kilometers:
            return meters / 1000d;
        case NauticalMiles:
            return meters / 1852d;
        default:
            throw new AssertionError("Unknown units: " + units);
        }
    }
    
    
    /**
     * Convert meters to the desired units.
     * 
     * @param meters
     *            The #of meters.
     * @param units
     *            The target units.
     * 
     * @return The converted distance.
     */
    public static double unitsToMeters(double val, UNITS units) {
       switch (units) {
       case Feet:
           return val / 3.2808399d;
       case Miles:
           return val * 1609.344d;
       case Meters:
           return val;
       case Kilometers:
           return val * 1000d;
       case NauticalMiles:
           return val * 1852d;
       default:
           throw new AssertionError("Unknown units: " + units);
       }
    }

    /**
     * Convert Degrees, Minutes, and Seconds to Decimal Degrees.
     * 
     * @param degrees
     *            Degrees
     * @param minutes
     *            Minutes (w/ fraction).
     * @param seconds
     *            Seconds (w/ fractial seconds).
     * 
     * @return The angle in decimal degrees.
     */
    public static double toDecimalDegrees(int degrees, int minutes,
            double seconds) {
        return degrees + minutes / 60d + seconds / 3600d;
    }
   
}
