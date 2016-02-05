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

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An immutable coordinate expressed as double precision decimal degrees on the
 * surface of (the Earth's) sphere.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CoordinateDD implements ICoordinate {
    /**
     * Decimal degrees north (or south iff negative).
     */
    final public double northSouth;

    /**
     * Decimal degrees east (or west iff negative).
     */
    final public double eastWest;

    /*
     * min and max for northSouth and eastWest.
     */
    private static final double MAX_NORTH_SOUTH = +90d;

    private static final double MIN_NORTH_SOUTH = -90d;

    private static final double MAX_EAST_WEST = +180d;

    private static final double MIN_EAST_WEST = -180d;

    /**
     * Format for decimal degrees with (at least) five digits of precision.
     */
    private static final DecimalFormat FMT_DECIMAL_DEGREES;
    static {
        FMT_DECIMAL_DEGREES = new DecimalFormat();
        // DecimalFormatSymbols symbols = new DecimalFormatSymbols(
        // Locale.ENGLISH);
        //
        // // override so that the grouping character looks like a decimal
        // // point.
        // symbols.setGroupingSeparator('.');
        //
        // // override so that the decimal point will not be confused with the
        // // grouping character during parsing.
        // symbols.setDecimalSeparator(':');
        // // override the symbols on our format.
        // FMT_DECIMAL_DEGREES.setDecimalFormatSymbols(symbols);
        // // change the grouping size to 5 digits to get our apparent 5 digits
        // // after the decimal.
        // FMT_DECIMAL_DEGREES.setGroupingSize(5);
        // // make sure that grouping is turned on.
        // FMT_DECIMAL_DEGREES.setGroupingUsed(true);
        // explicit positive prefix.
        FMT_DECIMAL_DEGREES.setPositivePrefix("+");
        // explicit negative prefix.
        FMT_DECIMAL_DEGREES.setNegativePrefix("-");
        FMT_DECIMAL_DEGREES.setMinimumFractionDigits(5);
    }

    /**
     * Constructor for a coordinate using decimal degrees.
     * 
     * @param northSouth
     *            Decimal degrees north (or south iff negative). The range is
     *            [-90:+90].
     * 
     * @param eastWest
     *            Decimal degrees east (or west iff negative). The range is
     *            (-180:+180]. Note that a value of 180W is normalized to 180E
     *            by this constructor.
     * 
     * @exception IllegalArgumentException
     *                if <i>northSouth</i> is out of the range [-90:+90].
     * @exception IllegalArgumentException
     *                if <i>eastWest</i> is out of the range [-180:+180].
     */
    public CoordinateDD(double northSouth, double eastWest) { 
       this(northSouth, eastWest, false);
    }
    
    
    /**
     * Constructor for a coordinate using decimal degrees.
     * 
     * @param northSouth
     *            Decimal degrees north (or south iff negative). The range is
     *            [-90:+90].
     * 
     * @param eastWest
     *            Decimal degrees east (or west iff negative). The range is
     *            (-180:+180].
     *            
     * @param adjustToMinMax
     *            If this parameter is set to true we may pass in values that are out of
     *            range. Such values are adjusted to the respective MIN and MAX
     *            values. If set to false, exceptions are thrown.
     * 
     * @exception IllegalArgumentException
     *                if <i>adjustToMinMax</i> is false AND <i>northSouth</i> is out of the range [-90:+90].
     * @exception IllegalArgumentException
     *                if <i>adjustToMinMax</i> is false AND if <i>eastWest</i> is out of the range [-180:+180].
     */
    public CoordinateDD(double northSouth, double eastWest, boolean adjustToMinMax) {
       
        if (northSouth < MIN_NORTH_SOUTH || northSouth > MAX_NORTH_SOUTH) {
           
           if (adjustToMinMax) {
            
              northSouth = northSouth < MIN_NORTH_SOUTH ? MIN_NORTH_SOUTH : MAX_NORTH_SOUTH;
              
           } else {
               throw new IllegalArgumentException("NorthSouth: "
                       + FMT_DECIMAL_DEGREES.format(northSouth));
           }
        }
        if (eastWest < MIN_EAST_WEST || eastWest > MAX_EAST_WEST) {
           
           if (adjustToMinMax) {
              
              eastWest = eastWest < MIN_EAST_WEST ? MIN_EAST_WEST : MAX_EAST_WEST;
              
           } else {
              
               throw new IllegalArgumentException("EastWest: "
                       + FMT_DECIMAL_DEGREES.format(eastWest));
               
           }
        }

        this.northSouth = northSouth;
        this.eastWest = eastWest;
    }

    /**
     * Returns the coordinate in decimal degrees as
     * 
     * <pre>
     *  +32.30642, -122.61458
     * </code>
     */
    public String toString() {
        return FMT_DECIMAL_DEGREES.format(northSouth) + ","
                + FMT_DECIMAL_DEGREES.format(eastWest);
    }

    /**
     * Matches a latitude expressed as decimal degrees.
     * <dl>
     * <dt>group(1)</dt>
     * <dd>decimal degrees</dd>
     * </dl>
     */
    static final String regex_lat = // 
    "([-+]?\\d{1,2}\\.\\d{3,})[�*]?" // decimal degrees longitude.
    ;

    /**
     * Matches a longitude expressed as decimal degrees.
     * <dl>
     * <dt>group(1)</dt>
     * <dd>decimal degrees</dd>
     * </dl>
     */
    static final String regex_long = //
    "([-+]?\\d{1,3}\\.\\d{3,})[�*]?" // decimal degrees longitude.
    ;

    /**
     * Matches a coordinate expressed as decimal degrees.
     * <dl>
     * <dt>group({@link #group_degreesNorth})</dt>
     * <dd>decimal degrees north/south (latitude)</dd>
     * <dt>group({@link #group_degreesEast})</dt>
     * <dd>decimal degrees east/west (longitude)</dd>
     * </dl>
     * 
     * @see #regex_lat
     * @see #regex_long
     */
    static final Pattern pattern_dd = Pattern.compile("^(" + //
            "(" + regex_lat + "(\\s?[/,]?\\s?)" + regex_long + ")" + //
            ")$"//
    );

    /*
     * The integer identifier of the group in pattern_dd in which the
     * corresponding named component of the coordinate will be found.
     */
    static final int group_degreesNorth = 3;

    static final int group_degreesEast = 5;

    /**
     * Parses coordinates expressed in decimal degrees. For example
     * 
     * <pre>
     * 36.07263, -79.79197
     * 
     * 36.07263/-79.79197
     * 
     * 36.07263 -79.79197
     * 
     * +36.0726355 -79.7919754
     * </pre>
     * 
     * @param text
     *            The text.
     * 
     * @return A coordinate
     */
    public static CoordinateDD parse(String text) throws ParseException {
        Matcher m = pattern_dd.matcher(text);
        if (m.matches()) {
            double northSouth = Double.parseDouble(m.group(group_degreesNorth));
            double eastWest = Double.parseDouble(m.group(group_degreesEast));
            return new CoordinateDD(northSouth, eastWest, false);
        }
        throw new ParseException("Not decimal degrees: [" + text + "]", 0);
    }

    public boolean equals(ICoordinate o) {
        if (o instanceof CoordinateDD) {
            return equals((CoordinateDD) o);
        }
        return false;
    }

    /**
     * True iff the two coordinates are exactly the same.
     * 
     * @param o
     * 
     * @return
     */
    public boolean equals(CoordinateDD o) {
        return northSouth == o.northSouth && eastWest == o.eastWest;
    }

    /**
     * Convert to degrees, minutes and (tenths of) seconds.
     * 
     * @return The coordinate expressed as degrees, minutes and (tenths of)
     *         seconds.
     */
    public CoordinateDMS toDMS() {
        /*
         * The whole units of degrees will remain the same (i.e. in 121.135�
         * longitude, start with 121�).
         * 
         * Multiply the decimal by 60 (i.e. .135 * 60 = 8.1).
         * 
         * The whole number becomes the minutes (8').
         * 
         * Take the remaining decimal and multiply by 60. (i.e. .1 * 60 = 6).
         * 
         * The resulting number becomes the seconds (6"). Seconds can remain as
         * a decimal.
         * 
         * Take your three sets of numbers and put them together, using the
         * symbols for degrees (�), minutes (�), and seconds (") (i.e. 121�8'6"
         * longitude)
         * 
         * @see http://geography.about.com/library/howto/htdegrees.htm
         * 
         * @see http://id.mind.net/~zona/mmts/trigonometryRealms/degMinSec/degMinSec.htm
         */
        // FIXME toDMS is not implemented
        throw new UnsupportedOperationException();
    }

    public double distance(ICoordinate o, UNITS units) {
        return CoordinateUtility.distance(this, o.toDD(), units);
    }

    public CoordinateDDM toDDM() {
        // TODO Auto-generated method stub (toDDM)
        return null;
    }

    public CoordinateDD toDD() {
        return this;
    }
}
