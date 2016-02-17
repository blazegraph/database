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
/*
 * Created on Aug 6, 2007
 */
package com.bigdata.rdf.internal.gis;

import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An immutable coordinate expressed as degrees and decimal minutes with 3
 * digits after the decimal.
 * 
 * <pre>
 *        Degrees and Decimal Minutes
 *       
 *        DDD� MM.MMM'
 *        32� 18.385' N 122� 36.875' W
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CoordinateDDM implements ICoordinate {
    public static Logger log = Logger.getLogger(CoordinateDDM.class.getName());

    public final int degreesNorth;

    public final int thousandthsOfMinutesNorth;

    public final int degreesEast;

    public final int thousandthsOfMinutesEast;

    /**
     * 
     * @param degreesNorth
     *            Degrees north/south.
     * @param thousandthsOfMinutesNorth
     *            Decimal minutes north/south expressed as minutes * 1000.
     * @param degreesEast
     *            Degrees east/west.
     * @param thousandthsOfMinutesEast
     *            Decimal minutes east/west expressed as minutes * 1000.
     */
    public CoordinateDDM(//
            int degreesNorth, int thousandthsOfMinutesNorth,//
            int degreesEast, int thousandthsOfMinutesEast //
    ) {
        if (degreesNorth > 90 || degreesNorth < -90)
            throw new IllegalArgumentException();
        if (thousandthsOfMinutesNorth > 60000
                || thousandthsOfMinutesNorth < -60000)
            throw new IllegalArgumentException();
        if (degreesEast > 180 || degreesEast < -180)
            throw new IllegalArgumentException();
        if (thousandthsOfMinutesEast > 60000
                || thousandthsOfMinutesEast < -60000)
            throw new IllegalArgumentException();
        /*
         * If the angle is negative, then all components must be negative.
         */
        if (degreesNorth < 0) {
            if (thousandthsOfMinutesNorth > 0)
                throw new IllegalArgumentException();
        }
        if (degreesEast < 0) {
            if (thousandthsOfMinutesEast > 0)
                throw new IllegalArgumentException();
        }
        /*
         * @todo normalize -180 to 180; do we also have to fiddle with the
         * minutes and seconds?
         */
        this.degreesNorth = degreesNorth;
        this.thousandthsOfMinutesNorth = thousandthsOfMinutesNorth;
        this.degreesEast = degreesEast;
        this.thousandthsOfMinutesEast = thousandthsOfMinutesEast;
    }

    /**
     * Representation of the coordinate in degrees and (thousandths of) decimal
     * minutes. For example:
     * 
     * <pre>
     *   32 18.385N 122 36.875W
     * </pre>
     */
    public String toString() {
        boolean northSouth = degreesNorth > 0;
        boolean eastWest = degreesEast > 0;
        String north = ""
                + (northSouth ? degreesNorth : -degreesNorth)
                + " "
                + formatThousandthsOfMinute(northSouth ? thousandthsOfMinutesNorth
                        : -thousandthsOfMinutesNorth)
                + (northSouth ? "N" : "S");
        String east = (eastWest ? "" + degreesEast : -degreesEast)
                + " "
                + formatThousandthsOfMinute(eastWest ? thousandthsOfMinutesEast
                        : -thousandthsOfMinutesEast) + (eastWest ? "E" : "W");
        return north + " " + east;
    }

    /**
     * Formats a value expressing tenths of a second. For example,
     * <code>18385</code> is formatted as <code>18.385</code>.
     * <p>
     * Note: This does NOT correct for South or West (negative angles).
     * 
     * @param thousandthsOfMinute
     *            An integer value containing minutes and thousandths of a
     *            minute.
     * 
     * @return A string representation of that value.
     */
    static String formatThousandthsOfMinute(int thousandthsOfMinute) {
        int minutes = thousandthsOfMinute / 1000;
        int thousandths = thousandthsOfMinute - minutes * 1000;
        return minutes + "." + thousandths;
    }

    /**
     * Matches a latitude expressed with any of the formats for expressing
     * degrees and decimal minutes.
     * <dl>
     * <dt>group(2)</dt>
     * <dd>degrees</dd>
     * <dt>group(3)</dt>
     * <dd>decimal minutes</dd>
     * <dt>group(4)</dt>
     * <dd>north/south (NnSs)</dd>
     * </dl>
     */
    static final String regex_lat = // 
    "(" + "(\\d{1,2})\\s?[:�*\\s]?\\s?" + // degrees (0:90)
            "(\\d{1,2}|\\d{1,2}\\.\\d*)\\s?[:'\\s]?\\s?" + // decimal minutes
            "([NnSs\\s])" + // north/south
            ")";

    /**
     * Matches a longitude expressed with any of the formats for expressing
     * degrees and decimal minutes.
     * 
     * <dl>
     * <dt>group(2)</dt>
     * <dd>degrees</dd>
     * <dt>group(3)</dt>
     * <dd>decimal minutes</dd>
     * <dt>group(4)</dt>
     * <dd>east/west (EeWw)</dd>
     * </dl>
     */
    static final String regex_long = //
    "(" + "(\\d{1,3})\\s?[:�*\\s]?\\s?" + // degrees (0:180)
            "(\\d{1,2}|\\d{1,2}\\.\\d*)\\s?[:'\\s]?\\s?" + // decimal minutes
            "([EeWw])" + // east/west
            ")";

    /**
     * Matches any of the formats for degrees and decimal minutes.
     * <dl>
     * <dt>group({@link #group_degreesNorth})</dt>
     * <dd>degrees</dd>
     * <dt>group({@link #group_minutesNorth})</dt>
     * <dd>minutes</dd>
     * <dt>group({@link #group_northSouth})</dt>
     * <dd>north/south (NnSs)</dd>
     * <dt>group({@link #group_degreesEast})</dt>
     * <dd>degrees</dd>
     * <dt>group({@link #group_minutesEast})</dt>
     * <dd>minutes</dd>
     * <dt>group({@link #group_eastWest})</dt>
     * <dd>east/west (EeWw)</dd>
     * </dl>
     * 
     * @see #regex_lat
     * @see #regex_long
     */
    static final Pattern pattern_ddm = Pattern.compile("^(" + //
            "(" + regex_lat + "(\\s?[/,]?\\s?)" + regex_long + ")" + //
            ")$"//
    );

    /*
     * The integer identifier of the group in pattern_ddm in which the
     * corresponding named component of the coordinate will be found.
     */
    static final int group_degreesNorth = 4;

    static final int group_minutesNorth = 5;

    static final int group_northSouth = 6;

    static final int group_degreesEast = 9;

    static final int group_minutesEast = 10;

    static final int group_eastWest = 11;

    /**
     * Some formats that are accepted:
     * 
     * <ul>
     * 
     * <li>32� 18.385' N 122� 36.875' W</li>
     * 
     * <li>32 18.385N 122 36.875W</li>
     * 
     * <li>32 18.385 N 122 36.875 W</li>
     * 
     * <li>32:18.385N 122:36.875W</li>
     * 
     * <li>32:18:23.1N/122:36:52.5W</li>
     * 
     * </ul>
     */
    public static CoordinateDDM parse(String text) throws ParseException {
        // See the pattern for the matched groups.
        Matcher m = pattern_ddm.matcher(text);
        if (m.matches()) {
            final int degreesNorth, thousandthsOfMinutesNorth;
            final boolean northSouth;
            try {
                degreesNorth = Integer.parseInt(m.group(group_degreesNorth));
            } catch (NumberFormatException ex) {
                log.log(Level.WARNING, "Parsing text: [" + text + "]", ex);
                throw ex;
            }
            // @todo round vs truncate.
            thousandthsOfMinutesNorth = (int) (Float.parseFloat(m
                    .group(group_minutesNorth)) * 1000);
            // Note: Use of double negative gets the default right (N).
            northSouth = !"S".equalsIgnoreCase(m.group(group_northSouth));
            // northSouth = "N".equalsIgnoreCase(m.group(group_northSouth));
            final int degreesEast, thousandthsOfMinutesEast;
            final boolean eastWest;
            degreesEast = Integer.parseInt(m.group(group_degreesEast));
            // @todo round vs truncate.
            thousandthsOfMinutesEast = (int) (Float.parseFloat(m
                    .group(group_minutesEast)) * 1000);
            // Note: Use of double negative gets the default right (E).
            eastWest = !"W".equalsIgnoreCase(m.group(group_eastWest));
            // eastWest = "E".equalsIgnoreCase(m.group(group_eastWest));
            /*
             * Note: When South or West then all components of the angle are
             * negative.
             */
            return new CoordinateDDM( //
                    northSouth ? degreesNorth : -degreesNorth,//
                    northSouth ? thousandthsOfMinutesNorth
                            : -thousandthsOfMinutesNorth,//
                    eastWest ? degreesEast : -degreesEast,//
                    eastWest ? thousandthsOfMinutesEast
                            : -thousandthsOfMinutesEast//
            );
        }
        throw new ParseException("Not recognized: " + text, 0);
    }

    public boolean equals(ICoordinate o) {
        if (o instanceof CoordinateDDM) {
            return equals((CoordinateDDM) o);
        }
        return false;
    }

    /**
     * Equal if the coordinates are exactly the same (precision to thousandths
     * of a minute).
     * 
     * @param o
     *            Another coordinate expressed in decimal minutes.
     */
    public boolean equals(CoordinateDDM o) {
        return degreesNorth == o.degreesNorth
                && thousandthsOfMinutesNorth == o.thousandthsOfMinutesNorth
                && degreesEast == o.degreesEast
                && thousandthsOfMinutesEast == o.thousandthsOfMinutesEast;
    }

    public double distance(ICoordinate o, UNITS units) {
        return CoordinateUtility.distance(toDD(), o.toDD(), units);
    }

    /**
     * Convert to degrees, minutes and (tenths of) seconds.
     */
    public CoordinateDMS toDMS() {
        // FIXME implement toDMS()
        throw new UnsupportedOperationException();
    }

    /**
     * Convert to decimal degrees.
     */
    public CoordinateDD toDD() {
        return new CoordinateDD(
                //
                degreesNorth + (thousandthsOfMinutesNorth / 1000d) / 60d,
                degreesEast + (thousandthsOfMinutesEast / 1000d) / 60d);
    }

    public CoordinateDDM toDDM() {
        return this;
    }
}
