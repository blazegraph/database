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

import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An immutable coordinate expressed in degrees, minutes and (tenths of)
 * seconds.
 * <p>
 * Note: When the angle is negative, all components must be negative. For
 * example, 0794731W corresponds to -79 deg 47' 31.111439999999998", but the
 * individual components are actually -79, -47, and -31.1...
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CoordinateDMS implements ICoordinate {
    public static Logger log = Logger.getLogger(CoordinateDMS.class.getName());

    public final int degreesNorth;

    public final int minutesNorth;

    public final int tenthsOfSecondsNorth;

    public final int degreesEast;

    public final int minutesEast;

    public final int tenthsOfSecondsEast;

    public CoordinateDMS(//
            int degreesNorth, int minutesNorth, int tenthsOfSecondsNorth,//
            int degreesEast, int minutesEast, int tenthsOfSecondsEast//
    ) {
        if (degreesNorth > 90 || degreesNorth < -90)
            throw new IllegalArgumentException();
        if (minutesNorth > 60 || minutesNorth < -60)
            throw new IllegalArgumentException();
        if (tenthsOfSecondsNorth > 600 || tenthsOfSecondsNorth < -600)
            throw new IllegalArgumentException();
        if (degreesEast > 180 || degreesEast < -180)
            throw new IllegalArgumentException();
        if (minutesEast > 60 || minutesEast < -60)
            throw new IllegalArgumentException();
        if (tenthsOfSecondsEast > 600 || tenthsOfSecondsEast < -600)
            throw new IllegalArgumentException();
        /*
         * If the angle is negative, then all components must be negative.
         */
        if (degreesNorth < 0) {
            if (minutesNorth > 0)
                throw new IllegalArgumentException();
            if (tenthsOfSecondsNorth > 0)
                throw new IllegalArgumentException();
        }
        if (degreesEast < 0) {
            if (minutesEast > 0)
                throw new IllegalArgumentException();
            if (tenthsOfSecondsEast > 0)
                throw new IllegalArgumentException();
        }
        /*
         * @todo normalize -180 to 180; do we also have to fiddle with the
         * minutes and seconds?
         */
        this.degreesNorth = degreesNorth;
        this.minutesNorth = minutesNorth;
        this.tenthsOfSecondsNorth = tenthsOfSecondsNorth;
        this.degreesEast = degreesEast;
        this.minutesEast = minutesEast;
        this.tenthsOfSecondsEast = tenthsOfSecondsEast;
    }

    /**
     * Representation of the coordinate in degrees, minutes, and (tenths of)
     * seconds. For example:
     * 
     * <pre>
     *  32 18 23.1N 122 36 52.5W
     * </pre>
     */
    public String toString() {
        boolean northSouth = degreesNorth > 0;
        boolean eastWest = degreesEast > 0;
        String north = ""
                + (northSouth ? degreesNorth : -degreesNorth)
                + " "
                + (northSouth ? minutesNorth : -minutesNorth)
                + " "
                + formatTenthsOfSecond(northSouth ? tenthsOfSecondsNorth
                        : -tenthsOfSecondsNorth) + (northSouth ? "N" : "S");
        String east = (eastWest ? "" + degreesEast : -degreesEast)
                + " "
                + (eastWest ? minutesEast : -minutesEast)
                + " "
                + formatTenthsOfSecond(eastWest ? tenthsOfSecondsEast
                        : -tenthsOfSecondsEast) + (eastWest ? "E" : "W");
        return north + " " + east;
    }

    /**
     * Rounds off the coordinate to the nearest seconds.
     * 
     * @return A new coordinate that has been rounded off to the nearest
     *         seconds.
     */
    public CoordinateDMS roundSeconds() {
        int secondsNorth = (tenthsOfSecondsNorth > 0 ? (int) Math
                .round(tenthsOfSecondsNorth / 10.) : -(int) Math
                .round(-tenthsOfSecondsNorth / 10.));
        int secondsEast = (tenthsOfSecondsEast > 0 ? (int) Math
                .round(tenthsOfSecondsEast / 10.) : -(int) Math
                .round(-tenthsOfSecondsEast / 10.));
        return new CoordinateDMS(//
                degreesNorth, minutesNorth, secondsNorth * 10,//
                degreesEast, minutesEast, secondsEast * 10//
        );
    }

    /**
     * Rounds off the coordinate to the nearest minutes (rounds up at 30.0
     * seconds to the next highest minute).
     * 
     * @return A new coordinate that has been rounded off to the nearest
     *         minutes.
     */
    public CoordinateDMS roundMinutes() {
        final int roundMinutesNorth = (tenthsOfSecondsNorth > 300 ? minutesNorth + 1
                : (tenthsOfSecondsNorth < -300 ? minutesNorth - 1
                        : minutesNorth));
        final int roundMinutesEast = (tenthsOfSecondsEast > 300 ? minutesEast + 1
                : (tenthsOfSecondsEast < -300 ? minutesEast - 1 : minutesEast));
        return new CoordinateDMS(
                //
                degreesNorth, roundMinutesNorth, 0, degreesEast,
                roundMinutesEast, 0//
        );
    }

    public boolean equals(ICoordinate o) {
        if (o instanceof CoordinateDMS) {
            return equals((CoordinateDMS) o);
        }
        return false;
    }

    /**
     * True iff the two coordinates are exactly the same (to the tenths of the
     * second).
     * 
     * @param o
     *            Another coordinate.
     * 
     * @return True if the coordinates are exactly the same.
     */
    public boolean equals(CoordinateDMS o) {
        return degreesNorth == o.degreesNorth && minutesNorth == o.minutesNorth
                && tenthsOfSecondsNorth == o.tenthsOfSecondsNorth
                && degreesEast == o.degreesEast && minutesEast == o.minutesEast
                && tenthsOfSecondsEast == o.tenthsOfSecondsEast;
    }

    /**
     * Some formats that are accepted:
     * 
     * <ul>
     * 
     * <li>32� 18' 23.1" N 122� 36' 52.5" W</li>
     * 
     * <li>32 18 23.1N 122 36 52.5 W</li>
     * 
     * <li>32 18 23.1N/122 36 52.5W</li>
     * 
     * <li>32:18:23N/122:36:52W</li>
     * 
     * <li>32:18:23.1N/122:36:52.5W</li>
     * 
     * <li>321823N/1223652W <br/> (zeros would need to go in front of single
     * digits and two zeros in front of the longitude degrees because it�s range
     * is up to 180 � the latitude range is only up to 90)</li>
     * 
     * <li>3218N/12236W</li>
     * 
     * </ul>
     */
    public static CoordinateDMS parse(String text) throws ParseException {
        // See the pattern for the matched groups.
        Matcher m = pattern_dms1.matcher(text);
        if (m.matches()) {
            final int degreesNorth, minutesNorth, tenthsOfSecondsNorth;
            final boolean northSouth;
            try {
                degreesNorth = Integer.parseInt(m.group(group_degreesNorth));
            } catch (NumberFormatException ex) {
                log.log(Level.WARNING, "Parsing text: [" + text + "]", ex);
                throw ex;
            }
            minutesNorth = Integer.parseInt(m.group(group_minutesNorth));
            // Note: null iff group not matched. "" if matched on a zero length
            // production.
            if (m.group(group_secondsNorth) != null
                    && m.group(group_secondsNorth).length() > 0) {
                // @todo round vs truncate.
                tenthsOfSecondsNorth = (int) (Float.parseFloat(m
                        .group(group_secondsNorth)) * 10);
            } else {
                tenthsOfSecondsNorth = 0;
            }
            // Note: Use of double negative gets the default right (N).
            northSouth = !"S".equalsIgnoreCase(m.group(group_northSouth));
            final int degreesEast, minutesEast, tenthsOfSecondsEast;
            final boolean eastWest;
            degreesEast = Integer.parseInt(m.group(group_degreesEast));
            minutesEast = Integer.parseInt(m.group(group_minutesEast));
            if (m.group(group_secondsEast) != null
                    && m.group(group_secondsEast).length() > 0) {
                // @todo round vs truncate.
                tenthsOfSecondsEast = (int) (Float.parseFloat(m
                        .group(group_secondsEast)) * 10);
            } else {
                tenthsOfSecondsEast = 0;
            }
            // Note: Use of double negative gets the default right (E).
            eastWest = !"W".equalsIgnoreCase(m.group(group_eastWest));
            /*
             * Note: When South or West then all components of the angle are
             * negative.
             */
            return new CoordinateDMS( //
                    northSouth ? degreesNorth : -degreesNorth,//
                    northSouth ? minutesNorth : -minutesNorth,//
                    northSouth ? tenthsOfSecondsNorth : -tenthsOfSecondsNorth,//
                    eastWest ? degreesEast : -degreesEast,//
                    eastWest ? minutesEast : -minutesEast,//
                    eastWest ? tenthsOfSecondsEast : -tenthsOfSecondsEast//
            );
        }
        throw new ParseException("Not recognized: " + text, 0);
    }

    /**
     * Matches a latitude expressed with any of the formats for expressing
     * degrees, minutes, and (optional) seconds (with optional tenths of
     * seconds). For example <code>32� 18' 23.1" N</code>,
     * <code>32 18 23.1 N</code> or <code>32:18:23N</code>.
     * <dl>
     * <dt>group(2)</dt>
     * <dd>degrees</dd>
     * <dt>group(3)</dt>
     * <dd>minutes</dd>
     * <dt>group(5)</dt>
     * <dd>seconds (optional, with optional tenths)</dd>
     * <dt>group(6)</dt>
     * <dd>north/south (NnSs)</dd>
     * </dl>
     */
    static final String regex_lat = // 
    "(" + "(\\d{1,2})\\s?[:�*\\s]?\\s?" + // degrees (0:90)
            "(\\d{1,2})\\s?[:'\\s]?\\s?" + // minutes
            "((\\d{1,2}|\\d{1,2}\\.\\d?)\\s?\"?)?\\s?" + // optional seconds
                                                            // (with optional
                                                            // tenths)
            "([NnSs])" + // north/south
            ")";

    /**
     * Matches a longitude expressed with any of the formats for expressing
     * degrees, minutes, and (optional) seconds (with optional tenths of
     * seconds).
     * 
     * <dl>
     * <dt>group(2)</dt>
     * <dd>degrees</dd>
     * <dt>group(3)</dt>
     * <dd>minutes</dd>
     * <dt>group(5)</dt>
     * <dd>seconds (optional, with optional tenths)</dd>
     * <dt>group(6)</dt>
     * <dd>east/west (EeWw)</dd>
     * </dl>
     */
    static final String regex_long = //
    "(" + "(\\d{1,3})\\s?[:�*\\s]?\\s?" + // degrees (0:180)
            "(\\d{1,2})\\s?[:'\\s]?\\s?" + // minutes
            "((\\d{1,2}|\\d{1,2}\\.\\d?)\\s?\"?)?\\s?" + // optional seconds
                                                            // (with optional
                                                            // tenths)
            "([EeWw])" + // east/west
            ")";

    /**
     * Matches any of the formats that put separators between degrees, minutes,
     * and seconds.
     * <dl>
     * <dt>group({@link #group_degreesNorth})</dt>
     * <dd>degrees</dd>
     * <dt>group({@link #group_minutesNorth})</dt>
     * <dd>minutes</dd>
     * <dt>group({@link #group_secondsNorth})</dt>
     * <dd>seconds</dd>
     * <dt>group({@link #group_northSouth})</dt>
     * <dd>north/south (NnSs)</dd>
     * <dt>group({@link #group_degreesEast})</dt>
     * <dd>degrees</dd>
     * <dt>group({@link #group_minutesEast})</dt>
     * <dd>minutes</dd>
     * <dt>group({@link #group_secondsEast})</dt>
     * <dd>seconds</dd>
     * <dt>group({@link #group_eastWest})</dt>
     * <dd>east/west (EeWw)</dd>
     * </dl>
     * 
     * @see #regex_lat
     * @see #regex_long
     */
    static final Pattern pattern_dms1 = Pattern.compile("^(" + //
            "(" + regex_lat + "(\\s?[/,]?\\s?)" + regex_long + ")" + //
            ")$"//
    );

    /*
     * The integer identifier of the group in pattern_dms1 in which the
     * corresponding named component of the coordinate will be found.
     */
    static final int group_degreesNorth = 4;

    static final int group_minutesNorth = 5;

    static final int group_secondsNorth = 7;

    static final int group_northSouth = 8;

    static final int group_degreesEast = 11;

    static final int group_minutesEast = 12;

    static final int group_secondsEast = 14;

    static final int group_eastWest = 15;

    /**
     * Formats a value expressing tenths of a second. For example,
     * <code>328</code> is formatted as <code>32.8</code>.
     * <p>
     * Note: This does NOT correct for South or West (negative angles).
     * 
     * @param tenthsOfSecond
     *            An integer value containing seconds and tenths of a second.
     * 
     * @return A string representation of that value.
     */
    static String formatTenthsOfSecond(int tenthsOfSecond) {
        int seconds = tenthsOfSecond / 10;
        int tenths = tenthsOfSecond - seconds * 10;
        return seconds + "." + tenths;
    }

    /**
     * Parses a value representing seconds and optional tenths of a second. For
     * example, <code>32.8</code> is returned as <code>328</code> and
     * <code>32</code> is returned as <code>320</code>.
     * 
     * @param text
     *            A representation of seconds and optional tenths of a second.
     * 
     * @return An integer expressing tenths of a second.
     */
    static int parseTenthsOfSecond(String text) throws ParseException {
        int pos = text.indexOf(".");
        int lastPos = text.lastIndexOf(".");
        if (pos != lastPos)
            throw new ParseException(text, lastPos);
        final int tenthsOfSecond;
        if (pos == -1) {
            /*
             * No tenths of a second.
             */
            int seconds = Integer.parseInt(text);
            tenthsOfSecond = seconds * 10;
        } else {
            /*
             * Tenths of a second are present.
             */
            String secondsStr = text.substring(0, pos);
            String tenthsStr = text.substring(pos + 1);
            int seconds = Integer.parseInt(secondsStr);
            int tenths = tenthsStr.length() == 0 ? 0 : Integer
                    .parseInt(tenthsStr);
            tenthsOfSecond = seconds * 10 + tenths;
        }
        return tenthsOfSecond;
    }

    /**
     * Convert to decimal degrees.
     * <p>
     * Decimal degrees = whole number of degrees, plus minutes divided by 60,
     * plus seconds divided by 3600.
     */
    public CoordinateDD toDD() {
        final double _degreesNorth = CoordinateUtility.toDecimalDegrees(
                degreesNorth, minutesNorth, tenthsOfSecondsNorth / 10d);
        final double _degreesEast = CoordinateUtility.toDecimalDegrees(
                degreesEast, minutesEast, tenthsOfSecondsEast / 10d);
        return new CoordinateDD(_degreesNorth, _degreesEast);
    }

    public double distance(ICoordinate o, UNITS units) {
        return CoordinateUtility.distance(toDD(), o.toDD(), units);
    }

    public CoordinateDMS toDMS() {
        return this;
    }

    public CoordinateDDM toDDM() {
        // TODO Auto-generated method stub
        return null;
    }
}
