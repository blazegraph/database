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

abstract public class AbstractHostMetrics implements IHostMetrics {

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Number> T getNumeric(final String name,
            final T defaultValue) {

        if (name == null)
            throw new IllegalArgumentException();

        if (defaultValue == null)
            throw new IllegalArgumentException();

        final Number v = getNumeric(name);

        if (v == null) {

            // Not found. Return the default.
            return defaultValue;

        }

        // Found. Coerce to the data type of the default.
        if (Double.class == defaultValue.getClass()) {
            return (T) (Double) v.doubleValue();
        } else if (Float.class == defaultValue.getClass()) {
            return (T) (Float) v.floatValue();
        } else if (Long.class == defaultValue.getClass()) {
            return (T) (Long) v.longValue();
        } else if (Integer.class == defaultValue.getClass()) {
            return (T) (Integer) v.intValue();
        } else if (Short.class == defaultValue.getClass()) {
            return (T) (Short) v.shortValue();
        } else if (Byte.class == defaultValue.getClass()) {
            return (T) (Byte) v.byteValue();
        } else {
            throw new UnsupportedOperationException("class="
                    + defaultValue.getClass().getName());
        }
    }

}
