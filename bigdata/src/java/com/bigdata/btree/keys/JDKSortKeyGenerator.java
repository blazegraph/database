/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.btree.keys;

import java.text.Collator;
import java.util.Locale;

/**
 * Implementation that uses the JDK library (does not support compressed sort
 * keys).
 * 
 * @todo has anyone written a standalone library to convert uncompressed unicode
 *       sort keys into compressed unicode sort keys?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class JDKSortKeyGenerator implements UnicodeSortKeyGenerator {

    private final Collator collator;

    /**
     * The {@link Locale} used to configure this object.
     */
    private final Locale locale;
    
    /**
     * The {@link Locale} used to configure this object.
     */
    public Locale getLocale() {
        
        return locale;
        
    }
    
    public JDKSortKeyGenerator(final Locale locale, final Object strength,
            final DecompositionEnum mode) {

        if (locale == null)
            throw new IllegalArgumentException();

        this.locale = locale;
        
        this.collator = Collator.getInstance(locale);

        if (strength != null) {

            if (strength instanceof Integer) {

                collator.setStrength(((Integer) strength).intValue());

            } else {

                final StrengthEnum str = (StrengthEnum) strength;

                switch (str) {

                case Primary:
                    collator.setStrength(Collator.PRIMARY);
                    break;

                case Secondary:
                    collator.setStrength(Collator.SECONDARY);
                    break;

                case Tertiary:
                    collator.setStrength(Collator.TERTIARY);
                    break;

                //                    case Quaternary:
                //                        collator.setStrength(Collator.QUATERNARY);
                //                        break;

                case Identical:
                    collator.setStrength(Collator.IDENTICAL);
                    break;

                default:
                    throw new UnsupportedOperationException("strength="
                            + strength);

                }

            }

        }

        if (mode != null) {

            switch (mode) {

            case None:
                collator.setDecomposition(Collator.NO_DECOMPOSITION);
                break;

            case Full:
                collator.setDecomposition(Collator.FULL_DECOMPOSITION);
                break;

            case Canonical:
                collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
                break;

            default:
                throw new UnsupportedOperationException("mode=" + mode);

            }

        }

    }

    public void appendSortKey(final KeyBuilder keyBuilder, final String s) {

        /*
         * Note: the collation key is expressed as signed bytes since that
         * is how the JDK normally compares byte[]s. Therefore append it
         * into the key builder using the API that translates signed bytes
         * to unsigned bytes.
         */

        final byte[] sortKey = collator.getCollationKey(s).toByteArray();

        keyBuilder.append( sortKey );

    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{locale=" + getLocale()); // Note: Not self-reported by Collator.
        sb.append(",strength=" + collator.getStrength() + "("
                + getStrength(collator.getStrength()) + ")");
        sb.append(",decomposition=" + collator.getDecomposition() + "("
                + getDecomposition(collator.getDecomposition()) + ")");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Decode a JDK collator strength, returning the corresponding type safe
     * enumeration value.
     * 
     * @param strength
     *            The JDK collator strength.
     * 
     * @return The type safe enumeration value.
     * 
     * @throws IllegalArgumentException
     *             if <i>strength</i> is not a known value.
     */
    public static StrengthEnum getStrength(final int strength) {
        switch (strength) {
        case Collator.PRIMARY:
            return StrengthEnum.Primary;
        case Collator.SECONDARY:
            return StrengthEnum.Secondary;
        case Collator.TERTIARY:
            return StrengthEnum.Tertiary;
        case Collator.IDENTICAL:
            return StrengthEnum.Identical;
        default:
            throw new IllegalArgumentException("Unknown value: " + strength);
        }
    }

    /**
     * Decode a JDK collator decomposition mode, returning the corresponding
     * type safe enumeration value.
     * 
     * @param decomposition
     *            The JDK collator decomposition mode.
     * 
     * @return The type safe enumeration value.
     * 
     * @throws IllegalArgumentException
     *             if <i>decomposition</i> is not a known value.
     */
    public static DecompositionEnum getDecomposition(final int decomposition) {
        switch (decomposition) {
        case Collator.CANONICAL_DECOMPOSITION:
            return DecompositionEnum.Canonical;
        case Collator.NO_DECOMPOSITION:
            return DecompositionEnum.None;
        case Collator.FULL_DECOMPOSITION:
            return DecompositionEnum.Full;
        default:
            throw new IllegalArgumentException("Unknown value: "
                    + decomposition);
        }
    }

}
