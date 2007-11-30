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
package com.bigdata.btree;

import java.text.Collator;
import java.util.Locale;

import com.bigdata.btree.KeyBuilder.DecompositionEnum;
import com.bigdata.btree.KeyBuilder.StrengthEnum;
import com.bigdata.btree.KeyBuilder.UnicodeSortKeyGenerator;

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

    public JDKSortKeyGenerator(Locale locale, Object strength,
            DecompositionEnum mode) {

        if (locale == null)
            throw new IllegalArgumentException();

        this.collator = Collator.getInstance(locale);

        if (strength != null) {

            if (strength instanceof Integer) {

                collator.setStrength(((Integer) strength).intValue());

            } else {

                StrengthEnum str = (StrengthEnum) strength;

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

    public void appendSortKey(KeyBuilder keyBuilder, String s) {

        /*
         * Note: the collation key is expressed as signed bytes since that
         * is how the JDK normally compares byte[]s. Therefore append it
         * into the key builder using the API that translates signed bytes
         * to unsigned bytes.
         */

        final byte[] sortKey = collator.getCollationKey(s).toByteArray();

        keyBuilder.append( sortKey );

    }

}
