/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.internal;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.BD;


public class ColorsEnumExtension implements IExtension {

    public static final URI COLOR = new URIImpl(BD.NAMESPACE + "Color");
    
    private BigdataURI color;
    
    public ColorsEnumExtension() {
        
    }
        
    public void resolveDatatype(final IDatatypeURIResolver resolver) {
        
        this.color = resolver.resolve(COLOR);
        
    }
    
    public BigdataURI getDatatype() {
        
        return color;
        
    }
    
    public ExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        final Literal l = (Literal) value;
        
        if (l.getDatatype() == null || !color.equals(l.getDatatype()))
            throw new IllegalArgumentException();
        
        final String s = value.stringValue();
        
        final Color c = Enum.valueOf(Color.class, s);
        
        // not a valid color
        if (c == null)
            return null;
        
        final AbstractLiteralIV delegate = new XSDByteIV(c.getByte());

        return new ExtensionIV(delegate, (TermId) getDatatype().getIV());
        
    }
    
    public Value asValue(final ExtensionIV iv, final BigdataValueFactory vf) {
        
        final byte b = iv.getDelegate().byteValue();
        
        final Color c = Color.valueOf(b);
        
        return vf.createLiteral(c.toString(), color);
        
    }
    
    /**
     * Simple demonstration enum for some common colors. Can fit up to 256 enum 
     * values into an enum projected onto a byte.
     */
    public enum Color {
        
        Red((byte) 0),
        Blue((byte) 1),
        Green((byte) 2),
        Yellow((byte) 3),
        Orange((byte) 4),
        Purple((byte) 5),
        Black((byte) 6),
        White((byte) 7),
        Brown((byte) 8);
        
        private Color(final byte b) {
            this.b = b;
        }
        
        static final public Color valueOf(final byte b) {
            /*
             * Note: This switch MUST correspond to the declarations above (you can
             * not made the cases of the switch from [v] since it is not considered
             * a to be constant by the compiler).
             * 
             * Note: This masks off everything but the lower 4 bits.
             */
            switch (b) {
            case 0:
                return Red;
            case 1:
                return Blue;
            case 2:
                return Green;
            case 3:
                return Yellow;
            case 4:
                return Orange;
            case 5:
                return Purple;
            case 6:
                return Black;
            case 7:
                return White;
            case 8:
                return Brown;
            default:
                throw new IllegalArgumentException(Byte.toString(b));
            }
        }

        private final byte b;
        
        public byte getByte() {
            return b;
        }
        
    }
    
}
