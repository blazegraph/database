package it.unimi.dsi.io;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2006-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */
import java.io.Closeable;

/** A marker interface for a closeable resource that implements safety measures to
 * make resource tracking easier.
 * 
 * <p>Classes implementing this interface must provide a <em>safety-net finaliser</em>&mdash;a
 * finaliser that closes the resource and logs that resource should have been closed.
 * 
 * <p>When the implementing class is abstract, concrete subclasses <strong>must</strong>
 * call <code>super.close()</code> in their own {@link java.io.Closeable#close()} method
 * to let the abstract class track correctly the resource. Moreover, 
 * they <strong>must</strong> run <code>super.finalize()</code> in
 * their own finaliser (if any), as finalisation chaining is not automatic.
 * 
 * <p>Note that if a concrete subclass implements <code>readResolve()</code>, it must
 * call <code>super.close()</code>, or actually return <code>this</code> (i.e., the deserialised
 * instance); otherwise, a spurious log could be generated when the deserialised instance is collected.
 * 
 * @author Sebastiano Vigna
 * @since 1.1
 */

public interface SafelyCloseable extends Closeable {}
