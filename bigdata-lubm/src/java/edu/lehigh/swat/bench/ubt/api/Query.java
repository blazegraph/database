/**
 * by Yuanbo Guo
 * Semantic Web and Agent Technology Lab, CSE Department, Lehigh University, USA
 * Copyright (C) 2004
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */

package edu.lehigh.swat.bench.ubt.api;

import java.util.Vector;

public class Query {
  /** original string */
  private String string_ = "";
  /** list of conjunctive atoms */
  private Vector atoms_ = new Vector(); //Atom

  /**
   * Constructor.
   */
  public Query() {
  }

  /**
   * Gets the constituent atoms.
   * @return List of atoms in this query.
   */
  public Vector getAtoms() {
    return atoms_;
  }

  /**
   * Adds an additional atom to this query.
   * @param predicate Name of the atom's predicate
   * @param args List of the atom's arguments
   */
  public void addAtom(String predicate, String[] args) {
    Atom atom;
    atom = new Atom(predicate, args);
    atoms_.addElement(atom);
  }

  /**
   * Gets the original string of the this query.
   * @return The original string of this query
   */
  public String getString() {
    return string_;
  }

  /**
   * Sets the original string of this query.
   * @param string The original string of this query.
   */
  public void setString(String string) {
    string_ = string;
  }
}