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

public class Atom {
  /** the predicate */
  private String predicate_;
  /** the arguments */
  private String[] arguments_;

  /**
   * Constructor.
   * @param predicate Name of the predicate
   * @param arguments List of the arguments
   */
  public Atom(String predicate, String[] arguments) {
    predicate_ = predicate;
    arguments_ = arguments;
  }

  /**
   * Gets the predicate.
   * @return Name of the predicate
   */
  public String getPredicate() {
    return predicate_;
  }

  /**
   * Gets the arguments.
   * @return List of the arguments
   */
  public String[] getArguments() {
    return arguments_;
  }
}