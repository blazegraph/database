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

public interface Repository {
  /**
   * Opens the repository.
   * @param database Path of the back-end database. Specified as null for
   * memory-based systems.
   */
  public void open(String database);

  /**
   * Closes the current repository.
   */
  public void close();

  /**
   * Loads the specified data to the current repository.
   * @param dataDir Directory where the data reside.
   * @return True on success, otherwise false.
   */
  public boolean load(String dataDir);

  /**
   * Specifies the url of the univ-bench ontology.
   * @param ontology Url of the univ-bench ontology.
   */
  public void setOntology(String ontology);

  /**
   * Issues a query to the current repository.
   * @param query The query.
   * @return The result.
   */
  public QueryResult issueQuery(Query query);

  /**
   * Clears the current repository.
   */
  public void clear();
}