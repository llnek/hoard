/**
 * Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.html at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice, or any other, from this software.
 */

package czlab.horde;

import czlab.jasal.Disposable;
import java.sql.Connection;

/**
 * A sql database interface.
 *
 * @author Kenneth Leung
 */
public interface DbApi extends Disposable {

  /**
   * All operations are done within a transaction.
   */
  public Transactable compositeSQLr();

  /**
   * Auto commits on each operation.
   */
  public SQLr simpleSQLr();

  /**
   * Metadata related to the database.
   */
  public Schema schema();

  /**
   * Product information.
   */
  public Object vendor();

  /**
   * Make a connection to the database.
   */
  public Connection open();

}



