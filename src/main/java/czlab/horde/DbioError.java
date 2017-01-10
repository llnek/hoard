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

import java.sql.SQLException;

/**
 * @author Kenneth Leung
 */
public class DbioError extends SQLException {

  private static final long serialVersionUID = 113241635256073760L;

  /**/
  public DbioError(String msg, Throwable t) {
    super(msg,t);
  }

  /**
   * @param msg
   */
  public DbioError(String msg) {
    this(msg, null);
  }

  /**
   * @param t
   */
  public DbioError(Throwable t) {
    this("",t);
  }

  /**/
  public DbioError() {
    this("");
  }


}



