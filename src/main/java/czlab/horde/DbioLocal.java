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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Kenneth Leung
 */
public enum DbioLocal {
;

  /**
  * A cache of { database1 - connection pool , database2 - connection pool , ... }
  */
  private static ThreadLocal<Map<Object,JdbcPool>> _cache=new ThreadLocal<Map<Object,JdbcPool>>() {
    protected Map<Object,JdbcPool> initialValue() {
      return new HashMap<Object,JdbcPool>();
    }
  };

  /**/
  public static ThreadLocal<Map<Object,JdbcPool>> cache() {
    return _cache;
  }

}


