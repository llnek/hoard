/**
 * Copyright © 2013-2020, Kenneth Leung. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.html at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice, or any other, from this software.
 */

package czlab.hoard;

import java.util.HashMap;
import java.util.Map;

/**
 */
public enum TLocalMap {
;

  /**
   */
  public static ThreadLocal<Map<Object,Object>> cache() {
    return _cache;
  }

  /**
   */
  private static ThreadLocal<Map<Object,Object>>
    _cache =new ThreadLocal<Map<Object,Object>>() {

    protected Map<Object,Object> initialValue() {
      return new HashMap<Object,Object>();
    }
  };

}


