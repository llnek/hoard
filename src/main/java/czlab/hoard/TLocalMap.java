/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright © 2013-2022, Kenneth Leung. All rights reserved. */

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


