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
 * Copyright (c) 2013-2016, Kenneth Leung. All rights reserved. */


package com.zotohlab.frwk.dbio;


import java.util.HashMap;
import java.util.Map;

/**
 * @author kenl
 */
public enum DBIOLocal {
;

  /**
  * A cache of { database1 - connection pool , database2 - connection pool , ... }
  */
  private static ThreadLocal<Map<Object,JDBCPool>> _cache=new ThreadLocal<Map<Object,JDBCPool>>() {
    protected Map<Object,JDBCPool> initialValue() {
      return new HashMap<Object,JDBCPool>();
    }
  };

  public static ThreadLocal<Map<Object,JDBCPool>> getCache() {
    return _cache;
  }

}


