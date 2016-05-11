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

import java.sql.SQLException;

/**
 * @author kenl
 */
public class DBIOError extends SQLException {

  private static final long serialVersionUID = 113241635256073760L;

  public DBIOError(String msg, Throwable t) {
    super(msg,t);
  }

  /**
   * @param msg
   */
  public DBIOError(String msg) {
    this(msg, null);
  }

  /**
   * @param t
   */
  public DBIOError(Throwable t) {
    this("",t);
  }

  public DBIOError() {
    this("");
  }


}



