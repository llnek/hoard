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

/**
 * @author kenl
 */
public interface SQLr {

  public Iterable<?> findSome(Object modeldef, Object  filters, Object  extras);

  public Iterable<?> findSome(Object modeldef, Object  filters);

  public Iterable<?> findAll(Object modeldef, Object extras);

  public Iterable<?> findAll(Object modeldef);

  public Object findOne(Object modeldef, Object  filters);

  public Object update(Object obj);
  public Object delete(Object obj);
  public Object insert(Object obj);

  public Iterable<?> select(Object modeldef, String sql, Iterable<?> params);
  public Iterable<?> select(String sql, Iterable<?> params);

  public Object execWithOutput(String sql, Iterable<?> params);
  public Object exec(String sql, Iterable<?> params);

  public int countAll(Object modeldef);

  public void purge(Object modeldef);

  public Object  metas();

}




