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

/**
 * @author Kenneth Leung
 */
public interface SQLr {

  /**/
  public Iterable<?> findSome(Object modeldef, Object filters, Object extras);

  /**/
  public Iterable<?> findSome(Object modeldef, Object filters);

  /**/
  public Iterable<?> findAll(Object modeldef, Object extras);

  /**/
  public Iterable<?> findAll(Object modeldef);

  /**/
  public Object findOne(Object modeldef, Object filters);

  /**/
  public int update(Object obj);

  /**/
  public int delete(Object obj);

  /**/
  public Object insert(Object obj);

  /**/
  public Iterable<?> select(Object modeldef, String sql, Iterable<?> params);

  /**/
  public Iterable<?> select(String sql, Iterable<?> params);

  /**/
  public Object execWithOutput(String sql, Iterable<?> params);

  /**/
  public int exec(String sql, Iterable<?> params);

  /**/
  public int countAll(Object modeldef);

  /**/
  public int purge(Object modeldef);

  /**/
  public Schema  metas();

  /**/
  public String fmtId(String s);

}




