/**
 * Copyright (c) 2013-2017, Kenneth Leung. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.html at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice, or any other, from this software.
 */

package czlabtest.horde;

import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Kenneth Leung
 */
public class JUnit {

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JUnit.class);
  }

  @BeforeClass
  public static void iniz() throws Exception {
  }

  @AfterClass
  public static void finz() {
  }

  @Before
  public void open() throws Exception {
  }

  @After
  public void close() throws Exception {
  }

  @Test
  public void testDummy() throws Exception {
    assertEquals(1, 1);
  }

}


