/*
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.snowflake;

import org.jooq.DSLContext;
import org.jooq.ExecuteListenerProvider;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import zipkin2.internal.Nullable;

import java.sql.Connection;

final class DSLContexts {
  private final Settings settings;
  private final ExecuteListenerProvider listenerProvider;

  DSLContexts(Settings settings, @Nullable ExecuteListenerProvider listenerProvider) {
    this.settings = settings;
    this.listenerProvider = listenerProvider;
  }

  DSLContext get(Connection conn) {
    return DSL.using(
      new DefaultConfiguration()
        .set(conn)
        .set(SQLDialect.DEFAULT)
        .set(settings)
        .set(listenerProvider));
  }
}
