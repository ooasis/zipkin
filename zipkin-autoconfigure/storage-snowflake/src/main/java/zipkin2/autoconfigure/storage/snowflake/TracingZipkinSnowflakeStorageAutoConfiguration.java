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
package zipkin2.autoconfigure.storage.snowflake;

import brave.Span;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.ThreadLocalSpan;
import org.jooq.ExecuteContext;
import org.jooq.ExecuteListenerProvider;
import org.jooq.impl.DefaultExecuteListener;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin2.Endpoint;

import java.util.concurrent.Executor;

/** Sets up the Snowflake tracing in Brave as an initialization. */
@ConditionalOnBean(Tracing.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "snowflake")
@Configuration
public class TracingZipkinSnowflakeStorageAutoConfiguration extends DefaultExecuteListener {

  @Autowired ZipkinSnowflakeStorageProperties snowflake;

  @Bean
  ExecuteListenerProvider tracingExecuteListenerProvider() {
    return new DefaultExecuteListenerProvider(this);
  }

  @Bean
  @ConditionalOnMissingBean(Executor.class)
  public Executor executor(CurrentTraceContext currentTraceContext) {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setThreadNamePrefix("SnowflakeStorage-");
    executor.initialize();
    return currentTraceContext.executor(executor);
  }

  /** Attach the IP of the remote datasource, knowing that DNS may invalidate this */
  @Bean
  @Qualifier("snowflake")
  Endpoint snowflake() {
    Endpoint.Builder builder = Endpoint.newBuilder().serviceName("snowflake");
    return builder.build();
  }

  /**
   * There's no attribute namespace shared across request and response. Hence, we need to save off a
   * reference to the span in scope, so that we can close it in the response.
   */
  @Bean
  @Qualifier("snowflake")
  ThreadLocalSpan snowflakeThreadLocalSpan(Tracing tracing) {
    return ThreadLocalSpan.create(tracing.tracer());
  }

  @Autowired
  @Qualifier("snowflake")
  Endpoint snowflakelEndpoint;

  @Autowired
  @Qualifier("snowflake")
  ThreadLocalSpan threadLocalSpan;

  @Autowired CurrentTraceContext currentTraceContext;

  @SuppressWarnings("Duplicates")
  @Override
  public void renderEnd(ExecuteContext ctx) {
    // don't start new traces (to prevent amplifying writes to local storage)
    if (currentTraceContext.get() == null) return;

    // Gets the next span (and places it in scope) so code between here and postProcess can read it
    Span span = threadLocalSpan.next();
    if (span == null || span.isNoop()) return;

    String sql = ctx.sql();
    int spaceIndex = sql.indexOf(' '); // Allow span names of single-word statements like COMMIT
    span.kind(Span.Kind.CLIENT).name(spaceIndex == -1 ? sql : sql.substring(0, spaceIndex));
    span.tag("sql.query", sql);
    span.remoteEndpoint(snowflakelEndpoint);
    span.start();
  }

  @Override
  public void executeEnd(ExecuteContext ctx) {
    Span span = ThreadLocalSpan.CURRENT_TRACER.remove();
    if (span == null || span.isNoop()) return;

    if (ctx.sqlException() != null) {
      span.tag("error", Integer.toString(ctx.sqlException().getErrorCode()));
    }
    span.finish();
  }
}
