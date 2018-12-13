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
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class SnowflakeSpanStore implements SpanStore {

  DataSourceCall.Factory dataSourceCallFactory;

  public SnowflakeSpanStore(SnowflakeStorage snowflakeStorage) {
    dataSourceCallFactory = new DataSourceCall.Factory(
      snowflakeStorage.datasource(),
      new DSLContexts(snowflakeStorage.settings(), snowflakeStorage.listenerProvider()),
      snowflakeStorage.executor());
  }

  Span buildSpan(long traceId, long id) {
    Span span = Span.newBuilder()
      .traceId(1000L, traceId)
      .id(id)
      .parentId(id + 1)
      .name("Span " + id)
      .timestamp(System.currentTimeMillis())
      .duration(1000L)
      .kind(Span.Kind.CLIENT)
      .localEndpoint(Endpoint.newBuilder()
        .serviceName("local service")
        .ip("127.0.0.1")
        .port(9899)
        .build())
      .remoteEndpoint(Endpoint.newBuilder()
        .serviceName("service me")
        .ip("192.168.0.8")
        .port(80)
        .build())
      .putTag("test", "case " + id)
      .addAnnotation(System.currentTimeMillis() + 100L, "step 1")
      .build();
    return span;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    Function<DSLContext, List<List<Span>>> f = dslContext -> {
      List<Span> trace1 = new ArrayList<>();
      trace1.add(buildSpan(1, 1));
      trace1.add(buildSpan(1, 2));
      trace1.add(buildSpan(1, 3));

      List<Span> trace2 = new ArrayList<>();
      trace2.add(buildSpan(2, 1));
      trace2.add(buildSpan(2, 2));
      trace2.add(buildSpan(2, 3));

      List<List<Span>> traces = new ArrayList<>();
      traces.add(trace1);
      traces.add(trace2);
      return traces;
    };
    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    Function<DSLContext, List<Span>> f = dslContext -> {
      List<Span> trace = new ArrayList<>();
      trace.add(buildSpan(1, 1));
      trace.add(buildSpan(1, 2));
      trace.add(buildSpan(1, 3));
      return trace;
    };
    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    Function<DSLContext, List<String>> f = dslContext -> {
      List<String> serviceNames = new ArrayList<>();
      serviceNames.add("Service A");
      serviceNames.add("Service B");
      serviceNames.add("Service C");
      return serviceNames;
    };
    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    Function<DSLContext, List<String>> f = dslContext -> {
      List<String> spanNames = new ArrayList<>();
      spanNames.add("Span A");
      spanNames.add("Span B");
      spanNames.add("Span C");
      return spanNames;
    };
    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return Call.emptyList();
  } // not final for testing

}
