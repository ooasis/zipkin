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
import org.jooq.Record;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.snowflake.internal.generated.tables.SpanAnnotations;
import zipkin2.storage.snowflake.internal.generated.tables.SpanTags;
import zipkin2.storage.snowflake.internal.generated.tables.Spans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class SnowflakeSpanStore implements SpanStore {

  private DataSourceCall.Factory dataSourceCallFactory;

  public SnowflakeSpanStore(SnowflakeStorage snowflakeStorage) {
    dataSourceCallFactory = new DataSourceCall.Factory(
      snowflakeStorage.datasource(),
      new DSLContexts(snowflakeStorage.settings(), snowflakeStorage.listenerProvider()),
      snowflakeStorage.executor());
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    Function<DSLContext, List<List<Span>>> f = dslContext -> {
      Map<String, List<Span>> allSpans = new HashMap<>();

      dslContext
        .select()
        .from(Spans.SPANS)
        .orderBy(Spans.SPANS.TRACE_ID.desc())
        .limit(100)
        .stream()
        .forEach(r -> {
          Span span = buildSpan(r);
          List<Span> spans;
          if (allSpans.containsKey(span.traceId())) {
            spans = allSpans.get(span.traceId());
          } else {
            spans = new ArrayList<>();
            allSpans.put(span.traceId(), spans);
          }
          spans.add(span);
        });
      return new ArrayList<>(allSpans.values());
    };

    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    Function<DSLContext, List<Span>> f = dslContext -> dslContext
      .select()
      .from(Spans.SPANS)
      .leftOuterJoin(SpanAnnotations.SPAN_ANNOTATIONS)
      .on(Spans.SPANS.TRACE_ID.eq(SpanAnnotations.SPAN_ANNOTATIONS.TRACE_ID))
      .leftOuterJoin(SpanTags.SPAN_TAGS)
      .on(Spans.SPANS.TRACE_ID.eq(SpanTags.SPAN_TAGS.TRACE_ID))
      .where(Spans.SPANS.TRACE_ID.eq(traceId))
      .stream()
      .map(this::buildSpan)
      .collect(Collectors.toList());

    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    Function<DSLContext, List<String>> f = dslContext -> dslContext
      .selectDistinct(Spans.SPANS.LOCAL_SERVICE_NAME)
      .from(Spans.SPANS)
      .fetch()
      .getValues(Spans.SPANS.LOCAL_SERVICE_NAME);

    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    Function<DSLContext, List<String>> f = dslContext -> dslContext
      .selectDistinct(Spans.SPANS.NAME)
      .from(Spans.SPANS)
      .where(Spans.SPANS.LOCAL_SERVICE_NAME.eq(serviceName))
      .fetch()
      .getValues(Spans.SPANS.NAME);

    return dataSourceCallFactory.create(f);
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return Call.emptyList();
  } // not final for testing


  private Span buildSpan(Record r) {
    Span.Builder builder = Span.newBuilder()
      .traceId(r.get(Spans.SPANS.TRACE_ID))
      .id(r.get(Spans.SPANS.ID))
      .name(r.get(Spans.SPANS.NAME))
      .kind(Span.Kind.valueOf(r.get(Spans.SPANS.KIND)))
      .timestamp(r.get(Spans.SPANS.TIMESTAMP))
      .duration(r.get(Spans.SPANS.DURATION));

    if (r.get(Spans.SPANS.DEBUG) != null) {
      builder.debug(r.get(Spans.SPANS.DEBUG));
    }
    if (r.get(Spans.SPANS.SHARE) != null) {
      builder.shared(r.get(Spans.SPANS.SHARE));
    }
    if (r.get(Spans.SPANS.PARENT_ID) != null) {
      builder.parentId(r.get(Spans.SPANS.PARENT_ID));
    }

    builder
      .localEndpoint(
        Endpoint.newBuilder()
          .serviceName(r.get(Spans.SPANS.LOCAL_SERVICE_NAME))
          .ip(r.get(Spans.SPANS.LOCAL_IPV4))
          .port(r.get(Spans.SPANS.LOCAL_PORT))
          .build()
      )
      .remoteEndpoint(
        Endpoint.newBuilder()
          .serviceName(r.get(Spans.SPANS.REMOTE_SERVICE_NAME))
          .ip(r.get(Spans.SPANS.REMOTE_IPV4))
          .port(r.get(Spans.SPANS.REMOTE_PORT))
          .build()
      );

    return builder.build();
  }

}
