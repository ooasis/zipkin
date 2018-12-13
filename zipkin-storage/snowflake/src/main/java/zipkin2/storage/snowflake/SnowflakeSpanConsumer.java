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
import org.jooq.InsertSetMoreStep;
import org.jooq.Query;
import org.jooq.Record;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.snowflake.internal.generated.tables.SpanAnnotations;
import zipkin2.storage.snowflake.internal.generated.tables.SpanTags;
import zipkin2.storage.snowflake.internal.generated.tables.Spans;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class SnowflakeSpanConsumer implements SpanConsumer {

  DataSourceCall.Factory dataSourceCallFactory;

  public SnowflakeSpanConsumer(SnowflakeStorage snowflakeStorage) {
    dataSourceCallFactory = new DataSourceCall.Factory(
      snowflakeStorage.datasource(),
      new DSLContexts(snowflakeStorage.settings(), snowflakeStorage.listenerProvider()),
      snowflakeStorage.executor());
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return dataSourceCallFactory.create(new BatchInsertSpans(spans));
  }

  @SuppressWarnings("Duplicates")
  static final class BatchInsertSpans implements Function<DSLContext, Void> {
    final List<Span> spans;

    BatchInsertSpans(List<Span> spans) {
      this.spans = spans;
    }

    @Override
    public Void apply(DSLContext create) {
      List<Query> inserts = new ArrayList<>();

      for (Span v2 : spans) {
        InsertSetMoreStep<Record> insertSpan = create
          .insertInto(Spans.SPANS)
          .set(Spans.SPANS.TRACE_ID, v2.traceId())
          .set(Spans.SPANS.NAME, v2.traceId())
          .set(Spans.SPANS.PARENT_ID, v2.traceId())
          .set(Spans.SPANS.ID, v2.traceId())
          .set(Spans.SPANS.KIND, v2.kind().name())
          .set(Spans.SPANS.TIMESTAMP, v2.timestampAsLong())
          .set(Spans.SPANS.DURATION, v2.durationAsLong())
          .set(Spans.SPANS.DEBUG, v2.debug())
          .set(Spans.SPANS.SHARE, v2.shared())
          .set(Spans.SPANS.LOCAL_SERVICE_NAME, v2.localServiceName())
          .set(Spans.SPANS.LOCAL_IPV4, v2.localEndpoint().ipv4())
          .set(Spans.SPANS.LOCAL_IPV6, v2.localEndpoint().ipv6())
          .set(Spans.SPANS.LOCAL_PORT, v2.localEndpoint().port())
          .set(Spans.SPANS.REMOTE_SERVICE_NAME, v2.remoteServiceName())
          .set(Spans.SPANS.REMOTE_IPV4, v2.remoteEndpoint().ipv4())
          .set(Spans.SPANS.REMOTE_IPV6, v2.remoteEndpoint().ipv6())
          .set(Spans.SPANS.REMOTE_PORT, v2.remoteEndpoint().port());
        inserts.add(insertSpan);

        for (Annotation anno : v2.annotations()) {
          InsertSetMoreStep<Record> insertSpanAnnotation = create
            .insertInto(SpanAnnotations.SPAN_ANNOTATIONS)
            .set(SpanAnnotations.SPAN_ANNOTATIONS.TRACE_ID, v2.traceId())
            .set(SpanAnnotations.SPAN_ANNOTATIONS.ID, v2.traceId())
            .set(SpanAnnotations.SPAN_ANNOTATIONS.A_TIMESTAMP, anno.timestamp())
            .set(SpanAnnotations.SPAN_ANNOTATIONS.A_VALUE, anno.value());
          inserts.add(insertSpanAnnotation);

        }

        for (Map.Entry<String, String> tag : v2.tags().entrySet()) {
          InsertSetMoreStep<Record> insertSpanTag = create
            .insertInto(SpanTags.SPAN_TAGS)
            .set(SpanTags.SPAN_TAGS.TRACE_ID, v2.traceId())
            .set(SpanTags.SPAN_TAGS.ID, v2.traceId())
            .set(SpanTags.SPAN_TAGS.T_KEY, tag.getKey())
            .set(SpanTags.SPAN_TAGS.T_VALUE, tag.getValue());
          inserts.add(insertSpanTag);
        }
      }

      create.batch(inserts).execute();
      return null;
    }

  }
}
