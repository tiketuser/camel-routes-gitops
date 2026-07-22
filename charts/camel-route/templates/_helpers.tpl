{{/*
Each of source:/sink: declares its endpoint by nesting exactly one kind key:
  kafka: {topic, groupId?, autoOffsetReset?, brokers?, securityProtocol?,
          saslMechanism?, credentialsSecret?, userKey?, passwordKey?}
  http:  {path}  (source)  |  {url}  (sink)
Per-side kafka blocks are merged over the chart-wide kafka: defaults, so source
and sink can point at different clusters with different credentials.
*/}}

{{/* Kind of one side. Args: (dict "side" <map> "label" "source"|"sink") */}}
{{- define "camel-route.kind" -}}
{{- $side := .side | default dict -}}
{{- $kinds := list -}}
{{- if $side.kafka }}{{- $kinds = append $kinds "kafka" }}{{- end -}}
{{- if $side.http }}{{- $kinds = append $kinds "http" }}{{- end -}}
{{- if ne (len $kinds) 1 -}}
{{- fail (printf "%s: set exactly one of kafka:/http: (found %d)" .label (len $kinds)) -}}
{{- end -}}
{{- first $kinds -}}
{{- end -}}

{{- define "camel-route.sourceKind" -}}
{{- include "camel-route.kind" (dict "side" .Values.source "label" "source") -}}
{{- end -}}

{{- define "camel-route.sinkKind" -}}
{{- include "camel-route.kind" (dict "side" .Values.sink "label" "sink") -}}
{{- end -}}

{{/* Derived route type, e.g. kafka-kafka / https-kafka — label-compatible with the old routeType */}}
{{- define "camel-route.type" -}}
{{- $names := dict "kafka" "kafka" "http" "https" -}}
{{- printf "%s-%s" (get $names (include "camel-route.sourceKind" .)) (get $names (include "camel-route.sinkKind" .)) -}}
{{- end -}}

{{/* Does the route touch Kafka at all? */}}
{{- define "camel-route.usesKafka" -}}
{{- if or (eq (include "camel-route.sourceKind" .) "kafka") (eq (include "camel-route.sinkKind" .) "kafka") }}true{{ end -}}
{{- end -}}

{{/* Is the source HTTP? */}}
{{- define "camel-route.httpSource" -}}
{{- if eq (include "camel-route.sourceKind" .) "http" }}true{{ end -}}
{{- end -}}

{{/* Is the sink HTTP? */}}
{{- define "camel-route.httpSink" -}}
{{- if eq (include "camel-route.sinkKind" .) "http" }}true{{ end -}}
{{- end -}}

{{/* One side's effective kafka config: chart defaults overridden by the side's
     kafka: block. Returns YAML — callers fromYaml it.
     Args: (dict "side" <source or sink map> "root" $) */}}
{{- define "camel-route.kafkaConfig" -}}
{{- mergeOverwrite (deepCopy .root.Values.kafka) (deepCopy (.side.kafka | default dict)) | toYaml -}}
{{- end -}}

{{/* Shared kafka endpoint parameters (indented by caller).
     Takes a merged side config from camel-route.kafkaConfig.
     The {{kafka.user}}-style placeholders must reach the CR verbatim; they are
     resolved by Camel from the mounted credentialsSecret's keys, so a side using
     its own secret alongside the default one must give its keys distinct names
     via userKey/passwordKey. */}}
{{- define "camel-route.kafkaParams" -}}
brokers: "{{ required "kafka brokers required" .brokers }}"
securityProtocol: {{ .securityProtocol }}
saslMechanism: {{ .saslMechanism }}
saslJaasConfig: 'org.apache.kafka.common.security.scram.ScramLoginModule required username="{{ printf "{{%s}}" (.userKey | default "kafka.user") }}" password="{{ printf "{{%s}}" (.passwordKey | default "kafka.password") }}";'
{{- end -}}

{{/* Rate-limit bean step (or nothing when disabled) */}}
{{- define "camel-route.rateLimitStep" -}}
{{- if .Values.rateLimit.enabled -}}
{{- $key := .Values.rateLimit.key | default .Values.name -}}
{{- $mode := .Values.rateLimit.mode | default (ternary "reject" "block" (eq (include "camel-route.httpSource" .) "true")) -}}
{{- if and (eq $mode "reject") (ne (include "camel-route.httpSource" .) "true") -}}
{{- fail "rateLimit.mode=reject requires an HTTP source (429 has nowhere to go on a kafka source)" -}}
{{- end -}}
{{- if eq $mode "reject" -}}
- bean:
    ref: rateLimit
    method: "http(${exchange}, '{{ $key }}', {{ .Values.rateLimit.rate }}, {{ .Values.rateLimit.burst }})"
{{- else -}}
- bean:
    ref: rateLimit
    method: "block('{{ $key }}', {{ .Values.rateLimit.rate }}, {{ .Values.rateLimit.burst }})"
{{- end -}}
{{- end -}}
{{- end -}}

{{/* JSON Schema validation step (or nothing when disabled). json-validator throws
     JsonValidationException on a bad body, which the onException below handles per
     source kind (http -> 400, kafka -> dead-letter/drop). */}}
{{- define "camel-route.validateStep" -}}
{{- if .Values.validate.enabled -}}
- to:
    uri: "json-validator:file:/etc/schemas/{{ .Values.name }}.json?contentCache=true"
{{- end -}}
{{- end -}}

{{/* The Camel YAML route, composed from the source and sink kinds */}}
{{- define "camel-route.routeYaml" -}}
{{- $httpSource := eq (include "camel-route.httpSource" .) "true" -}}
{{- $httpSink := eq (include "camel-route.httpSink" .) "true" -}}
{{- if .Values.validate.enabled }}
- onException:
    handled:
      constant: "true"
    exception:
      - org.apache.camel.component.jsonvalidator.JsonValidationException
    steps:
      {{- if $httpSource }}
      - setHeader:
          name: CamelHttpResponseCode
          constant: "400"
      - setHeader:
          name: Content-Type
          constant: "text/plain"
      - setBody:
          constant: "invalid payload\n"
      {{- else }}
      - log:
          message: "schema validation failed, dropping message: ${exception.message}"
          loggingLevel: WARN
      {{- if .Values.validate.deadLetterTopic }}
      {{- $src := fromYaml (include "camel-route.kafkaConfig" (dict "side" .Values.source "root" $)) }}
      - to:
          uri: "kafka:{{ .Values.validate.deadLetterTopic }}"
          parameters:
            {{- include "camel-route.kafkaParams" $src | nindent 12 }}
      {{- end }}
      {{- end }}
{{- end }}
- route:
    id: {{ .Values.name }}
    from:
      {{- if $httpSource }}
      uri: "platform-http:{{ required "source.http.path required" .Values.source.http.path }}"
      {{- else }}
      {{- $src := fromYaml (include "camel-route.kafkaConfig" (dict "side" .Values.source "root" $)) }}
      uri: "kafka:{{ required "source.kafka.topic required" .Values.source.kafka.topic }}"
      parameters:
        groupId: {{ .Values.source.kafka.groupId | default (printf "%s-group" .Values.name) }}
        autoOffsetReset: {{ .Values.source.kafka.autoOffsetReset | default "earliest" }}
        {{- include "camel-route.kafkaParams" $src | nindent 8 }}
      {{- end }}
      steps:
        {{- with include "camel-route.rateLimitStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        {{- with include "camel-route.validateStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        {{- if $httpSink }}
        - removeHeaders:
            pattern: "Camel*"
        - setHeader:
            name: Content-Type
            constant: "text/plain"
        - to:
            uri: "https:{{ trimPrefix "https:" (trimPrefix "http:" (required "sink.http.url required" .Values.sink.http.url)) | trimPrefix "//" }}"
            parameters:
              httpMethod: POST
              bridgeEndpoint: true
              throwExceptionOnFailure: false
              sslContextParameters: "#echoClientSSL"
        {{- else }}
        {{- $snk := fromYaml (include "camel-route.kafkaConfig" (dict "side" .Values.sink "root" $)) }}
        - to:
            uri: "kafka:{{ required "sink.kafka.topic required" .Values.sink.kafka.topic }}"
            parameters:
              {{- include "camel-route.kafkaParams" $snk | nindent 14 }}
        {{- end }}
        {{- if $httpSource }}
        - setHeader:
            name: CamelHttpResponseCode
            constant: "202"
        - setBody:
            constant: "{{ ternary "forwarded" "accepted" $httpSink }}\n"
        {{- end }}
{{- end -}}
