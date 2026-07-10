{{/* Validate routeType */}}
{{- define "camel-route.type" -}}
{{- $t := required "routeType is required (kafka-kafka | https-kafka | https-https)" .Values.routeType -}}
{{- if not (has $t (list "kafka-kafka" "https-kafka" "https-https")) -}}
{{- fail (printf "unknown routeType %q" $t) -}}
{{- end -}}
{{- $t -}}
{{- end -}}

{{/* Does the route touch Kafka at all? */}}
{{- define "camel-route.usesKafka" -}}
{{- if ne (include "camel-route.type" .) "https-https" }}true{{ end -}}
{{- end -}}

{{/* Is the source HTTP? */}}
{{- define "camel-route.httpSource" -}}
{{- if hasPrefix "https-" (include "camel-route.type" .) }}true{{ end -}}
{{- end -}}

{{/* SASL JAAS line — Camel {{kafka.user}} placeholders must reach the CR verbatim */}}
{{- define "camel-route.jaas" -}}
org.apache.kafka.common.security.scram.ScramLoginModule required username="{{ `{{kafka.user}}` }}" password="{{ `{{kafka.password}}` }}";
{{- end -}}

{{/* Shared kafka endpoint parameters (indented by caller) */}}
{{- define "camel-route.kafkaParams" -}}
brokers: "{{ .Values.kafka.brokers }}"
securityProtocol: {{ .Values.kafka.securityProtocol }}
saslMechanism: {{ .Values.kafka.saslMechanism }}
saslJaasConfig: '{{ include "camel-route.jaas" . }}'
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

{{/* The Camel YAML route, per routeType */}}
{{- define "camel-route.routeYaml" -}}
{{- $t := include "camel-route.type" . -}}
{{- if eq $t "kafka-kafka" -}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "kafka:{{ required "source.topic required for kafka-kafka" .Values.source.topic }}"
      parameters:
        groupId: {{ .Values.source.groupId | default (printf "%s-group" .Values.name) }}
        autoOffsetReset: {{ .Values.source.autoOffsetReset | default "earliest" }}
        {{- include "camel-route.kafkaParams" . | nindent 8 }}
      steps:
        {{- with include "camel-route.rateLimitStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        - to:
            uri: "kafka:{{ required "sink.topic required for kafka-kafka" .Values.sink.topic }}"
            parameters:
              {{- include "camel-route.kafkaParams" . | nindent 14 }}
{{- else if eq $t "https-kafka" -}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "platform-http:{{ required "source.httpPath required for https-kafka" .Values.source.httpPath }}"
      steps:
        {{- with include "camel-route.rateLimitStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        - to:
            uri: "kafka:{{ required "sink.topic required for https-kafka" .Values.sink.topic }}"
            parameters:
              {{- include "camel-route.kafkaParams" . | nindent 14 }}
        - setHeader:
            name: CamelHttpResponseCode
            constant: "202"
        - setBody:
            constant: "accepted\n"
{{- else -}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "platform-http:{{ required "source.httpPath required for https-https" .Values.source.httpPath }}"
      steps:
        {{- with include "camel-route.rateLimitStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        - removeHeaders:
            pattern: "Camel*"
        - setHeader:
            name: Content-Type
            constant: "text/plain"
        - to:
            uri: "http:{{ trimPrefix "http:" (required "sink.url required for https-https" .Values.sink.url) | trimPrefix "//" }}"
            parameters:
              httpMethod: POST
              bridgeEndpoint: true
              throwExceptionOnFailure: false
        - setHeader:
            name: CamelHttpResponseCode
            constant: "202"
        - setBody:
            constant: "forwarded\n"
{{- end -}}
{{- end -}}
