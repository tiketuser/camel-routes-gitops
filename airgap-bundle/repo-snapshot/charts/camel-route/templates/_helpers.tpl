{{/* Validate routeType */}}
{{- define "camel-route.type" -}}
{{- $t := required "routeType is required (kafka-kafka | https-kafka | https-https | mq-mq | https-mq | mq-https | mq-drain)" .Values.routeType -}}
{{- if not (has $t (list "kafka-kafka" "https-kafka" "https-https" "mq-mq" "https-mq" "mq-https" "mq-drain")) -}}
{{- fail (printf "unknown routeType %q" $t) -}}
{{- end -}}
{{- $t -}}
{{- end -}}

{{/* Does the route touch Kafka at all? */}}
{{- define "camel-route.usesKafka" -}}
{{- if has (include "camel-route.type" .) (list "kafka-kafka" "https-kafka") }}true{{ end -}}
{{- end -}}

{{/* Does the route touch IBM MQ at all? */}}
{{- define "camel-route.usesMq" -}}
{{- if has (include "camel-route.type" .) (list "mq-mq" "https-mq" "mq-https" "mq-drain") }}true{{ end -}}
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

{{/* Shared JMS/MQ endpoint parameters (indented by caller) */}}
{{- define "camel-route.mqParams" -}}
connectionFactory: "#mqConnectionFactory"
username: "{{ `{{mq.user}}` }}"
password: "{{ `{{mq.password}}` }}"
{{- end -}}

{{/* Rate-limit bean step (or nothing when disabled) */}}
{{- define "camel-route.rateLimitStep" -}}
{{- if .Values.rateLimit.enabled -}}
{{- $key := .Values.rateLimit.key | default .Values.name -}}
{{- $mode := .Values.rateLimit.mode | default (ternary "reject" "block" (eq (include "camel-route.httpSource" .) "true")) -}}
{{- if and (eq $mode "reject") (ne (include "camel-route.httpSource" .) "true") -}}
{{- fail "rateLimit.mode=reject requires an HTTP source (429 has nowhere to go on a kafka/mq source)" -}}
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
{{- else if eq $t "https-https" -}}
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
            uri: "https:{{ trimPrefix "https:" (trimPrefix "http:" (required "sink.url required for https-https" .Values.sink.url)) | trimPrefix "//" }}"
            parameters:
              httpMethod: POST
              bridgeEndpoint: true
              throwExceptionOnFailure: false
              sslContextParameters: "#echoClientSSL"
        - setHeader:
            name: CamelHttpResponseCode
            constant: "202"
        - setBody:
            constant: "forwarded\n"
{{- else if eq $t "mq-mq" -}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "jms:queue:{{ required "source.queue required for mq-mq" .Values.source.queue }}"
      parameters:
        {{- include "camel-route.mqParams" . | nindent 8 }}
      steps:
        {{- with include "camel-route.rateLimitStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        - to:
            uri: "jms:queue:{{ required "sink.queue required for mq-mq" .Values.sink.queue }}"
            parameters:
              {{- include "camel-route.mqParams" . | nindent 14 }}
              # Explicit fire-and-forget — without it camel-jms may infer request-reply.
              exchangePattern: "InOnly"
{{- else if eq $t "https-mq" -}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "platform-http:{{ required "source.httpPath required for https-mq" .Values.source.httpPath }}"
      steps:
        {{- with include "camel-route.rateLimitStep" . }}
        {{- . | nindent 8 }}
        {{- end }}
        - to:
            uri: "jms:queue:{{ required "sink.queue required for https-mq" .Values.sink.queue }}"
            parameters:
              {{- include "camel-route.mqParams" . | nindent 14 }}
              # Without this, camel-jms infers InOut (request-reply) because the route
              # originates from platform-http — the dev "app" user isn't authorized to
              # open a temporary reply queue outside its DEV.** grant.
              exchangePattern: "InOnly"
        - setHeader:
            name: CamelHttpResponseCode
            constant: "202"
        - setBody:
            constant: "accepted\n"
{{- else if eq $t "mq-https" -}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "jms:queue:{{ required "source.queue required for mq-https" .Values.source.queue }}"
      parameters:
        {{- include "camel-route.mqParams" . | nindent 8 }}
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
            uri: "https:{{ trimPrefix "https:" (trimPrefix "http:" (required "sink.url required for mq-https" .Values.sink.url)) | trimPrefix "//" }}"
            parameters:
              httpMethod: POST
              bridgeEndpoint: true
              throwExceptionOnFailure: false
              sslContextParameters: "#echoClientSSL"
{{- else -}}
{{/* mq-drain: consume a terminal demo queue as fast as possible and discard.
     No rate limit — draining faster than the source route can ever fill the
     queue is the point, otherwise it fills to MAXDEPTH and the upstream
     producer route starts failing sends with MQRC_Q_FULL. */}}
- route:
    id: {{ .Values.name }}
    from:
      uri: "jms:queue:{{ required "source.queue required for mq-drain" .Values.source.queue }}"
      parameters:
        {{- include "camel-route.mqParams" . | nindent 8 }}
      steps:
        - to:
            uri: "log:{{ .Values.name }}?level=OFF"
{{- end -}}
{{- end -}}
