{{- define "cassandra-soak.name" -}}
{{- default .Chart.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cassandra-soak.runSlug" -}}
{{- regexReplaceAll "[^a-z0-9-]+" (lower .Values.runId) "-" | trimAll "-" | trunc 30 | trimSuffix "-" -}}
{{- end -}}

{{- define "cassandra-soak.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "cassandra-soak.runSlug" .) | trunc 52 | trimSuffix "-" -}}
{{- end -}}

{{/* Run-scoped immutable failure manifest name. */}}
{{- define "cassandra-soak.failureManifestName" -}}
{{- printf "%s-failure-%s" (include "cassandra-soak.fullname" .) (sha256sum .Values.runId | trunc 8) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cassandra-soak.labels" -}}
app.kubernetes.io/name: {{ include "cassandra-soak.name" . }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
loadgen.newchat/run: {{ include "cassandra-soak.runSlug" . | quote }}
{{- end -}}

{{- define "cassandra-soak.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cassandra-soak.name" . }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
loadgen.newchat/run: {{ include "cassandra-soak.runSlug" . | quote }}
{{- end -}}

{{- define "cassandra-soak.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else if and .Values.image.allowMutableTag .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- fail "image.digest is required unless image.allowMutableTag=true and image.tag is set" -}}
{{- end -}}
{{- end -}}

{{- define "cassandra-soak.validate" -}}
{{- if and (eq .Values.phase "teardown") (not .Values.teardown.approved) -}}
{{- fail "teardown.approved=true is required when phase=teardown" -}}
{{- end -}}
{{- if and (eq .Values.cassandra.cleanup "truncate") (ne .Values.cassandra.confirmKeyspace .Values.cassandra.keyspace) -}}
{{- fail "cassandra.confirmKeyspace must exactly match cassandra.keyspace when cleanup=truncate" -}}
{{- end -}}
{{- if ne .Values.soak.runMode "continuous" -}}
{{- fail "soak.runMode must be continuous for the Deployment" -}}
{{- end -}}
{{- if and .Values.failureEvidence.enabled (not .Values.ledger.enabled) -}}
{{- fail "ledger.enabled=true is required when failureEvidence.enabled=true" -}}
{{- end -}}
{{- if and .Values.failureEvidence.enabled (not .Values.failureEvidence.gitSha) -}}
{{- fail "failureEvidence.gitSha is required when failureEvidence.enabled=true" -}}
{{- end -}}
{{- if and .Values.failureEvidence.enabled (not .Values.failureEvidence.createdAt) -}}
{{- fail "failureEvidence.createdAt is required when failureEvidence.enabled=true" -}}
{{- end -}}
{{- $image := include "cassandra-soak.image" . -}}
{{- end -}}

{{- define "cassandra-soak.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "cassandra-soak.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop:
    - ALL
{{- end -}}

{{- define "cassandra-soak.commonEnv" -}}
- name: NATS_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: nats-url
- name: NATS_CREDS_FILE
  value: /var/run/secrets/loadgen/backend.creds
- name: MONGO_URI
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: mongo-uri
- name: MONGO_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: mongo-username
      optional: true
- name: MONGO_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: mongo-password
      optional: true
{{- end -}}

{{- define "cassandra-soak.credentialsVolume" -}}
- name: nats-creds
  secret:
    secretName: {{ .Values.existingSecret }}
    items:
      - key: backend.creds
        path: backend.creds
{{- end -}}

{{- define "cassandra-soak.credentialsMount" -}}
- name: nats-creds
  mountPath: /var/run/secrets/loadgen
  readOnly: true
{{- end -}}
