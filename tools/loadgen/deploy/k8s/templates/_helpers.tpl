{{- define "cassandra-soak.name" -}}
{{- default .Chart.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cassandra-soak.runSlug" -}}
{{- regexReplaceAll "[^a-z0-9-]+" (lower .Values.runId) "-" | trimAll "-" | trunc 30 | trimSuffix "-" -}}
{{- end -}}

{{- define "cassandra-soak.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "cassandra-soak.runSlug" .) | trunc 52 | trimSuffix "-" -}}
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
{{- if and .Values.recipientObserver.enabled (not .Values.ledger.enabled) -}}
{{- fail "ledger.enabled=true is required when recipientObserver.enabled=true" -}}
{{- end -}}
{{- if and (not .Values.encryptionPreflight.enabled) (or (eq .Values.soak.environment "staging") (eq .Values.soak.environment "production")) -}}
{{- fail "encryptionPreflight.enabled=false is not allowed when soak.environment is staging or production; such a run proves nothing about at-rest encryption" -}}
{{- end -}}
{{- if not .Values.ledger.epoch -}}
{{- fail "ledger.epoch is required; bump it whenever the loadgen image changes the ledger contract" -}}
{{- end -}}
{{- $mutationRate := addf .Values.soak.memberMutationRate .Values.soak.roomMutationRate .Values.soak.roomCreateRate .Values.soak.readReceiptRate -}}
{{- $reconcileCapacity := mulf .Values.soak.roomReadRate .Values.ledger.roomReconcileReadShare -}}
{{- if and (gt $mutationRate 0.0) (lt $reconcileCapacity $mutationRate) -}}
{{- fail "soak.roomReadRate * ledger.roomReconcileReadShare must be at least the sum of soak.memberMutationRate, soak.roomMutationRate, soak.roomCreateRate and soak.readReceiptRate" -}}
{{- end -}}
{{- if and (gt (float64 .Values.soak.presenceRate) 0.0) (lt (int .Values.soak.presenceConnections) 1) -}}
{{- fail "soak.presenceConnections must be at least 1 when soak.presenceRate is greater than zero" -}}
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
