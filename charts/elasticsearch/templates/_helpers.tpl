{{- define "elasticsearch-exporter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "elasticsearch-exporter.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "elasticsearch-exporter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Pod-level resource block for an ES nodeSet. Output goes directly under
`podTemplate.spec:` so it must start with PodSpec fields.
Args: ESJavaOpts, RequestsMemory, RequestsCpu, LimitsMemory, LimitsCpu, ClusterImage
*/}}
{{- define "node-resource-config" -}}
securityContext:
  fsGroup: 1000
  runAsGroup: 1000
  runAsUser: 1000
containers:
- name: elasticsearch
  env:
  - name: ES_JAVA_OPTS
    value: {{ .ESJavaOpts | quote }}
  resources:
    requests:
      memory: {{ .RequestsMemory }}
      cpu: {{ .RequestsCpu }}
    limits:
      memory: {{ .LimitsMemory }}
      cpu: {{ .LimitsCpu }}
  image: {{ .ClusterImage }}
{{- end -}}

{{/*
PVC block. Output is a sibling of `podTemplate:` inside a nodeSet item.
Args: StorageSize, StorageClassName
*/}}
{{- define "pvc-config" -}}
volumeClaimTemplates:
- metadata:
    name: elasticsearch-data
  spec:
    accessModes:
    - ReadWriteOnce
    resources:
      requests:
        storage: {{ .StorageSize }}
    storageClassName: {{ .StorageClassName }}
{{- end -}}

{{/*
Per-nodeSet config block. Output is sibling-fields of `- name:` inside a nodeSet item.
Args: Count, NodeZone, NodeRoles, ExtraConfig (string, optional — appended verbatim under config:)
*/}}
{{- define "config-per-node" -}}
count: {{ .Count }}
config:
  node.attr.zone: {{ .NodeZone }}
  cluster.routing.allocation.awareness.attributes: k8s_node_name,zone
  node.roles: {{ .NodeRoles | toJson }}
  node.store.allow_mmap: true
  transport.ping_schedule: 15s
  {{- with .ExtraConfig }}
  {{ . }}
  {{- end }}
{{- end -}}

{{/*
Resolves the namespace ingress gateway name (the platform-team-owned wildcard one).
Kept for compatibility with other charts; CCS / ES / Kibana use their own gateways.
*/}}
{{- define "chat.gateway" -}}
{{- if eq .Values.properties.stage "PROD" }}
{{- printf "chat-%s-gateway" .Values.properties.site | lower }}
{{- else if and (eq .Values.properties.stage "STAGING") (eq .Values.properties.site "site-k8s") }}
{{- printf "chat-site-k8s-pirun-stg-site-k8s-gateway" }}
{{- else }}
{{- printf "chat-%s-%s-gateway" .Values.properties.stage .Values.properties.site | lower | replace "staging" "stg" }}
{{- end }}
{{- end }}

{{/*
Cluster fullname: <cluster.name>-<properties.division>
Used as the `metadata.name` of the Elasticsearch resource and as the prefix
for every per-cluster Service / Secret / certificate name ECK creates.
*/}}
{{- define "chat.es.fullname" -}}
{{- printf "%s-%s" .Values.cluster.name .Values.properties.division -}}
{{- end -}}

{{/*
Kibana fullname: <kibana.name>-<properties.division>
*/}}
{{- define "chat.kibana.fullname" -}}
{{- printf "%s-%s" .Values.kibana.name .Values.properties.division -}}
{{- end -}}

{{/*
External hostnames. Auto-derived from properties.site + properties.publicDomain
when not explicitly set in values.hosts.
*/}}
{{- define "chat.hosts.esHttp" -}}
{{- if .Values.hosts.esHttp -}}
{{- .Values.hosts.esHttp -}}
{{- else -}}
{{- printf "es-%s.%s" .Values.properties.site .Values.properties.publicDomain -}}
{{- end -}}
{{- end -}}

{{- define "chat.hosts.kibana" -}}
{{- if .Values.hosts.kibana -}}
{{- .Values.hosts.kibana -}}
{{- else -}}
{{- printf "kibana-%s.%s" .Values.properties.site .Values.properties.publicDomain -}}
{{- end -}}
{{- end -}}

{{- define "chat.hosts.esRemote" -}}
{{- if .Values.hosts.esRemote -}}
{{- .Values.hosts.esRemote -}}
{{- else -}}
{{- printf "es-remote-%s.%s" .Values.properties.site .Values.properties.publicDomain -}}
{{- end -}}
{{- end -}}

{{/*
Vault paths. Auto-derived from properties.site when not explicitly set.
*/}}
{{- define "chat.vault.path.elasticUser" -}}
{{- if .Values.vault.paths.elasticUser -}}
{{- .Values.vault.paths.elasticUser -}}
{{- else -}}
{{- printf "elasticsearch/%s/elastic-user" .Values.properties.site -}}
{{- end -}}
{{- end -}}

{{- define "chat.vault.path.minio" -}}
{{- if .Values.vault.paths.minio -}}
{{- .Values.vault.paths.minio -}}
{{- else -}}
{{- printf "elasticsearch/%s/minio" .Values.properties.site -}}
{{- end -}}
{{- end -}}

{{/*
Vault path holding the SHARED transport CA cert+key. Same path across every
site so all clusters reference one CA and mutually trust.
*/}}
{{- define "chat.vault.path.transportCA" -}}
{{- if .Values.vault.paths.transportCA -}}
{{- .Values.vault.paths.transportCA -}}
{{- else -}}
{{- "elasticsearch/transport-ca" -}}
{{- end -}}
{{- end -}}
