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

{{- define "node-resource-config" -}}
securityContext:
  fsGroup: 1000
  runAsGroup: 1000
  runAsUser: 1000
containers:
- name: elasticsearch  
  env:
  - name: ES_JAVA_OPTS
    value: {{ .ESJavaOpts }} 
  resources:
    requests:
      memory: {{ .RequestsMemory }} 
      cpu: {{ .RequestsCpu }} 
    limits:
      memory: {{ .LimitsMemory }}
      cpu: {{ .LimitsCpu }}
  image: {{ .ClusterImage }}
{{- end -}}

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

{{- define "config-per-node" -}}
count: {{ .Count }} 
config:
  node.attr.zone: {{ .NodeZone }}
  cluster.routing.allocation.awareness.attributes: k8s_node_name,zone
  node.roles: {{ .NodeRoles | toJson }}      
  node.store.allow_mmap: true
  transport.ping_schedule: 15s   
{{- end -}}

{{- define "chat.gateway" -}}
{{- if eq .Values.properties.stage "PROD" }}
{{- printf "chat-%s-gateway" .Values.properties.site | lower }}
{{- else if and (eq .Values.properties.stage "STAGING") (eq .Values.properties.site "site-k8s")}}
{{- printf "chat-site-k8s-pirun-stg-site-k8s-gateway"}}
{{- else }}
{{- printf "chat-%s-%s-gateway" .Values.properties.stage .Values.properties.site | lower | replace "staging" "stg" }}
{{- end }}
{{- end}}
