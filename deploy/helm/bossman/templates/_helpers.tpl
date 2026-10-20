{{- define "bossman.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "bossman.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "bossman.labels" -}}
app.kubernetes.io/name: {{ include "bossman.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "bossman.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bossman.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: control-plane
{{- end -}}

{{- define "bossman.secretName" -}}
{{- default (printf "%s-auth" (include "bossman.fullname" .)) .Values.auth.existingSecret -}}
{{- end -}}

{{- define "bossman.image" -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end -}}

{{- define "bossman.url" -}}
{{- default (printf "http://%s:%v" (include "bossman.fullname" .) .Values.service.port) .Values.publicUrl -}}
{{- end -}}

{{/* $(POSTGRES_PASSWORD) is expanded by Kubernetes from the env var defined just before it. */}}
{{- define "bossman.databaseUrl" -}}
{{- if .Values.postgresql.enabled -}}
postgresql+asyncpg://{{ .Values.postgresql.username }}:$(POSTGRES_PASSWORD)@{{ include "bossman.fullname" . }}-postgres:5432/{{ .Values.postgresql.database }}
{{- else -}}
{{ required "externalDatabaseUrl is required when postgresql.enabled=false" .Values.externalDatabaseUrl }}
{{- end -}}
{{- end -}}

{{- define "bossman.redisUrl" -}}
{{- if .Values.redis.enabled -}}
redis://{{ include "bossman.fullname" . }}-redis:6379/0
{{- else -}}
{{ required "externalRedisUrl is required when redis.enabled=false" .Values.externalRedisUrl }}
{{- end -}}
{{- end -}}
