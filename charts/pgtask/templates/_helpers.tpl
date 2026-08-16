{{- define "pgtask.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pgtask.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "pgtask.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "pgtask.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "pgtask.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "pgtask.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pgtask.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "pgtask.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pgtask.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "pgtask.databaseSecretName" -}}
{{- if .Values.developmentPostgres.enabled -}}
{{- printf "%s-postgres" (include "pgtask.fullname" .) -}}
{{- else -}}
{{- required "database.existingSecret.name is required" .Values.database.existingSecret.name -}}
{{- end -}}
{{- end -}}

{{- define "pgtask.listenerDatabaseSecretName" -}}
{{- if .Values.developmentPostgres.enabled -}}
{{- printf "%s-postgres" (include "pgtask.fullname" .) -}}
{{- else -}}
{{- default (include "pgtask.databaseSecretName" .) .Values.database.listenerExistingSecret.name -}}
{{- end -}}
{{- end -}}

{{- define "pgtask.image" -}}
{{- $root := index . 0 -}}
{{- $image := index . 1 -}}
{{- $repository := default $root.Values.image.repository $image.repository -}}
{{- $tag := default (default $root.Chart.AppVersion $root.Values.image.tag) $image.tag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
