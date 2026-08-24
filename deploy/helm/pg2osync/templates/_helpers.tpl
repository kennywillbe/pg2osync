{{- define "pg2osync.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pg2osync.fullname" -}}
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

{{- define "pg2osync.labels" -}}
app.kubernetes.io/name: {{ include "pg2osync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "pg2osync.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pg2osync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "pg2osync.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pg2osync.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "pg2osync.secretName" -}}
{{- if .Values.existingSecret -}}
{{- .Values.existingSecret -}}
{{- else -}}
{{- include "pg2osync.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Render the values tree as TOML. Helm has no toToml, so tables are emitted
explicitly: [source], [target], [metrics], [engine] and one [sync.<key>] per
table. Anything else belongs in .Values.extraConfig.
*/}}
{{- define "pg2osync.config" -}}
{{- range $section := list "source" "target" "engine" "metrics" }}
{{- with index $.Values.config $section }}
[{{ $section }}]
{{- range $k, $v := . }}
{{ $k }} = {{ toJson $v }}
{{- end }}
{{ end }}
{{- end }}
{{- range $key, $table := .Values.config.sync }}
[sync.{{ $key }}]
{{- range $k, $v := $table }}
{{- if ne $k "transform" }}
{{ $k }} = {{ toJson $v }}
{{- end }}
{{- end }}
{{- with $table.transform }}

[sync.{{ $key }}.transform]
{{- range $col, $op := . }}
{{ $col }} = {{ toJson $op }}
{{- end }}
{{- end }}
{{ end }}
{{- with .Values.extraConfig }}
{{ . }}
{{- end }}
{{- end -}}
