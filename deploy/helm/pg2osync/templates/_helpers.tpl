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
Render one config tree as TOML. Helm has no toToml, so tables are emitted
explicitly: [source], [target], [engine], [metrics], [api], [log] and one
[sync.<key>] per table. Anything else belongs in that tree's extraConfig.

Takes the tree itself, not the root context, so one file and one of many are
rendered by the same template.
*/}}
{{- define "pg2osync.configTree" -}}
{{- range $section := list "source" "target" "engine" "metrics" "api" "log" }}
{{- with index $ $section }}
[{{ $section }}]
{{- range $k, $v := . }}
{{ $k }} = {{ toJson $v }}
{{- end }}
{{ end }}
{{- end }}
{{- range $key, $table := (index $ "sync") }}
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
{{- with (index $ "extraConfig") }}
{{ . }}
{{- end }}
{{- end -}}

{{- define "pg2osync.config" -}}
{{- include "pg2osync.configTree" .Values.config }}
{{- with .Values.extraConfig }}
{{ . }}
{{- end }}
{{- end -}}

{{/*
The ConfigMap's data, and what the pod's checksum is taken over: one
pg2osync.toml, or one <name>.toml per entry in `configs`. Both the ConfigMap
and the Deployment include it, so a source added to the set is a config file
and a rollout, never one without the other.
*/}}
{{- define "pg2osync.configData" -}}
{{- if .Values.configs }}
{{- if .Values.config.sync }}
{{- fail "config and configs are both set: a process reads one file or one directory, not both. Move the tables under `config` into an entry of `configs`, or leave `config.sync` empty" }}
{{- end }}
{{- range $name, $tree := .Values.configs }}
{{ $name }}.toml: |
  {{- include "pg2osync.configTree" $tree | trim | nindent 2 }}
{{- end }}
{{- else }}
pg2osync.toml: |
  {{- include "pg2osync.config" . | trim | nindent 2 }}
{{- end }}
{{- end -}}

{{/*
What the container runs when `args` is not overridden: one file, or the whole
mounted directory as one process.
*/}}
{{- define "pg2osync.args" -}}
{{- if .Values.configs -}}
{{- toYaml (list "run" "--config-dir" "/etc/pg2osync") -}}
{{- else -}}
{{- toYaml (list "run" "-c" "/etc/pg2osync/pg2osync.toml") -}}
{{- end -}}
{{- end -}}
