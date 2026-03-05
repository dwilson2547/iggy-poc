{{/*
Expand the name of the chart.
*/}}
{{- define "iggy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "iggy.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label.
*/}}
{{- define "iggy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "iggy.labels" -}}
helm.sh/chart: {{ include "iggy.chart" . }}
{{ include "iggy.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "iggy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "iggy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "iggy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "iggy.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the credentials Secret.
Returns the name of an existing secret when existingSecret is set,
otherwise returns the generated secret name for this release.
*/}}
{{- define "iggy.secretName" -}}
{{- if .Values.existingSecret }}
{{- .Values.existingSecret }}
{{- else }}
{{- printf "%s-credentials" (include "iggy.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Generate the comma-separated list of Raft cluster node addresses.
Each entry uses the StatefulSet pod DNS via the headless Service:
  <release>-<ordinal>.<release>-headless.<namespace>.svc.cluster.local:<port>
*/}}
{{- define "iggy.clusterNodes" -}}
{{- $nodes := list -}}
{{- $fullname := include "iggy.fullname" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $port := int .Values.cluster.internalPort -}}
{{- range $i := until (int .Values.replicaCount) -}}
{{- $nodes = append $nodes (printf "%s-%d.%s-headless.%s.svc.cluster.local:%d" $fullname $i $fullname $namespace $port) -}}
{{- end -}}
{{- join "," $nodes -}}
{{- end }}
