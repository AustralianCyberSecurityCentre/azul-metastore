SCRIPT_DIR="../../../azul-app-azure/azul-dev"
get_raw() {
  yq eval $1 $SCRIPT_DIR/values.yaml
}

# return json value from instance yaml
get_json() {
  yq eval $1 $SCRIPT_DIR/values.yaml -o json
}

echo os.environ[\"security_allow_releasability_priority_gte\"]=\"$(get_raw .security.allow_releasability_priority_gte)\"
echo os.environ[\"security_labels\"]=\'$(get_json .security.labels)\'
echo os.environ[\"security_presets\"]=\'$(get_json .security.presets)\'
echo os.environ[\"security_default\"]=\"$(get_raw .security.default)\"
echo os.environ[\"security_minimum_required_access\"]=\'$(get_json .security.minimumRequiredAccess)\'
echo os.environ[\"metastore_sources\"]=\'$(get_json .sources)\'


