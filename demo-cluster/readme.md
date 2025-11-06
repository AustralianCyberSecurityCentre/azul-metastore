Docker compose file for creating a local Opensearch cluster.

This should only be used to run the opensearch integration tests for the project
and for local development.

This must not be used for production deployments due to use of plaintext keys and lack of replication.
See azul-app infra chart for an alternative running in kubernetes or deploy your own cluster.

# Startup

1. Start the cluster using `docker-compose up`.
   - On first time starting up, errors are expected in the output of this command.
     Wait two minutes and continue to next step.
1. Open a new terminal and run the initialisation script.
   - `docker compose exec opensearch azul/init.sh`
1. Either run integration tests or create the security roles and role mappings needed for azul via
   - `azul-metastore apply-opensearch-config --rolesmapping`

On further restarts of the cluster, it will not be necessary to rerun this script unless the
script or opensearch image versions are updated.

## init.sh

Initialises the opensearch security plugin with some default certificates and trivial passwords.

This is necessary so that opensearch-dashboards can connect to the opensearch cluster correctly.

Also create Azul Metastore roles and users via the opensearch security plugin restapi.

This is needed so that azul-metastore restapi can save and load data to the opensearch cluster.

# Direct vs Indirect read

## direct

This method is deprecated for production workloads but is required for integration testing.
Do not use this method in production.

Use roles on JWT to verify access to security labels in dls

This means that the OIDC provider has full control.
Issues with OIDC restrictions (invalid characters, etc) make it difficult to fully configure.

## indirect

Convert JWT roles to opensearch roles to verify access to security labels in dls

maximum compatibility with OIDC providers, removes config from OIDC and into opensearch
