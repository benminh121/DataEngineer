## Start the Prefect Orion server locally

Create another window and activate your conda environment. Start the Orion API server locally with 

```bash
prefect orion start
```

## Register the block types that come with prefect-gcp

`prefect block register -m prefect_gcp`

# Create deployments

Create and apply your deployments.

```bash
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```
or with scheduled deployment
```bash
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL" --"0 0 * * *" -a
```
to apply that
## Run a deployment or create a schedule

Run a deployment ad hoc from the CLI or UI.

```bash
prefect deployment appy etl_parent_flow-deployment.yaml
```

## Start an agent

Make sure your agent set up to poll the work queue you created when you made your deployment (*default* if you didn't specify a work queue).

```bash
prefect agent start --work-queue "default"
```

## Later: create a Docker Image and use a DockerContainer infrastructure block

Bake your flow code into a Docker image, create a DockerContainer, and your flow code in a Docker container.

```bash
prefect profile ls
```

```bash
# use a local Orion API server
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# use Prefect Cloud
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/[ACCOUNT-ID]/workspaces/[WORKSPACE-ID]"

```