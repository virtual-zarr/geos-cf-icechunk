## GEOS-CF Virtualizarr Data Pipeline

This pipeline creates and manages Virtualizarr/Icechunk stores for GEOS-CF (Goddard Earth Observing System Composition Forecasting) data on AWS. It is based on the [virtualizarr-data-pipelines](https://github.com/NASA-IMPACT/virtualizarr-data-pipelines) template and provides scalable infrastructure for processing and concatenating GEOS-CF archival files into virtual datasets.

### Data source :satellite:
GEOS-CF is NASA's global, near-real-time atmospheric composition forecasting system, produced by the [Global Modeling and Assimilation Office (GMAO)](https://gmao.gsfc.nasa.gov/) at NASA Goddard. It couples the GEOS Earth system model with the GEOS-Chem chemistry module to produce hourly, global fields of aerosols, trace gases (O3, NO2, CO, SO2, PM2.5, etc.), and supporting meteorology on a 0.25° × 0.25° grid (1440 × 721).

The pipeline indexes the **GEOS-CF v2 analysis (replay) product**, which is the assimilated, best-estimate reconstruction of the atmospheric state — distinct from the forecast product and from the earlier v1 stream. Files are published as hourly NetCDF4 (`.nc4`) granules through the GMAO THREDDS Data Server at [`https://ds.nccs.nasa.gov/thredds/`](https://ds.nccs.nasa.gov/thredds/) under the `GEOS-CF/analysis-v2/` collection, organized into `Y<year>/M<month>/D<day>/` directories with timestamps embedded in each filename (for example `GEOS.cf.ana.aqc_tavg_1hr_glo_L1440x721_slv.20250804_0930z.nc4`).

For background on the product see the [GEOS-CF project page](https://gmao.gsfc.nasa.gov/gmao-products/geos-cf/) and the [data access portal](https://fluid.nccs.nasa.gov/cf/).

### Documentation :books:
- [Pipeline design (brief)](./docs/pipeline-design.md)

#### Pipeline design overview
The pipeline separates discovery from ingestion so each stage stays small and testable:
- `query_thredds` handles THREDDS catalog traversal and URL matching.
- `search_latest` uses `query_thredds` to discover candidate files and emit downstream work.
- `process_file` performs the ingestion step, virtualizing source files into the Icechunk-backed dataset.
- `initialize` bootstraps repository state, and `garbage_collect` handles periodic repository cleanup.

### Configuration :wrench:
The pipeline uses a strongly-typed [settings module](./cdk/settings.py) to configure deployment parameters like bucket names and SNS topics. Settings can be overridden using a `.env` file (see [.env.sample](.env.sample) for an example).

The current configuration is driven by `StackSettings` in `cdk/settings.py`.

| Setting | Required | Default | Scope | Description |
|---|---|---|---|---|
| `STAGE` | Yes | None | Core | Deployment stage (`dev` or `prod`). |
| `ACCOUNT_ID` | Yes | None | Core | AWS account ID used by CDK stack environment. |
| `ICECHUNK_PREFIX` | Yes | None | Core | S3 prefix for the Icechunk repository (for example `geos-cf/analysis-v2`). |
| `ACCOUNT_REGION` | No | `us-east-1` | Core | AWS region for stack deployment. |
| `STACK_NAME` | No | `geos-cf-virtualizarr-data-pipelines` | Core | CDK stack/resource naming prefix. |
| `PROJECT_NAME` / `PROJECT` | No | `geos-cf-virtualizarr-data-pipelines` | Core | Project tags applied to resources. |
| `ICECHUNK_BUCKET` | No | None | Core | Existing bucket to use; if omitted, CDK creates `ICECHUNK_BUCKET_NAME`. |
| `ICECHUNK_BUCKET_NAME` | No | `icechunk-output` | Core | Name for the bucket CDK creates when `ICECHUNK_BUCKET` is not set. |
| `SNS_TOPIC` | No | None | Integration | Existing SNS topic ARN to subscribe to the processing queue. |
| `MAX_CONCURRENCY` | No | `25` | Runtime | SQS event source max concurrency for `process_file`. |
| `THREDDS_ANALYSIS_V2_CATALOG` | No | None | Search Latest | Catalog URL; must be set with `THREDDS_QUERY_PATTERN` to create `search_latest`. |
| `THREDDS_QUERY_PATTERN` | No | None | Search Latest | Glob pattern used by `search_latest` to discover files. |
| `SEARCH_LATEST_SCHEDULE_MINUTES` | No | None | Search Latest | If set, creates an EventBridge schedule for `search_latest`. |
| `GARBAGE_COLLECTION_FREQUENCY` | No | None | Garbage Collection | Frequency (days) for scheduled Icechunk garbage collection. |
| `VPC_ID` | Conditionally | None | Garbage Collection | Required when `GARBAGE_COLLECTION_FREQUENCY` is set. |
| `AMI_ID` | No | `resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id` | Garbage Collection | AMI for AWS Batch compute environment (supports `resolve:ssm:`). |
| `BATCH_MAX_VCPU` | No | `10` | Garbage Collection | Max vCPU for AWS Batch compute environment. |

Current processing behavior:
- `initialize` creates/scaffolds the repository using the current production bootstrap key and time range
- `process_file` appends by default (`append_dim="time"`) and supports overwrite mode with `{"overwrite": true}`.
- accepted message payloads:
	- single URL: `url`
	- multiple URLs: `urls`
- URLs can be full THREDDS HTTP URLs or relative file keys.

### Development :hammer:
#### Set up the development environment
```
./scripts/setup.sh
```

#### Run tests
```
uv run pytest
```

#### Review infrastructure before deploying
```
uv run --env-file .env cdk synth
```

#### Deploy the CDK infrastructure
```
uv run --env-file .env cdk deploy
```
