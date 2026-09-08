# cicd/ — the "official way" applied to this project, at minimal scale

This is the smallest real slice of the AWS CI/CD reference pattern
(Source -> Build/Test -> Deploy, using CodePipeline + CodeBuild +
CloudFormation) applied to one concrete thing this project already does
by hand: pushing `fintech_cdc_pipeline.py` / `fintech_config.py` into the
MWAA `dags/` S3 prefix.

Modeled on `aws-samples/amazon-mwaa-automating-dag-deployment`, cut down
to the parts that fit a solo exam project — no ECR, no separate MWAA
local-runner Docker test image, no CodeCommit (source is this repo's
GitHub remote instead).

## The three stages

| Stage | What it does | AWS service |
|---|---|---|
| **Source** | Watches the GitHub repo/branch, pulls the latest commit | CodePipeline + CodeStar Connections |
| **Test** | Runs `python -m py_compile` on every DAG file, then actually loads them with Airflow's `DagBag` — catches syntax errors *and* import errors (a missing operator, a typo'd module) before anything touches AWS. Fails the pipeline if either check fails. | CodeBuild (`buildspec-test.yml`) |
| **Deploy** | Only runs if Test passed. `aws s3 sync`s the DAG files to the MWAA bucket's `dags/` prefix — the exact step this project ran manually with `aws s3 cp` | CodeBuild (`buildspec-deploy.yml`) |

This is the whitepaper's core principle in miniature: a bad DAG can never
reach MWAA, because Deploy structurally cannot run before Test passes.

## What's real here vs. what's a placeholder

`pipeline.yaml`, `buildspec-test.yml`, and `buildspec-deploy.yml` are
real, deployable CloudFormation/CodeBuild definitions — not pseudocode.
Nothing has been deployed to AWS yet; writing these files creates no
AWS resources and costs nothing.

**One thing to decide before deploying it:** this repo's `origin` remote
is `github.com/surendersara1/D-NBS_PySpark` — a shared class repo, not
something to point a personal CI/CD pipeline at without checking first.
Either point `GitHubOwner`/`GitHubRepo` at your own fork, or confirm
with the class before wiring a pipeline to the shared repo.

## To actually stand this up

1. Fork the repo (or confirm using the shared one is fine), so
   `GitHubOwner`/`GitHubRepo` point somewhere you control.
2. Deploy the stack:
   ```
   aws cloudformation deploy \
     --template-file fintech_cdc_ledger/cicd/pipeline.yaml \
     --stack-name ali-fintech-cicd \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides GitHubOwner=<your-github-username>
   ```
3. **One manual step CloudFormation can't do:** open the CodePipeline
   console -> Settings -> Connections -> find `ali-fintech-github-connection`
   -> click **Update pending connection** -> authorize GitHub. Until this
   is done the pipeline's Source stage can't pull anything.
4. Push a change to `fintech_cdc_ledger/airflow/dags/` on the configured
   branch -> watch it flow through Source -> Test -> Deploy in the
   CodePipeline console.

## Cost

Far cheaper than the MWAA environment itself: CodePipeline is $1/active
pipeline/month (after a free tier), CodeBuild bills per build-minute
(a few cents per run at this size), S3 artifact storage is negligible.
The MWAA environment this deploys *to* still costs the same ~$0.49/hr
it always did whenever it exists — this pipeline only automates what
goes into its `dags/` folder, it doesn't change MWAA's own pricing.

## Tearing it down

```
aws cloudformation delete-stack --stack-name ali-fintech-cicd
```
(The `ArtifactBucket` needs emptying first if it has objects in it —
CloudFormation won't delete a non-empty S3 bucket.)
