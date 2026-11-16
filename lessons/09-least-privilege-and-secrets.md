# Lesson 9 — Least-privilege ECS access and encrypted configuration

Time box: 2 hours.

## Outcome

Repeat the Session 8 end-to-end Fargate integration test with two security
improvements:

1. Replace broad AWS-managed user policies with an action-limited policy for
   the `order-service` learning workflow.
2. Remove the database password from the task definition and inject an
   encrypted Systems Manager Parameter Store `SecureString` at task startup.

```text
learning-developer ── creates task ──> ECS
                                      │
ecsTaskExecutionRole ─ GetParameters ─┤
                                      v
                          encrypted SSM parameter
                                      │
                                      v
                  POSTGRES_PASSWORD / datasource password
```

The Java application does not call AWS APIs, so it still needs no task role.

## Refresh from Lesson 8

- Containers can use `localhost` when they belong to the same Fargate task and
  share its network namespace. Separate tasks do not share that loopback
  interface.
- `HEALTHY` dependencies gate startup but do not guarantee continued health or
  successful business behavior.
- Tomcat needed temporary writable storage. We kept the root filesystem
  read-only and mounted a writable ephemeral volume only at `/app/tmp`.
- The essential smoke-test container defined job completion. Exit `143` on the
  long-running containers meant ECS sent `SIGTERM` after the test passed.
- A database projection, matching HTTP/listener order IDs, and smoke-test exit
  `0` together proved the end-to-end event flow.

## Security model

Three identities are distinct:

```text
operator identity       learning-developer creates and observes AWS resources
task execution role     ECS pulls images, writes logs, and injects the secret
task role               application code calls AWS APIs; unused in this lab
```

The task definition stores only the parameter ARN. ECS retrieves the decrypted
value while starting each container and exposes it as an environment variable.
Changing the parameter does not update a running container; a new task must be
launched. Environment injection is an improvement over plaintext task-definition
values, but code and diagnostics inside the container can still access the
environment, so secrets must never be logged.

## Cost boundary

Use an explicitly Standard Parameter Store parameter. Standard parameters at
standard throughput have no additional charge. The one Fargate task still has
compute and public IPv4 charges while running and must stop after the smoke test.

## Exercise A — replace broad operator policies

Open `infra/session-09/learning-operator-policy.json`, replace `<account-id>` in
a temporary copy, and validate the JSON. As root:

1. Add it as an inline policy named `OrderServiceLearningOperator` on
   `learning-developer`.
2. Detach `AmazonECS_FullAccess`.
3. Detach `CloudWatchLogsFullAccess`.
4. Detach `AmazonEC2ContainerRegistryFullAccess`.
5. Keep the sign-in/local-development policy.

The replacement is action-limited and scopes ECR, Parameter Store, and
`iam:PassRole` to the learning resources. Some ECS, Logs, and discovery actions
still require or use `Resource: "*"`; least privilege is iterative rather than
an all-or-nothing label.

## Exercise B — create an encrypted standard parameter

```powershell
$awsProfile = "learning-developer"
$awsRegion = "us-east-1"
$accountId = aws sts get-caller-identity `
  --profile $awsProfile `
  --query Account `
  --output text

$parameterName = "/order-service/learning/session-09/db-password"
$dbPassword = [guid]::NewGuid().ToString("N") +
  [guid]::NewGuid().ToString("N")

aws ssm put-parameter `
  --name $parameterName `
  --description "Temporary Session 9 PostgreSQL password" `
  --type SecureString `
  --tier Standard `
  --value $dbPassword `
  --region $awsRegion `
  --profile $awsProfile

$dbPassword = $null
$parameterArn =
  "arn:aws:ssm:$($awsRegion):$($accountId):parameter$parameterName"
```

The command history contains the variable name, not the generated value. Do not
print the parameter with decryption.

Verify metadata only:

```powershell
aws ssm describe-parameters `
  --parameter-filters "Key=Name,Option=Equals,Values=$parameterName" `
  --region $awsRegion `
  --profile $awsProfile `
  --query "Parameters[0].{Name:Name,Type:Type,Tier:Tier}" `
  --output table
```

## Exercise C — authorize secret injection

As root, resolve `<account-id>` in
`infra/session-09/execution-role-ssm-policy.json` and add it as an inline policy
named `ReadOrderServiceLearningParameters` on `ecsTaskExecutionRole`.

The execution role needs `ssm:GetParameters` because ECS performs environment
injection before the application starts. `kms:Decrypt` is required for a custom
KMS key; this lesson uses the default Parameter Store key.

## Exercise D — inspect and resolve the secure task definition

Open `infra/session-09/task-definition.json` and verify:

- No password value appears under `environment`.
- `postgres`, `order-service`, and `smoke-test` each map the required variable
  through `secrets`.
- All three refer to `<db-password-parameter-arn>`.
- There is still no `taskRoleArn`.

Resolve `<repository-uri>`, `<execution-role-arn>`, and
`<db-password-parameter-arn>` into a BOM-free temporary file. Never commit the
resolved file.

## Exercise E — run the secure integration task

Create:

```text
cluster:   order-service-secure-integration
log group: /ecs/order-service-session-09, retention 1 day
```

Register the `order-service-secure-integration` task definition, discover the
default VPC networking as in Session 8, and run exactly one Fargate task with a
public IP. Wait for it to stop.

Success evidence remains:

```text
smoke-test exit code 0
POST /orders response contains orderId and eventId
OrderCreatedListener logs the same orderId
PASS: projection found in PostgreSQL
```

Do not print environment variables or decrypted parameter values while
debugging.

## Exercise F — cleanup

1. Confirm no task is `RUNNING` or `PENDING`.
2. Deregister the task-definition revision.
3. Delete the cluster and CloudWatch log group.
4. Delete the temporary resolved task-definition and policy files.
5. Delete the SSM parameter.
6. Remove `ReadOrderServiceLearningParameters` from the execution role.
7. Keep `OrderServiceLearningOperator` for subsequent course sessions; it
   replaces the three broad AWS-managed policies.

## Interview debrief

1. Why does ECS environment injection use the execution role rather than the
   task role?
2. What security problem is solved by replacing plaintext with a parameter ARN?
3. What risks remain when a secret is injected as an environment variable?
4. Why does secret rotation require a new ECS task or deployment?
5. How is an action-limited learning policy better than the previous managed
   policies, and where is it still broader than ideal?

## Completed outcome

- Replaced `AmazonECS_FullAccess`, `CloudWatchLogsFullAccess`, and
  `AmazonEC2ContainerRegistryFullAccess` on `learning-developer` with the
  action-limited `OrderServiceLearningOperator` inline policy.
- Created a Standard Parameter Store `SecureString` encrypted by
  `alias/aws/ssm`; the task definition stored only its ARN and mapped it into
  the three required container environment variables through `secrets`.
- Added path-scoped `ssm:GetParameters` permission to
  `ecsTaskExecutionRole`. No application task role was required.
- Diagnosed a real `ResourceInitializationError`: an accidentally attached
  `AmazonECSTaskExecutionRolePolicy` permissions boundary prevented the new
  SSM allow from becoming effective. Removed only the boundary while retaining
  the managed policy as a normal permission policy.
- The corrected task returned an order/event ID, consumed the matching order
  from Kafka partition 2 at offset 0, created the PostgreSQL projection, and
  completed with smoke-test exit code `0`.
- Deleted the task definition, cluster, log group, encrypted parameter, and
  temporary resolved files. Kept the learning operator policy and the
  execution role's access to `/order-service/learning/*` for subsequent course
  sessions; only disposable course secrets may use that path.

## References

- [Pass sensitive data to ECS containers](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/specifying-sensitive-data.html)
- [ECS task execution role permissions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html)
- [ECS task-definition secret parameters](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html)
- [Systems Manager pricing](https://aws.amazon.com/systems-manager/pricing/)

