# Lesson 8 — Multi-container Fargate integration task

Time box: 2 hours.

## Outcome

Run one temporary Fargate task containing four containers:

```text
PostgreSQL ──HEALTHY──┐
                     ├──> order-service ──HEALTHY──> smoke-test
Kafka ──────HEALTHY──┘          │                         │
                                └── Kafka event ──────────┘
                                      │
                                      └── DB projection verified
```

The smoke-test container will call `POST /orders`, then query PostgreSQL until
the Kafka consumer's projection exists. It exits `0` only when the complete
HTTP -> producer -> Kafka -> consumer -> PostgreSQL path succeeds. Because the
smoke-test container is essential, its exit ends the one-off task.

## Refresh from Lesson 7

- A task definition is a versioned runtime template. A task is one instance of
  it. A service continuously restores its desired number of tasks, even when a
  short-lived task exits successfully.
- ECS infrastructure uses the task execution role to pull private ECR images
  and deliver logs. Application code uses a task role when it calls AWS APIs.
- The Session 7 task proved image pull, Fargate startup, Java execution, and log
  delivery. It did not start Spring or prove Kafka and database connectivity.
- Immutable tags prevent an existing tag from being reassigned to different
  bytes. Lifecycle policies expire old images to control storage growth.

## Why this architecture is deliberately temporary

All Linux containers in one Fargate task share its network namespace, so they
can communicate through `localhost`. They also share the task's lifecycle and
resource allocation.

This makes a compact integration environment, but it is not production design:

- Kafka and PostgreSQL data disappear with the task.
- There is no Kafka replication or database high availability.
- The application, broker, and database scale and fail as one unit.
- The database password is a demo value in the task definition rather than a
  secret from AWS Secrets Manager.

A production system would normally place the stateless application in an ECS
service and use independently managed, durable Kafka and database services.

## Cost and safety boundary

The task uses:

```text
1 vCPU
3 GiB memory
one public IPv4 address
one task only
```

The task should live only for a few minutes. If it remains active for more than
10 minutes, inspect its status and logs, then stop it rather than leaving it
unattended. No inbound security-group rule is required: the smoke client is
inside the same task.

## Exercise A — inspect the task as a failure boundary

Open `infra/session-08/task-definition.json` and identify:

1. The three nonessential workload containers.
2. The one essential smoke-test container.
3. The `HEALTHY` startup dependencies.
4. The `localhost` addresses used instead of Docker Compose service names.
5. The per-container CloudWatch log streams.

The smoke test defines the outcome of this short-lived job:
- If it verifies the projection, it exits 0, and ECS stops the whole task.
- If verification fails, it exits 1, and ECS still stops the task.
- Kafka, PostgreSQL, and Spring may then receive termination signals and return shutdown-related exit codes. 
- Those codes should not override the smoke test’s result.

`HEALTHY` dependencies gate startup. They do not provide permanent supervision
after dependent containers have started.

## Exercise B — temporarily restore operator permissions

As root, temporarily attach these policies to `learning-developer`:

```text
AmazonECS_FullAccess
CloudWatchLogsFullAccess
```

Do not attach `AdministratorAccess`. Remove both policies again during cleanup.
The existing `ecsTaskExecutionRole` keeps its
`AmazonECSTaskExecutionRolePolicy`.

## Exercise C — create the control-plane resources

```powershell
$awsProfile = "learning-developer"
$awsRegion = "us-east-1"
$clusterName = "order-service-integration"
$logGroup = "/ecs/order-service-session-08"

aws ecs create-cluster `
  --cluster-name $clusterName `
  --region $awsRegion `
  --profile $awsProfile

aws logs create-log-group `
  --log-group-name $logGroup `
  --region $awsRegion `
  --profile $awsProfile

aws logs put-retention-policy `
  --log-group-name $logGroup `
  --retention-in-days 1 `
  --region $awsRegion `
  --profile $awsProfile
```

## Exercise D — resolve and register the task definition

Resolve the account-specific values without committing an account ID:

```powershell
$repositoryUri = aws ecr describe-repositories `
  --repository-names order-service `
  --region $awsRegion `
  --profile $awsProfile `
  --query "repositories[0].repositoryUri" `
  --output text

$executionRoleArn = aws iam list-roles `
  --profile $awsProfile `
  --query "Roles[?RoleName=='ecsTaskExecutionRole'].Arn | [0]" `
  --output text

$resolvedTaskDefinition = Join-Path $env:TEMP "session-08-task-definition.json"
$template = Get-Content -Raw "infra/session-08/task-definition.json"
$resolvedJson = $template.Replace("<repository-uri>", $repositoryUri).
  Replace("<execution-role-arn>", $executionRoleArn)
$utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText(
  $resolvedTaskDefinition,
  $resolvedJson,
  $utf8WithoutBom
)

Get-Content -Raw $resolvedTaskDefinition | ConvertFrom-Json | Out-Null

aws ecs register-task-definition `
  --cli-input-json "file://$resolvedTaskDefinition" `
  --region $awsRegion `
  --profile $awsProfile
```

The account-specific resolved file lives in the temporary directory, not the
repository.

## Exercise E — run exactly one integration task

Discover one public subnet and the default security group. Use the actual IDs
from the account, not example IDs:

```powershell
$defaultVpcId = aws ec2 describe-vpcs `
  --filters Name=is-default,Values=true `
  --region $awsRegion `
  --profile $awsProfile `
  --query "Vpcs[0].VpcId" `
  --output text

$subnetId = aws ec2 describe-subnets `
  --filters Name=vpc-id,Values=$defaultVpcId `
  --region $awsRegion `
  --profile $awsProfile `
  --query "Subnets[0].SubnetId" `
  --output text

$securityGroupId = aws ec2 describe-security-groups `
  --filters Name=vpc-id,Values=$defaultVpcId Name=group-name,Values=default `
  --region $awsRegion `
  --profile $awsProfile `
  --query "SecurityGroups[0].GroupId" `
  --output text

$networkConfiguration =
  "awsvpcConfiguration={subnets=[$subnetId],securityGroups=[$securityGroupId],assignPublicIp=ENABLED}"

$runResult = aws ecs run-task `
  --cluster $clusterName `
  --task-definition order-service-integration `
  --launch-type FARGATE `
  --count 1 `
  --network-configuration $networkConfiguration `
  --region $awsRegion `
  --profile $awsProfile |
  ConvertFrom-Json

$runResult.failures
$taskArn = $runResult.tasks[0].taskArn
$taskArn
```

`failures` should be empty and `$taskArn` should contain exactly one ARN.

## Exercise F — observe, verify, and diagnose

Wait for the essential smoke test to finish:

```powershell
aws ecs wait tasks-stopped `
  --cluster $clusterName `
  --tasks $taskArn `
  --region $awsRegion `
  --profile $awsProfile
```

Inspect every container rather than relying only on the task-level stop code:

```powershell
aws ecs describe-tasks `
  --cluster $clusterName `
  --tasks $taskArn `
  --region $awsRegion `
  --profile $awsProfile `
  --query "tasks[0].{StopCode:stopCode,StoppedReason:stoppedReason,Containers:containers[].{Name:name,ExitCode:exitCode,Reason:reason}}"

aws logs tail $logGroup `
  --since 30m `
  --format short `
  --region $awsRegion `
  --profile $awsProfile
```

Success evidence is:

```text
smoke-test exit code = 0
POST /orders returned orderId and eventId
smoke-test logged PASS: projection found in PostgreSQL
order-service logged Consumed OrderCreated
```

The other containers can show termination-related exit codes because ECS stops
them after the essential smoke-test process finishes. Evaluate the exit code of
the container whose job defines success.

## Emergency stop

If the task remains active longer than 10 minutes:

```powershell
aws ecs stop-task `
  --cluster $clusterName `
  --task $taskArn `
  --reason "Session 8 time limit" `
  --region $awsRegion `
  --profile $awsProfile
```

## Exercise G — cleanup

1. Confirm there are no `RUNNING` or `PENDING` tasks.
2. Deregister the `order-service-integration` task-definition revision.
3. Delete the `order-service-integration` cluster.
4. Delete `/ecs/order-service-session-08` after capturing the evidence.
5. Delete the temporary resolved task-definition file.
6. As root, detach `AmazonECS_FullAccess` and
   `CloudWatchLogsFullAccess` from `learning-developer`.
7. Keep ECR and `ecsTaskExecutionRole` for later exercises.

## Interview debrief

1. Why can these containers use `localhost`, while separate Docker Compose
   containers use service DNS names?
2. What does a `HEALTHY` container dependency guarantee, and what does it not
   guarantee?
3. Why is the smoke-test container essential while Kafka, PostgreSQL, and the
   application are nonessential in this particular task?
4. Why is this useful as an integration task but unsuitable as production
   architecture?
5. Which container exit code defines success, and why might the other exit
   codes be misleading?

## References

- [Fargate task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html)
- [Fargate task-definition differences and valid sizes](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks-services.html)
- [ECS container dependencies](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerDependency.html)
- [ECS container health checks](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/healthcheck.html)

