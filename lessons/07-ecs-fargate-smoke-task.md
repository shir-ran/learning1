# Lesson 7 — ECS and Fargate smoke task

Time box: 90 minutes.

## Outcome

Run the `order-service:session-06` image from private ECR as a short-lived ECS
task on Fargate and read its output in CloudWatch Logs.

The task will override the image entry point and execute only:

```text
java -version
```

This deliberately tests the AWS container path without pretending that Kafka
and PostgreSQL already exist in AWS:

```text
ECR image
    -> ECS task definition
    -> Fargate task
    -> CloudWatch Logs
    -> task exits with code 0
```

We will not create an ECS service, load balancer, NAT Gateway, MSK cluster, RDS
instance, or persistent compute in this lesson.

## Refresh from Lesson 6

- `mvn package` compiles, tests, and packages the executable JAR. `docker build`
  packages that existing artifact with a Java runtime. `docker run` creates a
  process, while health and end-to-end checks prove runtime behavior.
- A container has its own network namespace. `localhost` refers to that
  container; Docker Compose service names such as `kafka` and `postgres` are
  resolved through the Compose network's DNS.
- ECR stores images. ECS schedules and supervises tasks made from those images;
  Fargate supplies managed compute for the tasks.
- Runtime configuration lets the same immutable image move between
  environments without rebuilding it or embedding credentials and addresses.
- Root has account-wide authority. `learning-developer` is a human identity with
  limited permissions. An ECS task execution role belongs to ECS infrastructure;
  a task role supplies temporary AWS permissions to application code.
- An immutable image tag cannot be silently reassigned to different bytes. This
  improves reproducibility and rollback, but new immutable tags still accumulate
  storage unless lifecycle rules expire old images.

## Cost boundary

This lab uses the smallest supported Linux Fargate size:

```text
0.25 vCPU
0.5 GiB memory
one task
task exits immediately
```

Fargate is billed per second with a one-minute minimum. A public IPv4 address is
also billed while attached. CloudWatch Logs may charge for ingestion and
storage. The expected usage is tiny, but the task must still be observed until
it reaches `STOPPED` and all lab resources must be reviewed afterward.

The public IP is used only to give the task outbound access to ECR and
CloudWatch from a public subnet. We add no inbound security-group rule and
expose no application port.

## Mental model: definitions versus running resources

```text
ECS cluster
    logical boundary for tasks and services

task definition
    versioned template: image, CPU, memory, roles, command, logging

task
    one running or stopped instantiation of a task definition

service
    controller that tries to keep a desired number of tasks running
```

We use `RunTask`, not a service. A service with desired count `1` would restart
our deliberately short-lived task and continue creating compute cost.

## Exercise A — add time-boxed operator permissions

`learning-developer` currently has ECR permissions only. As root, temporarily
attach these AWS-managed policies:

```text
AmazonECS_FullAccess
CloudWatchLogsFullAccess
```

`AmazonECS_FullAccess` is broad: besides ECS, it contains integration permissions
for VPC, load-balancing, CloudFormation, and other services. We use it to learn
the workflow, then remove it after the lab. A production identity should use a
customer-managed least-privilege policy.

Do not attach `AdministratorAccess`.

## Exercise B — create the task execution role

As root, create one role in IAM:

```text
Trusted entity: AWS service
Use case: Elastic Container Service Task
Role name: ecsTaskExecutionRole
Managed policy: AmazonECSTaskExecutionRolePolicy
```

This role lets ECS/Fargate pull the private image from ECR and send container
stdout/stderr to CloudWatch Logs. It does not grant permissions to the Java
process.

The smoke task has no task role because `java -version` calls no AWS API. Giving
it a task role would add unused authority.

## Exercise C — add the ECR storage guardrail

Create `infra/session-07/ecr-lifecycle-policy.json`:

```json
{
  "rules": [
    {
      "rulePriority": 1,
      "description": "Keep the two newest session images",
      "selection": {
        "tagStatus": "tagged",
        "tagPatternList": ["session-*"],
        "countType": "imageCountMoreThan",
        "countNumber": 2
      },
      "action": {
        "type": "expire"
      }
    }
  ]
}
```

Apply it:

```powershell
aws ecr put-lifecycle-policy `
  --repository-name order-service `
  --lifecycle-policy-text file://infra/session-07/ecr-lifecycle-policy.json `
  --region us-east-1 `
  --profile learning-developer
```

The rule keeps two matching tagged images. ECR evaluates lifecycle rules
asynchronously; applying the rule does not immediately delete the current image.

## Exercise D — create the ECS control-plane resources

Create a cluster and a short-retention log group:

```powershell
aws ecs create-cluster `
  --cluster-name order-service-learning `
  --region us-east-1 `
  --profile learning-developer

aws logs create-log-group `
  --log-group-name /ecs/order-service-session-07 `
  --region us-east-1 `
  --profile learning-developer

aws logs put-retention-policy `
  --log-group-name /ecs/order-service-session-07 `
  --retention-in-days 1 `
  --region us-east-1 `
  --profile learning-developer
```

The cluster and task definition are control-plane metadata. The Fargate task is
the resource that consumes CPU and memory.

## Exercise E — register a task definition

Resolve the account-specific values without copying credentials:

```powershell
$awsProfile = "learning-developer"
$awsRegion = "us-east-1"

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
```

Create `infra/session-07/task-definition.json` using the resolved ECR URI and
role ARN:

```json
{
  "family": "order-service-smoke",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "<execution-role-arn>",
  "containerDefinitions": [
    {
      "name": "order-service",
      "image": "<repository-uri>:session-06",
      "essential": true,
      "entryPoint": ["java"],
      "command": ["-version"],
      "user": "10001",
      "readonlyRootFilesystem": true,
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/order-service-session-07",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

Register it:

```powershell
aws ecs register-task-definition `
  --cli-input-json file://infra/session-07/task-definition.json `
  --region us-east-1 `
  --profile learning-developer
```

Overriding `ENTRYPOINT` is intentional. Starting Spring would require Kafka,
PostgreSQL, and secrets that do not exist in AWS yet. This smoke task proves
image retrieval, compute startup, non-root execution, and log delivery only.

## Exercise F — run and observe exactly one task

Use one public subnet in the default VPC. The default security group needs no
inbound rule because this task accepts no traffic.

Run one Fargate task with a public IP, record its task ARN, and wait until it
reaches `STOPPED`. Then inspect:

```text
lastStatus
stopCode
stoppedReason
container exitCode
```

Success means:

```text
lastStatus     = STOPPED
exitCode       = 0
CloudWatch log = OpenJDK 21 version output
```

The exact network commands will use the account's discovered VPC and subnet IDs;
never copy example IDs from documentation.

## Exercise G — teardown and permission rollback

After evidence is captured:

1. Confirm there are no `RUNNING` or `PENDING` tasks.
2. Deregister the task-definition revision.
3. Delete the ECS cluster.
4. Delete `/ecs/order-service-session-07` after reviewing the logs.
5. As root, detach `AmazonECS_FullAccess` and
   `CloudWatchLogsFullAccess` from `learning-developer`.
6. Keep ECR and `ecsTaskExecutionRole` for the next ECS exercise, unless the
   learning plan changes.
7. Check Billing and the zero-spend budget after AWS billing data refreshes.

## Interview debrief

Be ready to explain:

1. What is the difference between a task definition, task, and service?
2. Why did this lab use `RunTask` instead of an ECS service?
3. Why does Fargate require `awsvpc` networking?
4. Why did the task need an execution role but no task role?
5. Why did the task need outbound network access even though it exposed no port?
6. What did an exit code of `0` prove, and what did it not prove?
7. How do immutable ECR tags and lifecycle policies solve different problems?

## Completed outcome

- Registered revision `order-service-smoke:1` and launched exactly one Fargate
  task using the private `order-service:session-06` ECR image.
- The task ran as the configured non-root user, executed `java -version`, exited
  successfully with code `0`, and delivered the OpenJDK 21 output to CloudWatch
  Logs.
- Confirmed that `EssentialContainerExited` was expected: the task stops when
  its essential container's main process ends, including a successful exit.
- Deregistered the task definition and deleted the ECS cluster and lesson log
  group after capturing the evidence.
- Removed the temporary `AmazonECS_FullAccess` and `CloudWatchLogsFullAccess`
  policies from `learning-developer`. ECR and `ecsTaskExecutionRole` remain for
  later ECS exercises.

## References

- [ECS task execution IAM role](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html)
- [Fargate task networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html)
- [RunTask CLI reference](https://docs.aws.amazon.com/cli/latest/reference/ecs/run-task.html)
- [CloudWatch logging for ECS](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html)
- [AWS Fargate pricing](https://aws.amazon.com/fargate/pricing/)
- [Amazon VPC public IPv4 pricing](https://aws.amazon.com/vpc/pricing/)

