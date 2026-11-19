# Lesson 10 — Infrastructure as Code with CloudFormation

Time box: 2 hours.

## Outcome

Replace the manual creation of the Session 9 ECS resources with a declarative,
version-controlled CloudFormation stack:

```text
infra/session-10/template.yaml
        │
        v
CloudFormation stack
        ├── ECS cluster
        ├── CloudWatch log group
        └── versioned Fargate task definition

RunTask remains an explicit one-off job execution.
```

The SSM `SecureString`, existing ECR repository, and execution role remain
external inputs. CloudFormation receives their ARNs or URI, never the password.

## Refresh from Lesson 9

- ECS infrastructure uses the task execution role to retrieve an SSM parameter
  before container startup. The Java application still needs no task role.
- The task definition contains a `secrets` mapping and parameter ARN, not the
  decrypted database password.
- Permission policies grant actions; a boundary limits their maximum. Effective
  permissions are the intersection.
- A changed parameter value reaches containers only when a new task starts.
- Runtime environment variables can still leak through logs, diagnostics,
  child processes, or compromised code.
- The action-limited learning policy reduces blast radius compared with three
  broad AWS-managed policies.

## Core model

CloudFormation is declarative: the template describes desired state and the
service determines the API operations required to reach it.

```text
template       desired state in source control
stack          CloudFormation's managed instance of that template
change set     proposed difference between current and desired state
stack events   ordered record of resource operations and failures
outputs        stable values exported by the deployed stack
drift          live resource state no longer matches the template
```

Running the same template with the same parameters should produce no resource
change. Updating a property can mutate a resource in place or replace it,
depending on that property's CloudFormation update behavior.

## Cost boundary

The cluster, log group, and task definition are control-plane resources. The
one Fargate task and its public IPv4 address are the billable runtime resources.
The task must stop after the smoke test, and the stack must be deleted before
the session ends.

## Exercise A — migrate to a customer-managed learning policy

The updated `infra/session-09/learning-operator-policy.json` adds only the
CloudFormation stack and change-set actions needed for stacks named
`order-service-learning-*`.

The complete document exceeds the 2,048-character aggregate limit for inline
policies on one IAM user, but fits the 6,144-character limit for one
customer-managed policy. Resolve `<account-id>`, validate the JSON, and as root:

1. Create a customer-managed policy named `OrderServiceLearningOperator`.
2. Attach it to `learning-developer`.
3. Only after attachment succeeds, remove the older inline policy with the same
   name.

Do not restore the broad AWS-managed policies. The customer-managed policy is a
standalone IAM resource, can be attached to more than one identity, and supports
versioned updates.

## Exercise B — inspect and validate the template

Open `infra/session-10/template.yaml` and identify:

1. Inputs under `Parameters`.
2. Managed resources under `Resources`.
3. References that create implicit dependencies.
4. Values exposed through `Outputs`.
5. Resources deliberately kept outside the stack.

Validate through CloudFormation before deployment:

```powershell
aws cloudformation validate-template `
  --template-body file://infra/session-10/template.yaml `
  --region us-east-1 `
  --profile learning-developer
```

Validation checks template structure, not whether the deployment will have
enough permissions or whether every runtime dependency is reachable.

## Exercise C — create the external secret

Create a Standard `SecureString` at:

```text
/order-service/learning/session-10/db-password
```

Generate its value in memory and pass only its ARN into CloudFormation.

## Exercise D — deploy the stack

Resolve the existing ECR repository URI and execution-role ARN, then deploy:

```powershell
$stackName = "order-service-learning-session-10"

aws cloudformation deploy `
  --template-file infra/session-10/template.yaml `
  --stack-name $stackName `
  --parameter-overrides `
      ExecutionRoleArn=$executionRoleArn `
      RepositoryUri=$repositoryUri `
      DbPasswordParameterArn=$parameterArn `
      LogRetentionDays=1 `
  --no-fail-on-empty-changeset `
  --region $awsRegion `
  --profile $awsProfile
```

No `CAPABILITY_IAM` flag is needed because this stack does not create or modify
IAM resources.

Inspect stack status, events, resources, and outputs. The outputs become the
inputs to `RunTask`; do not rediscover names that the stack already knows.

## Exercise E — observe idempotency and a change set

Deploy the unchanged template and parameters again. CloudFormation should
report that there are no changes.

Then prepare an update with `LogRetentionDays=3` and
`--no-execute-changeset`. Inspect the proposed change before executing it. This
demonstrates the review boundary between defining desired state and applying it.

## Exercise F — run the integration job

Use stack outputs for the cluster and task-definition ARN. Discover only the
VPC subnet and security group, which this stack does not own. Run exactly one
Fargate task, wait for `STOPPED`, and verify:

```text
smoke-test exit code 0
POST /orders returned orderId and eventId
OrderCreatedListener logged the matching orderId
projection found in PostgreSQL
```

CloudFormation owns the task definition but does not own an individual
`RunTask` execution.

## Exercise G — stack-owned cleanup

1. Confirm no task is `RUNNING` or `PENDING`.
2. Delete the CloudFormation stack and wait for `stack-delete-complete`.
3. Confirm the cluster, log group, and active task-definition revision were
   removed or deregistered by the stack.
4. Delete the external SSM parameter separately because the stack did not own
   it.
5. Keep the action-limited operator and execution-role learning-path policies
   for subsequent sessions.

## Interview debrief

1. How is a template different from a stack?
   A template is the version-controlled desired-state definition. A stack is one deployed, managed instance of that template.
2. Why is `RunTask` outside this stack?
   RunTask is an imperative job execution, not one of the persistent resources in our stack. The stack created the reusable infrastructure; we explicitly decided when to execute the job.
3. What is a change set, and why review it before execution?
   A change set describes the difference between the current stack and proposed template plus parameters. Replacement: False meant CloudFormation would update the existing log group in place. There was a real change, but no resource replacement
4. What is an implicit dependency in CloudFormation?
    A resource that references another resource's name, ARN, or attribute creates an implicit dependency. CloudFormation ensures the referenced resource is created first.
5. Why is the secret external, and what does the template store instead?
   The secret is external to avoid storing sensitive values in source control. The template stores the ARN of the SSM parameter, which CloudFormation uses to retrieve the secret at runtime.
6. What is drift, and why can manual console changes be dangerous?
   Drift occurs when a live resource's state no longer matches the template. Manual console changes can introduce drift, leading to unexpected behavior during stack updates or rollbacks, as CloudFormation may not be aware of those changes and could overwrite them.
7. What happens during rollback after a failed stack update?
   During rollback, CloudFormation attempts to revert the stack to its previous state. If the rollback fails (e.g., due to missing permissions or resource dependencies), the stack enters an `UPDATE_ROLLBACK_FAILED` state, requiring manual intervention to resolve the issue before proceeding with further updates or rollbacks

## Completed outcome

- Migrated `OrderServiceLearningOperator` from a user inline policy to a
  customer-managed policy after reaching the 2,048-character aggregate inline
  policy limit. The managed policy supports a larger document and versioned
  updates.
- Validated and deployed `infra/session-10/template.yaml`, which created and
  owned the ECS cluster, CloudWatch log group, and Fargate task definition while
  accepting the existing ECR repository, execution role, and SSM parameter as
  external inputs.
- Confirmed idempotency: redeploying the same template and parameters produced
  no changes and did not register a new task-definition revision.
- Created and reviewed a change set that modified only log retention, with
  `Replacement: False`, before executing it.
- Diagnosed `UPDATE_ROLLBACK_FAILED`: CloudFormation's log-group handler needed
  `logs:ListTagsForResource` during both the update and rollback. Added the
  narrowly required Logs tag actions plus `ContinueUpdateRollback`, recovered
  to `UPDATE_ROLLBACK_COMPLETE`, and successfully executed a fresh change set.
- Used stack outputs to run the one-off integration task. The task returned an
  order/event ID, consumed the matching order from Kafka partition 0 at offset
  0, created the PostgreSQL projection, and completed with smoke-test exit code
  `0`.
- Deleted the stack, which removed its cluster and log group and deregistered
  its task definition. Deleted the external SSM parameter separately.

## References

- [How CloudFormation works](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cloudformation-overview.html)
- [CloudFormation ECS secret property](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-ecs-taskdefinition-secret.html)
- [AWS CLI CloudFormation deploy](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy.html)
- [View stack events](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/view-stack-events.html)
- [Monitor stack updates and rollback](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-monitor-stack.html)

