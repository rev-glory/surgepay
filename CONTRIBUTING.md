# Contributing to SurgePay

Thank you for your interest in contributing to SurgePay. This document defines the repository standards, development workflows, and coding conventions that ensure SurgePay remains a production-grade, highly reliable payment orchestration system.

---

## 1. Repository Philosophy

SurgePay is a portfolio-grade distributed systems project. Although it operates as a simulation (not integrating with a real payment gateway), we treat correctness of distributed state and financial transactions as non-negotiable.

When contributing, prioritize:
- **Correctness over delivery speed**: Take the time to trace edge cases, failure states, and concurrent transactions.
- **Clean service boundaries**: Services must strictly own their data; cross-service DB access is prohibited.
- **Idempotency everywhere**: All mutating actions must be safe to repeat.
- **Explicit error handling**: Avoid swallowing errors or failing silently.

Every contribution must:
- Compile successfully.
- Pass all static analysis and linting checks.
- Pass all unit, integration, and contract tests.
- Adhere strictly to the defined architecture.
- Preserve logical database and service boundaries.

---

## 2. Branch Strategy

We follow a clean, trunk-based branching workflow. We do **not** use a long-lived `develop` branch.

- **Main Branch (`main`)**: The source of truth. It must always be deployable and buildable.
- **Feature Branches (`feature/<feature-name>`)**: All development must occur on isolated branches branched off `main`.
- **Bugfix Branches (`bugfix/<issue-description>`)**: Dedicated branches for resolving bugs off `main`.

### Workflow
```
Create feature/bugfix branch from main
        ↓
Develop changes locally
        ↓
Commit using Conventional Commits
        ↓
Push to remote repository
        ↓
Open a Pull Request (PR) to main
        ↓
Code Review & CI Validation
        ↓
Merge PR into main
        ↓
Delete remote/local branch
```

---

## 3. Development Workflow

1. **Before Writing Code**:
   - Check if there is an open issue. If not, open one (Bug Report or Feature Request).
   - Ensure you understand the relevant sections.
2. **Local Environment Setup**:
   - Run `pnpm install` to install dependencies.
   - Run `pnpm build` to compile the workspace.
   - Run `pnpm dev` to run services in development mode.
3. **Linting and Formatting**:
   - We use Prettier and ESLint. Ensure your editor respects the `.editorconfig` rules.
   - Run `pnpm lint` and `pnpm format` before committing.

---

## 4. Commit Message Conventions

We enforce the **Conventional Commits** specification. Commit messages are linted automatically via `commitlint`.

### Format
```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types
- `feat`: A new feature (e.g., a new service, endpoint, or worker).
- `fix`: A bug fix.
- `docs`: Documentation updates.
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc.).
- `refactor`: A code change that neither fixes a bug nor adds a feature.
- `test`: Adding missing tests or correcting existing tests.
- `chore`: Changes to the build process, workspace configuration, or auxiliary tools.

### Examples
- `feat(payment): implement payment initiation handler`
- `fix(order): prevent duplicate saga execution on duplicate payment completion event`
- `refactor(common): simplify standard outbox logger`
- `docs(readme): update system context architecture diagram`
- `test(payment): add integration tests using Testcontainers`
- `chore(repo): update workspace TypeScript compile targets`

---

## 5. Coding Standards

- **Strict Type Safety**: Avoid using `any` or other escape hatches. Type definitions must be explicit.
- **Separation of Layers**: Within each service, enforce separation between:
  - **API/Controller Layer**: Thin, handles HTTP routing and parameter validation only.
  - **Business Logic / Domain Layer**: Pure business logic, handles state transitions.
  - **Persistence / Repository Layer**: Interacts with the database (no business logic here).
  - **Event Pub/Sub Layer**: Handles Kafka event consumption and publication (isolated from business/DB logic).
- **Transactional Outbox**: Any state change that publishes an event must record the event to the service's `Outbox` table in the *same* database transaction.
- **Inbox Pattern**: Any service consuming Kafka events must verify the event ID against an `Inbox` table as its first action to enforce event-level idempotency.

---

## 6. Pull Request Process

1. Open a PR using the repository's PR template.
2. Fill out all sections (Summary, Checklist, Testing Performed, etc.).
3. Ensure the CI suite passes.
4. Request reviews from the engineering team.
5. Address review feedback and obtain a minimum of one approval.
6. Merge using "Squash and Merge" to keep Git history clean.

---

## 7. Testing & Quality Expectations

- **Unit Tests**: Required for domain models, validation, and deterministic business logic.
- **Integration Tests**: Required for API controllers, database repositories, and Kafka consumers/producers.
- **Testcontainers**: Use Testcontainers for real database (PostgreSQL) and broker (Redpanda) tests. Mocking is prohibited for critical paths (Sagas, Outbox, Inbox).
- **No Swallowed Errors**: Catch blocks must either handle the error explicitly, schedule a retry, or bubble it up. Never use empty catch blocks.

---

## 8. Issue Reporting & Feature Requests

- **Bug Reports**: Use the Bug Report template. Provide clear reproduction steps, environment details, and expected versus actual behavior.
- **Feature Requests**: Use the Feature Request template. Align the request with document's core design principles and explain why the addition is required.
