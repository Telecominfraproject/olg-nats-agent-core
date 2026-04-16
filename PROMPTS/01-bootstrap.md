Read PROMPTS/IMPLEMENTATION_PROMPT.md carefully.
Also read SPEC.md for design context.
Use PROMPTS/IMPLEMENTATION_PROMPT.md as the primary implementation source of truth for code generation scope and API shape.

Generate only the initial project bootstrap for a production-grade Go library module named nats-agent-core.

In this phase, create only:
1. go.mod
2. public package files under agentcore/

Implement only the public API surface and public types:
- Config and nested configs
- Client struct or interface skeleton
- BaseEnvelope
- ConfigureCommand
- DesiredConfigRecord
- ConfigureNotification
- ActionCommand
- ResultEnvelope
- StatusEnvelope
- StoredDesiredConfig
- SubmissionAck
- HealthSnapshot
- Logger interface
- Metrics interface
- typed Error with Code enum

Rules:
- Do not implement internal packages yet
- Do not implement business logic
- Do not generate tests yet
- Keep exported APIs documented
- Keep files small and focused
- Use clear JSON tags
- Use context.Context in all networked public method signatures

After generating files:
- summarize which files were created
- list any assumptions
- list which requirements are partially covered by this phase
