# <h1black>Step 2: Get Sample code and Launch Cortex Code</h1blue>

**Duration:** 20 minutes

## <h1sub>Objective</h1sub>

Clone the Apache Spark sample code, launch Cortex Code CLI, and get familiar with both.

---

## <h1sub>Task 1: Clone the sample code</h1sub>

### Step 1: Open the terminal

In your VS Code IDE, press **Ctrl+\`** (or **Cmd+\`** on Mac) to open the integrated terminal if it isn't already open.

### Step 2: Clone the sample code

```bash
git clone https://github.com/sfc-gh-jfreeberg/summit-spark-migration-workshop.git
```

### Step 3: Get oriented with the sample code

TODO

---

## <h1sub>Task 2: Get Familiar with Cortex Code</h1sub>

### Step 1: Launch Cortex Code CLI

```bash
cortex
```

You will see the Cortex Code welcome screen — an interactive terminal UI with a prompt at the bottom. This is your conversational interface to Snowflake.

!!! info "What you're looking at"
    Cortex Code CLI is an agentic AI assistant. You type natural language requests; it plans steps, writes SQL/Python/DDL, and executes against your Snowflake account. All output is shown inline.

### Step 2: Verify your connection

At the Cortex Code prompt, type:

```
What Snowflake account am I connected to? Show me my current role and warehouse.
```

Cortex Code will execute a SQL query and return your account details. You should see `CORTEX_CODE_IDE_ROLE` as your role and your account identifier.

### Key keyboard shortcuts

| Shortcut | Action |
|----------|--------|
| `Enter` | Send message |
| `Shift+Enter` | New line in message |
| `/` | Open command palette |
| `Ctrl+C` | Cancel current operation |
| `↑` / `↓` | Navigate message history |


### Step 3: Open the command palette

Type `/` at the prompt to see available commands:

```
/help
```

This lists all CLI commands including `/add-connection`, `/new-session`, `/skills`, and more.

### Step 4: List available skills

```
list skills
```

Cortex Code will show all bundled skills. You'll see skills like:
- `semantic-view` / `semantic-view-optimization`
- `cortex-agent`
- `cost-intelligence`
- `data-governance`
- `machine-learning`
- `developing-with-streamlit`

Skills are reusable AI workflows. You'll use one in Step 3.

### Step 5: Check what databases you have access to

```
List all databases I have access to in my Snowflake account.
```

Cortex Code will run `SHOW DATABASES` and summarise the results for you.

---

## <h1sub>Validation Checklist</h1sub>

Before moving to Step 3, confirm:

- [ ] You successfully cloned the sample code into VS Code
- [ ] `cortex` launched successfully in the terminal
- [ ] Connection was verified (correct account and role shown)

---

## <h1sub>Key Takeaways</h1sub>

1. **Natural language → SQL** — Describe what you want; Cortex Code writes and runs the query
2. **Conversational context** — Follow-up questions build on previous answers without repeating yourself
3. **Auditable** — You can always ask to see the SQL that was generated
4. **Skills extend capability** — Bundled skills give Cortex Code deep knowledge of specific Snowflake workflows
5. **Sessions are disposable** — Use `/new-session` to start fresh without losing your Snowflake objects

---

## <h1sub>Next Steps</h1sub>

Proceed to [Step 3: Hands-On with Cortex Code](step3.md) where you'll use a skill to build a Cortex Analyst semantic view from scratch.
