# <h1black>Section 1: Access the</h1black> <h1blue>Cortex Code IDE</h1blue>

**Duration:** 10 minutes

## <h1sub>Objective</h1sub>

Get your URL to your VS Code instance, open the browser-based VS Code environment, and verify everything is ready to use.

---

## <h1sub>Task 1: Log In to Snowsight</h1sub>

### Step 1: Open your Snowflake account

Open a browser and navigate to [https://app.snowflake.com](https://app.snowflake.com).

Log in with the credentials provided at the event:

| Field | Value |
|-------|-------|
| **Account** | Provided on your event card |
| **Username** | Provided on your event card |
| **Password** | Provided on your event card |

### Step 2: Verify your role

Once logged in, confirm you are using **ACCOUNTADMIN**:

- Click your username in the bottom-left corner of Snowsight
- Select **Switch Role** → **ACCOUNTADMIN**

---


## <h1sub>Task 2: Get Your Service URL</h1sub>

The Cortex Code IDE service is already running. You just need to retrieve the URL.

### Step 1: Open a SQL worksheet

- In Snowsight, click **Projects** → **Workspaces**
- Click **+ Add new** > **SQL file** (top-left) to open a new SQL file. (You can specify any file name)

### Step 2: Run the GET_SERVICE_URL procedure

```sql
CALL CORTEX_CODE_IDE.CORE.GET_SERVICE_URL();
```

This returns a URL like:

```
https://abcdefg-<account>.snowflakecomputing.app
```

### Step 3: Copy the URL

Copy the full URL from the result. This is your personal IDE endpoint — bookmark it for the rest of the lab.

!!! tip "Bookmark it"
    Right-click the URL and open it in a new tab. Keep this tab open throughout the lab.

---

## <h1sub>Task 3: Open the IDE</h1sub>

### Step 1: Navigate to the URL

1. Paste the URL into a new browser tab and press Enter.
1. Enter your username and password (the same ones you used to log into the Snowflake account earlier)
1. You should see the VS Code web interface (code-server). If you see an error, go back to the SQL file from Step 2 and run the following commands:

  ```SQL
  USE DATABASE CORTEX_CODE_IDE;
  USE SCHEMA CORE;
  ALTER SERVICE CORTEX_CODE_IDE_SERVICE RESUME;
  ```

### Step 2: Open a terminal

- Press **Ctrl+\`** (or **Cmd+\`** on Mac) to open the integrated terminal
- Or go to **Terminal** → **New Terminal** in the menu bar

You should see a bash prompt like:

```
user@cortex-code-ide-XXXXX:~$
```

### Step 3: Verify Cortex Code CLI is installed

```bash
cortex --version
```

You should see a version string confirming the CLI is installed and ready. If you see `command not found`, let a lab facilitator know.

---

## <h1sub>Task 4: Understand the Environment</h1sub>

Take a moment to orient yourself in the IDE.

### What's pre-configured

| Item | Detail |
|------|--------|
| **CLI binary** | `cortex` — available on `$PATH` |
| **Snowflake connection** | Pre-configured with your account and PAT |
| **Default role** | `CORTEX_CODE_IDE_ROLE` (has ACCOUNTADMIN rights) |
| **Default warehouse** | Pre-configured by DataOps |
| **Working directory** | `/home/user` — write files here |

### Check your connection config

```bash
cat ~/.snowflake/connections.toml
```

You will see a pre-configured connection entry with your account and PAT-based authentication. This was set up automatically by the DataOps deployment pipeline.

---

## <h1sub>Validation Checklist</h1sub>

Before moving to Step 2, confirm:

- [ ] You are logged in to Snowsight as ACCOUNTADMIN
- [ ] `GET_SERVICE_URL()` returned a URL
- [ ] The VS Code IDE loaded in your browser
- [ ] A terminal is open inside the IDE
- [ ] `cortex --version` returned a version number

---

## <h1sub>Key Takeaways</h1sub>

1. **Zero installation** — The IDE, CLI, and auth are all pre-configured in the container
2. **Browser-based** — VS Code runs in Snowpark Container Services; you only need a browser to use it
3. **PAT authentication** — The container connects to Snowflake using a Programmatic Access Token automatically mounted at container startup

---

## <h1sub>Next Steps</h1sub>

Proceed to [Step 2: Clone Sample code and Launch Cortex Code](step2.md) where you'll clone the sample code repository and get familiar with the Cortex Code CLI.
