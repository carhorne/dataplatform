# Agent Interaction Log: Prefect Flow
---

## 1. Setup

**Agent tool used:** Claude Code, and Claude web

**Why this tool?** I already have a premium subscription

**Date:** April 13th 2k026

**Total time spent:** 1 hour

---

## 2. Initial Specification

referencing prd.md in your context, build the flow in task 2 of milestone 2

**Did you share the PRD with the agent?** Yes. I used a file reference for Claude Code, and I downloaded it and added it to the files in my Claude web project.

---

## 3. Iteration Log

### Iteration 1: [Brief description]
- **What I asked:** referencing prd.md in your context, build the flow in task 2 of milestone 2
- **What the agent produced:** A modified web_analytics_flow.py; a modified pyproject.toml, and a modified Dockerfile
- **What worked:** it didn't run yet, because the python version was incorrect.
- **What didn't work:** The dependencies didn't work together in the pyproject.toml file.

### Iteration 2: [Brief description]
- **What I asked:** I pasted the errors that I was getting from the missing dependencies.
- **What the agent produced:** A new pyproject.toml file
- **What worked:** The dockerfile would start up now
- **What didn't work:** The flows wouldn't schedule or run. The snoflake tabel wasn't getting populated.
- **What I changed:** The pyproject.toml as well as the environment variable for scheduling.

### Iteration 3: [Brief description]
- **What I asked:** so the real issue that i found is that it scheduled all of the runs for too far in advance, where could i configure this? why were thye to run the next day?
- **What the agent produced:** A modified web_analytics_flow.py file
- **What worked:** The entire process.
- **What I changed:** The web_analytics_flow.py as well as the environment variable for scheduling.

---

## 4. Final Result

**Did the agent-generated code work on first run?** No

**If no, what broke?** Different things, the dependencies in the virtual envornment, then it was the flow file.

**Percentage of final code written by the agent vs. you:**
- Agent wrote: 92%
- I wrote/modified: 8%

**Key files the agent created or modified:**
- [web_analytics_flow.py]: [brief description of what the agent did]
The other files I just edited manually
---

## 5. What I Learned

### What the agent was good at:
- Generating the full Prefect flow structure quickly including tasks, flow decorator, retry logic, and error handling
- Writing the watermark strategy correctly on the first try, including the key detail of deriving the timestamp from the data rather than the system clock
### What the agent struggled with:
- Python and library version compatibility 
- Prefect's scheduling API
- get_run_logger() failing silently when no Prefect server is running locally, which caused the entire flow to appear to run but actually do nothing

### What I would do differently next time:
- Give more context about the codebase
- Ask the agent to include a standalone test script that exercises each task function individually with print statements before wiring everything into the full flow

### Time comparison estimate:
- **With agent:** 1 hour
- **Without agent (estimate):** 5 hours
- **Net impact:** Faster because the agent was better

---

## 6. Reflection

Using an AI agent for this feature was significantly faster than building it manually, especially for generating the overall Prefect flow structure and handling patterns like retries and watermarking. What surprised me most was how confidently the agent produced code that looked correct but failed due to subtle issues like dependency conflicts and scheduling misconfigurations. The biggest concern is that these hidden errors can take time to debug, especially when the agent lacks full awareness of the runtime environment. Moving forward, I would still rely on AI to accelerate initial development, but I would validate components incrementally and test pieces in isolation before integrating everything into a full system.
