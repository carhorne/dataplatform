# Agent Data Access Reflection

> Now that you've set up the dbt MCP server and seen an AI agent interact with your data models, take some time to think critically about what this means for data engineering. This reflection should be thoughtful (500-800 words), not a checklist.

---

## 1. What Worked Well

Honestly, the biggest surprise for me was how directly the documentation I wrote showed up in the agent’s understanding of the data. When the demo client called get_node_details_dev on stg_web_analytics, it returned exactly what I had written. That was kind of a wake-up call. It made it really clear that the agent is only as good as the documentation behind it. If I’m vague or lazy, the agent will be too. But if I’m clear and intentional, it can actually do something useful. The lineage tool was also pretty cool to see in action. Watching it trace something like stg_ecom__sales_orders back to raw sources and forward to downstream models made the whole pipeline feel more real. It’s one thing to have that mental model in your head, but seeing it mapped out programmatically is different.

---

## 2. What Was Difficult or Confusing

The setup definitely had more friction than I expected. The biggest issue was that the dbt-mcp package hardcodes 127.0.0.1 as the host, so even with port mapping, it’s not accessible outside the container. Without the start_mcp.py workaround to switch it to 0.0.0.0, everything would look like it’s working… but you just wouldn’t be able to connect. That’s the kind of thing that could easily eat hours. Then once it was running, the list tool threw a Pydantic error because it expected a list instead of a string for resource_type. Not a huge deal, but it highlights something important: agents aren’t great at handling small edge cases like that. A human would try a different format and move on, but an agent might just fail and stop.
---

## 3. Documentation Quality

This project completely changed how I think about documentation. Before, I treated model descriptions as kind of a “nice to have.” Good for onboarding, but not essential. Now it feels like the opposite. If an agent is consuming your data, the documentation basically is the interface. It’s the only way the agent understands what anything means. The biggest improvement I made was being really explicit about joins. Instead of just saying “customer ID,” I’d say something like “foreign key to stg_adventure_db__customers.customer_id, use this to get customer name and location.” That level of detail tells the agent exactly what to do next. And honestly, it also just makes things way clearer for humans too.

---

## 4. Production Considerations

Thinking about this in a real production environment raises a lot of concerns we didn’t really deal with here. Right now, the MCP server just connects to Snowflake using full credentials from a .env file. That means any agent using it has full access to everything, which would be a huge problem in a real company. You’d definitely need role-based access control so an agent answering marketing questions doesn’t accidentally pull financial or sensitive data. Logging would also be important — both for auditing and for cost tracking. An agent stuck in a loop hitting Snowflake could burn through credits fast without anyone noticing. Data freshness is another big one. An agent could give a confident answer using stale data if a pipeline didn’t run. That kind of silent failure would kill trust pretty quickly. Ideally, the agent would check freshness before answering anything time-sensitive.

---

## 5. Business Use Cases

The most obvious use case is a natural language interface for analytics. Someone on a sales team could ask something like “what were our top countries by order volume last month?” and the agent could figure it out without needing a data analyst to write a query. That’s a huge time saver. Another really interesting use case is automated data quality debugging. If a dbt test fails, instead of an engineer digging through lineage manually, an agent could trace it back, check sources, and give a rough root cause. That could save a lot of time during incidents.
---

## 6. The Bigger Picture

I think this shifts the role of data engineers more than people realize. Traditionally, we’ve been building pipelines for dashboards and analysts. But if agents become primary consumers of data, we’re now designing systems for machines to interpret. Documentation becomes way more important. Naming conventions and model structure need to make sense to something that doesn’t have intuition. And we need to understand how agents think well enough to predict how they might misinterpret things. It doesn’t necessarily make the job harder, but it definitely makes it broader. And it raises the bar for doing things well.