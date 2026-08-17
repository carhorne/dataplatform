#!/usr/bin/env python3
"""
dbt MCP Server Demo Client
"""

import asyncio
import json
import sys
from datetime import datetime

from mcp.client.sse import sse_client
from mcp.client.session import ClientSession

MCP_SERVER_URL = "http://localhost:8000/sse"
OUTPUT_LOG = "demo_output.log"

log_lines = []

def log(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    log_lines.append(line)


def save_log():
    with open(OUTPUT_LOG, "w") as f:
        f.write(f"dbt MCP Demo Output - {datetime.now().isoformat()}\n")
        f.write("=" * 60 + "\n\n")
        for line in log_lines:
            f.write(line + "\n")
    log(f"Output saved to {OUTPUT_LOG}")


def parse_result(result) -> str:
    """Extract text content from an MCP tool result."""
    try:
        if result and result.content:
            return result.content[0].text
    except Exception:
        pass
    return str(result)


async def run_demo():
    log("=" * 60)
    log("dbt MCP Server Demo")
    log("=" * 60)
    log("")

    # ----- STEP 1: Connect -----
    log("Step 1: Connecting to dbt MCP server...")
    try:
        async with sse_client(MCP_SERVER_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                log("Connected successfully!")

                # ----- STEP 2: List available tools -----
                log("")
                log("Step 2: Listing available tools...")
                try:
                    tools = await session.list_tools()
                    for tool in tools.tools:
                        desc = (tool.description or "")[:100]
                        log(f"  - {tool.name}: {desc}")
                except Exception as e:
                    log(f"  ERROR listing tools: {e}")

                # ----- STEP 3: List all dbt models -----
                log("")
                log("Step 3: Discovering dbt models...")
                try:
                    result = await session.call_tool("list", {"resource_type": ["model"]})
                    text = parse_result(result)
                    log(f"  Models found:\n{text}")
                except Exception as e:
                    log(f"  ERROR listing models: {e}")

                # ----- STEP 4: Get details on a specific model -----
                log("")
                log("Step 4: Getting details for stg_web_analytics...")
                try:
                    result = await session.call_tool(
                        "get_node_details_dev",
                        {"node_id": "stg_web_analytics"}
                    )
                    text = parse_result(result)
                    log(f"  Model details:\n{text}")
                except Exception as e:
                    log(f"  ERROR getting model details: {e}")

                # ----- STEP 5: Compile SQL for a model -----
                log("")
                log("Step 5: Compiling SQL for int_web_analytics_with_customers...")
                try:
                    result = await session.call_tool(
                        "compile",
                        {"models": "int_web_analytics_with_customers"}
                    )
                    text = parse_result(result)
                    log(f"  Compiled SQL:\n{text}")
                except Exception as e:
                    log(f"  ERROR compiling model: {e}")

                # ----- STEP 6: Explore model lineage -----
                log("")
                log("Step 6: Exploring lineage for stg_ecom__sales_orders...")
                try:
                    result = await session.call_tool(
                        "get_lineage_dev",
                        {
                            "unique_id": "model.adventure.stg_ecom__sales_orders",
                            "depth": 2,
                        }
                    )
                    text = parse_result(result)
                    log(f"  Lineage:\n{text}")
                except Exception as e:
                    log(f"  ERROR getting lineage: {e}")

                # ----- WRAP UP -----
                log("")
                log("=" * 60)
                log("Demo complete!")
                log("=" * 60)

    except Exception as e:
        log(f"Connection error: {e}")
        log("Make sure the dbt MCP server is running.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        log("\nDemo interrupted by user.")
    except Exception as e:
        log(f"\nUnexpected error: {e}")
        sys.exit(1)
    finally:
        save_log()