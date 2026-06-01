"""
==============================================================================
DAY 10 — CASE STUDY: MERCHANT RISK INTELLIGENCE SYSTEM
==============================================================================

LangGraph + CrewAI Combined — Vibe Coding Challenge

Architecture:
  START
    |
  screen_node  (LangGraph — pure Python, no LLM)
    |                   Pulls txn stats from DuckDB.
    |                   Calculates risk_score (0-100).
    |
    +------+------+
    |             |
  risk < 50    risk >= 50
    |             |
  clear_node   investigate_node  ← CrewAI crew runs here
    |             |                 3 agents: Scout → Analyst → Reporter
    |             |                 + BONUS: Recommender (4th agent)
    +------+------+
           |
          END

Output:
  agent_outputs/risk_verdict.json — full state with verdict
==============================================================================
"""

import os, sys, json, duckdb
from datetime import datetime
from typing import TypedDict

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    from langgraph.graph import StateGraph, END
except ImportError:
    print("[ERROR] Run: pip install langgraph")
    sys.exit(1)

try:
    from crewai import Agent, Task, Crew, Process, LLM
except ImportError:
    print("[ERROR] Run: pip install crewai")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH    = os.path.join(os.path.dirname(__file__), "sigma_platform.duckdb")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "agent_outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# ── LLM setup (CrewAI → LiteLLM → Bedrock) ───────────────────────────────────
llm_lite = LLM(model="bedrock/amazon.nova-lite-v1:0", aws_region_name="us-east-1")
llm_pro  = LLM(model="bedrock/amazon.nova-pro-v1:0",  aws_region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Define the LangGraph State
# ═══════════════════════════════════════════════════════════════════════════════
class State(TypedDict):
    merchant_id: str      # input — the merchant to investigate
    risk_score: int       # set by screen_node (0-100)
    txn_summary: str      # brief stats string from screen_node
    verdict: str          # final output — CLEARED or investigation result


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Build screen_node (Pure Python, NO LLM)
# ═══════════════════════════════════════════════════════════════════════════════
def screen_node(state: State) -> dict:
    """
    Pulls transaction stats from DuckDB for the given merchant and computes
    a risk_score (0–100) using pure Python logic — no LLM needed.

    Queries both bronze_transactions and silver_transactions for a combined view.

    Scoring logic:
      - High txn count (> 3):      +40 points
      - Low average amount (< 200): +40 points
      - Any nulls in merchant_id:   +20 points
    """
    print(f"\n{'─'*60}")
    print(f"[SCREEN_NODE] Screening merchant: {state['merchant_id']}")
    print(f"{'─'*60}")

    merchant_id = state["merchant_id"]

    try:
        conn = duckdb.connect(DB_PATH, read_only=True)

        # Combined transaction count across bronze + silver (simulates "last 30 days")
        txn_count = conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT transaction_id FROM silver_transactions WHERE merchant_id = ?
                UNION ALL
                SELECT transaction_id FROM bronze_transactions WHERE merchant_id = ?
            )""",
            [merchant_id, merchant_id]
        ).fetchone()[0]

        # Average transaction amount (combined)
        avg_result = conn.execute(
            """SELECT AVG(amount) FROM (
                SELECT amount FROM silver_transactions WHERE merchant_id = ?
                UNION ALL
                SELECT amount FROM bronze_transactions WHERE merchant_id = ?
            )""",
            [merchant_id, merchant_id]
        ).fetchone()[0]
        avg_amount = avg_result if avg_result is not None else 0.0

        # Null rate on merchant_id column (across ALL silver rows — data quality signal)
        null_count = conn.execute(
            "SELECT COUNT(*) FROM silver_transactions WHERE merchant_id IS NULL"
        ).fetchone()[0]

        conn.close()

    except Exception as e:
        print(f"[SCREEN_NODE] DuckDB error: {e}")
        txn_count = 0
        avg_amount = 0.0
        null_count = 0

    # ── Risk scoring logic ────────────────────────────────────────────────────
    score = 0
    reasons = []

    if txn_count > 3:
        score += 40
        reasons.append(f"high volume ({txn_count} txns)")
    elif txn_count > 1:
        score += 20
        reasons.append(f"moderate volume ({txn_count} txns)")

    if avg_amount < 200:
        score += 40
        reasons.append(f"low avg amount (${avg_amount:.2f})")
    elif avg_amount < 500:
        score += 20
        reasons.append(f"moderate avg amount (${avg_amount:.2f})")

    if null_count > 0:
        score += 20
        reasons.append(f"{null_count} null merchant_ids in table")

    txn_summary = f"{txn_count} txns, avg ${avg_amount:.2f}, {null_count} nulls"

    print(f"  Txn count:  {txn_count}")
    print(f"  Avg amount: ${avg_amount:.2f}")
    print(f"  Null merchant_ids: {null_count}")
    print(f"  Risk score: {score}")
    print(f"  Reasons:    {', '.join(reasons) if reasons else 'none'}")

    return {"risk_score": score, "txn_summary": txn_summary}


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Build investigate_node with CrewAI (3 agents + BONUS 4th agent)
# ═══════════════════════════════════════════════════════════════════════════════
def investigate_node(state: State) -> dict:
    """
    Creates and runs a 3-agent (+ bonus Recommender) CrewAI crew.
    Each agent gets state['txn_summary'] in their task description.
    """
    print(f"\n{'─'*60}")
    print(f"[INVESTIGATE_NODE] Launching CrewAI investigation crew")
    print(f"  Merchant: {state['merchant_id']}")
    print(f"  Risk Score: {state['risk_score']}")
    print(f"  Summary: {state['txn_summary']}")
    print(f"{'─'*60}")

    merchant_id = state["merchant_id"]
    txn_summary = state["txn_summary"]

    # ── Agent 1: Scout — Data Retriever ───────────────────────────────────────
    scout = Agent(
        role="Data Retriever",
        goal="Pull raw transactional evidence for the flagged merchant and identify suspicious patterns.",
        backstory="""You are a sharp-eyed data retriever at Sigma DataTech's fraud
investigation unit. You have pulled evidence on 500+ merchant investigations.
You focus on raw numbers — transaction counts, amounts, time patterns, and
anomalies. You present data cleanly for the Analyst to interpret.""",
        llm=llm_lite,
        verbose=True,
        allow_delegation=False,
    )

    # ── Agent 2: Analyst — Pattern Detective ──────────────────────────────────
    analyst = Agent(
        role="Pattern Detective",
        goal="Analyse the Scout's data and flag suspicious signals — micro-transactions, velocity spikes, or unusual patterns.",
        backstory="""You are a fraud analytics expert who has investigated card testing
rings and money laundering patterns. You look beyond raw numbers — you connect
dots. If you see 100 transactions under $10 in a day, you know it's card testing.
If you see amounts clustering at round numbers, you suspect structuring. You never
dismiss anomalies as noise.""",
        llm=llm_lite,
        verbose=True,
        allow_delegation=False,
    )

    # ── Agent 3: Reporter — Risk Officer ──────────────────────────────────────
    reporter = Agent(
        role="Risk Officer",
        goal="Summarise the investigation findings as a structured risk verdict with a clear RISK LEVEL and actionable recommendation.",
        backstory="""You are the senior risk officer who signs off on merchant
investigations. Your verdicts go directly to the fraud team lead. You write
exactly 3 lines: the risk level, the key finding, and the recommended action.
No fluff, no hedging. You have been doing this for 10 years.""",
        llm=llm_pro,
        verbose=True,
        allow_delegation=False,
    )

    # ── BONUS Agent 4: Recommender — SQL Fix Advisor ──────────────────────────
    recommender = Agent(
        role="SQL Fix Advisor",
        goal="Suggest one specific, safe SQL fix query to clean or flag the merchant's data in the Silver layer.",
        backstory="""You are a DBA who writes surgical SQL fixes. You always write
DuckDB-compatible SQL with WHERE clauses. Your fixes are idempotent — safe to
run multiple times. You suggest exactly one fix, never a list of maybes.""",
        llm=llm_lite,
        verbose=True,
        allow_delegation=False,
    )

    # ── Tasks ─────────────────────────────────────────────────────────────────
    task_scout = Task(
        description=f"""You are investigating merchant '{merchant_id}'.
Pre-computed summary: {txn_summary}

Your job:
1. List the top transactions for this merchant (amounts, dates, statuses)
2. Identify any patterns: clustering of small amounts, time spikes, failed txns
3. Note anything unusual compared to a normal merchant profile

Present your findings as a structured evidence summary.""",
        expected_output="A structured evidence summary with transaction details and pattern observations for the flagged merchant.",
        agent=scout,
    )

    task_analyst = Task(
        description=f"""Based on the Scout's evidence for merchant '{merchant_id}':
Pre-computed summary: {txn_summary}

Analyse the data for fraud signals:
1. Micro-transaction pattern (many small txns = possible card testing)
2. Velocity anomaly (too many txns in short time)
3. Amount anomaly (avg amount much lower/higher than category average)
4. Failure rate anomaly (high failure rate = possible testing)
5. Any other suspicious signals

Flag each signal as: SUSPICIOUS / NORMAL / INCONCLUSIVE
Give a brief reason for each.""",
        expected_output="A signal-by-signal analysis with SUSPICIOUS/NORMAL/INCONCLUSIVE flags and a brief reason for each.",
        agent=analyst,
        context=[task_scout],
    )

    task_reporter = Task(
        description=f"""Based on the Analyst's findings for merchant '{merchant_id}':
Pre-computed summary: {txn_summary}
Risk score from screening: {state['risk_score']}

Write a concise risk verdict in exactly this format:
RISK LEVEL: HIGH / MEDIUM / LOW
KEY FINDING: <one sentence describing the main concern>
RECOMMENDATION: <one sentence — the specific action the fraud team should take>

Nothing else. No preamble, no extra text.""",
        expected_output="A 3-line risk verdict: RISK LEVEL, KEY FINDING, and RECOMMENDATION.",
        agent=reporter,
        context=[task_scout, task_analyst],
    )

    task_recommender = Task(
        description=f"""Based on the investigation of merchant '{merchant_id}':
Pre-computed summary: {txn_summary}

Suggest exactly ONE DuckDB-compatible SQL query that would fix or flag this
merchant's data in the silver_transactions table.

The table has columns: transaction_id, amount, status, merchant_id, customer_id,
transaction_date, payment_method, merchant_name, category, city, quality_flag.

Rules:
- Must have a WHERE clause
- Must be idempotent (safe to run again)
- Add a comment explaining what it does
- Format: just the SQL, nothing else

Example:
-- Flag merchant for review in quality_flag column
UPDATE silver_transactions SET quality_flag = 'REVIEW' WHERE merchant_id = 'MXXX' AND quality_flag != 'REVIEW';""",
        expected_output="A single DuckDB-compatible SQL UPDATE/INSERT statement with a comment, WHERE clause, and idempotent design.",
        agent=recommender,
        context=[task_analyst, task_reporter],
    )

    # ── Crew ──────────────────────────────────────────────────────────────────
    investigation_crew = Crew(
        agents=[scout, analyst, reporter, recommender],
        tasks=[task_scout, task_analyst, task_reporter, task_recommender],
        process=Process.sequential,
        verbose=True,
    )

    # ── Run the crew ──────────────────────────────────────────────────────────
    result = investigation_crew.kickoff()

    # Get the Reporter's verdict (task 3) and Recommender's SQL fix (task 4)
    verdict_parts = []

    # Reporter verdict
    if hasattr(task_reporter, 'output') and task_reporter.output:
        reporter_text = task_reporter.output.raw if hasattr(task_reporter.output, 'raw') else str(task_reporter.output)
    else:
        reporter_text = str(result)
    verdict_parts.append(reporter_text.strip())

    # Recommender SQL fix (BONUS)
    if hasattr(task_recommender, 'output') and task_recommender.output:
        recommender_text = task_recommender.output.raw if hasattr(task_recommender.output, 'raw') else str(task_recommender.output)
        verdict_parts.append(f"\nSQL FIX RECOMMENDATION:\n{recommender_text.strip()}")

    full_verdict = "\n".join(verdict_parts)
    print(f"\n[INVESTIGATE_NODE] Crew verdict:\n{full_verdict[:500]}")

    return {"verdict": full_verdict}


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Build clear_node
# ═══════════════════════════════════════════════════════════════════════════════
def clear_node(state: State) -> dict:
    """Simple node — merchant is below risk threshold, mark as CLEARED."""
    print(f"\n[CLEAR_NODE] Merchant {state['merchant_id']} CLEARED (risk_score={state['risk_score']})")
    return {"verdict": f"CLEARED: risk_score {state['risk_score']} below threshold"}


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5 — Wire the LangGraph graph
# ═══════════════════════════════════════════════════════════════════════════════
def route_by_risk(state: State) -> str:
    """Conditional edge: investigate if risk_score >= 50, else clear."""
    if state["risk_score"] >= 50:
        print(f"[ROUTER] risk_score={state['risk_score']} >= 50 → INVESTIGATE path")
        return "investigate"
    else:
        print(f"[ROUTER] risk_score={state['risk_score']} < 50 → CLEAR path")
        return "clear"


# Build the graph
g = StateGraph(State)
g.add_node("screen", screen_node)
g.add_node("investigate", investigate_node)
g.add_node("clear", clear_node)

g.set_entry_point("screen")
g.add_conditional_edges(
    "screen",
    route_by_risk,
    {"investigate": "investigate", "clear": "clear"}
)
g.add_edge("investigate", END)
g.add_edge("clear", END)

app = g.compile()


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Run and save output
# ═══════════════════════════════════════════════════════════════════════════════
def run_investigation(merchant_id: str) -> dict:
    """Run the full LangGraph pipeline for a single merchant."""
    print(f"\n{'='*70}")
    print(f"MERCHANT RISK INTELLIGENCE SYSTEM")
    print(f"Investigating: {merchant_id}")
    print(f"{'='*70}")

    result = app.invoke({
        "merchant_id": merchant_id,
        "risk_score": 0,
        "txn_summary": "",
        "verdict": "",
    })

    return result


def main():
    print("\n" + "="*70)
    print("DAY 10 — CASE STUDY: Merchant Risk Intelligence System")
    print("LangGraph (routing) + CrewAI (investigation)")
    print("="*70)

    all_results = []

    # ── Test 1: Low-risk merchant (should take CLEAR path) ────────────────────
    # M004: 2+3=5 txns, avg $1990 → high amount = low score (only volume points)
    print("\n\n" + "▓"*70)
    print("TEST 1: Low-risk merchant — expecting CLEAR path")
    print("▓"*70)

    result_low = run_investigation("M004")
    all_results.append(result_low)

    print(f"\n[RESULT] Merchant M004:")
    print(f"  risk_score:   {result_low['risk_score']}")
    print(f"  txn_summary:  {result_low['txn_summary']}")
    print(f"  verdict:      {result_low['verdict'][:200]}")

    # ── Test 2: High-risk merchant (should trigger CrewAI investigation) ──────
    # M003: 1+2=3 txns moderate volume (+20), avg ~$42 low amount (+40) = 60 → INVESTIGATE
    print("\n\n" + "▓"*70)
    print("TEST 2: High-risk merchant — expecting INVESTIGATE path")
    print("▓"*70)

    result_high = run_investigation("M003")
    all_results.append(result_high)

    print(f"\n[RESULT] Merchant M003:")
    print(f"  risk_score:   {result_high['risk_score']}")
    print(f"  txn_summary:  {result_high['txn_summary']}")
    print(f"  verdict:      {result_high['verdict'][:200]}")

    # ── Save all results ──────────────────────────────────────────────────────
    output_path = os.path.join(OUTPUT_DIR, "risk_verdict.json")
    output = {
        "timestamp": datetime.now().isoformat(),
        "system": "Merchant Risk Intelligence System",
        "framework": "LangGraph (routing) + CrewAI (investigation)",
        "results": []
    }

    for r in all_results:
        output["results"].append({
            "merchant_id": r["merchant_id"],
            "risk_score": r["risk_score"],
            "txn_summary": r["txn_summary"],
            "verdict": r["verdict"],
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n[SAVED] {output_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("CASE STUDY COMPLETE")
    print("="*70)
    print(f"  Results saved to: {output_path}")
    print(f"  Merchants screened: {len(all_results)}")

    cleared = sum(1 for r in all_results if r["verdict"].startswith("CLEARED"))
    investigated = len(all_results) - cleared

    print(f"  CLEARED:      {cleared}")
    print(f"  INVESTIGATED: {investigated}")
    print()
    print("Architecture used:")
    print("  LangGraph   → screen_node (pure Python risk scoring)")
    print("              → route_by_risk (conditional edge)")
    print("              → clear_node (fast path, no LLM)")
    print("  CrewAI      → investigate_node (3+1 agents: Scout → Analyst → Reporter → Recommender)")
    print()

    # ── Debrief questions ─────────────────────────────────────────────────────
    print("─"*60)
    print("DEBRIEF QUESTIONS — answer before leaving:")
    print("─"*60)
    print("1. Why does screen_node NOT use an LLM? What would break if it did?")
    print("2. The clear_node and investigate_node both end at END. Why is that fine?")
    print("3. What happens if you remove context= from the Analyst's task? Try it.")
    print("4. Could you replace the CrewAI crew with a single LangGraph node that")
    print("   calls Bedrock 3 times? What would you lose?")
    print()
    print("✅ Case study complete — show risk_verdict.json to the trainer.")


if __name__ == "__main__":
    main()
