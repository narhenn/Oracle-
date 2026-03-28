import os
import logging
from datetime import datetime, date

from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.alert_agent import AlertAgent

load_dotenv()

logger = logging.getLogger(__name__)


class ReportComposerAgent:
    """Creates daily intelligence briefs with thesis revisions, contradictions, and mind changes."""

    def __init__(self, db: TiDBClient, alert_agent: AlertAgent) -> None:
        self._db = db
        self._alert_agent = alert_agent

    def compose_daily_report(self) -> dict:
        """Main daily report generation at 08:00 SGT."""
        logger.info("ReportComposerAgent: composing daily intelligence brief")

        developments = self._rank_developments()
        revisions = self._get_thesis_revisions()
        contradictions = self._get_unresolved_contradictions()
        sector_heat = self._get_sector_heatmap()
        source_perf = self._get_source_performance()
        mind_changes = self._get_mind_changes()

        html = self._build_html_report(developments, revisions, contradictions, sector_heat, source_perf, mind_changes)

        # Store report
        today = date.today().isoformat()
        self._db.insert_report("daily_brief", today, html, "telegram")

        # Send via Telegram
        summary = self._build_telegram_summary(developments, revisions, mind_changes)
        self._alert_agent.send_message(summary)

        logger.info("ReportComposerAgent: daily brief sent")
        return {"developments": len(developments), "revisions": len(revisions), "mind_changes": len(mind_changes)}

    def _rank_developments(self) -> list[dict]:
        theses = self._db.get_theses_since_days(days=1)
        return sorted(theses, key=lambda t: t.get("alert_score", 0), reverse=True)[:10]

    def _get_thesis_revisions(self) -> list[dict]:
        theses = self._db.get_theses_since_days(days=1)
        revisions = []
        for t in theses:
            state = t.get("thesis_state", "candidate")
            if state in ("conflicted", "stale", "discarded", "resolved"):
                revisions.append(t)
            elif t.get("contradiction_score", 0) > 30:
                revisions.append(t)
        return revisions

    def _get_unresolved_contradictions(self) -> list[dict]:
        all_c = self._db.get_contradictions(limit=20)
        return [c for c in all_c if c.get("action_taken") in ("none", "reinvestigate")]

    def _get_sector_heatmap(self) -> list[dict]:
        return self._db.get_entity_heat(limit=15)

    def _get_source_performance(self) -> dict:
        sources = self._db.get_active_sources()
        top = sorted(sources, key=lambda s: s.get("quality_score", 0), reverse=True)[:3]
        bottom = sorted(sources, key=lambda s: s.get("quality_score", 0))[:3]
        return {"top": top, "bottom": bottom}

    def _get_mind_changes(self) -> list[dict]:
        """Most important section — theses Oracle downgraded or reversed."""
        theses = self._db.get_theses_since_days(days=1)
        changes = []
        for t in theses:
            if t.get("thesis_state") in ("discarded", "conflicted"):
                changes.append({
                    "company": t["company"],
                    "thesis": t["thesis_text"],
                    "state": t["thesis_state"],
                    "confidence": t.get("confidence", 0),
                    "contradiction": t.get("contradiction_score", 0),
                })
        return changes

    def _build_telegram_summary(self, developments: list, revisions: list, mind_changes: list) -> str:
        lines = ["<b>Oracle Daily Intelligence Brief</b>\n"]

        if developments:
            lines.append(f"<b>Top Developments ({len(developments)}):</b>")
            for d in developments[:5]:
                lines.append(f"  {d['company']} — {d.get('confidence',0):.0f}% [{d.get('thesis_state','?')}]")

        if mind_changes:
            lines.append(f"\n<b>Mind Changes ({len(mind_changes)}):</b>")
            for m in mind_changes[:3]:
                lines.append(f"  {m['company']} — {m['state']} (was {m['confidence']:.0f}%)")

        if revisions:
            lines.append(f"\n<b>Under Review:</b> {len(revisions)} theses")

        lines.append(f"\n<i>{datetime.now().strftime('%Y-%m-%d %H:%M SGT')}</i>")
        return "\n".join(lines)

    def _build_html_report(self, developments, revisions, contradictions, sector_heat, source_perf, mind_changes) -> str:
        sections = []

        # Mind changes (most important)
        if mind_changes:
            items = "".join(f'<div style="padding:12px;margin:8px 0;background:#1a1a2e;border-left:3px solid #ef4444;border-radius:4px">'
                           f'<b>{m["company"]}</b> — {m["state"]} (confidence was {m["confidence"]:.0f}%)<br>'
                           f'<span style="color:#94a3b8;font-size:13px">{m["thesis"][:200]}</span></div>' for m in mind_changes)
            sections.append(f'<h2 style="color:#ef4444">What Oracle Changed Its Mind About</h2>{items}')

        # Top developments
        if developments:
            items = "".join(f'<div style="padding:12px;margin:8px 0;background:#1a1a2e;border-radius:4px">'
                           f'<b>{d["company"]}</b> — {d.get("confidence",0):.0f}% [{d.get("thesis_state","")}]<br>'
                           f'<span style="color:#94a3b8;font-size:13px">{d["thesis_text"][:200]}</span></div>' for d in developments[:5])
            sections.append(f'<h2 style="color:#3b82f6">Top Developments</h2>{items}')

        # Unresolved contradictions
        if contradictions:
            items = "".join(f'<div style="padding:12px;margin:8px 0;background:#1a1a2e;border-left:3px solid #f59e0b;border-radius:4px">'
                           f'<b>{c.get("company_a","?")} vs {c.get("company_b","?")}</b> — {c["severity"]}<br>'
                           f'{c["contradiction"]}</div>' for c in contradictions[:5])
            sections.append(f'<h2 style="color:#f59e0b">Unresolved Contradictions</h2>{items}')

        # Entity heat
        if sector_heat:
            items = "".join(f'<div style="display:inline-block;padding:8px 16px;margin:4px;background:#1a1a2e;border-radius:8px">'
                           f'{h.get("company","")} <b style="color:#ef4444">{h.get("heat_score",0):.0f}</b></div>' for h in sector_heat[:10])
            sections.append(f'<h2 style="color:#22c55e">Entity Heat Map</h2><div>{items}</div>')

        body = "".join(sections)
        return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Oracle Daily Brief</title></head>
        <body style="background:#0a0e17;color:#e0e0e0;font-family:sans-serif;padding:32px;max-width:800px;margin:0 auto">
        <h1 style="color:#3b82f6">Oracle Daily Intelligence Brief</h1>
        <p style="color:#64748b">{datetime.now().strftime('%A, %B %d %Y')}</p>
        {body}
        <hr style="border-color:#1e2a3a;margin:32px 0">
        <p style="color:#475569;font-size:12px">Generated by Oracle v2 Autonomous Intelligence System</p>
        </body></html>"""
