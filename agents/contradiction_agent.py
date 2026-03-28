import os
import json
import logging
from typing import Optional

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

load_dotenv()

logger = logging.getLogger(__name__)

AGNES_API_URL = "https://api.agnesclaw.com/v1/chat/completions"

CONTRADICTION_PROMPT = """You are a critical analyst challenging investment theses about Singapore companies.

Review these theses and actively look for:
1. Direct contradictions between theses
2. Evidence that weakens a thesis
3. Mutually incompatible narratives
4. Outdated evidence that should be questioned
5. Cases where a thesis should be split into separate hypotheses

For each finding, specify:
- thesis_a_id: first thesis ID
- thesis_b_id: second thesis ID (or same as thesis_a_id if self-contradiction)
- contradiction: one sentence description
- severity: "hard" (direct logical conflict), "soft" (tension but not impossible), or "outdated" (evidence has aged)
- recommended_action: "downgrade" (reduce confidence), "reinvestigate" (need more data), "split" (thesis covers too much), "discard" (evidence is wrong)

Return ONLY a JSON array. Return [] if no issues found."""


class ContradictionAgent:
    """Actively challenges Oracle's theses — detects contradictions and triggers downgrades."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db
        self._api_key = os.getenv("AGNES_API_KEY", "")
        self._client = httpx.Client(timeout=120.0)

    def run(self) -> list[dict]:
        """Scan all active theses for contradictions and take action."""
        logger.info("ContradictionAgent: scanning for contradictions")

        theses = self._db.get_theses_since_days(days=7)
        if len(theses) <= 1:
            logger.debug("ContradictionAgent: not enough theses to check")
            return []

        theses_text = "\n".join(
            f"[Thesis #{t['id']}] {t['company']} (state: {t.get('thesis_state', 'candidate')}, "
            f"confidence: {t.get('confidence', 0):.0f}%): {t.get('thesis_text', '')}"
            for t in theses
        )

        findings = self._call_agnes(theses_text)
        if not findings:
            logger.info("ContradictionAgent: no contradictions found")
            return []

        valid_ids = {t["id"] for t in theses}
        thesis_map = {t["id"]: t for t in theses}
        actions_taken = []

        for f in findings:
            a_id = f.get("thesis_a_id")
            b_id = f.get("thesis_b_id")
            text = f.get("contradiction", "")
            severity = f.get("severity", "soft")
            action = f.get("recommended_action", "reinvestigate")

            if a_id not in valid_ids or b_id not in valid_ids:
                continue
            if not text:
                continue
            if severity not in ("hard", "soft", "outdated"):
                severity = "soft"
            if action not in ("downgrade", "reinvestigate", "split", "discard"):
                action = "reinvestigate"

            # Store contradiction
            try:
                cid = self._db.insert_contradiction(a_id, b_id, text, severity, action_taken=action)
            except Exception as e:
                logger.error("Failed to store contradiction: %s", e)
                continue

            # Take action
            self._apply_action(a_id, b_id, severity, action, text, thesis_map)

            actions_taken.append({
                "id": cid,
                "thesis_a_id": a_id,
                "thesis_b_id": b_id,
                "contradiction": text,
                "severity": severity,
                "action": action,
            })

        logger.info("ContradictionAgent: found %d issues, took %d actions", len(findings), len(actions_taken))
        return actions_taken

    def _apply_action(self, a_id: int, b_id: int, severity: str, action: str,
                      text: str, thesis_map: dict) -> None:
        """Apply the recommended action to affected theses."""

        if action == "downgrade":
            # Reduce confidence and update contradiction score
            for tid in (a_id, b_id):
                thesis = thesis_map.get(tid, {})
                current_conf = thesis.get("confidence", 50)
                current_contradiction = thesis.get("contradiction_score", 0)

                penalty = {"hard": 25, "soft": 10, "outdated": 15}.get(severity, 10)
                new_conf = max(current_conf - penalty, 10)
                new_contradiction = min(current_contradiction + penalty * 2, 100)

                self._db.update_thesis_scores(
                    tid,
                    confidence=new_conf,
                    contradiction_score=new_contradiction,
                )
                logger.info("ContradictionAgent: downgraded thesis #%d confidence %.0f → %.0f", tid, current_conf, new_conf)

            # Move to conflicted if hard contradiction
            if severity == "hard":
                for tid in (a_id, b_id):
                    current_state = thesis_map.get(tid, {}).get("thesis_state", "candidate")
                    if current_state not in ("conflicted", "discarded"):
                        self._db.update_thesis_state(tid, "conflicted", f"Hard contradiction: {text}", "ContradictionAgent")

        elif action == "discard":
            # Discard the weaker thesis
            a_conf = thesis_map.get(a_id, {}).get("confidence", 0)
            b_conf = thesis_map.get(b_id, {}).get("confidence", 0)
            weaker = a_id if a_conf <= b_conf else b_id
            self._db.update_thesis_state(weaker, "discarded", f"Discarded due to contradiction: {text}", "ContradictionAgent")
            logger.info("ContradictionAgent: discarded thesis #%d", weaker)

        elif action == "reinvestigate":
            # Bump contradiction score to flag for investigation
            for tid in (a_id, b_id):
                current_contradiction = thesis_map.get(tid, {}).get("contradiction_score", 0)
                self._db.update_thesis_scores(
                    tid,
                    contradiction_score=min(current_contradiction + 20, 100),
                )

        elif action == "split":
            logger.info("ContradictionAgent: thesis #%d/#%d flagged for split (requires manual review)", a_id, b_id)

    def _call_agnes(self, theses_text: str) -> list[dict]:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "agnes-claw-v1",
            "messages": [
                {"role": "system", "content": CONTRADICTION_PROMPT},
                {"role": "user", "content": f"Review these theses:\n\n{theses_text}"},
            ],
            "temperature": 0.3,
            "max_tokens": 4096,
        }

        try:
            response = self._client.post(AGNES_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"]

            json_str = raw
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0]
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0]

            parsed = json.loads(json_str.strip())
            return parsed if isinstance(parsed, list) else []
        except Exception as e:
            logger.error("ContradictionAgent Agnes call failed: %s", e)
            return []

    def close(self) -> None:
        self._client.close()
