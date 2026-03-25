import os
import sys
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
import json

import pandas as pd

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv


# =========================
# ENV + CONFIG
# =========================
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
REPORTS_DIR = BASE_DIR / "reports"

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

REPORT1_FNAME = "Health Algo Daily Statistic.csv"
REPORT2_FNAME = "Health Algo Commands Daily Statistic.csv"

REPORT1_NAME = "Health Algo Daily Statistic Report"
REPORT2_NAME = "Health Algo Commands Daily Statistic Report"

DEFAULT_PUSH_API_URL = "https://rcdemo-api.moovingon.ai/api/pushes/?format=json"
PUSH_API_URL = os.getenv("PUSH_API_URL", DEFAULT_PUSH_API_URL)

PUSH_USERNAME = os.getenv("PUSH_USERNAME")  # DO NOT hardcode
PUSH_PASSWORD_OR_TOKEN = os.getenv("PUSH_PASSWORD_OR_TOKEN")  # DO NOT hardcode

NOT_AVAILABLE_TOKEN = os.getenv("NOT_AVAILABLE_TOKEN", "not available yet")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


DEBUG = _env_bool("DEBUG", False)
SEND_ALERTS = _env_bool("SEND_ALERTS", False)  # safer default for repo


# =========================
# Push API
# =========================
def send_push_alert(
    payload: dict,
    api_url: str = PUSH_API_URL,
    username: str | None = None,
    password_or_token: str | None = None,
    timeout: int = 20,
) -> tuple[bool, str]:
    headers = {"Content-Type": "application/json"}

    auth = None
    if username and password_or_token:
        auth = HTTPBasicAuth(username, password_or_token)

    try:
        resp = requests.post(api_url, json=payload, headers=headers, auth=auth, timeout=timeout)
        ok = 200 <= resp.status_code < 300
        return ok, f"{resp.status_code} {resp.text}"
    except requests.RequestException as e:
        return False, f"RequestException: {e}"


def alert_to_push_payload(alert_obj: dict) -> dict:
    return {
        "title": alert_obj.get("Message"),
        "severity": alert_obj.get("Severity"),
        "reason": alert_obj.get("Reason"),
        "context": alert_obj.get("Context"),
        "tags": alert_obj.get("Tags"),
        "action": alert_obj.get("Action"),
    }


def maybe_send_alert(alert_obj: dict) -> None:
    """
    Safe wrapper:
    - sends only if SEND_ALERTS=True
    - if creds missing, it prints a warning (debug) and skips
    - never crashes the run
    """
    if not SEND_ALERTS:
        return

    if not PUSH_USERNAME or not PUSH_PASSWORD_OR_TOKEN:
        if DEBUG:
            print(json.dumps(
                {"push_status": "skipped", "reason": "Missing PUSH_USERNAME or PUSH_PASSWORD_OR_TOKEN"},
                indent=2
            ))
        return

    payload = alert_to_push_payload(alert_obj)
    ok, details = send_push_alert(payload, username=PUSH_USERNAME, password_or_token=PUSH_PASSWORD_OR_TOKEN)

    if DEBUG:
        print(json.dumps(
            {"push_status": "sent" if ok else "failed", "details": details},
            indent=2
        ))


# =========================
# Logging (console + file)
# =========================
class TeeStdout:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self._file = open(log_path, "w", encoding="utf-8")
        self._console = sys.__stdout__

    def write(self, msg: str):
        self._console.write(msg)
        self._file.write(msg)

    def flush(self):
        self._console.flush()
        self._file.flush()

    def close(self):
        try:
            self._file.close()
        except Exception:
            pass


# =========================
# PRE-RUN: clean reports dir
# =========================
def clean_reports_folder(reports_dir: Path) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    for item in reports_dir.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        except Exception as e:
            print(f"WARNING: Could not delete {item}: {e}")


def run_download_script(script_path: str) -> None:
    """
    Runs download_reports.py WITHOUT writing its logs into the alerts log file.
    """
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        sys.__stdout__.write("download_reports.py failed.\n")
        if result.stdout:
            sys.__stdout__.write("STDOUT:\n" + result.stdout + "\n")
        if result.stderr:
            sys.__stderr__.write("STDERR:\n" + result.stderr + "\n")
        raise SystemExit(1)


def _find_latest_matching_file(reports_dir: Path, expected_filename: str) -> Path | None:
    expected_stem = Path(expected_filename).stem
    candidates = [p for p in reports_dir.glob("*.csv") if p.stem.strip().startswith(expected_stem)]
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0]


def ensure_reports_exist(reports_dir: Path) -> tuple[Path, Path]:
    r1 = _find_latest_matching_file(reports_dir, REPORT1_FNAME)
    r2 = _find_latest_matching_file(reports_dir, REPORT2_FNAME)

    missing = []
    if r1 is None:
        missing.append(str(reports_dir / REPORT1_FNAME))
    if r2 is None:
        missing.append(str(reports_dir / REPORT2_FNAME))

    if missing:
        raise FileNotFoundError(f"Missing required report files in '{reports_dir}': {missing}")

    return r1, r2


# =========================
# "not available yet" skipping helpers
# =========================
def _is_not_available_value(x) -> bool:
    if pd.isna(x):
        return False
    if isinstance(x, str):
        s = x.strip().lower()
        return NOT_AVAILABLE_TOKEN in s
    return False


def _row_has_not_available(row: pd.Series) -> bool:
    return any(_is_not_available_value(v) for v in row.values)


def _drop_not_available_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.apply(_row_has_not_available, axis=1)
    return df.loc[~mask].copy()


# =========================
# REPORT 1 logic
# =========================
def report1_discrepancy_severity(sent: float, other: float) -> str:
    diff = abs(sent - other)
    if sent <= 0:
        return "OK" if diff == 0 else "P1"

    ratio = diff / sent
    if ratio > 0.50 and diff > 5:
        return "P1"
    if 0.10 <= ratio <= 0.50:
        return "P2"
    if diff > 0 and ratio < 0.10:
        return "P3"
    return "OK"


def _alert_route(sev):
    if sev == "P1":
        return "Send to: qag-monitoring-prod | Escalation: QA immediately (then Developer if persistent)"
    if sev == "P2":
        return "Send to: qag-monitoring-prod | Escalation: QA if repeated/persistent"
    return "Send to: qag-monitoring-prod | Escalation: log/monitor"


def print_report1_alert_json(sev: str, company: str, message: str, reason: str, context: dict | None = None):
    if context is None:
        context = {}

    alert_obj = {
        "Severity": sev,
        "Message": f"Health Algo Daily Statistics | {company} | {message}",
        "Reason": reason,
        "Context": context,
        "Action": _alert_route(sev),
        "Tags": []
    }
    print(json.dumps(alert_obj, indent=2))
    print("")

    maybe_send_alert(alert_obj)


def run_noc_checks(report_1_path, report_2_path=None):
    try:
        df1 = pd.read_csv(report_1_path)
    except FileNotFoundError:
        print(f"Error: {report_1_path} not found.")
        return

    df1 = _drop_not_available_rows(df1)

    required_cols = [
        "CompanyName", "status", "AnimalsInPullList", "TotalCommandsSent",
        "CurrentlyActiveTags", "TagsActivatedOnTime"
    ]
    missing_cols = [c for c in required_cols if c not in df1.columns]
    if missing_cols:
        print_report1_alert_json(
            sev="P1",
            company="Health Algo Daily Statistics",
            message="Structure error",
            reason="Missing required columns",
            context={"missing_columns": missing_cols}
        )
        return

    df1["CompanyName"] = df1["CompanyName"].astype(str).str.strip()
    df1["status"] = df1["status"].astype(str).str.strip().str.upper()

    for col in ["AnimalsInPullList", "TotalCommandsSent", "CurrentlyActiveTags"]:
        df1[col] = pd.to_numeric(df1[col], errors="coerce")

    df1["TagsActivatedOnTime_raw"] = df1["TagsActivatedOnTime"].astype(str).str.strip()
    df1["TagsActivatedOnTime_num"] = pd.to_numeric(df1["TagsActivatedOnTime_raw"], errors="coerce")

    for _, row in df1.iterrows():
        company = row["CompanyName"]
        status = row["status"]
        sent = row["TotalCommandsSent"]
        active = row["CurrentlyActiveTags"]
        animals = row["AnimalsInPullList"]
        tags_on_time_num = row["TagsActivatedOnTime_num"]

        issue_found = False
        reasons = []

        critical_fields = {
            "status": status,
            "AnimalsInPullList": animals,
            "TotalCommandsSent": sent,
            "CurrentlyActiveTags": active,
        }
        missing_critical = [
            k for k, v in critical_fields.items()
            if pd.isna(v) or (isinstance(v, str) and not v)
        ]
        if missing_critical:
            print_report1_alert_json(
                sev="P2",
                company=company,
                message="Missing/invalid values",
                reason="Health Algo Daily Statistics report has missing/invalid fields",
                context={"missing_fields": missing_critical}
            )
            issue_found = True
            reasons.append(f"missing={missing_critical}")

        if status != "COMPLETED":
            print_report1_alert_json(
                sev="P1",
                company=company,
                message="Calculation failed/incomplete",
                reason=f"status={status}",
                context={"status": status}
            )
            issue_found = True
            reasons.append("status!=COMPLETED")

        if pd.notnull(sent) and pd.notnull(animals) and sent != animals:
            print_report1_alert_json(
                sev="P1",
                company=company,
                message="Command mismatch",
                reason="TotalCommandsSent != AnimalsInPullList",
                context={"TotalCommandsSent": sent, "AnimalsInPullList": animals}
            )
            issue_found = True
            reasons.append("sent!=animals")

        if pd.notnull(sent) and pd.notnull(active):
            sev = report1_discrepancy_severity(sent, active)
            if sev != "OK":
                diff = abs(sent - active)
                pct = (diff / sent * 100) if sent > 0 else 100.0
                print_report1_alert_json(
                    sev=sev,
                    company=company,
                    message="Active tags mismatch",
                    reason="TotalCommandsSent != CurrentlyActiveTags",
                    context={
                        "TotalCommandsSent": sent,
                        "CurrentlyActiveTags": active,
                        "diff": diff,
                        "diff_pct": round(pct, 1)
                    }
                )
                issue_found = True
                reasons.append(f"active_mismatch={sev}")

        if pd.notnull(tags_on_time_num) and pd.notnull(sent):
            sev = report1_discrepancy_severity(sent, tags_on_time_num)
            if sev != "OK":
                diff = abs(sent - tags_on_time_num)
                pct = (diff / sent * 100) if sent > 0 else 100.0
                print_report1_alert_json(
                    sev=sev,
                    company=company,
                    message="TagsActivatedOnTime mismatch",
                    reason="TotalCommandsSent != TagsActivatedOnTime",
                    context={
                        "TotalCommandsSent": sent,
                        "TagsActivatedOnTime": tags_on_time_num,
                        "TagsActivatedOnTime_raw": row["TagsActivatedOnTime_raw"],
                        "diff": diff,
                        "diff_pct": round(pct, 1)
                    }
                )
                issue_found = True
                reasons.append(f"tags_on_time_mismatch={sev}")

        if issue_found and report_2_path:
            if DEBUG:
                print(f"--- Running Detailed Analysis for {company} (triggered by Health Algo Daily Statistics: {', '.join(reasons)}) ---")
            process_tag_details(report_2_path, company)


# =========================
# REPORT 2 helpers + JSON grouping
# =========================
def _parse_time_only(x):
    if pd.isna(x) or str(x).strip() == "":
        return pd.NaT
    return pd.to_datetime(str(x).strip(), format="%I:%M:%S %p", errors="coerce")


def _build_pull_dt(base_dt, pull_hour):
    if pd.isna(base_dt) or pd.isna(pull_hour):
        return pd.NaT
    return base_dt.normalize() + pd.Timedelta(hours=int(pull_hour))


def _gap_hours(curr_dt, prev_dt):
    if pd.isna(curr_dt) or pd.isna(prev_dt):
        return None
    gap = (curr_dt - prev_dt).total_seconds() / 3600.0
    if gap < 0:
        gap += 24.0
    return gap


class AlertCollector:
    def __init__(self):
        self._store = {}

    @staticmethod
    def _freeze_context(context: dict) -> tuple:
        if not context:
            return tuple()
        return tuple(sorted((str(k), str(v)) for k, v in context.items()))

    @staticmethod
    def _freeze_tag_obj(tag_obj: dict) -> tuple:
        if not tag_obj:
            return tuple()
        return tuple(sorted((str(k), str(v)) for k, v in tag_obj.items()))

    def add(self, sev: str, msg: str, reason: str, context: dict, tag_obj: dict):
        action = _alert_route(sev)
        key = (sev, msg, reason, self._freeze_context(context), action)

        if key not in self._store:
            self._store[key] = {}

        tag_key = self._freeze_tag_obj(tag_obj)
        self._store[key][tag_key] = tag_obj

    def flush(self):
        for (sev, msg, reason, frozen_ctx, action), tags_map in self._store.items():
            ctx = dict(frozen_ctx)
            tags_list = list(tags_map.values())

            alert_obj = {
                "Severity": sev,
                "Message": msg,
                "Reason": reason,
                "Context": ctx,
                "Action": action,
                "Tags": tags_list,
            }

            print(json.dumps(alert_obj, indent=2))
            print("")

            maybe_send_alert(alert_obj)


def process_tag_details(file_path, company_name):
    try:
        df2 = pd.read_csv(file_path)
        df2.columns = [c.strip() for c in df2.columns]

        if "LastReportReceivedAt" not in df2.columns and "LastReportedReceivedAt" in df2.columns:
            df2.rename(columns={"LastReportedReceivedAt": "LastReportReceivedAt"}, inplace=True)

        df2 = _drop_not_available_rows(df2)

        for col in ["LedStart", "CurrentTime", "LastReportReceivedAt", "CommandSentAt"]:
            if col in df2.columns:
                df2[col] = df2[col].apply(_parse_time_only)

        if "PullListTime" in df2.columns:
            df2["PullListTime"] = pd.to_numeric(df2["PullListTime"], errors="coerce")

        df2["CompanyName"] = df2["CompanyName"].astype(str).str.strip()
        tags = df2[df2["CompanyName"] == company_name.strip()]

    except Exception as e:
        print(f"Could not read detailed report: {e}")
        return

    if tags.empty:
        return

    collector = AlertCollector()

    mismatch_count = 0
    mismatch_tag_objs = []
    total_rows = len(tags)

    for _, tag in tags.iterrows():
        pen = str(tag.get("Pen", "")).strip()
        tag_id = str(tag.get("Tag", "")).strip()

        expected = str(tag.get("ExpectedLedState", "")).strip()
        reported = str(tag.get("LastReportedLedState", "")).strip()

        led_start = tag.get("LedStart")
        curr = tag.get("CurrentTime")
        last_rep = tag.get("LastReportReceivedAt")
        cmd_sent = tag.get("CommandSentAt")
        pull_hour = tag.get("PullListTime")

        base = curr if pd.notna(curr) else (cmd_sent if pd.notna(cmd_sent) else last_rep)
        pull_dt = _build_pull_dt(base, pull_hour)
        pull_passed = (pd.notna(curr) and pd.notna(pull_dt) and curr >= pull_dt)

        gap = _gap_hours(curr, last_rep)
        if gap is not None:
            msg = f"Health Algo Commands Daily Statistics | {company_name} | Stale reporting"
            tag_obj = {
                "tag": tag_id,
                "pen": pen,
                "gap_hours": round(gap, 2),
                "LastReportReceivedAt": str(last_rep.time()) if pd.notna(last_rep) else None,
                "CurrentTime": str(curr.time()) if pd.notna(curr) else None,
            }

            if gap > 7:
                collector.add("P1", msg, "LastReportReceivedAt > 7h behind CurrentTime", {"gap_bucket": ">7h"}, tag_obj)
            elif gap > 3:
                collector.add("P2", msg, "LastReportReceivedAt > 3h behind CurrentTime", {"gap_bucket": ">3h"}, tag_obj)
            elif gap > 1:
                collector.add("P3", msg, "LastReportReceivedAt > 1h behind CurrentTime", {"gap_bucket": ">1h"}, tag_obj)

        if pd.notna(led_start) and pd.notna(pull_dt) and led_start > pull_dt:
            late_hours = _gap_hours(led_start, pull_dt)
            if late_hours is not None:
                msg = f"Health Algo Commands Daily Statistics | {company_name} | LED started late"
                reason = "LedStart is later than PullListTime"

                if late_hours > 2:
                    sev, bucket = "P1", ">2h"
                elif late_hours > 1:
                    sev, bucket = "P2", ">1h"
                else:
                    sev, bucket = "P3", "0-1h"

                tag_obj = {
                    "tag": tag_id,
                    "pen": pen,
                    "late_hours": round(late_hours, 2),
                    "LedStart": str(led_start.time()) if pd.notna(led_start) else None,
                    "PullListTime": str(pull_dt.time()) if pd.notna(pull_dt) else None,
                }
                collector.add(sev, msg, reason, {"late_bucket": bucket}, tag_obj)

        effective_expected = expected
        inference_used = False

        if pd.isna(led_start) and pull_passed and pd.notna(last_rep) and pd.notna(cmd_sent):
            inference_used = True
            effective_expected = "LED_ON" if last_rep > cmd_sent else "LED_OFF"

        if effective_expected and reported and effective_expected != reported:
            mismatch_count += 1
            mismatch_tag_objs.append({"tag": tag_id, "pen": pen})

            sev = "P2"
            msg = f"Health Algo Commands Daily Statistics | {company_name} | LED state mismatch"
            reason = f"Expected={effective_expected}, Reported={reported}"

            context = {
                "ledstart_empty": bool(pd.isna(led_start)),
                "pull_passed": bool(pull_passed),
                "inference_used": bool(inference_used),
                "raw_expected_present": bool(expected != ""),
            }

            tag_obj = {
                "tag": tag_id,
                "pen": pen,
                "ExpectedEffective": effective_expected,
                "Reported": reported,
                "LedStartEmpty": bool(pd.isna(led_start)),
                "PullPassed": bool(pull_passed),
                "InferenceUsed": bool(inference_used),
                "LastReportReceivedAt": str(last_rep.time()) if pd.notna(last_rep) else None,
                "CommandSentAt": str(cmd_sent.time()) if pd.notna(cmd_sent) else None,
            }
            collector.add(sev, msg, reason, context, tag_obj)

    if total_rows > 0:
        mismatch_pct = (mismatch_count / total_rows) * 100.0

        if mismatch_pct > 50:
            sev = "P1"
            msg = f"Health Algo Commands Daily Statistics | {company_name} | Tag discrepancy high"
            reason = f"{mismatch_pct:.1f}% of tags have mismatch"
            context = {"mismatch_pct": round(mismatch_pct, 1), "total_tags": total_rows}
            for obj in (mismatch_tag_objs or [{"tag": None, "pen": None}]):
                collector.add(sev, msg, reason, context, obj)

        elif mismatch_pct > 25:
            sev = "P2"
            msg = f"Health Algo Commands Daily Statistics | {company_name} | Tag discrepancy moderate"
            reason = f"{mismatch_pct:.1f}% of tags have mismatch"
            context = {"mismatch_pct": round(mismatch_pct, 1), "total_tags": total_rows}
            for obj in (mismatch_tag_objs or [{"tag": None, "pen": None}]):
                collector.add(sev, msg, reason, context, obj)

    collector.flush()


def save_excel_copy(csv_path: Path, out_dir: Path | None = None) -> Path:
    out_dir = out_dir or csv_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    xlsx_path = out_dir / f"{csv_path.stem}.xlsx"
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df.to_excel(xlsx_path, index=False)
    return xlsx_path


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    now = datetime.now()

    day_folder = now.strftime("%Y-%m-%d")
    file_time = now.strftime("%H-%M-%S")

    daily_dir = LOGS_DIR / day_folder
    daily_dir.mkdir(parents=True, exist_ok=True)

    log_file = daily_dir / f"{file_time}.txt"

    tee = TeeStdout(log_file)
    sys.stdout = tee
    sys.stderr = tee

    try:
        clean_reports_folder(REPORTS_DIR)
        run_download_script("download_reports.py")
        report1_path, report2_path = ensure_reports_exist(REPORTS_DIR)

        save_excel_copy(Path(report1_path))
        save_excel_copy(Path(report2_path))

        run_noc_checks(str(report1_path), str(report2_path))

    finally:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        tee.close()