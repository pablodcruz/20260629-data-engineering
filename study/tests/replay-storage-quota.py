#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
import tempfile

from playwright.sync_api import sync_playwright

# Run with existing Playwright and Chromium:
# CHROMIUM_PATH=/path/to/chromium python study/tests/replay-storage-quota.py

source = Path(__file__).resolve().parents[1]
browser_path = os.environ.get("CHROMIUM_PATH") or None
url = "http://127.0.0.1/study/"
key = "de-study-lab-progress-v1"
fill = """() => {
  const filler = "synthetic-quota-filler";
  let low = 0, high = 6 * 1024 * 1024;
  while (low + 1 < high) {
    const size = Math.floor((low + high) / 2);
    try { localStorage.setItem(filler, "x".repeat(size)); low = size; }
    catch (e) { if (e.name !== "QuotaExceededError") throw e; high = size; }
  }
  let rejection;
  try { localStorage.setItem(filler, "x".repeat(low + 1)); } catch (e) { rejection = e.name; }
  if (rejection !== "QuotaExceededError") throw new Error("Browser did not reject the extra byte");
  return rejection;
}"""
question = {
    "id": "synthetic", "topic": "spark", "difficulty": "foundational",
    "question": "Synthetic question", "answers": ["Sample correct", "Sample incorrect"],
    "correct": 0, "explanation": "Synthetic explanation", "reference": "#synthetic",
}


def run(browser, case):
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda err: errors.append(str(err)))
    page.goto(url)
    page.locator("#start-button").click()

    if case in {"full", "recover"}:
        rejection = page.evaluate(fill)
    else:
        rejection = None
    page.get_by_role("button", name="Sample correct", exact=False).click()
    page.get_by_role("button", name="Confident", exact=True).click()
    page.get_by_role("button", name="Check answer").click()

    def warned():
        return page.get_by_role("status").filter(has_text="Progress could not be saved").count() == 1 and page.get_by_role("status").filter(has_text="Progress could not be saved").is_visible()

    def saved():
        return page.evaluate("(key) => JSON.parse(localStorage.getItem(key))", key)

    feedback = page.locator("#feedback").is_visible()
    first_warning = warned()
    after_first_save = saved()
    if case == "finish":
        rejection = page.evaluate(fill)
    elif case == "recover":
        page.evaluate('localStorage.removeItem("synthetic-quota-filler")')
    if page.locator("#next-button").is_visible():
        page.locator("#next-button").click()
    result = page.locator("#results-view").is_visible()
    result_warning = warned()
    summary = page.locator("#result-summary").inner_text() if result else None
    page.get_by_role("button", name="Progress", exact=True).click()

    def counts():
        return page.locator("#dashboard-stats strong").all_inner_texts()[:2]

    live = counts()
    warning_in_dashboard = warned()
    disk = saved()
    page.reload()
    page.get_by_role("button", name="Progress", exact=True).click()
    after_reload = counts()
    page.evaluate("localStorage.clear()")
    page.close()
    expected_warnings = {
        "normal": [False, False, False],
        "full": [True, True, True],
        "finish": [False, True, True],
        "recover": [True, False, False],
    }[case]
    expected_reload = {"normal": ["1","1"], "recover": ["1","1"], "full": ["0","0"], "finish": ["0","1"]}[case]
    expected_saved_cases = {"normal": (True, True), "full": (False, False), "finish": (True, True), "recover": (False, True)}
    ok = (
        (rejection == "QuotaExceededError" if case != "normal" else rejection is None)
        and feedback and result and summary == "You answered 1 of 1 questions correctly."
        and [first_warning, result_warning, warning_in_dashboard] == expected_warnings
        and live == ["1","1"] and after_reload == expected_reload and not errors
        and (bool(after_first_save), bool(disk)) == expected_saved_cases[case]
    )
    return {
        "case": case, "native_rejection": rejection, "feedback": feedback, "results": result,
        "warnings_quiz_results_progress": [first_warning, result_warning, warning_in_dashboard],
        "in_tab_sessions_answers": live, "reloaded_sessions_answers": after_reload,
        "saved_after_answer": bool(after_first_save), "saved_sessions": None if disk is None else len(disk["sessions"]),
        "page_errors": errors, "pass": ok,
    }


with sync_playwright() as p, tempfile.TemporaryDirectory(prefix="study-lab-replay-") as profile:
    browser = p.chromium.launch_persistent_context(
        profile, executable_path=browser_path, headless=True, chromium_sandbox=False,
        args=["--disable-background-networking", "--disable-breakpad", "--disable-crash-reporter", "--disable-dev-shm-usage", "--no-first-run"],
    )
    files = {"": ("index.html", "text/html"), "js/app.js": ("js/app.js", "text/javascript"), "css/styles.css": ("css/styles.css", "text/css")}

    def route(r):
        path = r.request.url.removeprefix(url)
        if path in files:
            f, typ = files[path]
            r.fulfill(content_type=typ, body=(source / f).read_text())
        elif path == "questions/bank.js":
            r.fulfill(content_type="text/javascript", body="window.STUDY_QUESTIONS = "+json.dumps([question])+";")
        else:
            r.abort()

    browser.route("**/*", route)
    print("Engine:", browser.pages[0].evaluate("navigator.userAgent"), flush=True)
    cases = [run(browser, case) for case in ("normal", "full", "finish", "recover")]
    for case in cases:
        print(json.dumps(case), flush=True)
    browser.close()
    sys.exit(0 if all(c["pass"] for c in cases) else 1)
