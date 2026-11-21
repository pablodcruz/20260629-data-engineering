"""Exercise interrupted Flashcards + native storage quota in one recoverable Practice session.

Run with existing Playwright and Chromium:
  CHROMIUM_PATH=/path/to/chromium python study/tests/replay-progress-interaction.py
"""
import importlib.util
import json
import os
from pathlib import Path
import tempfile

from playwright.sync_api import sync_playwright

tests = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("isolation_fixture", tests / "replay-session-isolation.py")
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)

fill_storage = """() => {
  const key = 'synthetic-quota-filler';
  let low = 0, high = 6 * 1024 * 1024;
  while (high - low > 1) {
    const size = Math.floor((low + high) / 2);
    try { localStorage.setItem(key, 'x'.repeat(size)); low = size; }
    catch (error) { if (error.name !== 'QuotaExceededError') throw error; high = size; }
  }
  try { localStorage.setItem(key, 'x'.repeat(low + 1)); }
  catch (error) { if (error.name === 'QuotaExceededError') return error.name; throw error; }
  throw new Error('Did not reach browser storage quota');
}"""


def replay(browser):
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda err: errors.append(str(err)))
    page.goto(fixtures.URL)

    def click(selector):
        page.locator(selector).click(timeout=4000)

    def begin(mode, topic):
        click(f'label.mode-card:has(input[value="{mode}"])')
        for chip in page.locator("#topic-filters label").all():
            field = chip.locator("input")
            if field.is_checked() != (field.input_value() == topic):
                chip.click()
        page.locator("#seed").fill("synthetic-cross-case")
        click("#start-button")

    def read_progress():
        return page.evaluate("(key) => JSON.parse(localStorage.getItem(key))", fixtures.KEY)

    warning = page.get_by_role("status").filter(has_text="Progress could not be saved")
    begin("flashcard", "spark")
    click("#reveal-button")
    click('[data-confidence="3"]')
    click("#next-button")
    saved = read_progress()
    click("#new-session-button")
    begin("flashcard", "spark")
    click("#reveal-button")
    click("#exit-button")
    begin("practice", "kafka")
    rejection = page.evaluate(fill_storage)
    page.get_by_role("button", name="Fixture correct", exact=False).click()
    click('[data-confidence="3"]')
    before = (
        read_progress() == saved and not warning.is_visible()
        and page.locator("#next-button").is_hidden() and not errors
    )
    submitted = False
    if page.locator("#submit-button").is_visible():
        click("#submit-button")
        submitted = (
            page.locator("#feedback").is_visible() and warning.is_visible()
            and read_progress() == saved
        )
    page.evaluate('localStorage.removeItem("synthetic-quota-filler")')
    finished = False
    if page.locator("#next-button").is_visible():
        click("#next-button")
        finished = (
            page.locator("#results-view").is_visible() and not warning.is_visible()
            and page.locator("#result-summary").inner_text() == "You answered 1 of 1 questions correctly."
        )
    result = read_progress()
    sessions = result.get("sessions", [])
    persisted = (
        len(sessions) == 2 and sessions[0]["mode"] == "practice"
        and sessions[0]["answered"] == 1 and sessions[1] == saved["sessions"][0]
        and result["questions"]["old"] == saved["questions"]["old"]
        and result["questions"].get("new", {}).get("attempts") == 1
    )
    page.reload()
    reload_matches = read_progress() == result
    report = {
        "case": "revealed then exited, quota full during Practice, save at completion",
        "native_rejection": rejection,
        "confidence_has_no_write_or_feedback": before,
        "answer_shows_feedback_and_warning": submitted,
        "results_and_warning_cleared": finished,
        "one_new_practice_answer_and_prior_flashcard_unchanged": persisted,
        "reload_matches": reload_matches,
        "page_errors": errors,
        "pass": all((before, submitted, finished, persisted, reload_matches)) and not errors,
    }
    page.close()
    return report


def main():
    directory = tests.parent
    files = {
        "": ("index.html", "text/html"),
        "js/app.js": ("js/app.js", "text/javascript"),
        "css/styles.css": ("css/styles.css", "text/css"),
    }

    def route(response):
        path = response.request.url.removeprefix(fixtures.URL)
        if path in files:
            filename, mime = files[path]
            response.fulfill(content_type=mime, body=(directory / filename).read_text())
        elif path == "questions/bank.js":
            questions = [fixtures.question("old", "spark"), fixtures.question("new", "kafka")]
            response.fulfill(content_type="text/javascript", body="window.STUDY_QUESTIONS = " + json.dumps(questions) + ";")
        else:
            response.abort()

    with sync_playwright() as driver, tempfile.TemporaryDirectory(prefix="study-cross-case-") as profile:
        browser = driver.chromium.launch_persistent_context(
            profile, executable_path=os.environ.get("CHROMIUM_PATH") or None, headless=True,
            args=["--disable-background-networking", "--disable-breakpad", "--disable-crash-reporter", "--disable-dev-shm-usage", "--no-first-run"],
        )
        browser.route("**/*", route)
        print(browser.pages[0].evaluate("navigator.userAgent"))
        report = replay(browser)
        print(json.dumps(report))
        browser.close()
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
