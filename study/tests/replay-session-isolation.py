"""Replay Flashcards-to-Practice on an isolated origin with synthetic questions.

Run with existing Playwright and Chromium:
  CHROMIUM_PATH=/path/to/chromium python study/tests/replay-session-isolation.py
"""
import json
import os
from pathlib import Path
import tempfile

from playwright.sync_api import sync_playwright


URL = "http://127.0.0.1/study/"
KEY = "de-study-lab-progress-v1"


def question(id, topic):
    return {
        "id": id,
        "topic": topic,
        "difficulty": "foundational",
        "question": "Synthetic " + id,
        "answers": ["Fixture correct", "Fixture incorrect"],
        "correct": 0,
        "explanation": "Synthetic explanation",
        "reference": "#synthetic",
    }


def replay(browser, interrupt):
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(URL)
    page.evaluate("localStorage.clear()")
    page.reload()

    def click(css):
        page.locator(css).click(timeout=4000)

    def progress():
        return page.evaluate("(key) => JSON.parse(localStorage.getItem(key))", KEY)

    def begin(mode, topic):
        click(f'label.mode-card:has(input[value="{mode}"])')
        for chip in page.locator("#topic-filters label").all():
            field = chip.locator("input")
            if field.is_checked() != (field.input_value() == topic):
                chip.click()
        page.locator("#seed").fill("synthetic-isolation")
        click("#start-button")

    begin("flashcard", "spark")
    click("#reveal-button")
    click('[data-confidence="3"]')
    click("#next-button")
    before = progress()
    click("#new-session-button")
    if interrupt:
        begin("flashcard", "spark")
        click("#reveal-button")
        click("#exit-button")
    begin("practice", "kafka")
    page.get_by_role("button", name="Fixture correct", exact=False).click()
    click('[data-confidence="3"]')
    pending = progress()
    no_phantom = pending == before and page.locator("#next-button").is_hidden()
    end = {}
    reloaded = False
    if no_phantom:
        click("#submit-button")
        click("#next-button")
        end = progress()
        page.reload()
        reloaded = progress() == end
    sessions = end.get("sessions", [])
    passed = (
        before["questions"]["old"]["attempts"] == 1
        and no_phantom
        and len(sessions) == 2
        and sessions[0]["mode"] == "practice"
        and sessions[0]["answered"] == 1
        and sessions[1] == before["sessions"][0]
        and end["questions"]["old"] == before["questions"]["old"]
        and end["questions"]["new"]["attempts"] == 1
        and reloaded
        and not errors
    )
    page.close()
    return {
        "case": "revealed then exited" if interrupt else "finished flashcard",
        "old_attempts_before_confidence": before["questions"]["old"]["attempts"],
        "old_attempts_after_confidence": pending["questions"]["old"]["attempts"],
        "no_phantom": no_phantom,
        "practice_answers_after_submit": sessions[0]["answered"] if sessions else None,
        "reloaded": reloaded,
        "page_errors": errors,
        "pass": passed,
    }


def main():
    directory = Path(__file__).resolve().parents[1]
    files = {
        "": ("index.html", "text/html"),
        "js/app.js": ("js/app.js", "text/javascript"),
        "css/styles.css": ("css/styles.css", "text/css"),
    }

    def route(response):
        path = response.request.url.removeprefix(URL)
        if path in files:
            filename, mime = files[path]
            response.fulfill(content_type=mime, body=(directory / filename).read_text())
        elif path == "questions/bank.js":
            fixtures = [question("old", "spark"), question("new", "kafka")]
            response.fulfill(content_type="text/javascript", body="window.STUDY_QUESTIONS = " + json.dumps(fixtures) + ";")
        else:
            response.abort()

    with sync_playwright() as driver, tempfile.TemporaryDirectory(prefix="study-isolation-") as profile:
        browser = driver.chromium.launch_persistent_context(
            profile,
            executable_path=os.environ.get("CHROMIUM_PATH") or None,
            headless=True,
            args=["--disable-background-networking", "--disable-breakpad", "--disable-crash-reporter", "--disable-dev-shm-usage"],
        )
        browser.route("**/*", route)
        print(browser.pages[0].evaluate("navigator.userAgent"))
        results = [replay(browser, interrupt) for interrupt in (False, True)]
        for result in results:
            print(json.dumps(result))
        browser.close()
    return 0 if all(case["pass"] for case in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
