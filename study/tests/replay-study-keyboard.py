"""Run the Study Lab's keyboard loop against an isolated synthetic question bank.

CHROMIUM_PATH=/path/to/chromium python study/tests/replay-study-keyboard.py
"""
import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
URL = "http://127.0.0.1/study/"
BANK = [{
    "id": "keyboard-only", "topic": "spark", "difficulty": "foundational",
    "question": "Synthetic keyboard practice question",
    "answers": ["Fixture correct", "Fixture incorrect"], "correct": 0,
    "explanation": "Synthetic explanation.", "reference": "#fixture",
}]

def route_request(route):
    name = route.request.url.removeprefix(URL)
    if route.request.url != URL + name:
        return route.abort()
    if name == "questions/bank.js":
        return route.fulfill(content_type="text/javascript", body="window.STUDY_QUESTIONS = " + json.dumps(BANK))
    paths = {"": "index.html", "css/styles.css": "css/styles.css", "js/app.js": "js/app.js"}
    if name in paths:
        return route.fulfill(path=ROOT / paths[name])
    return route.abort()

def reach(page, target):
    for _ in range(30):
        page.keyboard.press("Tab")
        if target.evaluate("(element) => element === document.activeElement"):
            return
    raise AssertionError(f"Could not Tab to {target}")

def active(page, selector):
    return page.locator(selector).evaluate("(element) => element === document.activeElement")

def replay(browser, width):
    page = browser.new_page(viewport={"width": width, "height": 900}, reduced_motion="reduce")
    page.route("**/*", route_request)
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(URL)
    practice = page.locator('input[value="practice"]')
    exam = page.locator('input[value="exam"]')
    reach(page, practice)
    page.keyboard.press("ArrowRight")
    assert exam.is_checked() and active(page, 'input[value="exam"]')
    mode_style = exam.locator("..").evaluate("""(label) => {
      const style = getComputedStyle(label), box = label.getBoundingClientRect();
      return {
        visible_ring: style.outlineStyle !== 'none' && parseFloat(style.outlineWidth) >= 2,
        contained: box.left >= 0 && box.right <= innerWidth,
        still: style.transitionDuration === '0s' && style.transform === 'none',
      };
    }""")
    if os.environ.get("REPLAY_SCREEN_DIR"):
        page.screenshot(path=Path(os.environ["REPLAY_SCREEN_DIR"]) / f"mode-{width}.png")
    page.keyboard.press("ArrowLeft")
    assert practice.is_checked()
    reach(page, page.locator("#start-button"))
    page.keyboard.press("Enter")
    assert page.locator("#quiz-view").is_visible()
    start_focus = active(page, "#quiz-view")
    reach(page, page.get_by_role("button", name="Fixture incorrect", exact=False))
    page.keyboard.press("Space")
    reach(page, page.get_by_role("button", name="Guessing"))
    page.keyboard.press("Space")
    reach(page, page.locator("#submit-button"))
    page.keyboard.press("Enter")
    assert page.locator("#feedback-result").inner_text() == "Not quite"
    reach(page, page.locator("#next-button"))
    page.keyboard.press("Enter")
    assert page.locator("#result-summary").inner_text() == "You answered 0 of 1 questions correctly."
    results_focus = active(page, "#result-heading")
    if os.environ.get("REPLAY_SCREEN_DIR"):
        page.screenshot(path=Path(os.environ["REPLAY_SCREEN_DIR"]) / f"results-{width}.png")
    page.keyboard.press("Tab")
    review_is_next = active(page, "#review-mistakes-button")
    if not review_is_next:
        reach(page, page.locator("#review-mistakes-button"))
    page.keyboard.press("Enter")
    assert page.locator("#question-text").inner_text() == BANK[0]["question"]
    review_focus = active(page, "#quiz-view")
    reach(page, page.locator("#exit-button"))
    page.keyboard.press("Enter")
    assert page.locator("#home-view").is_visible()
    home_focus = active(page, "#home-view h1")
    assert not errors
    checks = dict(**mode_style, start_focus=start_focus, results_focus=results_focus,
                  review_is_next=review_is_next, review_focus=review_focus, home_focus=home_focus)
    print(f"{width}px {json.dumps(checks)}")
    page.close()
    return all(checks.values())

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(executable_path=os.environ.get("CHROMIUM_PATH"), args=["--no-sandbox"])
    print(f"Chromium {browser.version}")
    result = [replay(browser, width) for width in (1440, 390, 320)]
    browser.close()
    raise SystemExit(0 if all(result) else 1)
