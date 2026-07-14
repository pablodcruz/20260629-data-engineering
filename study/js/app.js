(() => {
  "use strict";

  const BANK = window.STUDY_QUESTIONS || [];
  const STORAGE_KEY = "de-study-lab-progress-v1";
  const state = { view: "home", session: null, timer: null, progress: loadProgress() };
  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => [...document.querySelectorAll(selector)];
  const topicLabels = { spark: "Spark & RDDs", spark_sql: "Spark SQL", kafka: "Kafka", airflow: "Airflow" };
  const difficulties = ["foundational", "intermediate", "advanced"];

  function loadProgress() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY)) || { questions: {}, sessions: [] }; }
    catch { return { questions: {}, sessions: [] }; }
  }
  function saveProgress() { localStorage.setItem(STORAGE_KEY, JSON.stringify(state.progress)); }
  function hashSeed(text) { let h = 2166136261; for (const c of text) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return h >>> 0; }
  function rng(seed) { let n = seed || Date.now(); return () => { n |= 0; n = n + 0x6D2B79F5 | 0; let t = Math.imul(n ^ n >>> 15, 1 | n); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }
  function shuffled(items, random = Math.random) { const copy = [...items]; for (let i = copy.length - 1; i > 0; i--) { const j = Math.floor(random() * (i + 1)); [copy[i], copy[j]] = [copy[j], copy[i]]; } return copy; }
  function escapeHtml(value) { const div = document.createElement("div"); div.textContent = value; return div.innerHTML; }

  function init() {
    $("#bank-count").textContent = BANK.length;
    renderFilters();
    bindEvents();
    updateAvailability();
    renderDashboard();
  }

  function renderFilters() {
    $("#topic-filters").innerHTML = Object.entries(topicLabels).map(([key, label]) => `<label class="chip"><input type="checkbox" value="${key}" checked>${label}</label>`).join("");
    $("#difficulty-filters").innerHTML = difficulties.map(d => `<label class="chip"><input type="checkbox" value="${d}" checked>${d[0].toUpperCase() + d.slice(1)}</label>`).join("");
  }

  function bindEvents() {
    $$(".nav-link").forEach(button => button.addEventListener("click", () => showView(button.dataset.view)));
    $$("input[name=mode]").forEach(input => input.addEventListener("change", () => {
      $$(".mode-card").forEach(card => card.classList.toggle("selected", card.contains(input)));
      $("#time-option").style.visibility = input.value === "exam" ? "visible" : "hidden";
      updateAvailability();
    }));
    $$("#topic-filters input, #difficulty-filters input").forEach(input => input.addEventListener("change", updateAvailability));
    $("#start-button").addEventListener("click", startSession);
    $("#exit-button").addEventListener("click", () => { stopTimer(); showView("home"); });
    $("#submit-button").addEventListener("click", submitAnswer);
    $("#next-button").addEventListener("click", nextQuestion);
    $("#skip-button").addEventListener("click", skipQuestion);
    $("#reveal-button").addEventListener("click", revealFlashcard);
    $$("[data-confidence]").forEach(button => button.addEventListener("click", () => selectConfidence(Number(button.dataset.confidence))));
    $("#new-session-button").addEventListener("click", () => showView("home"));
    $("#review-mistakes-button").addEventListener("click", startMistakeReview);
    $("#reset-progress").addEventListener("click", resetProgress);
  }

  function showView(name) {
    $$(".view").forEach(view => view.classList.toggle("active", view.id === `${name}-view`));
    $$(".nav-link").forEach(link => link.classList.toggle("active", link.dataset.view === name));
    state.view = name;
    if (name === "dashboard") renderDashboard();
    window.scrollTo(0, 0);
  }

  function chosen(selector) { return $$(selector).filter(i => i.checked).map(i => i.value); }
  function eligibleQuestions(mode) {
    const topics = chosen("#topic-filters input");
    const levels = chosen("#difficulty-filters input");
    let questions = BANK.filter(q => topics.includes(q.topic) && levels.includes(q.difficulty));
    if (mode === "review") questions = questions.filter(q => { const p = state.progress.questions[q.id]; return p && (p.wrong > 0 || p.confidenceTotal / Math.max(p.attempts, 1) < 2.4); });
    return questions;
  }
  function updateAvailability() {
    const mode = $("input[name=mode]:checked").value;
    const count = eligibleQuestions(mode).length;
    $("#available-count").textContent = `${count} matching question${count === 1 ? "" : "s"}`;
  }

  function startSession() {
    const mode = $("input[name=mode]:checked").value;
    const pool = eligibleQuestions(mode);
    if (!pool.length) { $("#setup-message").textContent = mode === "review" ? "No review questions yet. Complete a practice session first." : "Select at least one topic and difficulty."; return; }
    $("#setup-message").textContent = "";
    const seedText = $("#seed").value.trim() || `${Date.now()}`;
    const random = rng(hashSeed(seedText));
    const requested = $("#question-count").value === "all" ? pool.length : Number($("#question-count").value);
    const questions = shuffled(pool, random).slice(0, Math.min(requested, pool.length)).map(question => ({ ...question, answers: shuffled(question.answers.map((text, original) => ({ text, original })), random) }));
    state.session = { mode, seed: seedText, questions, index: 0, responses: [], selected: null, confidence: 0, started: Date.now(), seconds: mode === "exam" ? Number($("#exam-minutes").value) * 60 : null };
    showView("quiz"); renderQuestion(); if (mode === "exam") startTimer();
  }

  function current() { return state.session.questions[state.session.index]; }
  function renderQuestion() {
    const session = state.session, question = current(), index = session.index;
    session.selected = null; session.confidence = 0;
    $("#question-position").textContent = `${index + 1} / ${session.questions.length}`;
    $("#progress-bar").style.width = `${index / session.questions.length * 100}%`;
    $("#question-topic").textContent = topicLabels[question.topic];
    $("#question-difficulty").textContent = question.difficulty;
    $("#question-text").textContent = question.question;
    const code = $("#question-code"); code.hidden = !question.code; code.querySelector("code").textContent = question.code || "";
    $("#answer-list").innerHTML = question.answers.map((answer, i) => `<button class="answer-option" data-index="${i}"><span class="answer-letter">${String.fromCharCode(65 + i)}</span><span>${escapeHtml(answer.text)}</span></button>`).join("");
    $$(".answer-option").forEach(button => button.addEventListener("click", () => selectAnswer(Number(button.dataset.index))));
    const flash = session.mode === "flashcard";
    $("#answer-list").hidden = flash;
    $("#flashcard-prompt").hidden = !flash;
    $("#feedback").hidden = true; $("#feedback").classList.remove("wrong");
    $("#confidence-panel").hidden = false; $$("[data-confidence]").forEach(b => b.classList.remove("selected"));
    $("#submit-button").hidden = flash; $("#submit-button").disabled = true;
    $("#next-button").hidden = true; $("#skip-button").hidden = false;
  }

  function selectAnswer(index) { state.session.selected = index; $$(".answer-option").forEach((b, i) => b.classList.toggle("selected", i === index)); updateSubmit(); }
  function selectConfidence(value) { state.session.confidence = value; $$("[data-confidence]").forEach(b => b.classList.toggle("selected", Number(b.dataset.confidence) === value)); updateSubmit(); }
  function updateSubmit() { $("#submit-button").disabled = state.session.selected === null || !state.session.confidence; }

  function submitAnswer() {
    const session = state.session, question = current(), selected = question.answers[session.selected];
    const correct = selected.original === question.correct;
    const response = { id: question.id, correct, confidence: session.confidence, selected: selected.text, answer: question.answers.find(a => a.original === question.correct).text };
    session.responses.push(response); recordProgress(response);
    if (session.mode === "exam") { nextQuestion(); return; }
    showFeedback(correct);
  }

  function showFeedback(correct) {
    const question = current(), feedback = $("#feedback");
    $$(".answer-option").forEach((button, i) => { button.disabled = true; const answer = question.answers[i]; if (answer.original === question.correct) button.classList.add("correct"); else if (i === state.session.selected) button.classList.add("wrong"); });
    feedback.hidden = false; feedback.classList.toggle("wrong", !correct);
    $("#feedback-result").textContent = correct ? "Correct" : "Not quite";
    $("#feedback-explanation").textContent = question.explanation;
    $("#note-link").href = question.reference;
    $("#confidence-panel").hidden = true; $("#submit-button").hidden = true; $("#next-button").hidden = false; $("#skip-button").hidden = true;
  }

  function revealFlashcard() {
    const question = current(); $("#answer-list").hidden = false; $("#flashcard-prompt").hidden = true;
    $$(".answer-option").forEach((button, i) => { button.disabled = true; if (question.answers[i].original === question.correct) button.classList.add("correct"); });
    $("#feedback").hidden = false; $("#feedback-result").textContent = "Answer revealed"; $("#feedback-explanation").textContent = question.explanation; $("#note-link").href = question.reference;
    $$("[data-confidence]").forEach(button => { button.onclick = () => { const confidence = Number(button.dataset.confidence); const correct = confidence >= 2; const answer = question.answers.find(a => a.original === question.correct).text; const response = { id: question.id, correct, confidence, selected: "Self-rated", answer }; state.session.responses.push(response); recordProgress(response); $("#confidence-panel").hidden = true; $("#next-button").hidden = false; $("#skip-button").hidden = true; }; });
  }

  function skipQuestion() { const session = state.session; session.questions.push(session.questions.splice(session.index, 1)[0]); renderQuestion(); }
  function nextQuestion() { const session = state.session; session.index++; if (session.index >= session.questions.length) finishSession(); else renderQuestion(); }
  function recordProgress(response) { const p = state.progress.questions[response.id] || { attempts: 0, correct: 0, wrong: 0, confidenceTotal: 0 }; p.attempts++; p.correct += response.correct ? 1 : 0; p.wrong += response.correct ? 0 : 1; p.confidenceTotal += response.confidence; p.lastSeen = Date.now(); state.progress.questions[response.id] = p; saveProgress(); }
  function startTimer() { $("#timer").hidden = false; updateTimer(); state.timer = setInterval(() => { state.session.seconds--; updateTimer(); if (state.session.seconds <= 0) finishSession(); }, 1000); }
  function updateTimer() { const s = state.session.seconds; $("#timer").textContent = `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`; }
  function stopTimer() { clearInterval(state.timer); state.timer = null; $("#timer").hidden = true; }

  function finishSession() {
    stopTimer(); const session = state.session, answered = session.responses.length, correct = session.responses.filter(r => r.correct).length, score = answered ? Math.round(correct / answered * 100) : 0;
    state.progress.sessions.unshift({ date: Date.now(), mode: session.mode, answered, correct, score }); state.progress.sessions = state.progress.sessions.slice(0, 30); saveProgress();
    $("#result-score").textContent = `${score}%`; $("#result-heading").textContent = score >= 90 ? "Excellent command." : score >= 75 ? "Strong work." : score >= 60 ? "Good foundation." : "Keep building.";
    $("#result-summary").textContent = `You answered ${correct} of ${answered} questions correctly.`;
    const lowConfidence = session.responses.filter(r => r.confidence === 1).length;
    $("#result-breakdown").innerHTML = [{v:correct,l:"Correct"},{v:answered-correct,l:"Needs review"},{v:lowConfidence,l:"Low confidence"},{v:session.seed,l:"Quiz seed"}].map(x => `<div class="breakdown-card"><strong>${escapeHtml(String(x.v))}</strong><span>${x.l}</span></div>`).join("");
    $("#result-list").innerHTML = session.responses.map(r => { const q = BANK.find(item => item.id === r.id); return `<div class="result-item"><span class="result-icon ${r.correct ? "correct" : "wrong"}">${r.correct ? "✓" : "×"}</span><div><strong>${escapeHtml(q.question)}</strong><p>${r.correct ? "Correct" : `Correct answer: ${escapeHtml(r.answer)}`}</p></div></div>`; }).join("");
    $("#review-mistakes-button").hidden = !session.responses.some(r => !r.correct); showView("results");
  }

  function startMistakeReview() { const missed = new Set(state.session.responses.filter(r => !r.correct).map(r => r.id)); const questions = BANK.filter(q => missed.has(q.id)).map(q => ({ ...q, answers: q.answers.map((text, original) => ({ text, original })) })); state.session = { mode:"practice", seed:"mistakes", questions, index:0, responses:[], selected:null, confidence:0, started:Date.now(), seconds:null }; showView("quiz"); renderQuestion(); }

  function renderDashboard() {
    const entries = Object.entries(state.progress.questions), attempts = entries.reduce((n,[,p]) => n+p.attempts,0), correct = entries.reduce((n,[,p]) => n+p.correct,0), accuracy = attempts ? Math.round(correct/attempts*100) : 0;
    $("#dashboard-stats").innerHTML = [{v:state.progress.sessions.length,l:"Sessions"},{v:attempts,l:"Answers"},{v:`${accuracy}%`,l:"Accuracy"},{v:entries.filter(([,p])=>p.wrong>0).length,l:"Review queue"}].map(x=>`<div class="stat-card"><strong>${x.v}</strong><span>${x.l}</span></div>`).join("");
    $("#mastery-list").innerHTML = Object.entries(topicLabels).map(([key,label]) => { const ids = new Set(BANK.filter(q=>q.topic===key).map(q=>q.id)); const ps=entries.filter(([id])=>ids.has(id)).map(([,p])=>p); const a=ps.reduce((n,p)=>n+p.attempts,0), c=ps.reduce((n,p)=>n+p.correct,0), pct=a?Math.round(c/a*100):0; return `<div class="mastery-row"><strong>${label}</strong><div class="mastery-bar"><span style="width:${pct}%"></span></div><span>${pct}%</span></div>`; }).join("");
    const queue = entries.filter(([,p])=>p.wrong>0 || p.confidenceTotal/Math.max(p.attempts,1)<2.4).sort((a,b)=>b[1].wrong-a[1].wrong).slice(0,8);
    $("#study-queue").innerHTML = queue.length ? queue.map(([id,p])=>{const q=BANK.find(x=>x.id===id);return `<div class="result-item"><span class="result-icon wrong">↻</span><div><strong>${escapeHtml(q.question)}</strong><p>${p.wrong} missed · ${p.attempts} attempt${p.attempts===1?"":"s"}</p></div></div>`;}).join("") : `<p class="queue-empty">Complete a session and difficult questions will appear here.</p>`;
  }

  function resetProgress() { if (confirm("Delete all saved study progress in this browser?")) { state.progress={questions:{},sessions:[]}; saveProgress(); renderDashboard(); updateAvailability(); } }
  init();
})();
