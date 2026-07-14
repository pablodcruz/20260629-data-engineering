# Data Engineering Study Lab

A zero-dependency browser study application for the Week 6–8 Spark, Spark SQL, Kafka, and Airflow material.

## Run the App

Open `index.html` in a modern browser. No installation or web server is required.

For the best local-file linking behavior, you can optionally serve the repository:

```bash
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/study/
```

## Study Modes

| Mode | Behavior |
| ---- | -------- |
| Practice | Shows the answer and explanation after each question |
| Exam | Runs with a timer and delays feedback until the results page |
| Flashcards | Prompts recall before revealing the answer |
| Smart Review | Selects previously missed or low-confidence questions |

## Features

* Topic and difficulty filters.
* Randomized questions and answer choices.
* Optional quiz seeds for reproducible sessions.
* Confidence ratings.
* Missed-question review.
* Topic mastery dashboard.
* Local browser persistence with no student account or data collection.
* Links back to the relevant review notes.

## Add a Question

Edit `questions/bank.js` and append an object with this shape:

```javascript
{
  id: "k13",
  topic: "kafka",
  difficulty: "intermediate",
  question: "Question text",
  code: "optional code sample",
  answers: ["Correct answer", "Distractor", "Distractor", "Distractor"],
  correct: 0,
  explanation: "Why the answer is correct.",
  reference: "../week8-review-notes.md#section"
}
```

Requirements:

* IDs must be unique.
* `topic` must be `spark`, `spark_sql`, `kafka`, or `airflow` unless the filter configuration is expanded in `js/app.js`.
* `difficulty` must be `foundational`, `intermediate`, or `advanced`.
* `correct` is the zero-based index of the correct answer before answers are shuffled.
* Distractors should be plausible enough to test understanding.

## Progress Data

Progress is stored in the browser under the local-storage key `de-study-lab-progress-v1`.
Use **Progress → Reset progress** to remove it.
