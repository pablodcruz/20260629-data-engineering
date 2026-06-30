# Lab 12 - Team Git Workflow

## Objective

Practice working safely in a shared project repository.

## Scenario

The StreamFlow project is a group project.
Teams need a simple Git workflow so people can work in parallel without overwriting each other.
This lab practices branches, commits, pulls, conflicts, and review habits.

## What You Will Build

You will create:

* A feature branch.
* A small documentation change.
* A clear commit.
* A simulated merge conflict.
* A pull request checklist or review note.

## Prerequisites

* Git is installed.
* You have access to a shared repository or a local practice repository.

Check Git:

```bash
git --version
```

## Suggested Folder

Use your project repository.
If you need a local practice repo, create one:

```bash
mkdir -p lab-12-git-workflow
cd lab-12-git-workflow
git init -b main
```

Create starter files:

```bash
mkdir -p docs
touch docs/team-notes.md
```

Add this content to `docs/team-notes.md`:

```markdown
# Team Notes

## Pipeline Owner

TBD

## Current Focus

TBD
```

Commit the starter file:

```bash
git add docs/team-notes.md
git commit -m "Add starter team notes"
```

## Step 1 - Check Your Starting State

Before changing files:

```bash
git status
git branch
```

You want a clean working tree before creating a branch.

## Step 2 - Create a Feature Branch

Use a branch name that describes the work:

```bash
git switch -c feature/update-team-notes
```

If `git switch` is unavailable:

```bash
git checkout -b feature/update-team-notes
```

## Step 3 - Make a Small Change

Edit `docs/team-notes.md`:

```markdown
# Team Notes

## Pipeline Owner

Add your name here.

## Current Focus

Building the StreamFlow Phase 1 mini pipeline.

## Next Step

Confirm the Docker Compose stack runs on each teammate's machine.
```

Check the diff:

```bash
git diff
```

## Step 4 - Commit Clearly

Stage and commit:

```bash
git add docs/team-notes.md
git commit -m "Update team project notes"
```

Check recent commits:

```bash
git log --oneline --max-count=5
```

## Step 5 - Practice Pulling Latest Changes

In a real shared repo, run:

```bash
git fetch origin
git pull --rebase origin main
```

If your team's default branch is named something else, replace `main` with that branch name.

If you are in a local practice repo without a remote, write this note in your deliverable:

```text
No remote was configured, so I could not run the pull/rebase step against origin.
```

## Step 6 - Simulate a Merge Conflict

This exercise creates a conflict locally so you can practice resolving it.

Switch back to `main`:

```bash
git switch main
```

If your repository uses a different default branch name, switch to that branch instead.

Edit the `Current Focus` section in `docs/team-notes.md`:

```markdown
## Current Focus

Designing the event schema.
```

Commit the change:

```bash
git add docs/team-notes.md
git commit -m "Update main branch focus"
```

Switch back to your feature branch:

```bash
git switch feature/update-team-notes
```

Merge `main` into the feature branch:

```bash
git merge main
```

Git should report a conflict in `docs/team-notes.md`.

## Step 7 - Resolve the Conflict

Open the file and look for conflict markers:

```text
<<<<<<< HEAD
Building the StreamFlow Phase 1 mini pipeline.
=======
Designing the event schema.
>>>>>>> main
```

Replace the conflict with a combined version:

```markdown
## Current Focus

Designing the event schema and building the StreamFlow Phase 1 mini pipeline.
```

Mark the conflict resolved:

```bash
git add docs/team-notes.md
git commit -m "Resolve team notes focus conflict"
```

Confirm the working tree is clean:

```bash
git status
```

## Step 8 - Push the Branch

In a real shared repo:

```bash
git push -u origin feature/update-team-notes
```

If you are using a local-only practice repo, skip the push and document that no remote was configured.

## Pull Request Checklist

Before opening a pull request, check:

* The branch name describes the work.
* The commit message explains the change.
* `git status` is clean.
* Generated output, secrets, logs, and large data files are not committed.
* The README or lab instructions were updated if behavior changed.
* Another teammate can understand the change.

## Review Etiquette

When reviewing a teammate's change:

* Ask questions before assuming something is wrong.
* Comment on specific lines or files.
* Separate required fixes from suggestions.
* Keep feedback focused on the work.
* Confirm the change runs before approving when possible.

## Useful Commands

| Command | Purpose |
| ------- | ------- |
| `git status` | Shows changed files and branch state |
| `git diff` | Shows unstaged changes |
| `git add <file>` | Stages a file |
| `git commit -m "message"` | Saves a commit |
| `git switch -c <branch>` | Creates and switches to a branch |
| `git fetch origin` | Downloads remote branch information |
| `git pull --rebase origin main` | Updates your branch with latest `main` |
| `git push -u origin <branch>` | Pushes your branch to the remote |
| `git log --oneline --max-count=5` | Shows recent commits |

## Checkpoints

You are done when:

* You created a feature branch.
* You made and committed a small change.
* You practiced or documented the pull/rebase step.
* You created and resolved a merge conflict.
* Your final `git status` is clean.

## Deliverables

Submit:

* The final `docs/team-notes.md`.
* Output from `git log --oneline --max-count=5`.
* A short note explaining how you resolved the conflict.
* A pull request checklist for your team.

## Reflection Questions

Answer briefly:

* Why should each task happen on a branch?
* Why should commits be small and understandable?
* What files should not be committed to a data engineering project?
