---
name: phase
description: How to work one roadmap phase of topic-scope-mcp from start to hand-over. Use when the user says to start, continue or finish a Phase (for example "Phase 1 시작", "start phase 3"), or asks what the next phase is.
---

# Working one phase

A phase is one line of the Roadmap in `README.md`. It ends with a check that can be run.

## 1. Before writing code

1. Read the phase's line in the Roadmap and the README sections it refers to ("What it shows", "Tools", "Safety").
2. Confirm the earlier phases are ticked. If one is not, say so and stop.
3. Check what the phase needs on this machine (Docker running, Java 21, network). If something is missing, say what and how to fix it before starting.
4. If the phase needs a decision that the README does not make (a library, a version, a name), look it up in the official documentation, decide, and write the decision into the README or the code. Do not guess a version or an API from memory.

## 2. While working

- Write the check first when that is practical: the test that proves the phase, against the local deterministic stack.
- Stay inside the phase. Something that belongs to a later phase is written down as a note for the hand-over, not built.
- If the hook blocks an action, do not work around it. Tell the user what was blocked and why it was needed.

## 3. Finishing

1. Run the phase's check and every earlier check that still exists (hook tests, `./mvnw verify` once there is a build).
2. Update `README.md`: tick the phase, change the Status column of what now works from Planned to Done, and replace "planned" commands with the ones that were actually tried. Anything not tried stays marked as not tried.
3. Update `CLAUDE.md` only if a command or an always-rule changed. Keep it short. A new repeated procedure becomes a skill, not a paragraph there.

## 4. Hand-over (what the user asked for)

Reply in Korean with:

1. A short summary: what was built, the result of the check, and anything that is not done or not verified.
2. **One commit message, as text only.** Do not run `git add` or `git commit`. The user commits.
3. A question whether to go on to the next phase. **Never start the next phase without the user saying so.**
