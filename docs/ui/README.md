# Kiln UI direction

The first client is the **Kiln Agent Workspace**. Its main surface is one coding
agent conversation. A narrow project rail shows that other work exists. Thread
history, agents, changes, files, preview, and terminal open only when needed.

The Pencil design `kiln.pen` is the component and screen source of truth. The
interactive static example is [kiln-workspace.html](kiln-workspace.html). It
does not connect to the daemon and remains a portable interaction reference.

## Design brief

- Audience: developers who use one or more coding agents on real repositories.
- Subject: a local-first agent workspace for conversation, context, and review.
- Single job: steer the active agent and resume other work without losing
  context.
- Primary action: write a prompt, add files or images, and send it.
- Secondary action: inspect or steer tasks, agents, changes, files, artifacts,
  preview, or terminal state.

## Reference rules

Kiln uses tested interface rules from the named products. It does not combine
all of their panels into one screen.

- [T3 Code](https://t3.codes) supplies the conversation hierarchy, compact
  project and thread state, inline subagent call-to-action, and stable fleet
  rows.
- [Codex](https://openai.com/index/introducing-the-codex-app/) supplies the
  conversation-first command center, project threads, worktree isolation,
  inline task activity, and review in the thread.
- [Orca](https://www.onorca.dev/) supplies compact worktree and agent status,
  direct file drag, optional file and terminal tools, and split review
  surfaces.
- [Superset](https://docs.superset.sh/overview) supplies workspace and branch
  state, task lists, diff review, files, ports, and tools that stay secondary
  to the selected work.
- [Conductor](https://www.conductor.build/) supplies the task-to-workspace
  model and the conversation-plus-changes review path.
- [assistant-ui](https://www.assistant-ui.com/docs/architecture) remains a
  reference for smooth text streaming, reasoning, tool-group, and attachment
  motion. It is not a runtime dependency.
- [GPUI](https://gpui.rs/) supplies the native Rust renderer, window, input,
  action, animation, and application state primitives.
- [GPUI Component](https://longbridge.github.io/gpui-component/) supplies
  reusable controls such as inputs, lists, collapsibles, dialogs, Markdown,
  progress, themes, and resizable surfaces.

The resulting rule is simple: one primary work surface and temporary detail
surfaces. Kiln is not a terminal wrapper and it is not an operations dashboard.

## Native component strategy

- Build reusable GPUI components before composing screens.
- Use `gpui-component` for general controls and interaction mechanics.
- Build Kiln-specific transcript, composer, delegated Run, approval, and
  review components in `apps/desktop`.
- Apply Kiln tokens to all components. Do not ship the component gallery theme
  unchanged.
- Keep daemon state outside the component tree. Components render the ordered
  protocol projection and emit user intent through the Rust client layer.

## Visual system

### Palette

| Token | Value | Use |
| --- | --- | --- |
| --canvas | #0D0E0F | Conversation and application background |
| --chrome | #141516 | Navigation and temporary drawers |
| --surface | #1B1C1E | Hover, composer, and selected state |
| --line | #2B2D30 | Structure and boundaries |
| --text | #E8E8E5 | Primary text |
| --signal | #8CAEFF | Current work and keyboard focus |

Success, warning, and danger colors appear only when they carry state. Color
never replaces a text label.

### Type

- Reading and controls: Geist Sans, with native system fallbacks.
- Paths, branches, commands, identifiers, and metrics: Geist Mono, with
  native monospace fallbacks.

There is no display face. This is a work surface, so hierarchy comes from
placement, weight, and density.

### Shape and spacing

- Use square or lightly rounded application chrome.
- Reserve the 14 to 15 pixel radius for the user message and composer.
- Use 5 to 9 pixel radii for controls, rows, and compact groups.
- Use hairline separators. Do not place every section in a card.
- Do not use gradients, glass effects, large dashboard tiles, or ornamental
  status graphics.

### Layout

~~~text
┌────┬───────────────────────────────────────────────┐
│rail│ active conversation                    agents│
│    │                                       changes│
│ KL │ user and agent messages                       │
│ EM │ inline tools, plan, tasks, and subagent CTA   │
│    │                                               │
│    │ composer: text + files + images               │
└────┴───────────────────────────────────────────────┘

When requested:

┌────┬─ thread drawer ─────┐   ┌─ agents or changes drawer ─┐
│rail│ search and history  │   │ temporary details          │
└────┴─────────────────────┘   └─────────────────────────────┘
~~~

The selected layout keeps a 44 logical pixel project rail on desktop. The measured
control is 30 pixels, with 7 pixels of space on each side. The 248 logical pixel
thread drawer overlays the conversation and closes after selection. It is wide
enough for a 20-character title plus a short time label in the static example.

The 328 logical pixel details drawer is temporary. The width fits the three
fixed agent lines used in the static example: identity, activity, and metrics.
GPUI interaction tests in Phase 2 must measure real labels before these
prototype measurements become implementation limits.

The signature element is a one-pixel run line inside an active task group. It
connects task and subagent state and makes run lineage readable. There is no
global decorative status line.

## Interaction rules

### Projects, worktrees, and threads

- The desktop rail shows project or worktree marks, active state, and unread
  state. It does not show titles.
- Activating the rail or top-bar thread control opens the thread drawer as an
  overlay. Normal conversation width does not change.
- The drawer shows search, active work, recent threads, branch or worktree, run
  state, last activity, and New thread.
- Sort threads by updated_at, newest first. Grouping is optional when a group
  does not improve scanning.
- Use the state labels working, needs input, done, and failed.
- Opening a thread restores its transcript, last event cursor, local draft,
  attachment queue, active run, and selected detail tab.
- New thread creates the thread before the first prompt so uploads have a
  stable owner.

### Conversation and activity

- Messages are the primary reading order.
- User messages use one quiet filled surface. Agent messages do not use cards.
- Durable events appear as compact inline rows at the point they occur.
- A routine row uses a verb and result, such as Read 4 files or Tests passed.
- Expanding a row shows command output, permission scope, artifacts, event ID,
  and cursor.
- Approval requests appear inline and remain pending until the durable
  decision event arrives.
- Older routine tool calls fold under previous log entries.

### Tasks and subagents

- The parent conversation is the supervision surface.
- One inline task group shows the objective, completed count, and ordered task
  states.
- The progress indicator has one segment per visible task. It is state, not
  decoration.
- A running task shows the assigned agent, current activity, worktree, and
  elapsed time.
- The T3 Code-style subagent row uses one concise call-to-action, such as
  Kicked off 2 subagents. Selecting it opens the Agents drawer.
- The Agents drawer keeps stable rows for identity, activity, and metrics.
  Status updates do not move the row.
- Focus opens the child transcript. Guide queues targeted input.
  Interrupt is an explicit delivery mode. Cancel affects the selected run
  and its descendants, not its parent or siblings.
- Active task groups start expanded. Settled groups collapse but remain in the
  transcript.

### Composer attachments

- The picker accepts all files. It does not use an image-only accept filter.
- Picker selection, drag and drop, and clipboard paste feed the same queue.
- Images show thumbnails. Other files show name, media type, and byte size.
- Client states are local, uploading, ready, and failed.
- Send waits for retained attachments to reach ready.
- Upload uses POST /v1/artifacts with the owning thread ID. The message uses
  the returned artifact IDs.
- Text is optional when at least one attachment is present.
- Remove changes only the draft. Storage cleanup is a daemon policy.

### Temporary details

- The details drawer opens only when requested or when work needs attention.
- Changes is the default panel after an agent edits files.
- Diff comments become a follow-up message with file and line context.
- Agents shows run lineage, task, model, worktree, activity, elapsed time,
  token usage when available, and terminal state.
- Files, artifacts, preview, ports, and terminal can use the same temporary
  detail region in later phases.
- Terminal and preview are tools. They are not the main transcript.

### Motion

- Use GPUI animation primitives for smooth model-text streaming.
- Implement the reasoning and tool-group collapse and reveal patterns as
  native Kiln components.
- Animate only drawer entry, task insertion or completion, attachment upload,
  and subagent state changes.
- Keep spatial motion at or below 170 milliseconds in the static example. This
  is the shortest measured duration that keeps the drawer edge readable at
  1280 pixels without feeling delayed.
- Do not animate stable content or use motion as the only state signal.

## Accessibility and responsive behavior

- Every action is keyboard reachable and has a visible focus state.
- State always has a text label. Color is secondary.
- Escape closes either temporary drawer.
- Reduced-motion preferences remove streaming, task, attachment, and drawer
  animation while preserving state text.
- The composer remains reachable while the transcript scrolls.
- At 900 logical pixels or less, the details drawer uses a scrim.
- At 680 logical pixels or less, the project rail is hidden. The thread and details
  drawers enter from the screen edges. The conversation and composer remain
  the default view.

The 900 and 680 pixel breakpoints are prototype tripwires. They match the first
points where the measured top bar and composer controls no longer fit without
hiding labels. Phase 2 GPUI interaction tests must replace them if real content
fails earlier.
