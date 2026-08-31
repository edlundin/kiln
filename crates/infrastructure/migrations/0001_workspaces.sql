PRAGMA foreign_keys = ON;

CREATE TABLE workspaces (
    workspace_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE workspace_roots (
    workspace_root_id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    display_path TEXT NOT NULL,
    canonical_path TEXT NOT NULL,
    git_common_directory_path TEXT NOT NULL,
    position INTEGER NOT NULL,
    state TEXT NOT NULL,
    UNIQUE (workspace_id, name),
    UNIQUE (workspace_id, git_common_directory_path),
    UNIQUE (workspace_id, position)
);

CREATE INDEX workspace_roots_workspace_position
    ON workspace_roots (workspace_id, position);
