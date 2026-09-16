ALTER TABLE sessions ADD COLUMN workspace_root_id TEXT;
ALTER TABLE sessions ADD COLUMN relative_directory TEXT;
ALTER TABLE sessions ADD COLUMN root_path TEXT;
ALTER TABLE sessions ADD COLUMN git_common_directory_path TEXT;
ALTER TABLE sessions ADD COLUMN filesystem_identity TEXT;
