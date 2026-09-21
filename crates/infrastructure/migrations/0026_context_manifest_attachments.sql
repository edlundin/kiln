-- Historical text-only manifest hashes and invocation bindings remain intact.
ALTER TABLE context_manifests ADD COLUMN encoding_version INTEGER NOT NULL
    DEFAULT 1 CHECK (encoding_version IN (1, 2));

CREATE TABLE context_manifest_attachments (
    context_manifest_id TEXT NOT NULL
        REFERENCES context_manifests(context_manifest_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    content_hash TEXT NOT NULL REFERENCES artifacts(content_hash),
    media_type TEXT NOT NULL CHECK (length(media_type) > 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    PRIMARY KEY (context_manifest_id, position),
    UNIQUE (context_manifest_id, message_id, content_hash),
    FOREIGN KEY (message_id, content_hash)
        REFERENCES message_attachments(message_id, content_hash)
);
