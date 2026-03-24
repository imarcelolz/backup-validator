BEGIN;

CREATE TABLE sources (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    mount_path  TEXT NOT NULL,
    role        VARCHAR(20) NOT NULL CHECK (role IN ('source', 'consolidated')),
    indexed_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sources_role ON sources(role);

CREATE TABLE files (
    id             BIGSERIAL PRIMARY KEY,
    source_id      INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    relative_path  TEXT NOT NULL,
    filename       TEXT NOT NULL,
    file_size      BIGINT NOT NULL,
    sha256         CHAR(64),
    mime_type      VARCHAR(255),
    media_type     VARCHAR(20) CHECK (media_type IN ('image', 'video', 'audio', 'other')),
    is_symlink     BOOLEAN NOT NULL DEFAULT FALSE,
    hash_status    VARCHAR(20) NOT NULL DEFAULT 'pending'
                   CHECK (hash_status IN ('pending', 'hashed', 'error')),
    hash_error     TEXT,
    indexed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_source_path UNIQUE (source_id, relative_path)
);

CREATE INDEX idx_files_sha256 ON files(sha256) WHERE sha256 IS NOT NULL;
CREATE INDEX idx_files_source_id ON files(source_id);
CREATE INDEX idx_files_media_type ON files(media_type);
CREATE INDEX idx_files_hash_status ON files(hash_status);

CREATE TABLE integrity_checks (
    id              BIGSERIAL PRIMARY KEY,
    file_id         BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    check_type      VARCHAR(30) NOT NULL
                    CHECK (check_type IN ('ffprobe', 'ffmpeg_deep', 'exiftool', 'pil', 'sha256_verify')),
    status          VARCHAR(20) NOT NULL
                    CHECK (status IN ('ok', 'corrupted', 'error', 'skipped')),
    error_message   TEXT,
    duration_ms     INTEGER,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_file_check UNIQUE (file_id, check_type)
);

CREATE INDEX idx_integrity_file_id ON integrity_checks(file_id);
CREATE INDEX idx_integrity_status ON integrity_checks(status);

CREATE TABLE comparison_results (
    id                   BIGSERIAL PRIMARY KEY,
    source_file_id       BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    status               VARCHAR(20) NOT NULL
                         CHECK (status IN ('found', 'missing', 'corrupted')),
    consolidated_file_id BIGINT REFERENCES files(id) ON DELETE SET NULL,
    notes                TEXT,
    compared_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_comparison_source UNIQUE (source_file_id)
);

CREATE INDEX idx_comparison_status ON comparison_results(status);

COMMIT;
