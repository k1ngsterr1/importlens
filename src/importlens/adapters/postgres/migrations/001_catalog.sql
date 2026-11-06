CREATE TABLE catalog_versions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_count integer NOT NULL CHECK (item_count >= 0)
);

CREATE TABLE catalog_items (
    version_id bigint NOT NULL REFERENCES catalog_versions(id),
    sku text NOT NULL,
    name text NOT NULL,
    price_cents integer NOT NULL CHECK (price_cents > 0),
    PRIMARY KEY (version_id, sku)
);

CREATE TABLE catalog_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL CHECK (revision >= 0),
    version_id bigint NOT NULL REFERENCES catalog_versions(id)
);

CREATE TABLE previews (
    id uuid PRIMARY KEY,
    filename text NOT NULL,
    base_revision bigint NOT NULL,
    rows integer NOT NULL,
    added integer NOT NULL,
    updated integer NOT NULL,
    missing integer NOT NULL,
    unchanged integer NOT NULL,
    errors integer NOT NULL,
    warnings integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE preview_changes (
    preview_id uuid NOT NULL REFERENCES previews(id),
    position integer NOT NULL,
    kind text NOT NULL CHECK (kind IN ('added', 'updated', 'missing')),
    sku text NOT NULL,
    old_name text,
    old_price integer CHECK (old_price > 0),
    new_name text,
    new_price integer CHECK (new_price > 0),
    flagged boolean NOT NULL,
    PRIMARY KEY (preview_id, position),
    UNIQUE (preview_id, sku),
    CHECK ((old_name IS NULL) = (old_price IS NULL)),
    CHECK ((new_name IS NULL) = (new_price IS NULL)),
    CHECK (
        (kind = 'added' AND old_name IS NULL AND new_name IS NOT NULL)
        OR (kind = 'updated' AND old_name IS NOT NULL AND new_name IS NOT NULL)
        OR (kind = 'missing' AND old_name IS NOT NULL AND new_name IS NULL)
    )
);

CREATE INDEX preview_changes_kind_idx ON preview_changes (preview_id, kind, position);

CREATE TABLE preview_issues (
    preview_id uuid NOT NULL REFERENCES previews(id),
    position integer NOT NULL,
    severity text NOT NULL CHECK (severity IN ('error', 'warning')),
    code text NOT NULL,
    message text NOT NULL,
    line integer,
    sku text,
    PRIMARY KEY (preview_id, position)
);

CREATE TABLE catalog_events (
    revision bigint PRIMARY KEY CHECK (revision > 0),
    kind text NOT NULL CHECK (kind IN ('apply', 'rollback')),
    previous_version_id bigint NOT NULL REFERENCES catalog_versions(id),
    version_id bigint NOT NULL REFERENCES catalog_versions(id),
    preview_id uuid UNIQUE REFERENCES previews(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'apply') = (preview_id IS NOT NULL))
);

WITH initial_version AS (
    INSERT INTO catalog_versions (item_count) VALUES (0) RETURNING id
)
INSERT INTO catalog_state (revision, version_id) SELECT 0, id FROM initial_version;
