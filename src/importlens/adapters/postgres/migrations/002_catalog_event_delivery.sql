CREATE TABLE catalog_event_delivery (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    stream_id uuid NOT NULL DEFAULT gen_random_uuid(),
    last_published_revision bigint NOT NULL DEFAULT 0 CHECK (last_published_revision >= 0)
);

INSERT INTO catalog_event_delivery DEFAULT VALUES;
