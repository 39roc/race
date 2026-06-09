CREATE TABLE IF NOT EXISTS events (
  id      TEXT PRIMARY KEY,
  name    TEXT NOT NULL,
  stock   INTEGER NOT NULL CHECK (stock >= 0),
  version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reservations (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id   TEXT NOT NULL REFERENCES events(id),
  user_id    TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
