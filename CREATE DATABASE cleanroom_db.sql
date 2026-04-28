CREATE DATABASE cleanroom_db
  WITH OWNER = postgres
  ENCODING = 'UTF8'
  LC_COLLATE = 'en_IN'
  LC_CTYPE = 'en_IN'
  TEMPLATE = template0
  CONNECTION LIMIT 50;

\c cleanroom_db;

CREATE TABLE IF NOT EXISTS cleanroom_logs (
  id SERIAL PRIMARY KEY,
  particles INT NOT NULL,
  temperature REAL NOT NULL,
  humidity REAL NOT NULL,
  pressure REAL NOT NULL,
  door_status TEXT NOT NULL,
  user_id TEXT NOT NULL,
  attack_status TEXT NOT NULL DEFAULT 'NORMAL',
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cleanroom_time ON cleanroom_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_cleanroom_particles ON cleanroom_logs (particles);
CREATE INDEX IF NOT EXISTS idx_cleanroom_attack ON cleanroom_logs (attack_status);
