//! Embedded, checksum-verified chat-history migrations.

#[derive(Clone, Copy)]
pub(crate) struct Migration {
    pub(crate) version: i64,
    pub(crate) name: &'static str,
    pub(crate) checksum: &'static str,
    pub(crate) sql: &'static str,
}

pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_chat_history_tables",
        checksum: "494f9129f53ee7d0e7ac46434341cae8cfb84960fe5543ba9091ab894febebe2",
        sql: r#"
CREATE TABLE chats (
  id TEXT PRIMARY KEY CHECK(length(id) = 32 AND id = lower(id)),
  title TEXT NOT NULL CHECK(length(title) BETWEEN 1 AND 160 AND instr(title, char(0)) = 0),
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms)
) STRICT;
CREATE TABLE turns (
  id TEXT PRIMARY KEY CHECK(length(id) = 32 AND id = lower(id)),
  chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
  state TEXT NOT NULL CHECK(state IN ('queued', 'streaming', 'completed', 'cancelled', 'failed')),
  model_alias TEXT NOT NULL CHECK(model_alias = 'loxa'),
  recipe_id TEXT NOT NULL,
  engine_name TEXT,
  engine_version TEXT,
  error_code TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
  UNIQUE(chat_id, ordinal)
) STRICT;
CREATE TABLE messages (
  id TEXT PRIMARY KEY CHECK(length(id) = 32 AND id = lower(id)),
  turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
  content TEXT NOT NULL CHECK(instr(content, char(0)) = 0 AND length(CAST(content AS BLOB)) <= 2097152),
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
  UNIQUE(turn_id, role)
) STRICT;
CREATE INDEX chats_recent ON chats(updated_at_ms DESC, id DESC);
CREATE INDEX turns_by_chat ON turns(chat_id, ordinal);
"#,
    },
    Migration {
        version: 2,
        name: "index_messages_by_turn",
        checksum: "4a72334ae7d38bfd22c1f575ddb1c6d42863ff3a51b56b637970c9d1b4199539",
        sql: r#"
CREATE INDEX messages_by_turn ON messages(turn_id, role);
"#,
    },
];
