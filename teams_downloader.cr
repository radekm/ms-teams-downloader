require "env"
require "log"

require "sqlite3"

require "./ms_login"
require "./ms_teams_client"

client_id = ENV["TEAMS_CLIENT_ID"]? || raise "Missing environment variable TEAMS_CLIENT_ID"
scopes = ["User.Read", "Chat.Read", "Team.ReadBasic.All", "Channel.ReadBasic.All"]
db_url = "sqlite3://./ms_teams.db"

sql_create_tables = [
  <<-SQL,
  CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY,
    ms_team_id TEXT NOT NULL,
    ms_channel_id TEXT NOT NULL,
    team_name TEXT NOT NULL,
    channel_name TEXT NOT NULL,
    channel_json TEXT NOT NULL,
    -- Date `YYYY-MM-DD` when channel messages were downloaded or empty string.
    last_download TEXT NOT NULL DEFAULT '',
    deleted INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT ak__channels
      UNIQUE (ms_team_id, ms_channel_id)
  )
  SQL
  <<-SQL,
  CREATE TABLE IF NOT EXISTS channel_messages (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    ms_message_id TEXT NOT NULL,
    message_json TEXT NOT NULL,
    replies_json TEXT NOT NULL,  -- JSON array
    CONSTRAINT ak__channel_messages
      UNIQUE (channel_id, ms_message_id),
    CONSTRAINT fk__channel_messages__channels
      FOREIGN KEY (channel_id)
      REFERENCES "channels" (id)
      ON DELETE CASCADE
  )
  SQL
  <<-SQL,
  CREATE TABLE IF NOT EXISTS chats (
    id INTEGER PRIMARY KEY,
    ms_chat_id TEXT NOT NULL,
    chat_name TEXT NOT NULL,
    chat_json TEXT NOT NULL,
    members_json TEXT NOT NULL,
    -- Date `YYYY-MM-DD` when chat messages were downloaded or empty string.
    last_download TEXT NOT NULL DEFAULT '',
    CONSTRAINT ak__chats
      UNIQUE (ms_chat_id)
  )
  SQL
  <<-SQL
  CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    ms_message_id TEXT NOT NULL,
    message_json TEXT NOT NULL,
    CONSTRAINT ak__chat_messages
      UNIQUE (chat_id, ms_message_id),
    CONSTRAINT fk__chat_messages__chats
      FOREIGN KEY (chat_id)
      REFERENCES "chats" (id)
      ON DELETE CASCADE
  )
  SQL
]

def download_channels(db, client : MsTeamsClient)
  db.transaction do |tx|
    # We mark all existing channels as deleted.
    # Channels which are then returned by API are marked as non-deleted.
    con = tx.connection
    con.exec "UPDATE channels SET deleted = 1"

    res = client.list_teams
    res[:teams].each_value do |team|
      res = client.list_channels team.id
      res[:channels].each_value do |ch|
        Log.info { "Found channel #{ch.display_name} in team #{team.display_name}" }

        channel_json = ch.json.to_json

        sql = "
          INSERT INTO channels
            (ms_team_id, ms_channel_id, team_name, channel_name, channel_json)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (ms_team_id, ms_channel_id) DO UPDATE SET
            team_name = ?,
            channel_name = ?,
            channel_json = ?,
            deleted = 0
          WHERE ms_team_id = ? AND ms_channel_id = ?
        "
        args = [] of DB::Any
        # Insert.
        args << team.id
        args << ch.id
        args << team.display_name
        args << ch.display_name
        args << channel_json
        # Update.
        args << team.display_name
        args << ch.display_name
        args << channel_json
        # Where.
        args << team.id
        args << ch.id

        con.exec sql, args: args
      end
    end
  end
end

def download_messages_in_channel(
  db,
  client : MsTeamsClient,
  channel_id : Int32,
  ms_team_id : String,
  ms_channel_id : String
)
  n_messages = 0
  n_replies = 0

  res = client.list_messages_in_channel(ms_team_id, ms_channel_id)
  res[:messages].each_value do |message|
    res = client.list_replies_to_message_in_channel(ms_team_id, ms_channel_id, message.id)

    message_json = message.json.to_json
    replies_json = res[:replies].values
      .sort_by! { |reply| reply.created_date_time }
      .map { |reply| reply.json }
      .to_json

    sql = "
      INSERT INTO channel_messages
        (channel_id, ms_message_id, message_json, replies_json)
      VALUES (?, ?, ?, ?)
      ON CONFLICT (channel_id, ms_message_id) DO UPDATE SET
        message_json = ?,
        replies_json = ?
      WHERE channel_id = ? AND ms_message_id = ?
    "
    args = [] of DB::Any
    # Insert.
    args << channel_id
    args << message.id
    args << message_json
    args << replies_json
    # Update.
    args << message_json
    args << replies_json
    # Where.
    args << channel_id
    args << message.id

    db.exec sql, args: args

    n_messages += 1
    n_replies += res[:replies].size
  end

  Log.info { "Downloaded #{n_messages} messages and #{n_replies} replies" }
end

def download_messages_in_channels(
  db,
  client : MsTeamsClient,
  skip_if_last_download_after : Time
)
  time_format = "%Y-%m-%d"
  new_last_download = Time.utc.to_s time_format
  channels = [] of {Int32, String, String, String, String}

  # It's important that empty string (default value of `last_download`)
  # is smaller than all `YYYY-MM-DD` dates.
  # So channels where messages were not downloaded are returned.
  sql = "
    SELECT id, ms_team_id, ms_channel_id, team_name, channel_name
    FROM channels
    WHERE last_download <= ? AND deleted = 0
  "
  db.query sql, args: [skip_if_last_download_after.to_s time_format] do |rs|
    rs.each do
      channels << {rs.read(Int32), rs.read(String), rs.read(String),
                   rs.read(String), rs.read(String)}
    end
  end

  channels.each do |channel_id, ms_team_id, ms_channel_id, team_name, channel_name|
    Log.info { "Downloading messages from channel #{channel_name} in team #{team_name}" }

    download_messages_in_channel(db, client, channel_id, ms_team_id, ms_channel_id)

    args = [] of DB::Any
    args << new_last_download
    args << channel_id
    db.exec "UPDATE channels SET last_download = ? WHERE id = ?", args: args
  end
end

def download_chats(db, client : MsTeamsClient)
  res = client.list_chats
  res[:chats].each_value do |ch|
    res = client.list_members_of_chat ch.id

    chat_name = ch.topic || res[:members].values.map { |member| member.display_name }.join ", "

    Log.info { "Found chat #{chat_name} (chat id #{ch.id})" }

    chat_json = ch.json.to_json
    members_json = "[]"
    members_json = res[:members].values
      .map { |member| member.json }
      .to_json

    sql = "
      INSERT INTO chats
        (ms_chat_id, chat_name, chat_json, members_json)
      VALUES (?, ?, ?, ?)
      ON CONFLICT (ms_chat_id) DO UPDATE SET
        chat_name = ?,
        chat_json = ?,
        members_json = ?
      WHERE ms_chat_id = ?
    "
    args = [] of DB::Any
    # Insert.
    args << ch.id
    args << chat_name
    args << chat_json
    args << members_json
    # Update.
    args << chat_name
    args << chat_json
    args << members_json
    # Where.
    args << ch.id

    db.exec sql, args: args
  end
end

def download_messages_in_chat(
  db,
  client : MsTeamsClient,
  chat_id : Int32,
  ms_chat_id : String
)
  n_messages = 0

  res = client.list_messages_in_chat ms_chat_id
  res[:messages].each_value do |message|
    message_json = message.json.to_json

    sql = "
      INSERT INTO chat_messages
        (chat_id, ms_message_id, message_json)
      VALUES (?, ?, ?)
      ON CONFLICT (chat_id, ms_message_id) DO UPDATE SET
        message_json = ?
      WHERE chat_id = ? AND ms_message_id = ?
    "
    args = [] of DB::Any
    # Insert.
    args << chat_id
    args << message.id
    args << message_json
    # Update.
    args << message_json
    # Where.
    args << chat_id
    args << message.id

    db.exec sql, args: args

    n_messages += 1
  end

  Log.info { "Downloaded #{n_messages} messages" }
end

def download_messages_in_chats(
  db,
  client : MsTeamsClient,
  skip_if_last_download_after : Time
)
  time_format = "%Y-%m-%d"
  new_last_download = Time.utc.to_s time_format
  chats = [] of {Int32, String, String}

  # It's important that empty string (default value of `last_download`)
  # is smaller than all `YYYY-MM-DD` dates.
  # So chats where messages were not downloaded are returned.
  sql = "
    SELECT id, ms_chat_id, chat_name
    FROM chats
    WHERE last_download <= ?
  "
  db.query sql, args: [skip_if_last_download_after.to_s time_format] do |rs|
    rs.each do
      chats << {rs.read(Int32), rs.read(String), rs.read(String)}
    end
  end

  chats.each do |chat_id, ms_chat_id, chat_name|
    Log.info { "Downloading messages from chat #{chat_name} (chat id #{ms_chat_id})" }

    download_messages_in_chat(db, client, chat_id, ms_chat_id)

    args = [] of DB::Any
    args << new_last_download
    args << chat_id
    db.exec "UPDATE chats SET last_download = ? WHERE id = ?", args: args
  end
end

login = MsLogin.new client_id, scopes
login.get_verification_code

puts "Go to #{login.verification_uri} and enter code #{login.verification_code}"

login.wait_for_access_token()

client = MsTeamsClient.new(login.access_token)

DB.open db_url do |db|
  sql_create_tables.each do |sql|
    db.exec sql
  end

  skip_if_last_download_after = Time.utc(2022, 5, 6)

  download_channels(db, client)
  download_messages_in_channels(db, client, skip_if_last_download_after)

  download_chats(db, client)
  download_messages_in_chats(db, client, skip_if_last_download_after)
end
