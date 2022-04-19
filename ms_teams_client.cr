require "http/client"
require "json"
require "log"

require "./utils"

alias JSONObject = Hash(String, JSON::Any)

private class GraphPayload
  include JSON::Serializable

  def initialize()
    @odata_context = ""
    @odata_count = 0
    @value = [] of JSONObject
  end

  @[JSON::Field(key: "@odata.context")]
  property odata_context : String
  @[JSON::Field(key: "@odata.count")]
  property odata_count : Int32
  @[JSON::Field(key: "@odata.nextLink")]
  property odata_next_link : String?
  @[JSON::Field(key: "@odata.deltaLink")]
  property odata_delta_link : String?
  property value : Array(JSONObject)

  def to_s(io)
    gen_to_s(io)
  end
end

macro def_desc_type(name)
  class {{name.id}}Desc
    def initialize(@json : JSONObject)
      if json["id"]?.try &.as_s? .nil?
        raise "#{ {{name}} } without id: #{json}"
      end
    end

    def json
      @json
    end

    def id
      @json["id"].as_s
    end

    def to_s(io)
      gen_to_s(io)
    end
  end
end

def_desc_type(:Team)
def_desc_type(:Channel)
def_desc_type(:Chat)
def_desc_type(:Member)
def_desc_type(:Message)
def_desc_type(:Reply)

class MsTeamsClient
  @graph_url = "https://graph.microsoft.com/beta"

  def initialize(access_token : String)
    @headers = HTTP::Headers { "Authorization" => "bearer #{access_token}" }
  end

  private def get_payload(url)
    min_retry_after = 2
    max_retry_after = 300
    retry_after = 0

    while true
      resp = HTTP::Client.get url, headers: @headers
      case resp.status_code
      when 200
        return GraphPayload.from_json resp.body
      when 429
        # Prefer retry-after from HTTP header, otherwise implement exponential backoff.
        retry_after = Math.max min_retry_after, (Math.min max_retry_after, (retry_after * 2))
        retry_after = resp.headers["Retry-After"]?.try &.to_i || retry_after

        Log.info { "Too many requests waiting for #{retry_after} seconds" }
        sleep retry_after
      when 403
        # Not raising here makes client usage simpler.
        Log.info { "Access to resource is forbidden" }
        return GraphPayload.new
      else
        raise "Unexpected HTTP response: #{resp.inspect}"
      end
    end
  end

  private macro get_items(item_type, items_name, all_link, delta_link)
    items = Hash(String, {{item_type}}).new
    visited_urls = Set(String).new
    url = {{delta_link}} || {{all_link}}

    while !url.nil?
      if !visited_urls.add? url
        raise "Url was already visited when getting #{ {{items_name}} }: #{url}"
      end

      payload = get_payload url

      new_items = payload.value.map { |json| {{item_type}}.new(json) }
      really_new = 0
      new_items.each do |item|
        if items.has_key? item.id
          Log.warn { "Duplicate #{ {{items_name}} }: old #{items[item.id]}, new: #{item}" }
        end

        items[item.id] = item
      end

      url = payload.odata_next_link
      delta_link = payload.odata_delta_link

      if url && delta_link
        raise "Both next link and delta link are present: #{payload}"
      end
    end

    { {{items_name.id}}: items, delta_link: delta_link }
  end

  def list_teams(delta_link : String? = nil) : {teams: Hash(String, TeamDesc), delta_link: String?}
    all_link = "#{@graph_url}/teams"
    get_items(TeamDesc, :teams, all_link, delta_link)
  end

  def list_channels(
    team_id : String,
    delta_link : String? = nil
  ) : {channels: Hash(String, ChannelDesc), delta_link: String?}
    all_link = "#{@graph_url}/teams/#{team_id}/channels"
    get_items(ChannelDesc, :channels, all_link, delta_link)
  end

  def list_members_of_channel(
    team_id : String,
    channel_id : String,
    delta_link : String? = nil
  ) : {members: Hash(String, MemberDesc), delta_link: String?}
    all_link = "#{@graph_url}/teams/#{team_id}/channels/#{channel_id}/members"
    get_items(MemberDesc, :members, all_link, delta_link)
  end

  def list_messages_in_channel(
    team_id : String,
    channel_id : String,
    delta_link : String? = nil
  ) : {messages: Hash(String, MessageDesc), delta_link: String?}
    all_link = "#{@graph_url}/teams/#{team_id}/channels/#{channel_id}/messages"
    get_items(MessageDesc, :messages, all_link, delta_link)
  end

  def list_replies_to_message_in_channel(
    team_id : String,
    channel_id : String,
    message_id : String,
    delta_link : String? = nil
  ) : {replies: Hash(String, ReplyDesc), delta_link: String?}
    all_link = "#{@graph_url}/teams/#{team_id}/channels/#{channel_id}/messages/#{message_id}/replies"
    get_items(ReplyDesc, :replies, all_link, delta_link)
  end

  def list_chats(delta_link : String? = nil) : {chats: Hash(String, ChatDesc), delta_link: String?}
    all_link = "#{@graph_url}/chats"
    get_items(ChatDesc, :chats, all_link, delta_link)
  end

  def list_members_of_chat(
    chat_id : String,
    delta_link : String? = nil
  ) : {members: Hash(String, MemberDesc), delta_link: String?}
    all_link = "#{@graph_url}/chats/#{chat_id}/members"
    get_items(MemberDesc, :members, all_link, delta_link)
  end

  def list_messages_in_chat(
    chat_id : String,
    delta_link : String? = nil
  ) : {messages: Hash(String, MessageDesc), delta_link: String?}
    all_link = "#{@graph_url}/chats/#{chat_id}/messages"
    get_items(MessageDesc, :messages, all_link, delta_link)
  end
end
