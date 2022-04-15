require "http/client"
require "json"

require "./utils"

private class DeviceAuthorizationPayload
  include JSON::Serializable

  # A long string used to verify the session between the client app and the authorization server.
  # The client app uses this parameter to request the access token from the authorization server.
  property device_code : String
  # A short string shown to the user that's used to identify the session on a secondary device.
  # This string is entered by user at `verification_uri`.
  property user_code : String
  # The URI the user should go to with the `user_code` in order to sign in.
  property verification_uri : String
  # The number of seconds before the `device_code` and `user_code` expire.
  property expires_in : Int32
  # The number of seconds the client app should wait between polling requests.
  property interval : Int32

  def to_s(io)
    gen_to_s(io)
  end
end

private class UserAuthenticationErrorPayload
  include JSON::Serializable

  # Expected values:
  # - `authorization_pending`
  #    - Description: The user hasn't finished authenticating, but hasn't canceled the flow.
  #    - Client app should: Repeat the request after at least interval seconds.
  # - `authorization_declined`
  #    - Description: The end user denied the authorization request.
  #    - Client app should: Stop polling, and revert to an unauthenticated state.
  # - `bad_verification_code`
  #    - Description: The device_code sent to the /token endpoint wasn't recognized.
  #    - Client app should: Verify that the client is sending
  #      the correct device_code in the request.
  # - `expired_token`
  #   - Description: At least expires_in seconds have passed,
  #     and authentication is no longer possible with this device_code.
  #   - Client app should: Stop polling, and revert to an unauthenticated state.
  property error : String
  property error_description : String

  def to_s(io)
    gen_to_s(io)
  end
end

private class UserAuthenticationPayload
  include JSON::Serializable

  # Space separated strings. Lists of scopes the access token is valid for.
  property scope : String
  # Number of seconds before the included access token is valid for.
  property expires_in : Int32
  # Issued for the scopes that were requested.
  property access_token : String
  # JWT. Issued if the original scope parameter included the openid scope.
  property id_token : String?
  # Issued if the original scope parameter included offline_access.
  property refresh_token : String?

  def to_s(io)
    gen_to_s(io)
  end
end


# Implementation is based on article Microsoft identity platform and the OAuth 2.0
# device authorization grant flow available at
# https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-device-code
class MsLogin
  @device_authorization : DeviceAuthorizationPayload?
  @user_authentication : UserAuthenticationPayload?

  def initialize(@client_id : String, @scopes : Array(String))
    @tenant = "common"
    @login_url = "https://login.microsoftonline.com/#{@tenant}/oauth2/v2.0"
  end

  private def device_authorization
    @device_authorization.try { |x| return x  }
    raise "get_verification_code not called or not succeeded"
  end

  def verification_code
    device_authorization.user_code
  end

  def verification_uri
    device_authorization.verification_uri
  end

  def get_verification_code()
    # Device authorization.

    url = "#{@login_url}/devicecode"
    form = {
      "client_id" => @client_id,
      "scope" => @scopes.join " "
    }
    resp = HTTP::Client.post url, form: form

    @device_authorization = DeviceAuthorizationPayload.from_json resp.body
  end

  def wait_for_access_token()
    url = "#{@login_url}/token"
    form = {
      "client_id" => @client_id,
      "grant_type" => "urn:ietf:params:oauth:grant-type:device_code",
      "device_code" => device_authorization.device_code
    }
    sleep_interval = Math.max device_authorization.interval, 5
    @user_authentication = nil

    while @user_authentication == nil
      sleep sleep_interval

      resp = HTTP::Client.post url, form: form

      if resp.status_code == 400
        authentication_error_resp = UserAuthenticationErrorPayload.from_json resp.body
        if authentication_error_resp.error != "authorization_pending"
          raise "Unrecoverable authentication error: #{authentication_error_resp}"
        end
      else
        @user_authentication = UserAuthenticationPayload.from_json resp.body
      end
    end
  end

  private def user_authentication
    @user_authentication.try { |x| return x }
    raise "wait_for_access_token not called or not succeeded"
  end

  def access_token
    user_authentication.access_token
  end
end

# Example:
#
# client_id = ""  # TODO Fill client id.
# scopes = ["User.Read", "Chat.Read", "Team.ReadBasic.All", "Channel.ReadBasic.All"]
#
# login = MsLogin.new client_id, scopes
# login.get_verification_code
#
# puts "Go to #{login.verification_uri} and enter code #{login.verification_code}"
#
# login.wait_for_access_token()
#
# puts "Got access token #{login.access_token}"
