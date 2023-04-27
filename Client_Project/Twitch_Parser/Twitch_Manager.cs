using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Text;

using HtmlAgilityPack;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Twitch_Parser {
    public class Twitch_Manager {
        private string _client_id = string.Empty;
        private string _client_secret = string.Empty;
        private string _client_token = string.Empty;

        private HttpWebRequest _request = null;
        private CookieContainer _cookie = new CookieContainer();

        public Twitch_Manager(string _client_id, string _client_secret) {
            var _token_result = this._get_new_token(_client_id, _client_secret);
            // 토큰 받아오는거 실패하면 에러 발생 
            if (_token_result["TYPE"] != "OK") {
                throw new Exception((string)_token_result["VALUE"]);
            }

            // 기본 클래스 내부 변수 설정
            this._client_id = _client_id;
            this._client_secret = _client_secret;
            this._client_token = "Bearer ";
            this._client_token += ((JObject)_token_result["VALUE"]).Value<string>("access_token");

        }


        /// <summary> 방송중인지 아닌지 확인하는 용도 </summary>
        public Dictionary<string, dynamic> get_broadcast_streaming(List<string> _user_logins) {
            string url = "https://api.twitch.tv/helix/streams";
            if (_user_logins.Count != 0) {
                url = "https://api.twitch.tv/helix/streams?user_login=";
                url += string.Join("&user_login=", _user_logins);
            }
            // Connection
            var _connection_res = this._get_get_data(url, this._get_auth_header());
            if (_connection_res["TYPE"] != "OK") { return _connection_res; }

            // JObject Checker
            JObject _jobject_value = (JObject)_connection_res["VALUE"];
            if (!_jobject_value.ContainsKey("data")) {
                return this._get_result_map("ERR", "NO_DATA", "NO_DATA");
            }

            JArray _datas = _jobject_value.Value<JArray>("data")!;
            if (_datas.Count == 0) {
                return this._get_result_map("ERR", "NO_DATA", "NO_DATA");
            }

            List<STREAM_STATUS_JAR> _stream_jars = new();
            foreach (JObject i in _datas) {
                _stream_jars.Add(this._convert_stream_status_jar(i));
            }
            return this._get_result_map("OK", (List<STREAM_STATUS_JAR>) _stream_jars);
        }

        /// <summary> 채널 정보 확인하는 용도 -> CHANNEL_INFO_JAR </summary>
        public Dictionary<string, dynamic> get_broadcast_information(string broadcaster_id) {

            // Connection
            string _url = "https://api.twitch.tv/helix/search/channels?query=" + broadcaster_id;
            var _connection_res = this._get_get_data(_url, this._get_auth_header());
            if (_connection_res["TYPE"]!= "OK") { return _connection_res; }

            // JObject Checker
            JObject _jobject_value = (JObject)_connection_res["VALUE"];
            if (!_jobject_value.ContainsKey("data")) {
                return this._get_result_map("ERR", "NO_DATA", "NO_DATA");
            }

            JArray _datas = _jobject_value.Value<JArray>("data");
            if (_datas.Count == 0) {
                return this._get_result_map("ERR", "NO_DATA", "NO_DATA");
            }

            foreach (var i in (JArray)_datas) {
                if (i.Value<string>("broadcaster_login") == broadcaster_id) {
                    return this._get_result_map("OK", this._convert_channel_info_jar((JObject)i));
                }
            }

            return this._get_result_map("ERR", "NO DATA", "NO_DATA");
        }


        /// <summary> 토큰 가져오기 </summary>
        private Dictionary<string, dynamic> _get_new_token(string _client_id, string _client_secret) {
            // 기초 셋팅 
            string _url = "https://id.twitch.tv/oauth2/token";
            var _post_data = new Dictionary<string, dynamic>(){
                { "client_id", _client_id},
                {"client_secret", _client_secret },
                {"grant_type", "client_credentials" }
            };

            // 데이터 전송 -> JObject
            var _post_request = this._get_post_data(_url, _post_data) ;
            if (_post_request["TYPE"] != "OK") { return _post_request; }

            return this._get_result_map("OK", (JObject)_post_request["VALUE"]);

        }
        
        /// <summary> request 변수에 Url 셋팅 </summary>
        private Dictionary<string, dynamic> _set_request_data(string url, bool isPost = false) {
            try {
                this._request = (HttpWebRequest)WebRequest.Create(url);
                this._request.Method = isPost ? "POST" : "GET";
                this._request.ContentType = "application/x-www-form-urlencoded";
                this._request.CookieContainer = this._cookie;
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "CONN_ERR");
            }
            return this._get_result_map("OK", "CONFIRM");
        }

        public Dictionary<string, object> get_broadcast_streaming() {
            return this.get_broadcast_streaming(new List<string>() { });
        }

        /// <summary> auth 전송을 위한 데이터 반환 </summary>
        private Dictionary<string, string> _get_auth_header() {
            return new Dictionary<string, string>() {
                { "Accept" , "*/*"},
                { "client-id", this._client_id},
                {"Authorization", this._client_token}
            };
        }

        /// <summary> dict -> byte[] </summary>
        private byte[] get_convert_post_data(Dictionary<string, dynamic> _dic) {
            string post_Data_string = string.Empty;
            foreach (var i in _dic) {
                post_Data_string += WebUtility.UrlEncode(i.Key.ToString()) +"=" +WebUtility.UrlEncode(i.Value.ToString()) + "&";
            }
            post_Data_string = post_Data_string.Trim('&');
            byte[] data = Encoding.UTF8.GetBytes(post_Data_string);
            return data;
        }

        /// <summary> GET 전송 담당 -> Jobject로 반환 </summary>
        private Dictionary<string, dynamic> _get_get_data(string _url, Dictionary<string ,string> _header) {

            // 헤더 추가
            this._request.Headers.Clear();
            var _set_res = this._set_request_data(_url, false);
            foreach (string i in _header.Keys.ToList<string>()) {
                this._request.Headers.Add(i, _header[i]);
            }

            string h = string.Empty;
            foreach (string i in _request.Headers.Keys) {
            }
            if (_set_res["TYPE"] != "OK"){ return _set_res; }


            // 연결 실패 예외처리 
            HttpWebResponse _response_data;
            try {
                _response_data = (HttpWebResponse)this._request.GetResponse();
                if (_response_data.StatusCode != HttpStatusCode.OK) {
                    return this._get_result_map("ERR", (int)_response_data.StatusCode, "RESPONSE_ERR");
                }
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "RESPONSE_ERR");
            }

            StreamReader _stream = new StreamReader(_response_data.GetResponseStream());
            string html = _stream.ReadToEnd();

            // convert jobject
            var jobject_convert_res = this._get_converted_object_to_jobject(html);
            if (jobject_convert_res["TYPE"] != "OK") {
                return jobject_convert_res;
            }

            return this._get_result_map("OK", (JObject) jobject_convert_res["VALUE"]);
        }

        /// <summary> POST 전송 담당 -> jobject로 반환 </summary>
        private Dictionary<string, object> _get_post_data(string _url, Dictionary<string, object> _data) {
            _set_request_data(_url, true);
            byte[] post_byte_data = this.get_convert_post_data(_data);
            this._request.ContentLength = post_byte_data.Length;
            using (Stream stream = this._request.GetRequestStream()) {
                stream.Write(post_byte_data, 0, post_byte_data.Length);
            }
            HttpWebResponse _response = null;
            try {
                var _res = this._request.GetResponse();
                _response = (HttpWebResponse)_res;
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), description:"RESPONSE_ERR");
            }
            StreamReader _reader = new StreamReader( _response.GetResponseStream());
            string _res_data = _reader.ReadToEnd();
            var _json_convert = this._get_converted_object_to_jobject(_res_data);
            if (_json_convert["TYPE"] != "OK") { return _json_convert; };


            return this._get_result_map("OK", (JObject) _json_convert["VALUE"]);
            
        }


        /// <summary> JSON_STRING to jobject </summary>
        private Dictionary<string, object> _get_converted_object_to_jobject(string json_string) {
            try { return this._get_result_map("OK", JObject.Parse(json_string)); }
            catch (Exception e) { return this._get_result_map("ERR", e.ToString(), "CONVERT ERR"); }
        }

        /// <summary> dict -> jobject </summary>
        private object _get_converted_object_to_jobject(Dictionary<string, object> _dict) {
            try { return this._get_result_map("OK", JObject.Parse(JsonConvert.SerializeObject(_dict))); }
            catch(Exception e) { return this._get_result_map("ERR", e.ToString(), "CONVERT ERR"); }
        }

        ///<summary> JObject -> CHANNEL_INFO_JAR </summary>
        private CHANNEL_INFO_JAR _convert_channel_info_jar(JObject j) {
            List<string> _contain_keys = new() {
                "broadcaster_language",     "broadcaster_login",
                "display_name",             "id",
                "tags",                     "thumbnail_url"
            };

            foreach(string i in _contain_keys) {
                if (!j.ContainsKey(i)) {
                    throw new Exception(string.Format("{0} is not exist in JObject", i));
                }
            }

            var _channel_jar = new CHANNEL_INFO_JAR();
            _channel_jar.B_TAGS         = j.Value<JArray>("tags").ToObject<List<string>>();
            _channel_jar.B_LANG         = j.Value<string>("broadcaster_language");
            _channel_jar.B_LOGIN_PK     = j.Value<string>("broadcaster_login");
            _channel_jar.THUMB_URL      = j.Value<string>("thumbnail_url");
            _channel_jar.D_NAME         = j.Value<string>("display_name");
            _channel_jar.B_ID           = j.Value<int>("id");

            return _channel_jar;

        }

        ///<summary> JObject -> Stream_JAR </summary>
        private STREAM_STATUS_JAR _convert_stream_status_jar(JObject j) {

            foreach(string i in new List<string>() {
                "user_login", "user_id", "game_id", "game_name", "title", "viewer_count", "started_at", "thumbnail_url"
            }) {
                if (!j.ContainsKey(i)) {
                    throw new Exception(string.Format("{0} is not exist in JObject", i));
                }
            }

            var _stream_jar = new STREAM_STATUS_JAR();
            _stream_jar.START_AT =      DateTime.Parse(j.Value<string>("started_at"));
            _stream_jar.THUMB_IMG =     j.Value<string>("thumbnail_url");
            _stream_jar.B_LOGIN_PK =    j.Value<string>("user_login");
            _stream_jar.G_NAME =        j.Value<string>("game_name");
            _stream_jar.VIEW_COUNT =    j.Value<int>("viewer_count");
            _stream_jar.TITLE =         j.Value<string>("title");
            _stream_jar.B_ID =          j.Value<int>("user_id");
            _stream_jar.G_ID =          j.Value<int>("game_id");
            _stream_jar.UPDATE_AT =     DateTime.Now;

            return _stream_jar;
        }

        private Dictionary<string, dynamic> _get_result_map(string type, object value, string description = "") {
            return new Dictionary<string, dynamic>() { {"TYPE" , type }, {"VALUE" , value }, {"DESCRIPTION" , description } };
        }
    }
}

