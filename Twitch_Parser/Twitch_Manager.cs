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
            if (this._get_result_type(_token_result) != "OK") {
                Console.WriteLine(_token_result);
                return;
            }
            this._client_id = _client_id;
            this._client_secret = _client_secret;
            this._client_token = "Bearer ";
            this._client_token += ((JObject)this._get_result_value(_token_result)).Value<string>("access_token");

        }

        /// <summary> 토큰 가져오기 </summary>
        private object _get_new_token(string _client_id, string _client_secret) {
            // 기초 셋팅 
            string _url = "https://id.twitch.tv/oauth2/token";
            var _post_data = new Dictionary<string, dynamic>(){
                { "client_id", _client_id},
                {"client_secret", _client_secret },
                {"grant_type", "client_credentials" }
            };

            // 데이터 전송 -> JObject
            var _post_request = this._get_post_data(_url, _post_data) ;
            if (_get_result_type(_post_request) != "OK") { return _post_request; }

            return this._get_result_message("OK", (JObject)_get_result_value(_post_request));

        }
        
        /// <summary> request 변수에 Url 셋팅 </summary>
        private object _set_request_data(string url, bool isPost = false) {
            try {
                this._request = (HttpWebRequest)WebRequest.Create(url);
                this._request.Method = isPost ? "POST" : "GET";
                this._request.ContentType = "application/x-www-form-urlencoded";
                this._request.CookieContainer = this._cookie;
            } catch (Exception e) {
                return this._get_result_message("ERR", e.ToString(), "CONN_ERR");
            }
            return this._get_result_message("OK", "CONFIRM");
        }

        /// <summary> 방송중인지 아닌지 확인하는 용도 </summary>
        public object get_broadcast_streaming(List<string> _user_logins, bool test = false) {

            string url = "https://api.twitch.tv/helix/streams?user_login=";
            url += string.Join("&user_login=", _user_logins);
            if (test) {
                url = "https://api.twitch.tv/helix/streams";
            }
            // Connection
            var _connection_res = this._get_get_data(url, this._get_auth_header());
            if (this._get_result_type(_connection_res) != "OK") { return _connection_res; }

            // JObjec Checker
            JObject _jobject_value = (JObject)this._get_result_value(_connection_res);
            if (!_jobject_value.ContainsKey("data")) {
                return this._get_result_message("ERR", "NO_DATA", "NO_DATA");
            }

            JArray _datas = _jobject_value.Value<JArray>("data");
            if (_datas.Count == 0) {
                return this._get_result_message("ERR", "NO_DATA", "NO_DATA");
            }

            return this._get_result_message("OK", (JArray)_datas);
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
        private object _get_get_data(string _url, Dictionary<string ,string> _header) {

            // 헤더 추가
            this._request.Headers.Clear();
            object _set_res = this._set_request_data(_url, false);
            foreach (string i in _header.Keys.ToList<string>()) {
                this._request.Headers.Add(i, _header[i]);
            }
            string h = string.Empty;
            foreach (string i in _request.Headers.Keys) {
            }
            if (this._get_result_type(_set_res) != "OK"){ return _set_res; }


            // 연결 실패 예외처리 
            HttpWebResponse _response_data;
            try {
                _response_data = (HttpWebResponse)this._request.GetResponse();
                if (_response_data.StatusCode != HttpStatusCode.OK) {
                    return this._get_result_message("ERR", (int)_response_data.StatusCode, "RESPONSE_ERR");
                }
            } catch (Exception e) {
                return this._get_result_message("ERR", e.ToString(), "RESPONSE_ERR");
            }

            StreamReader _stream = new StreamReader(_response_data.GetResponseStream());
            string html = _stream.ReadToEnd();

            // convert jobject
            var jobject_convert_res = this._get_converted_object_to_jobject(html);
            if (_get_result_type(jobject_convert_res) != "OK") {
                return jobject_convert_res;
            }

            return this._get_result_message("OK", (JObject) _get_result_value(jobject_convert_res));


        }

        /// <summary> POST 전송 담당 -> jobject로 반환 </summary>
        private object _get_post_data(string _url, Dictionary<string, dynamic> _data) {
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
                return this._get_result_message("ERR", e.ToString(), description:"RESPONSE_ERR");
            }
            StreamReader _reader = new StreamReader( _response.GetResponseStream());
            string _res_data = _reader.ReadToEnd();
            var _json_convert = this._get_converted_object_to_jobject(_res_data);
            if (this._get_result_type(_json_convert) != "OK") { return _json_convert; }


            return this._get_result_message("OK", (JObject) this._get_result_value(_json_convert));
            
        }


        /// <summary> JSON_STRING to jobject </summary>
        private object _get_converted_object_to_jobject(string json_string) {
            try { return this._get_result_message("OK", JObject.Parse(json_string)); }
            catch (Exception e) { return this._get_result_message("ERR", e.ToString(), "CONVERT ERR"); }
        }

        /// <summary> dict -> jobject </summary>
        private object _get_converted_object_to_jobject(Dictionary<string, dynamic> _dict) {
            try { return this._get_result_message("OK", JObject.Parse(JsonConvert.SerializeObject(_dict))); }
            catch(Exception e) { return this._get_result_message("ERR", e.ToString(), "CONVERT ERR"); }
        }

        private object _get_result_message(string type, object _value, string description="") { return new { TYPE = type, VALUE =_value, DESCRIPTION = description}; }
        private string _get_result_type(object _res_object) { return _res_object.GetType().GetProperty("TYPE").GetValue(_res_object, null).ToString(); }
        private object _get_result_value(object _res_object) { return _res_object.GetType().GetProperty("VALUE").GetValue(_res_object, null); }
        private string _get_result_description(object _res_object) { return _res_object.GetType().GetProperty("DESCRIPTION").GetValue(_res_object, null).ToString(); }
    }
}

