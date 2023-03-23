using System;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MySqlConnector;


namespace Twitch_Parser {
    public class DB_Manager {
        MySqlConnection _conn = null;

        public DB_Manager(string _url, string _db_name, string _id, string _pw) {
            if (!this._set_connection_object(_url, _db_name, _id, _pw)) {
                throw new Exception("CONNECTION ERROR");
            }
        }

        /*----------- CHANNEL_INFO_TB ---------------*/
        /*B_LOGIN_PK, B_LANG, D_NAME, B_ID, B_TAGS, THUMB_URL, ADD_AT*/
        ///<summary> channel id list 받기  </summary>
        public List<string> _get_B_LOGIN_PK_lists(){
            _conn.Open();
            string _SQL = "SELECT B_LOGIN_PK FROM CHANNEL_INFO_TB";
            var _cmd = new MySqlCommand(_SQL, this._conn);

            List<string> B_LOGIN_PKs = new() { };
            var _reader = _cmd.ExecuteReader();
            while (_reader.Read()) {
                B_LOGIN_PKs.Add((string)_reader["B_LOGIN_PK"]);
            }
            return B_LOGIN_PKs;
        }

        ///<summary> channel 정보 받기  </summary>
        public CHANNEL_INFO_JAR _get_channel_info(string B_LOGIN_PK) {

            string _SQL = "SELECT * FROM CHANNEL_INFO_TB WHERE B_LOGIN_PK=@B_LOGIN_PK";
            _conn.Open();
            var _cmd = new MySqlCommand(_SQL, this._conn);

            _cmd.Parameters.AddWithValue("@B_LOGIN_PK", B_LOGIN_PK);
            var _reader = _cmd.ExecuteReader();

            if (!_reader.Read()) {
                _conn.Close();
                return null;
            }

            CHANNEL_INFO_JAR _jar = new();
            _jar.B_LOGIN_PK = (string)_reader["B_LOGIN_PK"];
            _jar.B_LANG = (string)_reader["B_LANG"];
            _jar.D_NAME = (string)_reader["D_NAME"];
            _jar.B_ID = (int)_reader["B_ID"];
            _jar.B_TAGS = JArray.Parse((string)_reader["B_TAGS"]).ToObject<List<string>>();
            _jar.THUMB_URL = (string)_reader["THUMB_URL"];
            _jar.ADD_AT = (DateTime)_reader["ADD_AT"];

            _conn.Close();
            return _jar;
        }

        ///<summary> channel 정보를 display_name으로 받기 </summary>
        public Dictionary<string, object> _get_channel_for_D_NAME (string D_NAME) {
            return null;
        }

        ///<summary> 해당 언어 코드의 모든 유저 받기 -> B_LOGIN_PK List </summary>
        public Dictionary<string, object> _get_channel_for_B_LANG(string B_LANG) {
            return null;
        }

        ///<summary> 채널 추가용 </summary>
        public Dictionary<string, object> _insert_channel(CHANNEL_INFO_JAR _jar) {
            return null;
        }

        ///<summary> 채널 업데이트용 </summary>
        public Dictionary<string, object> _update_channel(CHANNEL_INFO_JAR _jar) {
            return null;
        }

        ///<summary> 채널 삭제용 </summary>
        public Dictionary<string, object> _delete_channel(string B_LOGIN_PK) {
            return null;
        }


        /*----------- STREAM_STATUS_TB ---------------*/
        /* B_LOGIN_PK, B_ID, G_ID, G_NAME, TITLE, VIEW_COUNT, START_AT, THUMBIMG */

        ///<summary> 방송중인 모든 유저들 가져오기 </summary>
        public Dictionary<string, object> get_stream_user_lists() {
            return null;
        }

        ///<summary> 방송 정보 얻기 </summary>
        public Dictionary<string, object> get_user_streaming_info( string B_LOGIN_PK ) {
            return null;
        }

        ///<summary> 방송 정보 추가 </summary>
        public Dictionary<string, object> insert_streaming_data(STREAM_STATUS_JAR _jar) {
            return null;
        }

        ///<summary> 방송 정보 업데이트 </summary>
        public Dictionary<string, object> update_streaming_data(STREAM_STATUS_JAR _jar) {
            return null;
        }

        ///<summary> 방송 정보 삭제 </summary>
        public Dictionary<string, object> delete_streaming_data(string B_LOGIN_PK) {
            return null;
        }




        // 첫 Connection 테스트를 위한 연결 
        private bool _set_connection_object(string _url, string _db_name, string _id, string _pw) {
            // default connection setting (port)
            string _url_parse = _url;
            int port_parse = 3306;
            if (_url.Contains(':')) {
                _url_parse = _url.Split(':')[0];
                port_parse = int.Parse(_url.Split(':')[1]);
            }

            // connection information setup
            string _conn_str =      "server=" + _url_parse;
            _conn_str +=            ";port=" + port_parse.ToString();
            _conn_str +=            ";database=" + _db_name;
            _conn_str +=            ";user=" + _id;
            _conn_str +=            ";password=" + _pw;

            // sql connection
            try {
                MySqlConnection _sql_conn = new MySqlConnection(_conn_str);
                                _sql_conn.Open();
                this._conn =    _sql_conn;
                                _sql_conn.Close();
                return true;
            } catch (Exception e) {
                Console.WriteLine(e.ToString());
                return false;
            }
        }

        /// <summary> 리턴 타입 정규화(?) 적으로 하기 위한 함수임 </summary>
        private Dictionary<string, dynamic> _get_result_map(string type, object value, string description = "") {
            return new Dictionary<string, dynamic>() { {"TYPE" , type }, {"VALUE" , value }, {"DESCRIPTION" , description } };
        }
    }
}