using System;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MySql.Data.MySqlClient;       // for Connecting MySQL(Maria DB)
using MySqlX.XDevAPI.Common;


namespace Twitch_Parser {
    public class DB_Manager {
        MySqlConnection _conn = null;

        public DB_Manager(string _url, string _db_name, string _id, string _pw) {
            if (!this._set_connection_object(_url, _db_name, _id, _pw)) {
                throw new Exception("CONNECTION ERROR");
            }
        }

        // Connection 연결 
        private bool _set_connection_object(string _url, string _db_name, string _id, string _pw) {
            string _url_parse = _url;
            int port_parse = 3306;
            if (_url.Contains(':')) {
                _url_parse = _url.Split(':')[0];
                port_parse = int.Parse(_url.Split(':')[1]);
            }
            string _conn_str = "Server=" + _url_parse;
            _conn_str += ";Port=" + port_parse.ToString();
            _conn_str += ";Database=" + _db_name;
            _conn_str += ";Uid=" + _id;
            _conn_str += ";Pwd=" + _pw;

            try {
                MySqlConnection _sql_conn = new MySqlConnection(_conn_str);
                _sql_conn.Open();
                this._conn = _sql_conn;
                _sql_conn.Close();
                return true;
            } catch (Exception e) {
                Console.WriteLine(e.ToString());
                return false;
            }
        }

        /// <summary> 디비에 채널이 존재하는지 확인용 </summary>
        public Dictionary<string, object> _get_channel_info(string user_login) {
            string sql = string.Format("SELECT * FROM CHANNEL_INFO_TB WHERE B_LOGIN_PK='{0}'", user_login);

            this._conn.Open();
            MySqlCommand _cmd;
            MySqlDataReader _table;
            try {
                _cmd = new MySqlCommand(sql, _conn);
                _table = _cmd.ExecuteReader();
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR");
            }

            List<CHANNEL_INFO_JAR> _results = new();

            // CHANNEL_INFO_TB 작업용 액션 
            var ac = new Action<MySqlDataReader>(delegate (MySqlDataReader _reader) {
                var _jar = new CHANNEL_INFO_JAR();
                _jar.B_LOGIN_PK = (string)_reader["B_LOGIN_PK"];
                _jar.B_LANG = (string)_reader["B_LANG"];
                _jar.D_NAME = (string)_reader["D_NAME"];
                _jar.B_ID = (int)_reader["B_ID"];
                _jar.B_TAGS = JArray.Parse((string)_reader["B_TAGS"]).ToObject<List<string>>();
                _jar.THUMB_URL = (string)_reader["THUMB_URL"];
                _jar.ADD_AT = (DateTime)_reader["ADD_AT"];

                _results.Add(_jar);
            });

            while (_table.Read()) { ac(_table); }

            this._conn.Close();
            if (_results.Count == 0) { return this._get_result_map("ERR", "NO_DATA", "NO_DATA"); }
            return this._get_result_map("OK", _results);
        }

        /// <summary> 닉네임으로 (Display name) 유저 데이터 찾는 함수  </summary>
        public Dictionary<string, object> _get_channel_info_display_name(string D_NAME) {
            string _sql = string.Format(
                "SELECT * FROM `CHANNEL_INFO_TB` WHERE `D_NAME` LIKE '%{0}%'", D_NAME
            );

            try {
                _conn.Open();
            }catch(Exception e) {
                return this._get_result_map("ERR", "CONN_OPEN_ERR", "CONN_OPEN_ERR");
            }

            MySqlCommand _cmd;
            MySqlDataReader _table;
            try {
                _cmd = new MySqlCommand(_sql, _conn);
                _table = _cmd.ExecuteReader();
            }catch(Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR");
            } finally {
                this._conn.Close();
            }

            List<CHANNEL_INFO_JAR> _results = new();
            
            // CHANNEL_INFO_TB 작업용 액션 
            var ac = new Action<MySqlDataReader>(delegate (MySqlDataReader _reader) {
                var _jar = new CHANNEL_INFO_JAR();
                _jar.B_LOGIN_PK = (string)_reader["B_LOGIN_PK"];
                _jar.B_LANG = (string)_reader["B_LANG"];
                _jar.D_NAME = (string)_reader["D_NAME"];
                _jar.B_ID = (int)_reader["B_ID"];
                _jar.B_TAGS = JArray.Parse((string)_reader["B_TAGS"]).ToObject<List<string>>();
                _jar.THUMB_URL = (string)_reader["THUMB_URL"];
                _jar.ADD_AT = (DateTime)_reader["ADD_AT"];

                _results.Add(_jar);
            });

            while (_table.Read()) { ac(_table); }
            this._conn.Close();
            if (_results.Count == 0) { return this._get_result_map("ERR", "NO_DATA", "NO_DATA"); }
            return this._get_result_map("OK", _results);
        }


        /// <summary> 채널 추가용 </summary>
        public Dictionary<string, object> _insert_channel_info(
            string B_LOGIN_PK, string D_NAME, int B_ID,
            string THUMB_URL, string B_LANG = "KO", List<string> B_TAGS = null
        ) {

            // Select 로 데이터 존재하는지 확인 
            var _select = _get_channel_info(B_LOGIN_PK);
            if (_select["TYPE"] == "OK") { return this._get_result_map("ERR", "EXIST_DATA", "ALREADY_DATA"); }
            if ((string)_select["VALUE"] != "NO_DATA") { return _select; }

            string B_TAGS_string = string.Empty;
            if (B_TAGS == null || B_TAGS.Count == 0) { B_TAGS_string = "[]"; } else { B_TAGS_string = JsonConvert.SerializeObject(B_TAGS); }

            string _sql = string.Format("INSERT INTO `CHANNEL_INFO_TB` " +
                "(`B_LOGIN_PK`, `B_LANG`, `D_NAME`, `B_ID`, `B_TAGS`, `THUMB_URL`, `ADD_AT`) " +
                "VALUES ('{0}', '{1}', '{2}', {3}, '{4}', '{5}', current_timestamp())"
                , B_LOGIN_PK, B_LANG, D_NAME, B_ID, B_TAGS_string, THUMB_URL);
            this._conn.Open();
            MySqlCommand _cmd = new MySqlCommand(_sql, _conn);

            try {
                var _res = _cmd.ExecuteNonQuery();
                if (_res == 1) { return this._get_result_map("OK", "SNED_CONFIRM", "TRUE"); } else { return this._get_result_map("ERR", "SEND_ERROR", "SEND_ERR"); }
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_QUERY_ERR");
            } finally {
                this._conn.Close();
            }
        }

        /// <summary> 방송유저 업데이트 용 </summary>
        public Dictionary<string, object> _update_channel_info(
            string B_LOGIN_PK, string? D_NAME = null, int? B_ID = null,
            string? THUMB_URL = null, string? B_LANG = null, List<string> B_TAGS = null
        ) {
            // check null
            if (D_NAME == null && B_ID == null && THUMB_URL == null
                && B_LANG == null && B_TAGS == null) { return this._get_result_map("ERR", "NO_INSERST_DATA", "NO_INSERT_DATA"); }

            var _channel_select_res = this._get_channel_info(B_LOGIN_PK);
            if (_channel_select_res["TYPE"] != "OK") { return _channel_select_res; }

            // Update 함수 단 개발
            Func<string, string, dynamic, string> _update = (string B_LOGIN_PK, string column, dynamic _dt) => {
                var _data_string = string.Empty;
                if (_dt is string) { _data_string = string.Format("'{0}'", _dt); }
                else if (_dt is List<string>) { _data_string = JsonConvert.SerializeObject(_dt); }
                else if (_data_string == string.Empty) { _data_string = _dt.ToString(); }

                string _sql = string.Format("UPDATE `CHANNEL_INFO_TB` SET `{0}` = `{1}`", column, _data_string);
                _sql += string.Format(" WHERE `CHANNEL_INFO_TB`.`B_LOGIN_PK` = '{0}';", B_LOGIN_PK);
                return _sql;
            };

            // sql input
            this._conn.Open();
            string _sql = string.Empty;
            if (D_NAME != null) { _sql += _update(B_LOGIN_PK, "D_NAME", D_NAME!); }
            if (B_ID != null) { _sql += _update(B_LOGIN_PK, "B_ID", B_ID!); }
            if (THUMB_URL != null) { _sql += _update(B_LOGIN_PK, "THUMB_URL", THUMB_URL!); }
            if (B_LANG != null) { _sql += _update(B_LOGIN_PK, "B_LANG", B_LANG); }
            if (B_TAGS != null) { _sql += _update(B_LOGIN_PK, "B_TAGS", B_TAGS); }

            MySqlCommand _cmd = new MySqlCommand(_sql, this._conn);
            try {
                var _res = _cmd.ExecuteNonQuery();
                if (_res == 1) { return this._get_result_map("OK", "UPDATE_CONFIRM", "UPDATE_CONFIRM"); } else { return this._get_result_map("ERR", "UPDATE_ERROR", "UPDATE_ERR"); }
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXCUTE_QUERY_ERR");
            } finally {
                this._conn.Close();
            }
        }

        /// <summary> 방송유저 삭제 </summary>
        public Dictionary<string, object> _remove_channel_info (string B_LOGIN_PK){
            string _sql =
                string.Format("DELETE FROM `CHANNEL_INFO_TB` WHERE `CHANNEL_INFO_TB`.`B_LOGIN_PK`='{0}'", B_LOGIN_PK);

            try { 
                this._conn.Open();
            }catch(Exception e) {
                return this._get_result_map("ERR", e.ToString(), "CONN_ERR");
            }

            MySqlCommand _cmd = new MySqlCommand(_sql, this._conn);
            try {
                var _res = _cmd.ExecuteNonQuery();
                if (_res == 1) { return this._get_result_map("OK", "DELETE_CONFIRM", "DELETE_CONFIRM"); }
                else { return this._get_result_map("ERR", "NO_DATA", B_LOGIN_PK); }
            }catch(Exception e) {
                return this._get_result_map("ERR", e.ToString(), "QUERY_ERR");
            } finally {
                this._conn.Close();
            }
            
        }

        private Dictionary<string, dynamic> _get_result_map(string type, object value, string description = "") {
            return new Dictionary<string, dynamic>() { {"TYPE" , type }, {"VALUE" , value }, {"DESCRIPTION" , description } };
        }
    }

    public class CHANNEL_INFO_JAR {
        public string           B_LOGIN_PK; public string?  B_LANG;
        public string           D_NAME;     public int      B_ID;
        public List<string>?    B_TAGS;     public string   THUMB_URL;
        public DateTime?        ADD_AT;
    }

    public class STREAM_STATUS_JAR {
        public string           B_LOGIN_PK; public int      B_ID;
        public int              G_ID;       public string   G_NAME;
        public string           TITLE;      public int      VIEW_COUNT;
        public DateTime         START_AT;   public string   THUMB_IMG;
    }
}