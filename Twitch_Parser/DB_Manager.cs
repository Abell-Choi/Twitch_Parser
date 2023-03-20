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
            // default connection setting (port)
            string _url_parse = _url;
            int port_parse = 3306;
            if (_url.Contains(':')) {
                _url_parse = _url.Split(':')[0];
                port_parse = int.Parse(_url.Split(':')[1]);
            }

            // connection information setup
            string _conn_str = "Server=" + _url_parse;
            _conn_str += ";Port=" + port_parse.ToString();
            _conn_str += ";Database=" + _db_name;
            _conn_str += ";Uid=" + _id;
            _conn_str += ";Pwd=" + _pw;

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


        /// <summary> 등록된 모든 채널 가져오기 </summary>
        public Dictionary<string, object> _get_all_channel_user_ids() {
            string _sql = string.Format("SELECT B_LOGIN_PK FROM CHANNEL_INFO_TB");      // sql query setting
            var _conn_res = this._server_open();
            if (_conn_res["TYPE"] != "OK") { return _conn_res; }

            MySqlCommand    _cmd;
            MySqlDataReader _table;

            // set sql params executing
            try {
                _cmd = new MySqlCommand(_sql, _conn);
                _table = _cmd.ExecuteReader();
            }catch(Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR");
            } finally{
                _conn.Close();
            }

            // converting table -> List<string>
            List<string> _B_LOGIN_PK_Lists = new() {};
            while (_table.Read()) { _B_LOGIN_PK_Lists.Add((string)_table["B_LOGIN_PK"]);}

            return this._get_result_map("OK", _B_LOGIN_PK_Lists);
        }

        /// <summary> 디비에 채널이 존재하는지 확인용 </summary>
        public Dictionary<string, object> _get_channel_info(string user_login) {

            // SQL string 설정 
            string _sql = string.Format("SELECT * FROM CHANNEL_INFO_TB " +
                "WHERE B_LOGIN_PK=@B_LOGIN_PK");

            // Connection Open
            var _conn_res = this._server_open();
            if (_conn_res["TYPE"] != "OK") { return _conn_res; }

            MySqlCommand    _cmd;
            MySqlDataReader _table;

            // set sql params and executing
            try {
                _cmd = new MySqlCommand(_sql, _conn);
                _cmd.Parameters.AddWithValue("@B_LOGIN_PK", user_login);
                _table = _cmd.ExecuteReader();
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR");
            } finally {
                _conn.Close();
            }

            List<CHANNEL_INFO_JAR> _results = new();

            // CHANNEL_INFO_TB 작업용 액션 
            var ac = new Action<MySqlDataReader>(delegate (MySqlDataReader _reader) {
                var _jar = new CHANNEL_INFO_JAR();
                _jar.B_LOGIN_PK     = (string)_reader["B_LOGIN_PK"];
                _jar.B_LANG         = (string)_reader["B_LANG"];
                _jar.D_NAME         = (string)_reader["D_NAME"];
                _jar.B_ID           = (int)_reader["B_ID"];
                _jar.B_TAGS         = JArray.Parse((string)_reader["B_TAGS"]).ToObject<List<string>>();
                _jar.THUMB_URL      = (string)_reader["THUMB_URL"];
                _jar.ADD_AT         = (DateTime)_reader["ADD_AT"];

                _results.Add(_jar);
            });

            while (_table.Read()) { ac(_table); }

            this._conn.Close();
            if (_results.Count == 0) { return this._get_result_map("ERR", "NO_DATA", "NO_DATA"); }
            return this._get_result_map("OK", _results);
        }

        /// <summary> 닉네임으로 (Display name) 유저 데이터 찾는 함수  </summary>
        public Dictionary<string, object> _get_channel_info_display_name(string D_NAME) {

            // SQL string setting
            string _sql = "SELECT * FROM CHANNEL_INFO_TB WHERE `D_NAME` LIKE '@D_NAME'";


            // Connection Open
            var _conn_res = this._server_open();
            if (_conn_res["TYPE"] != "OK") { return _conn_res; }

            MySqlCommand    _cmd;
            MySqlDataReader _table;

            // set sql params and executing
            try {
                _cmd = new MySqlCommand(_sql, _conn);
                _cmd.Parameters.AddWithValue("@D_NAME", D_NAME);
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

            // read all columns and converting
            while (_table.Read()) { ac(_table); }
            this._conn.Close();
            if (_results.Count == 0) { return this._get_result_map("ERR", "NO_DATA", "NO_DATA"); }
            return this._get_result_map("OK", _results);
        }



        public Dictionary<string, object> insert_channel_info(CHANNEL_INFO_JAR _jar) {
            return this._insert_channel_info(
                _jar.B_LOGIN_PK, _jar.D_NAME, (int)_jar.B_ID, _jar.THUMB_URL, _jar.B_LANG, _jar.B_TAGS
            );
        }

        /// <summary> 채널 추가용 </summary>
        public Dictionary<string, object> _insert_channel_info(
            string B_LOGIN_PK, string D_NAME, int B_ID,
            string THUMB_URL, string B_LANG = "KO", List<string> B_TAGS = null
        ) {

            // SELECT 로 데이터 존재하는지 확인 
            var _select = _get_channel_info(B_LOGIN_PK);
            if ((string) _select["TYPE"] == "OK") { return this._get_result_map("ERR", "EXIST_DATA", "ALREADY_DATA"); }
            if ((string) _select["VALUE"] != "NO_DATA") { return _select; }

            // TAGS string setting
            string B_TAGS_string = string.Empty;
            if (B_TAGS == null || B_TAGS.Count == 0) { B_TAGS_string = "[]"; } else { B_TAGS_string = JsonConvert.SerializeObject(B_TAGS); }

            // SQL String setting
            string _SQL = "INSERT INTO CHANNEL_INFO_TB (B_LOGIN_PK, B_LANG, D_NAME, B_ID, B_TAGS, THUMB_URL, ADD_AT) " +
                   "VALUES (@B_LOGIN_PK, @B_LANG, @D_NAME, @B_ID, @B_TAGS, @THUMB_URL, DEFAULT)";

            // Connection Open
            var _conn_res = this._server_open();
            if (_conn_res["TYPE"] != "OK") { return _conn_res; }

            // Param Setting
            MySqlCommand _cmd = new MySqlCommand(_SQL, _conn);
            _cmd.Parameters.AddWithValue("@B_LOGIN_PK"  , B_LOGIN_PK        );
            _cmd.Parameters.AddWithValue("@B_LANG"      , B_LANG            );
            _cmd.Parameters.AddWithValue("@D_NAME"      , D_NAME            );
            _cmd.Parameters.AddWithValue("@B_ID"        , B_ID.ToString()   );
            _cmd.Parameters.AddWithValue("@B_TAGS"      , B_TAGS_string     );
            _cmd.Parameters.AddWithValue("@THUMB_URL"   , THUMB_URL         );

            // Execute Query
            try
            {
                var _res = _cmd.ExecuteNonQuery();
                if (_res == 1) { return this._get_result_map("OK", "SNED_CONFIRM", "TRUE"); } else { return this._get_result_map("ERR", "SEND_ERROR", "SEND_ERR"); }
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_QUERY_ERR");
            } finally {
                this._conn.Close();
            }
        }

        /// <summary> 채널 업데이트 용 </summary>
        public Dictionary<string, object> _update_channel_info(
            string B_LOGIN_PK, string? D_NAME = null, int? B_ID = null,
            string? THUMB_URL = null, string? B_LANG = null, List<string> B_TAGS = null
        ) {
            // check null
            if (D_NAME == null && B_ID == null && THUMB_URL == null
                && B_LANG == null && B_TAGS == null) { return this._get_result_map("ERR", "NO_INSERST_DATA", "NO_INSERT_DATA"); }

            // 채널이 존재하는지 확인
            var _channel_select_res = this._get_channel_info(B_LOGIN_PK);
            if (_channel_select_res["TYPE"] != "OK") { return _channel_select_res; }

            // Set Params
            string _sql = string.Empty;
            List<dynamic> _enable_columns = new() { };
            if (D_NAME != null)     { _enable_columns.Add("D_NAME"   ); }
            if (B_ID != null)       { _enable_columns.Add("B_ID"     ); }
            if (THUMB_URL != null)  { _enable_columns.Add("THUMB_URL"); }
            if (B_LANG != null)     { _enable_columns.Add("B_LANG"   ); }
            if (B_TAGS != null)     { _enable_columns.Add("B_TAGS"   ); }

            foreach(string i in _enable_columns) {
                _sql += string.Format("UPDATE `CHANNEL_INFO_TB` SET `{0}` = {1} ", i, "@" + i);
                _sql += string.Format("WHERE `CHANNEL_INFO_TB`.`B_LOGIN_PK` = @B_LOGIN_PK; ");
            }


            // Connection Open
            var _conn_res = this._server_open();
            if (_conn_res["TYPE"] != "OK") { return _conn_res; }

            // @B_TAGS 정리
            string B_TAGS_string = "[]";
            if (B_TAGS.Count != 0) { B_TAGS_string = string.Format("[\"{0}\"]", string.Join(',', B_TAGS)); }

            // @Params 정
            MySqlCommand _cmd = new MySqlCommand(_sql, this._conn);
            if (B_LOGIN_PK != null) { _cmd.Parameters.AddWithValue("@B_LOGIN_PK", B_LOGIN_PK    );}
            if (B_ID != null)       { _cmd.Parameters.AddWithValue("@B_ID", B_ID                );}
            if (D_NAME != null)     { _cmd.Parameters.AddWithValue("@D_NAME", D_NAME            );}
            if (THUMB_URL != null)  { _cmd.Parameters.AddWithValue(@"THUMB_URL", THUMB_URL      );}
            if (B_LANG != null)     { _cmd.Parameters.AddWithValue("@B_LANG", B_LANG            );}
            if (B_TAGS != null)     { _cmd.Parameters.AddWithValue("@B_TAGS", B_TAGS_string     );}

            // ExecuteNoneQuery
            try {
                var _res = _cmd.ExecuteNonQuery();
                if (_res != 0) { return this._get_result_map("OK", "UPDATE_CONFIRM", "UPDATE_CONFIRM"); }
                else { Console.WriteLine(_res.ToString()); return this._get_result_map("ERR", "UPDATE_ERROR", "UPDATE_ERR"); }
            } catch (Exception e) {
                return this._get_result_map("ERR", e.ToString(), "EXCUTE_QUERY_ERR");
            } finally {
                this._conn.Close();
            }
        }

        /// <summary> 채널 삭제 </summary>
        public Dictionary<string, object> _remove_channel_info (string B_LOGIN_PK){
            // setting sql query
            string _sql =
                string.Format("DELETE FROM `CHANNEL_INFO_TB` WHERE `CHANNEL_INFO_TB`.`B_LOGIN_PK`='@B_LOGIN_PK'", B_LOGIN_PK);

            // connection open
            try {  this._conn.Open(); }
            catch(Exception e) { return this._get_result_map("ERR", e.ToString(), "CONN_ERR");}

            // param set
            MySqlCommand _cmd = new MySqlCommand(_sql, this._conn);
            _cmd.Parameters.AddWithValue("@B_LOGIN_PK", B_LOGIN_PK);

            // commit query and send query
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

        /// <summary> DB에 저장된 방송중인 유저들 ID 긁어옴  </summary>
        Dictionary<string, dynamic> _get_stream_user_list (string B_LOGIN_PK) {
            string _sql = "SELECT B_LOGIN_PK FROM STREAM_STATUS_TB";

            // conneciton open
            try { this._conn.Open(); }
            catch(Exception e) { return this._get_result_map("ERR", e.ToString(), "CONN_ERR"); }
            finally { try { this._conn.Clone(); } catch { } }

            MySqlCommand _cmd;
            MySqlDataReader _table;

            List<string> _broadcast_user_lists = new();

            // set sql params executing
            try
            {
                _cmd = new MySqlCommand(_sql, _conn);
                _table = _cmd.ExecuteReader();
            }
            catch (Exception e)
            {
                return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR");
            }
            finally
            {
                _conn.Close();
            }

            // converting table -> List<string>
            while (_table.Read()) { _broadcast_user_lists.Add((string)_table["B_LOGIN_PK"]); }

            if (_broadcast_user_lists.Count == 0) { return this._get_result_map("ERR", "NO_DATA", "NO_DATA`"); }

            return this._get_result_map("OK", _broadcast_user_lists);
        }

        /// <summary> 지정한 유저의 방송정보를 가져옴 </summary>
        public Dictionary<string, object> _get_stream_info(string B_LOGIN_PK) {
            // check B_LOGIN_BK Exist
            var _check_channel_info = this._get_channel_info(B_LOGIN_PK);
            if (_check_channel_info["TYPE"] != "OK") { return _check_channel_info; }

            string _SQL = "SELECT * FROM STREAM_STATUS_TB WHERE B_LOGIN_PK=@B_LOGIN_PK";

            //open conn
            var _conn_res = this._server_open();
            if (_conn_res["TYPE"] != "OK") { return _conn_res; }

            MySqlCommand _cmd;
            MySqlDataReader _table;

            List<string> _broadcast_user_lists = new();

            // set sql params executing
            try {
                _cmd = new MySqlCommand(_SQL, _conn);
                _cmd.Parameters.AddWithValue("@B_LOGIN_PK", B_LOGIN_PK);
                _table = _cmd.ExecuteReader();
            }
            catch (Exception e) { return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR");}
            finally { _conn.Close(); }

            if(_table.Read() == false) { return this._get_result_map("ERR", "NO_DATA", "NO_DATA"); }

            // Data organize
            STREAM_STATUS_JAR _jar = new();
            _jar.B_LOGIN_PK     = (string) _table["B_LOGIN_PK"];
            _jar.B_ID           = (int) _table["B_ID"];
            _jar.G_ID           = (int) _table["G_ID"];
            _jar.G_NAME         = (string) _table["G_NAME"];
            _jar.TITLE          = (string) _table["TITLE"];
            _jar.VIEW_COUNT     = (int)_table["VIEW_COUNT"];
            _jar.START_AT       = DateTime.Parse((string)_table["START_AT"]);
            _jar.THUMB_IMG      = (string)_table["THUMB_IMG"];

            return this._get_result_map( "OK", _jar, "OK");

        }

        /// <summary> 방송 정보를 추가하는 코드 </summary>
        Dictionary<string, object> _insert_stream_data(STREAM_STATUS_JAR _jar) {
            return this._insert_stream_data(_jar.B_LOGIN_PK, _jar.B_ID, _jar.G_ID, _jar.G_NAME,
                _jar.TITLE, _jar.VIEW_COUNT, _jar.START_AT, _jar.THUMB_IMG);
        }

        Dictionary<string, object> _insert_stream_data(
         string B_LOGIN_PK, int B_ID, int G_ID, string G_NAME,
         string TITLE, int VIEW_COUNT, DateTime START_AT, string THUMB_IMG){
            // 데이터 존재 확인 -> 있으면 업데이트로 전
            var _exist_check = this._get_stream_info(B_LOGIN_PK);
            if (_exist_check["TYPE"] == "OK") {
                return this._update_stream_data(
                    B_LOGIN_PK, B_ID, G_ID, G_NAME, TITLE, VIEW_COUNT, START_AT, THUMB_IMG
                );
            }

            // Connection Check
            var _conn_check = this._server_open();
            if (_conn_check["TYPE"] != "OK") { return _conn_check; }

            // SQL
            string _SQL = "INSERT INTO `STREAM_STATUS_TB` (`B_LOGIN_PK`, `B_ID`, `G_ID`, `G_NAME`, `TITLE`, `VIEW_COUNT`, `START_AT`, `THUMB_IMG`)";
            _SQL += " VALUES (@B_LOGIN_PK, @B_ID, @G_ID, @G_NAME, @TITLE, @VIEW_COUNT, @START_AT, THUMB_IMG)";

            MySqlCommand _cmd;
            int _execute_res = -1;

            _cmd = new MySqlCommand(_SQL, _conn);
            _cmd.Parameters.AddWithValue("@B_LOGIN_PK", B_LOGIN_PK);

            // set sql params executing
            try
            { _execute_res = _cmd.ExecuteNonQuery();}
            catch (Exception e) { return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR"); }
            finally { _conn.Close(); }

            if (_execute_res <= 0) { return this._get_result_map("ERR", "EXECUTE_ERR", "EXECUTE_ERR"); }
            return this._get_result_map("OK", "INSERT_OK");

        }

        Dictionary<string, object> _update_stream_data(
            string B_LOGIN_PK, int B_ID, int G_ID, string G_NAME,
            string TITLE, int VIEW_COUNT, DateTime START_AT, string THUMB_IMG) {

            // Exist Checker
            var _exist_checker = this._get_stream_info(B_LOGIN_PK);
            if (_exist_checker["TYPE"] != "OK") { return this._insert_stream_data(
                    B_LOGIN_PK, B_ID, G_ID, G_NAME, TITLE, VIEW_COUNT, START_AT, THUMB_IMG
            ); }

            // Connection Checker
            var _conn_check = this._server_open();
            if (_conn_check["TYPE"] != "OK") { return _conn_check; }

            string _SQL = "UPDATE `STREAM_STATUS_TB` SET ";
            _SQL += "`B_ID` = @B_ID, `G_ID` = @G_ID, `G_NAME` = @G_NAME, `TITLE` = @TITLE,";
            _SQL += "`VIEW_COUNT` = @VIEW_COUNT, `START_AT` = @START_AT, `THUMB_IMG` = @THUMB_IMG";
            _SQL += " WHERE `STREAM_STATUS_TB`.`B_LOGIN_PK = @B_LOGIN_PK`";

            MySqlCommand _cmd;
            int _execute_res = -1;

            _cmd = new MySqlCommand(_SQL, _conn);
            _cmd.Parameters.AddWithValue("@B_LOGIN_PK", B_LOGIN_PK);
            _cmd.Parameters.AddWithValue("@B_ID", B_ID);
            _cmd.Parameters.AddWithValue("@G_ID", G_ID);
            _cmd.Parameters.AddWithValue("@G_NAME", G_NAME);
            _cmd.Parameters.AddWithValue("@TITLE", TITLE);
            _cmd.Parameters.AddWithValue("@VIEW_COUNT", VIEW_COUNT);
            _cmd.Parameters.AddWithValue("@START_AT", START_AT);
            _cmd.Parameters.AddWithValue("@THUMB_IMG", THUMB_IMG);

            try {
                _cmd.ExecuteNonQuery();
            }catch(Exception e){ return this._get_result_map("ERR", e.ToString(), "EXECUTE_ERR"); }
            finally { _conn.Close(); }

            if (_execute_res <= 0) { return this._get_result_map("ERR", "NO CHANGED", "NO_CHANGED"); }
            return this._get_result_map("OK", @"{B_LOGIN_PK} is changed", B_LOGIN_PK);
        }

        /// <summary> 리턴 타입 정규화(?) 적으로 하기 위한 함수임 </summary>
        private Dictionary<string, dynamic> _get_result_map(string type, object value, string description = "") {
            return new Dictionary<string, dynamic>() { {"TYPE" , type }, {"VALUE" , value }, {"DESCRIPTION" , description } };
        }

        /// <summary> Connection Open 을 하기 위한 함수 </summary>
        private Dictionary<string, dynamic> _server_open() {
            // Conn open
            try { this._conn.Open(); }
            catch (Exception e) { return this._get_result_map("ERR", e.ToString(), "CONN_ERR"); }

            return this._get_result_map("OK", "CONN_OK", "CONN_OPEN");
        }

    }
}
