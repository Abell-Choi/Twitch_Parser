using System;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MySqlConnector;
using System.Xml.Linq;


namespace Twitch_Parser {
    public class DB_Manager {
        MySqlConnection _conn = null;

        public DB_Manager ( string _url, string _db_name, string _id, string _pw, int port=3306) {
            string _conn_res = this._set_connection_object( _url, _db_name, _id, _pw, port );
            if (  _conn_res !="OK") {
                throw new Exception( _conn_res );
            }
        }

        /*----------- CHANNEL_INFO_TB ---------------*/
        /*B_LOGIN_PK, B_LANG, D_NAME, B_ID, B_TAGS, THUMB_URL, ADD_AT*/
        ///<summary> channel id list 받기  </summary>
        public List<string> _get_B_LOGIN_PK_lists ( ) {
            _conn.Open( );
            string _SQL = "SELECT B_LOGIN_PK FROM CHANNEL_INFO_TB";
            var _cmd = new MySqlCommand( _SQL, this._conn );

            List<string> B_LOGIN_PKs = new( ) {
            };
            var _reader = _cmd.ExecuteReader( );
            while ( _reader.Read( ) ) {
                B_LOGIN_PKs.Add( ( string ) _reader["B_LOGIN_PK"] );
            }
            _conn.Close( );
            return B_LOGIN_PKs;
        }

        ///<summary> channel 정보 받기  </summary>
        public CHANNEL_INFO_JAR _get_channel_info ( string B_LOGIN_PK ) {

            string _SQL = "SELECT * FROM CHANNEL_INFO_TB WHERE B_LOGIN_PK=@B_LOGIN_PK";
            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, this._conn );

            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", B_LOGIN_PK );
            var _reader = _cmd.ExecuteReader( );

            if ( !_reader.Read( ) ) {
                _conn.Close( );
                return null;
            }

            CHANNEL_INFO_JAR _jar = new( );
            _jar.B_LOGIN_PK = ( string ) _reader["B_LOGIN_PK"];
            _jar.B_LANG = ( string ) _reader["B_LANG"];
            _jar.D_NAME = ( string ) _reader["D_NAME"];
            _jar.B_ID = ( int ) _reader["B_ID"];
            _jar.B_TAGS = JArray.Parse( ( string ) _reader["B_TAGS"] ).ToObject<List<string>>( );
            _jar.THUMB_URL = ( string ) _reader["THUMB_URL"];
            _jar.ADD_AT = ( DateTime ) _reader["ADD_AT"];

            _conn.Close( );
            return _jar;
        }

        ///<summary> channel 정보를 display_name으로 받기 </summary>
        public CHANNEL_INFO_JAR _get_channel_for_D_NAME ( string D_NAME ) {
            string _SQL = "SELECT * FROM CHANNEL_INFO_TB WHERE D_NAME LIKE @D_NAME";
            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, this._conn );

            _cmd.Parameters.AddWithValue( "@D_NAME", "%" + D_NAME + "%" );
            var _reader = _cmd.ExecuteReader( );

            if ( !_reader.Read( ) ) {
                _conn.Close( );
                return null;
            }

            CHANNEL_INFO_JAR _jar = new( );
            _jar.B_LOGIN_PK = ( string ) _reader["B_LOGIN_PK"];
            _jar.B_LANG = ( string ) _reader["B_LANG"];
            _jar.D_NAME = ( string ) _reader["D_NAME"];
            _jar.B_ID = ( int ) _reader["B_ID"];
            _jar.B_TAGS = JArray.Parse( ( string ) _reader["B_TAGS"] ).ToObject<List<string>>( );
            _jar.THUMB_URL = ( string ) _reader["THUMB_URL"];
            _jar.ADD_AT = ( DateTime ) _reader["ADD_AT"];
            _conn.Close( );
            return _jar;
        }

        ///<summary> 해당 언어 코드의 모든 유저 받기 -> B_LOGIN_PK List </summary>
        public List<string> _get_channel_for_B_LANG ( string B_LANG ) {
            string _SQL = "SELECT B_LOGIN_PK FROM CHANNEL_INFO_TB WHERE B_LANG=@B_LANG";
            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, this._conn );

            _cmd.Parameters.AddWithValue( "@B_LANG", B_LANG );
            var _reader = _cmd.ExecuteReader( );


            List<string> _B_USER_LOGINs = new( ) {
            };
            while ( _reader.Read( ) ) {
                _B_USER_LOGINs.Add( ( string ) _reader["B_LOGIN_PK"] );
            }

            _conn.Close( );

            return _B_USER_LOGINs;

        }

        private bool _get_channel_info_exists ( string B_LOGIN_PK ) {
            List<string> _B_LOGIN_PKs = this._get_B_LOGIN_PK_lists( );
            if ( _B_LOGIN_PKs == null ) {
                return false;
            }

            return _B_LOGIN_PKs.Contains( B_LOGIN_PK );
        }

        ///<summary> 채널 추가용 </summary>
        public string _insert_channel ( CHANNEL_INFO_JAR _jar ) {

            string _SQL = "INSERT INTO `CHANNEL_INFO_TB` " +
                "(`B_LOGIN_PK`, `B_LANG`, `D_NAME`, `B_ID`, `B_TAGS`, `THUMB_URL`, `ADD_AT`) " +
                "VALUES (@B_LOGIN_PK, @B_LANG, @D_NAME, @B_ID, @B_TAGS, @THUMB_URL, DEFAULT) ON DUPLICATE KEY UPDATE " +
                "`B_LOGIN_PK` = @B_LOGIN_PK, `B_LANG` = @B_LANG , `D_NAME` = @D_NAME, `B_ID` = @B_ID, `B_TAGS` = @B_TAGS," +
                "`THUMB_URL` = @THUMB_URL, `ADD_AT` = DEFAULT";

            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );
            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", _jar.B_LOGIN_PK );
            _cmd.Parameters.AddWithValue( "@B_LANG", _jar.B_LANG );
            _cmd.Parameters.AddWithValue( "@D_NAME", _jar.D_NAME );
            _cmd.Parameters.AddWithValue( "@B_ID", _jar.B_ID );
            _cmd.Parameters.AddWithValue( "@B_TAGS", _jar.get_convert_tag_list( ) );
            _cmd.Parameters.AddWithValue( "@THUMB_URL", _jar.THUMB_URL );

            int _query_res = -1;
            try {
                _query_res = _cmd.ExecuteNonQuery( );
            } catch (Exception e) {return e.ToString(); }

            _conn.Close( );
            return "OK";
        }

        ///<summary> 채널 업데이트용 </summary>
        public int _update_channel ( CHANNEL_INFO_JAR _jar ) {
            string _SQL = "UPDATE `CHANNEL_INFO_TB` " +
                "SET `B_LANG` = @B_LANG, `D_NAME` = @D_NAME, `B_ID` = @B_ID, `B_TAGS` = @B_TAGS," +
                "`THUMB_URL` = @THUMB_URL WHERE B_LOGIN_PK = @B_LOGIN_PK";

            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );
            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", _jar.B_LOGIN_PK );
            _cmd.Parameters.AddWithValue( "@B_LANG", _jar.B_LANG );
            _cmd.Parameters.AddWithValue( "@D_NAME", _jar.D_NAME );
            _cmd.Parameters.AddWithValue( "@B_ID", _jar.B_ID );
            _cmd.Parameters.AddWithValue( "@B_TAGS", _jar.get_convert_tag_list( ) );
            _cmd.Parameters.AddWithValue( "@THUMB_URL", _jar.THUMB_URL );

            int _query_res = -1;
            try {
                _query_res = _cmd.ExecuteNonQuery( );
            } catch { }
            _conn.Close( );
            return _query_res;
        }

        ///<summary> 채널 삭제용 </summary>
        public int _delete_channel ( string B_LOGIN_PK ) {
            string _SQL = "DELETE FROM `CHANNEL_INFO_TB` WHERE `CHANNEL_INFO_TB`.`B_LOGIN_PK` = @B_LOGIN_PK";

            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );
            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", B_LOGIN_PK );

            int _query_res = -1;
            try {
                _query_res = _cmd.ExecuteNonQuery( );
            } catch { }

            _conn.Clone( );
            return _query_res;
        }


        /*----------- STREAM_STATUS_TB ---------------*/
        /* B_LOGIN_PK, B_ID, G_ID, G_NAME, TITLE, VIEW_COUNT, START_AT, THUMBIMG */

        ///<summary> 방송중인 모든 유저들 가져오기 </summary>
        public List<string> get_stream_user_lists ( ) {
            string _SQL = "SELECT B_LOGIN_PK FROM STREAM_STATUS_TB";
            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );

            List<string> _B_LOGIN_PKs = new List<string>( );
            var _reader = _cmd.ExecuteReader( );
            while ( _reader.Read( ) ) {
                _B_LOGIN_PKs.Add( ( string ) _reader["B_LOGIN_PK"] );
            }
            _conn.Close( );
            return _B_LOGIN_PKs;
        }

        ///<summary> 방송 정보 얻기 </summary>
        public STREAM_STATUS_JAR get_user_streaming_info ( string B_LOGIN_PK ) {
            string _SQL = "SELECT * FROM `STREAM_STATUS_TB` WHERE B_LOGIN_PK=@B_LOGIN_PK";
            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );
            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", B_LOGIN_PK );

            var _reader = _cmd.ExecuteReader( );
            if ( !_reader.Read( ) ) {
                _conn.Close( );
                return null;
            }
            STREAM_STATUS_JAR _jar = new STREAM_STATUS_JAR( );
            _jar.B_LOGIN_PK = ( string ) _jar.B_LOGIN_PK;
            _jar.B_ID = ( int ) _jar.B_ID;
            _jar.G_ID = ( int ) _jar.G_ID;
            _jar.G_NAME = ( string ) _jar.G_NAME;
            _jar.TITLE = ( string ) _jar.TITLE;
            _jar.VIEW_COUNT = ( int ) _jar.VIEW_COUNT;
            _jar.START_AT = ( DateTime ) _jar.START_AT;
            _jar.THUMB_IMG = ( string ) _jar.THUMB_IMG;
            _jar.UPDATE_AT = DateTime.Now;


            _conn.Close( );
            return _jar;
        }
        private bool _get_stream_info_exists ( string B_LOGIN_PK ) {
            List<string> _B_LOGIN_PKs = this.get_stream_user_lists( );
            if ( _B_LOGIN_PKs == null ) {
                return false;
            }

            return _B_LOGIN_PKs.Contains( B_LOGIN_PK );
        }

        ///<summary> 방송 정보 추가 </summary>
        public string insert_streaming_data ( STREAM_STATUS_JAR _jar ) {
            string _SQL = "INSERT INTO `STREAM_STATUS_TB` " +
                "(`B_LOGIN_PK`, `B_ID`, `G_ID`, `G_NAME`, `TITLE`, `VIEW_COUNT`, `START_AT`, `THUMB_IMG`) " +
                "VALUES (@B_LOGIN_PK, @B_ID, @G_ID, @G_NAME, @TITLE, @VIEW_COUNT, @START_AT, @THUMB_IMG) " +
                "ON DUPLICATE KEY UPDATE " +
                "`B_LOGIN_PK`=@B_LOGIN_PK, `B_ID`=@B_ID, `G_ID`=@G_ID, `G_NAME`=@G_NAME, `TITLE`=@TITLE, `VIEW_COUNT`=@VIEW_COUNT, `START_AT`=@START_AT, `THUMB_IMG`=@THUMB_IMG";

            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );
            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", _jar.B_LOGIN_PK );
            _cmd.Parameters.AddWithValue( "@B_ID", _jar.B_ID );
            _cmd.Parameters.AddWithValue( "@G_ID", _jar.G_ID );
            _cmd.Parameters.AddWithValue( "@G_NAME", _jar.G_NAME );
            _cmd.Parameters.AddWithValue( "@TITLE", _jar.TITLE );
            _cmd.Parameters.AddWithValue( "@VIEW_COUNT", _jar.VIEW_COUNT );
            _cmd.Parameters.AddWithValue( "@START_AT", _jar.START_AT );
            _cmd.Parameters.AddWithValue( "@THUMB_IMG", _jar.THUMB_IMG );

            int _query_res = -1;
            try {
                _query_res = _cmd.ExecuteNonQuery( );
            } catch ( Exception e ) { return e.ToString();}

            _conn.Close( );
            return "OK";
        }

        ///<summary> 방송 정보 삭제 </summary>
        public int delete_streaming_data ( string B_LOGIN_PK ) {
            string _SQL = "DELETE FROM `STREAM_STATUS_TB` WHERE `STREAM_STATUS_TB`.`B_LOGIN_PK` = @B_LOGIN_PK";
            _conn.Open( );
            var _cmd = new MySqlCommand( _SQL, _conn );
            _cmd.Parameters.AddWithValue( "@B_LOGIN_PK", B_LOGIN_PK );

            int _query_res = -1;
            try {
                _query_res = _cmd.ExecuteNonQuery( );
            } catch { }

            _conn.Close( );
            return _query_res;
        }




        // 첫 Connection 테스트를 위한 연결 
        private string _set_connection_object ( string _url, string _db_name, string _id, string _pw, int port=3306 ) {
            // default connection setting (port)
            string _url_parse = _url;
            int port_parse = port;
            if ( _url_parse.Contains( ":" ) ) {
                port_parse = int.Parse( _url_parse.Split( ':' )[1] );
                _url_parse = _url_parse.Split(':')[0];
            }

            // connection information setup
            string _conn_str = "server=" + _url_parse;
            _conn_str += ";port=" + port_parse.ToString( );
            _conn_str += ";database=" + _db_name;
            _conn_str += ";user=" + _id;
            _conn_str += ";password=" + _pw;

            // sql connection
            try {
                MySqlConnection _sql_conn = new MySqlConnection( _conn_str );
                _sql_conn.Open( );
                this._conn = _sql_conn;
                _sql_conn.Close( );
                return "OK";
            } catch ( Exception e ) {
                return e.ToString();
            }
        }

        /// <summary> 리턴 타입 정규화(?) 적으로 하기 위한 함수임 </summary>
        private Dictionary<string, dynamic> _get_result_map ( string type, object value, string description = "" ) {
            return new Dictionary<string, dynamic>( ) { { "TYPE", type }, { "VALUE", value }, { "DESCRIPTION", description } };
        }
    }
}
