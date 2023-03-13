using System;
using MySql.Data.MySqlClient;       // for Connecting MySQL(Maria DB)


namespace Twitch_Parser {
    public class DB_Manager {
        MySqlConnection _conn = null;

        public DB_Manager(string _url, string _db_name, string _id , string _pw) {
            if (!this._set_connection_object(_url, _db_name, _id, _pw)) {
                throw new Exception("CONNECTION ERROR");
            }
        }

        // Connection 연결 
        private bool _set_connection_object(string _url, string _db_name, string _id, string _pw) {
            string _conn_str = "Server=" + _url;
            _conn_str += ";Database=" + _db_name;
            _conn_str += ";Uid=" + _id;
            _conn_str += ";Pwd=" + _pw;

            try {
                MySqlConnection _sql_conn = new MySqlConnection(_conn_str);
                _sql_conn.Open();
                _sql_conn.Close();
                return true;
            } catch (Exception e) {
                Console.WriteLine(e.ToString());
                return false;
            }
        }
    }
}