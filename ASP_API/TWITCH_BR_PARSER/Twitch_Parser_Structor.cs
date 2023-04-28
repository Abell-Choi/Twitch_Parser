using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;


namespace Twitch_Parser
{
    public class CHANNEL_INFO_JAR
    {
        public string           B_LOGIN_PK; public string? B_LANG;
        public string?          D_NAME;     public int? B_ID;
        public List<string>?    B_TAGS;     public string? THUMB_URL;
        public DateTime? ADD_AT;

        public CHANNEL_INFO_JAR() { this.ADD_AT = DateTime.Now; }

        public string get_convert_tag_list ( ) {
            return JsonConvert.SerializeObject(this.B_TAGS);
        }

        public override string ToString() {
            return JsonConvert.SerializeObject(
                new{
                    B_LOGIN_PK = this.B_LOGIN_PK,   B_LANG = this.B_LANG,
                    D_NAME = this.D_NAME,           B_ID = this.B_ID,
                    B_TAGS = this.B_TAGS,           THUMB_URL = this.THUMB_URL,
                    ADD_AT = this.ADD_AT
                }, Formatting.Indented
            );
        }

        public Dictionary<string ,dynamic> ToDictionary ( ) {
            return new Dictionary<string, dynamic>( ) {
                { "B_LOGIN_PK", B_LOGIN_PK},    {"B_LANG", B_LANG}, {"D_NAME", D_NAME},
                {"B_ID", B_ID }, {"B_TAGS", B_TAGS}, {"THUMB_URL" , THUMB_URL}, {"ADD_AT", ADD_AT}
            };
        }
    }

    public class STREAM_STATUS_JAR
    {
        [Required]
        public string       B_LOGIN_PK;
        public int B_ID;
        public int          G_ID;       public string G_NAME;
        public string       TITLE;      public int VIEW_COUNT;
        public DateTime     START_AT;   public string THUMB_IMG;
        public DateTime?    UPDATE_AT;  // 디비로 부터 받아온 시간

        public override string ToString(){
            return JsonConvert.SerializeObject(
                new{
                    B_LOGIN_PK = this.B_LOGIN_PK,   B_ID = this.B_ID,
                    G_ID = this.G_ID,               G_NAME = this.G_NAME,
                    TITLE = this.TITLE,             VIEW_COUNT = this.VIEW_COUNT,
                    START_AT = this.START_AT,       THUMB_IMG = this.THUMB_IMG,
                    UPDATE_AT = UPDATE_AT
                }, Formatting.Indented
            );
        }

        public Dictionary<string,dynamic> ToDictionary ( ) {
            return new Dictionary<string, dynamic>(){
                {"B_LOGIN_PK", B_LOGIN_PK }, {"B_ID", B_ID}, { "G_ID", G_ID}, { "G_NAME", G_NAME}, {"TITLE", TITLE},
                {"VIEW_COUNT", VIEW_COUNT }, {"START_AT", START_AT}, {"THUMB_IMG", THUMB_IMG}, {"UPDATE_AT", UPDATE_AT}
            };
        }
    }
}
