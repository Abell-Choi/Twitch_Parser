using System;
namespace Twitch_Parser
{


    public class CHANNEL_INFO_JAR
    {
        public string           B_LOGIN_PK; public string? B_LANG;
        public string?          D_NAME;     public int? B_ID;
        public List<string>?    B_TAGS;     public string? THUMB_URL;
        public DateTime? ADD_AT;

        public CHANNEL_INFO_JAR() { this.ADD_AT = DateTime.Now; }
    }

    public class STREAM_STATUS_JAR
    {
        public string       B_LOGIN_PK; public int B_ID;
        public int          G_ID;       public string G_NAME;
        public string       TITLE;      public int VIEW_COUNT;
        public DateTime     START_AT;   public string THUMB_IMG;
        public DateTime?    UPDATE_AT;  // 디비로 부터 받아온 시간
    }
}

