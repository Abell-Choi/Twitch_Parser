using Twitch_Parser;
using Newtonsoft.Json;
using Swashbuckle.AspNetCore.Annotations;

/// need setting
string _db_url;
string _db_name;
string _db_id;
string _db_pw;

DB_Manager _get_db_manager ( ) {
    return new DB_Manager( _db_url, _db_name, _db_id, _db_pw );;
}

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}
app.UseHttpsRedirection();


app.MapGet("/", () => { return "is_running!";} );

// 채널 정보 확인용 
app.MapGet( "/get_db_channel_info", ( string user_id ) => {
    var _data = _get_db_manager( )._get_channel_info( user_id );
    return _data == null ? "{}" : _data.ToString( );
} ) .Produces<string>();


// 사용가능 채널 리스트 확인용
app.MapGet( "/get_db_user_logins", ( ) => {
    Dictionary<string, dynamic> _res = new( );
    try {
        _res.Add("RES" , "OK");
        _res.Add("VALUE", _get_db_manager()._get_B_LOGIN_PK_lists());
    } catch(Exception e ) {
        _res.Add("RES" , "ERR");
        _res.Add("VALUE", e.ToString());
    }
    return JsonConvert.SerializeObject( _res, Formatting.Indented );
} );


// 방송 중인지 확인용 
app.MapGet("/get_db_broadcast_info" , (string user_id)=> {
    Dictionary<string, dynamic> _res = new();
    try {
        var value = _get_db_manager( ).get_user_streaming_info( user_id );
        if (value == null){
            _res.Add("RES", "OFFLINE");
            _res.Add("VALUE", "OFFLINE");
        } else {
            _res.Add("RES", "OK");
            _res.Add("VALUE", value);
        }
        return JsonConvert.SerializeObject(_res, Formatting.Indented);
    }catch(Exception e ) {
        _res.Add("RES" , "ERR");
        _res.Add("VALUE", e.ToString());
        return JsonConvert.SerializeObject(_res, Formatting.Indented);
    }

    return JsonConvert.SerializeObject(_res, Formatting.Indented);

});

// 방송하는 채널들 확인용 
app.MapGet("/get_db_broadcast_online_user", ()=>{
    Dictionary<string, dynamic> _res = new();
    try {
        var value = _get_db_manager().get_stream_user_lists();
        if (value == null || value.Count == 0){ value = new List<string>();}
        _res.Add("RES", "OK");
        _res.Add("VALUE", value);
    }catch(Exception e ) {
        _res.Add("RES", "ERR");
        _res.Add("VALUE", e.ToString());
        return JsonConvert.SerializeObject(_res, Formatting.Indented);
    }

    return JsonConvert.SerializeObject(_res, Formatting.Indented);
});

app.Run();
