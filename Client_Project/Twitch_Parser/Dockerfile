#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["Twitch_Parser/Twitch_Parser.csproj", "Twitch_Parser/"]
RUN dotnet restore "Twitch_Parser/Twitch_Parser.csproj"
COPY . .
WORKDIR "/src/Twitch_Parser"
RUN dotnet build "Twitch_Parser.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Twitch_Parser.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Twitch_Parser.dll"]
