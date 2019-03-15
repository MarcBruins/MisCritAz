FROM microsoft/dotnet:2.2-aspnetcore-runtime AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM microsoft/dotnet:2.2-sdk AS build
WORKDIR /src
COPY ["MisCritAz/MisCritAz.csproj", "MisCritAz/"]
RUN dotnet restore "MisCritAz/MisCritAz.csproj"
COPY . .
WORKDIR "/src/MisCritAz"
RUN dotnet build "MisCritAz.csproj" -c Release -o /app

FROM build AS publish
RUN dotnet publish "MisCritAz.csproj" -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "MisCritAz.dll"]