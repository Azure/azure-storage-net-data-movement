
set BuildMode=Debug
if not {%1} == {} (
    set BuildMode=%1
)

set OriginalPath=%cd%
set TestPath=%~dp0..\test
"%ProgramFiles(x86)%\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\msbuild" %TestPath%\DMLibTest\DMLibTest.csproj /t:Rebuild /p:Configuration=%BuildMode%  >NUL
"%ProgramFiles(x86)%\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\msbuild" %TestPath%\DMLibTestCodeGen\DMLibTestCodeGen.csproj /t:Rebuild /p:Configuration=%BuildMode% >NUL
%TestPath%\DMLibTestCodeGen\bin\Debug\DMLibTestCodeGen.exe %TestPath%\DMLibTest\bin\Debug\DMLibTest.dll %TestPath%\DMLibTest\Generated DNetCore

dotnet restore -s https://www.nuget.org/api/v2/ %~dp0\DMLibTest
cd %~dp0\Microsoft.Azure.Storage.DataMovement
dotnet build -c %BuildMode%
cd %~dp0\MsTestLib
dotnet build -c %BuildMode%
cd %~dp0\DMTestLib
dotnet build -c %BuildMode%
cd %~dp0\DMLibTest
dotnet build -c %BuildMode%
dotnet publish -c %BuildMode%
cd %OriginalPath%

