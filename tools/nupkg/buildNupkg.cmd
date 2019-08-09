pushd %~dp0
rmdir /s /q .\workspace
mkdir .\workspace
copy .\Microsoft.Azure.Storage.DataMovement.nuspec .\workspace
mkdir .\workspace\lib\net45
mkdir .\workspace\lib\netstandard2.0
pushd ..\..
del /q /f *.nupkg
copy .\lib\bin\Release\Microsoft.WindowsAzure.Storage.DataMovement.dll .\tools\nupkg\workspace\lib\net45
copy .\lib\bin\Release\Microsoft.WindowsAzure.Storage.DataMovement.pdb .\tools\nupkg\workspace\lib\net45
copy .\lib\bin\Release\Microsoft.WindowsAzure.Storage.DataMovement.XML .\tools\nupkg\workspace\lib\net45
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.WindowsAzure.Storage.DataMovement.dll .\tools\nupkg\workspace\lib\netstandard2.0
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.WindowsAzure.Storage.DataMovement.pdb .\tools\nupkg\workspace\lib\netstandard2.0
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.WindowsAzure.Storage.DataMovement.xml .\tools\nupkg\workspace\lib\netstandard2.0
.\.nuget\nuget.exe pack .\tools\nupkg\workspace\Microsoft.Azure.Storage.DataMovement.nuspec
popd
rmdir /s /q .\workspace
popd
