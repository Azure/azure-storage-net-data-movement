pushd %~dp0
rmdir /s /q .\lib
mkdir .\lib\net45
mkdir .\lib\netstandard1.3
pushd ..\..
del /q /f *.nupkg
copy .\lib\bin\Debug\Microsoft.WindowsAzure.Storage.DataMovement.dll .\tools\nupkg\lib\net45
copy .\lib\bin\Debug\Microsoft.WindowsAzure.Storage.DataMovement.pdb .\tools\nupkg\lib\net45
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Debug\netstandard1.3\Microsoft.WindowsAzure.Storage.DataMovement.dll .\tools\nupkg\lib\netstandard1.3
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Debug\netstandard1.3\Microsoft.WindowsAzure.Storage.DataMovement.pdb .\tools\nupkg\lib\netstandard1.3
.\.nuget\nuget.exe pack .\tools\nupkg\Microsoft.Azure.Storage.DataMovement.nuspec
popd
rmdir /s /q .\lib
popd