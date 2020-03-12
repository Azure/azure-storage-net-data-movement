pushd %~dp0
rmdir /s /q .\workspace
mkdir .\workspace
copy .\Microsoft.Azure.Storage.DataMovement.nuspec .\workspace
mkdir .\workspace\lib\net452
mkdir .\workspace\lib\netstandard2.0
pushd ..\..
del /q /f *.nupkg
copy .\lib\bin\Release\Microsoft.Azure.Storage.DataMovement.dll .\tools\nupkg\workspace\lib\net452
copy .\lib\bin\Release\Microsoft.Azure.Storage.DataMovement.pdb .\tools\nupkg\workspace\lib\net452
copy .\lib\bin\Release\Microsoft.Azure.Storage.DataMovement.XML .\tools\nupkg\workspace\lib\net452
copy .\netcore\Microsoft.Azure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.Azure.Storage.DataMovement.dll .\tools\nupkg\workspace\lib\netstandard2.0
copy .\netcore\Microsoft.Azure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.Azure.Storage.DataMovement.pdb .\tools\nupkg\workspace\lib\netstandard2.0
copy .\netcore\Microsoft.Azure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.Azure.Storage.DataMovement.xml .\tools\nupkg\workspace\lib\netstandard2.0
.\.nuget\nuget.exe pack .\tools\nupkg\workspace\Microsoft.Azure.Storage.DataMovement.nuspec
popd
rmdir /s /q .\workspace
popd
