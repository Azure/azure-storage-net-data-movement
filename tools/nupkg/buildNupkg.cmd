pushd %~dp0
rmdir /s /q .\package
mkdir .\package\lib\net45
mkdir .\package\lib\netstandard2.0
pushd ..\..
del /q /f *.nupkg
copy .\lib\bin\Release\Microsoft.WindowsAzure.Storage.DataMovement.dll .\tools\nupkg\package\lib\net45
copy .\lib\bin\Release\Microsoft.WindowsAzure.Storage.DataMovement.pdb .\tools\nupkg\package\lib\net45
copy .\lib\bin\Release\Microsoft.WindowsAzure.Storage.DataMovement.XML .\tools\nupkg\package\lib\net45
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.WindowsAzure.Storage.DataMovement.dll .\tools\nupkg\package\lib\netstandard2.0
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.WindowsAzure.Storage.DataMovement.pdb .\tools\nupkg\package\lib\netstandard2.0
copy .\netcore\Microsoft.WindowsAzure.Storage.DataMovement\bin\Release\netstandard2.0\Microsoft.WindowsAzure.Storage.DataMovement.xml .\tools\nupkg\package\lib\netstandard2.0
copy .\tools\nupkg\Microsoft.Azure.Storage.DataMovement.nuspec .\tools\nupkg\package
.\.nuget\nuget.exe pack .\tools\nupkg\package\Microsoft.Azure.Storage.DataMovement.nuspec
popd
rmdir /s /q .\package
popd
