using System;

namespace Microsoft.Azure.Storage.DataMovement.Client.CommandLine
{
    internal interface IBaseOptions 
    {
        bool AddConsoleLogger { get; }
        
        Guid? JobId { get; }
    }
}