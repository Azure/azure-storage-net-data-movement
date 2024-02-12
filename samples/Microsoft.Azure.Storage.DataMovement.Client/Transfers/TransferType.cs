namespace Microsoft.Azure.Storage.DataMovement.Client.Transfers
{
    internal enum TransferType
    {
        UploadDirectory,
        UploadFile,
        UploadItems,
        DownloadDirectory,
        DownloadFile,
        ListOfItems
    }
}