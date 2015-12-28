//------------------------------------------------------------------------------
// <copyright file="SerializableListContinuationToken.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferEnumerators
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    internal sealed class SerializableListContinuationToken : ISerializable
    {
        private const string ListContinuationTokenTypeName = "TokenType";
        private const string ListContinuationTokenName = "Token";
        private const string TokenTypeFile = "FileListContinuationToken";
        private const string TokenTypeAzureBlob = "AzureBlobListContinuationToken";
        private const string TokenTypeAzureFile = "AzureFileListContinuationToken";

        public SerializableListContinuationToken(ListContinuationToken listContinuationToken)
        {
            this.ListContinuationToken = listContinuationToken;
        }

        private SerializableListContinuationToken(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            string tokenType = info.GetString(ListContinuationTokenTypeName);
            if (tokenType == TokenTypeFile)
            {
                this.ListContinuationToken = (FileListContinuationToken)info.GetValue(ListContinuationTokenName, typeof(FileListContinuationToken));
            }
            else if(tokenType == TokenTypeAzureBlob)
            {
                this.ListContinuationToken = (AzureBlobListContinuationToken)info.GetValue(ListContinuationTokenName, typeof(AzureBlobListContinuationToken));
            }
            else if(tokenType == TokenTypeAzureFile)
            {
                this.ListContinuationToken = (AzureFileListContinuationToken)info.GetValue(ListContinuationTokenName, typeof(AzureFileListContinuationToken));
            }
            else
            {
                throw new ArgumentException("ListContinuationToken Type");
            }
        }

        public ListContinuationToken ListContinuationToken
        {
            get;
            private set;
        }

        /// <summary>
        /// Serializes the object.
        /// </summary>
        /// <param name="info">Serialization info object.</param>
        /// <param name="context">Streaming context.</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }

            FileListContinuationToken fileLct = this.ListContinuationToken as FileListContinuationToken;
            if (fileLct != null)
            {
                info.AddValue(ListContinuationTokenTypeName, TokenTypeFile, typeof(string));
                info.AddValue(ListContinuationTokenName, fileLct, typeof(FileListContinuationToken));
                return;
            }

            AzureBlobListContinuationToken azureBlobLct = this.ListContinuationToken as AzureBlobListContinuationToken;
            if (azureBlobLct != null)
            {
                info.AddValue(ListContinuationTokenTypeName, TokenTypeAzureBlob, typeof(string));
                info.AddValue(ListContinuationTokenName, azureBlobLct, typeof(AzureBlobListContinuationToken));
                return;
            }

            AzureFileListContinuationToken azureFileLct = this.ListContinuationToken as AzureFileListContinuationToken;
            if (azureFileLct != null)
            {
                info.AddValue(ListContinuationTokenTypeName, TokenTypeAzureFile, typeof(string));
                info.AddValue(ListContinuationTokenName, azureFileLct, typeof(AzureFileListContinuationToken));
                return;
            }

            throw new ArgumentException("ListContinuationToken");
        }
    }
}
