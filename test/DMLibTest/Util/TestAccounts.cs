//------------------------------------------------------------------------------
// <copyright file="TestAccounts.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using Microsoft.Azure.Storage;
    using MS.Test.Common.MsTestLib;

    public static class TestAccounts
    {
        public static TestAccount Primary
        {
            get
            {
                return new TestAccount(AccountInConfig.Primary);
            }
        }

        public static TestAccount Secondary
        {
            get
            {
                return new TestAccount(AccountInConfig.Secondary);
            }
        }
    }

    public class TestAccount
    {
        public TestAccount(AccountInConfig accountInConfig)
            : this(GetConnectionString(accountInConfig))
        {
        }

        public TestAccount(string connectionString)
        {
            this.ConnectionString = connectionString;
            this.Account = CloudStorageAccount.Parse(connectionString);
        }

        public CloudStorageAccount Account { get; private set; }

        public string ConnectionString { get; private set; }

        public string AccountName
        {
            get
            {
                return this.Account.Credentials.AccountName;
            }
        }

        public string StorageKey
        {
            get
            {
                return this.Account.Credentials.ExportBase64EncodedKey();
            }
        }

        public string GetEndpointBaseUri(EndpointType endpoint, bool secondary = false)
        {
            return this.GetEndpointBaseUri(endpoint, DMLibTestHelper.RandomProtocol(), secondary);
        }

        public string GetEndpointBaseUri(EndpointType endpoint, string protocol, bool secondary = false)
        {
            string url = string.Empty;
            bool isHttps = (string.Compare(protocol, "https", StringComparison.OrdinalIgnoreCase) == 0);
            if (DMLibTestHelper.GetTestAgainst() == TestAgainst.DevFabric)
            {
                int port;
                string host;
                if (endpoint == EndpointType.Blob)
                {
                    port = isHttps ? 10100 : 10000;
                    host = this.Account.BlobEndpoint.Host;
                }
                else if (endpoint == EndpointType.Queue)
                {
                    port = isHttps ? 10101 : 10001;
                    host = this.Account.QueueEndpoint.Host;
                }
                else if (endpoint == EndpointType.Table)
                {
                    port = isHttps ? 10102 : 10002;
                    host = this.Account.TableEndpoint.Host;
                }
                else
                {
                    port = isHttps ? 10104 : 10004;
                    host = this.Account.FileEndpoint.Host;
                }

                url = string.Format(@"{0}://{1}:{2}/{3}", protocol, host, port, this.AccountName);
                if (secondary)
                {
                    Test.Error("DevFabric doesn't have secondary endpoint.");
                }
            }
            else
            {
                Uri endpointUri;
                if (endpoint == EndpointType.Blob)
                {
                    endpointUri = secondary ? this.Account.BlobStorageUri.SecondaryUri : this.Account.BlobStorageUri.PrimaryUri;
                }
                else if (endpoint == EndpointType.Queue)
                {
                    endpointUri = secondary ? this.Account.QueueStorageUri.SecondaryUri : this.Account.QueueStorageUri.PrimaryUri;
                }
                else if (endpoint == EndpointType.Table)
                {
                    endpointUri = secondary ? this.Account.TableStorageUri.SecondaryUri : this.Account.TableStorageUri.PrimaryUri;
                }
                else
                {
                    endpointUri = secondary ? this.Account.FileStorageUri.SecondaryUri : this.Account.FileStorageUri.PrimaryUri;
                }

                url = endpointUri.AbsoluteUri.Replace(endpointUri.Scheme, protocol);
            }

            if (url.EndsWith("/"))
            {
                url = url.Remove(url.Length - 1);
            }

            return url;
        }

        private static string GetConnectionString(AccountInConfig accountInConfig)
        {
            if (accountInConfig == AccountInConfig.Primary)
            {
                return Test.Data.Get(DMLibTestConstants.ConnStr);
            }
            else if (accountInConfig == AccountInConfig.Secondary)
            {
                return Test.Data.Get(DMLibTestConstants.ConnStr2);
            }

            throw new ArgumentException(string.Format("Invalid accountInConfig value: {0}", accountInConfig), "accountInConfig");
        }
    }

    public enum AccountInConfig
    {
        Primary,
        Secondary,
    }

    public enum EndpointType
    {
        Blob,
        Queue,
        Table,
        File,
    }
}
