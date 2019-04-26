using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Core;
using Microsoft.Azure.Storage.File;

namespace DMLibTest.Util
{
    internal static class SASGenerator
    {
        private const string SignedPermissions = "sp";

        private const string SignedStart = "st";

        private const string SignedExpiry = "se";

        private const string SignedResource = "sr";

        private const string SignedResourceTypes = "srt";

        private const string SignedServices = "ss";

        private const string SignedProtocols = "spr";

        private const string SignedIP = "sip";

        private const string SignedIdentifier = "si";

        private const string SignedKey = "sk";

        private const string SignedVersion = "sv";

        private const string Signature = "sig";

        private const string CacheControl = "rscc";

        private const string ContentType = "rsct";

        private const string ContentEncoding = "rsce";

        private const string ContentLanguage = "rscl";

        private const string ContentDisposition = "rscd";
        
        /// <summary>
        /// List of ports used for path style addressing.
        /// </summary>
        private static readonly int[] PathStylePorts = { 10000, 10001, 10002, 10003, 10004, 10100, 10101, 10102, 10103, 10104, 11000, 11001, 11002, 11003, 11004, 11100, 11101, 11102, 11103, 11104 };

        #region For blob service
        public static string GetSharedAccessSignature(CloudBlobContainer container,
            SharedAccessBlobPolicy policy,
            string groupPolicyIdentifier,
            SharedAccessProtocol? protocols,
            IPAddressOrRange ipAddressOrRange,
            string targetStorageVersion)
        {
            if (!container.ServiceClient.Credentials.IsSharedKey)
            {
                string errorMessage = string.Format(CultureInfo.CurrentCulture, "CannotCreateSASWithoutAccountKey");
                throw new InvalidOperationException(errorMessage);
            }

            string resourceName = GetCanonicalName(container, targetStorageVersion);

            string signature = GetHash(policy,
                null /* headers */,
                groupPolicyIdentifier,
                resourceName,
                targetStorageVersion,
                protocols,
                ipAddressOrRange,
                container.ServiceClient.Credentials.ExportKey());

            // Future resource type changes from "c" => "container"
            UriQueryBuilder builder = GetSignature(policy, null /* headers */, groupPolicyIdentifier, "c", signature, null, targetStorageVersion, protocols, ipAddressOrRange);

            return builder.ToString();
        }

        public static string GetSharedAccessSignature(
            CloudBlob blob,
            SharedAccessBlobPolicy policy,
            SharedAccessBlobHeaders headers,
            string groupPolicyIdentifier,
            SharedAccessProtocol? protocols,
            IPAddressOrRange ipAddressOrRange,
            string targetStorageVersion)
        {
            if (!blob.ServiceClient.Credentials.IsSharedKey)
            {
                string errorMessage = string.Format(CultureInfo.CurrentCulture, "CannotCreateSASWithoutAccountKey");
                throw new InvalidOperationException(errorMessage);
            }

            string resourceName = GetCanonicalName(blob, targetStorageVersion);

            string signature = GetHash(policy,
                headers,
                groupPolicyIdentifier,
                resourceName,
                targetStorageVersion,
                protocols,
                ipAddressOrRange,
                blob.ServiceClient.Credentials.ExportKey());

            // Future resource type changes from "c" => "container"
            UriQueryBuilder builder = GetSignature(policy, headers, groupPolicyIdentifier, "b", signature, null, targetStorageVersion, protocols, ipAddressOrRange);

            return builder.ToString();
        }

        private static string GetCanonicalName(CloudBlobContainer container, string targetStorageVersion)
        {
            if (String.CompareOrdinal(targetStorageVersion, "2015-02-21") < 0)
            {
                return "/" + container.ServiceClient.Credentials.AccountName + container.Uri.AbsolutePath;
            }

            string accountName = container.ServiceClient.Credentials.AccountName;
            string containerName = container.Name;

            string canonicalNameFormat = "/{0}/{1}/{2}";

            return string.Format(CultureInfo.InvariantCulture, canonicalNameFormat, "blob", accountName, containerName);
        }

        private static string GetCanonicalName(CloudBlob blob, string targetStorageVersion)
        {
            string accountName = blob.ServiceClient.Credentials.AccountName;
            string containerName = blob.Container.Name;

            // Replace \ with / for uri compatibility when running under .net 4.5. 
            string blobName = blob.Name.Replace('\\', '/');
            string canonicalName = null;
            
            if (String.CompareOrdinal(targetStorageVersion, "2015-02-21") < 0)
            {
                canonicalName = string.Format(CultureInfo.InvariantCulture, "/{0}/{1}/{2}", accountName, containerName, blobName);
            }
            else
            {
                string canonicalNameFormat = "/{0}/{1}/{2}/{3}";
                canonicalName = string.Format(CultureInfo.InvariantCulture, canonicalNameFormat, "blob", accountName, containerName, blobName);
            }

            return canonicalName;
        }

        private static UriQueryBuilder GetSignature(
          SharedAccessBlobPolicy policy,
          SharedAccessBlobHeaders headers,
          string accessPolicyIdentifier,
          string resourceType,
          string signature,
          string accountKeyName,
          string sasVersion,
          SharedAccessProtocol? protocols,
          IPAddressOrRange ipAddressOrRange)
        {

            UriQueryBuilder builder = new UriQueryBuilder();

            AddEscapedIfNotNull(builder, SignedVersion, sasVersion);
            AddEscapedIfNotNull(builder, SignedResource, resourceType);
            AddEscapedIfNotNull(builder, SignedIdentifier, accessPolicyIdentifier);
            AddEscapedIfNotNull(builder, SignedKey, accountKeyName);
            AddEscapedIfNotNull(builder, Signature, signature);
            AddEscapedIfNotNull(builder, SignedProtocols, GetProtocolString(protocols));
            AddEscapedIfNotNull(builder, SignedIP, ipAddressOrRange == null ? null : ipAddressOrRange.ToString());

            if (policy != null)
            {
                AddEscapedIfNotNull(builder, SignedStart, GetDateTimeOrNull(policy.SharedAccessStartTime));
                AddEscapedIfNotNull(builder, SignedExpiry, GetDateTimeOrNull(policy.SharedAccessExpiryTime));

                string permissions = SharedAccessBlobPolicy.PermissionsToString(policy.Permissions);
                if (!string.IsNullOrEmpty(permissions))
                {
                    AddEscapedIfNotNull(builder, SignedPermissions, permissions);
                }
            }

            if (headers != null)
            {
                AddEscapedIfNotNull(builder, CacheControl, headers.CacheControl);
                AddEscapedIfNotNull(builder, ContentType, headers.ContentType);
                AddEscapedIfNotNull(builder, ContentEncoding, headers.ContentEncoding);
                AddEscapedIfNotNull(builder, ContentLanguage, headers.ContentLanguage);
                AddEscapedIfNotNull(builder, ContentDisposition, headers.ContentDisposition);
            }

            return builder;
        }

        private static string GetHash(
           SharedAccessBlobPolicy policy,
           SharedAccessBlobHeaders headers,
           string accessPolicyIdentifier,
           string resourceName,
           string sasVersion,
           SharedAccessProtocol? protocols,
           IPAddressOrRange ipAddressOrRange,
           byte[] keyValue)
        {
            string permissions = null;
            DateTimeOffset? startTime = null;
            DateTimeOffset? expiryTime = null;
            if (policy != null)
            {
                permissions = SharedAccessBlobPolicy.PermissionsToString(policy.Permissions);
                startTime = policy.SharedAccessStartTime;
                expiryTime = policy.SharedAccessExpiryTime;
            }

            string cacheControl = null;
            string contentDisposition = null;
            string contentEncoding = null;
            string contentLanguage = null;
            string contentType = null;
            if (headers != null)
            {
                cacheControl = headers.CacheControl;
                contentDisposition = headers.ContentDisposition;
                contentEncoding = headers.ContentEncoding;
                contentLanguage = headers.ContentLanguage;
                contentType = headers.ContentType;
            }

            string stringToSign = null;

            if (sasVersion.StartsWith("2012-02-12"))
            {
                stringToSign = string.Format(
                            CultureInfo.InvariantCulture,
                            "{0}\n{1}\n{2}\n{3}\n{4}\n{5}",
                            permissions,
                            GetDateTimeOrEmpty(startTime),
                            GetDateTimeOrEmpty(expiryTime),
                            resourceName,
                            accessPolicyIdentifier,
                            sasVersion);
            }
            else if (string.CompareOrdinal(sasVersion, "2015-04-05") >= 0)
            {
                stringToSign = string.Format(
                                        CultureInfo.InvariantCulture,
                                        "{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n{6}\n{7}\n{8}\n{9}\n{10}\n{11}\n{12}",
                                        permissions,
                                        GetDateTimeOrEmpty(startTime),
                                        GetDateTimeOrEmpty(expiryTime),
                                        resourceName,
                                        accessPolicyIdentifier,
                                        ipAddressOrRange == null ? string.Empty : ipAddressOrRange.ToString(),
                                        GetProtocolString(protocols),
                                        sasVersion,
                                        cacheControl,
                                        contentDisposition,
                                        contentEncoding,
                                        contentLanguage,
                                        contentType);
            }
            else
            {
                stringToSign = string.Format(
                                     CultureInfo.InvariantCulture,
                                     "{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n{6}\n{7}\n{8}\n{9}\n{10}",
                                     permissions,
                                     GetDateTimeOrEmpty(startTime),
                                     GetDateTimeOrEmpty(expiryTime),
                                     resourceName,
                                     accessPolicyIdentifier,
                                     sasVersion,
                                     cacheControl,
                                     contentDisposition,
                                     contentEncoding,
                                     contentLanguage,
                                     contentType);
            }

            return ComputeHmac256(keyValue, stringToSign);
        }
        #endregion

        #region for file service

        public static string GetSharedAccessSignature(
            CloudFile file,
            SharedAccessFilePolicy policy,
            SharedAccessFileHeaders headers,
            string groupPolicyIdentifier,
            SharedAccessProtocol? protocols,
            IPAddressOrRange ipAddressOrRange,
            string targetStorageVersion)
        {
            if (!file.ServiceClient.Credentials.IsSharedKey)
            {
                string errorMessage = string.Format(CultureInfo.InvariantCulture, "CannotCreateSASWithoutAccountKey");
                throw new InvalidOperationException(errorMessage);
            }

            string resourceName = GetCanonicalName(file);
            string signature = GetHash(
                policy,
                headers,
                groupPolicyIdentifier,
                resourceName,
                targetStorageVersion,
                protocols,
                ipAddressOrRange,
                file.ServiceClient.Credentials.ExportKey());

            UriQueryBuilder builder =
                GetSignature(
                    policy,
                    headers,
                    groupPolicyIdentifier,
                    "f",
                    signature,
                    null,
                    targetStorageVersion,
                    protocols,
                    ipAddressOrRange);

            return builder.ToString();
        }

        public static string GetSharedAccessSignature(
            CloudFileShare share,
            SharedAccessFilePolicy policy, 
            string groupPolicyIdentifier, 
            SharedAccessProtocol? protocols, 
            IPAddressOrRange ipAddressOrRange,
            string targetStorageVersion)
        {
            if (!share.ServiceClient.Credentials.IsSharedKey)
            {
                string errorMessage = string.Format(CultureInfo.CurrentCulture, "CannotCreateSASWithoutAccountKey");
                throw new InvalidOperationException(errorMessage);
            }

            string resourceName = GetCanonicalName(share);
            string signature = GetHash(
                policy, 
                null /* headers */, 
                groupPolicyIdentifier, 
                resourceName, 
                targetStorageVersion, 
                protocols, 
                ipAddressOrRange,
                share.ServiceClient.Credentials.ExportKey());

            UriQueryBuilder builder = GetSignature(
                policy, 
                null /* headers */, 
                groupPolicyIdentifier, 
                "s", 
                signature, 
                null, 
                targetStorageVersion, 
                protocols, 
                ipAddressOrRange);

            return builder.ToString();
        }

        private static string GetCanonicalName(CloudFile file)
        {
            string accountName = file.ServiceClient.Credentials.AccountName;
            string shareName = file.Share.Name;

            // Replace \ with / for uri compatibility when running under .net 4.5. 
            string fileAndDirectoryName = GetFileAndDirectoryName(file.Uri, false).Replace('\\', '/');

            return string.Format(CultureInfo.InvariantCulture, "/{0}/{1}/{2}/{3}", "file", accountName, shareName, fileAndDirectoryName);
        }

        private static string GetCanonicalName(CloudFileShare share)
        {
            string accountName = share.ServiceClient.Credentials.AccountName;
            string shareName = share.Name;

            return string.Format(CultureInfo.InvariantCulture, "/{0}/{1}/{2}", "file", accountName, shareName);
        }

        private static string GetFileAndDirectoryName(Uri fileAddress, bool? usePathStyleUris)
        {
            if (usePathStyleUris == null)
            {
                // Automatically determine whether to use path style vs host style uris
                usePathStyleUris = UsePathStyleAddressing(fileAddress);
            }

            string[] addressParts = fileAddress.Segments;
            int shareIndex = usePathStyleUris.Value ? 2 : 1;

            if (addressParts.Length - 1 < shareIndex)
            {
                // No reference appears to any share or file.
                string error = string.Format(CultureInfo.CurrentCulture, "MissingShareInformation", fileAddress);
                throw new ArgumentException(error, "fileAddress");
            }
            else if (addressParts.Length - 1 == shareIndex)
            {
                // This is root directory of a share.
                return string.Empty;
            }
            else
            {
                // This is a file with directories (the relevant case).
                // Skip (shareIndex + 1) because Skip takes a count, not an index, as a param.
                return Uri.UnescapeDataString(string.Concat(addressParts.Skip(shareIndex + 1)));
            }
        }

        private static bool UsePathStyleAddressing(Uri uri)
        {
            if (uri.HostNameType != UriHostNameType.Dns)
            {
                return true;
            }

            return PathStylePorts.Contains(uri.Port);
        }

        private static string GetHash(
            SharedAccessFilePolicy policy,
            SharedAccessFileHeaders headers,
            string accessPolicyIdentifier,
            string resourceName,
            string sasVersion,
            SharedAccessProtocol? protocols,
            IPAddressOrRange ipAddressOrRange,
            byte[] keyValue)
        {
            string permissions = null;
            DateTimeOffset? startTime = null;
            DateTimeOffset? expiryTime = null;
            if (policy != null)
            {
                permissions = SharedAccessFilePolicy.PermissionsToString(policy.Permissions);
                startTime = policy.SharedAccessStartTime;
                expiryTime = policy.SharedAccessExpiryTime;
            }

            string cacheControl = null;
            string contentDisposition = null;
            string contentEncoding = null;
            string contentLanguage = null;
            string contentType = null;
            if (headers != null)
            {
                cacheControl = headers.CacheControl;
                contentDisposition = headers.ContentDisposition;
                contentEncoding = headers.ContentEncoding;
                contentLanguage = headers.ContentLanguage;
                contentType = headers.ContentType;
            }

            string stringToSign = null;

            if (string.CompareOrdinal(sasVersion, "2015-04-05") >= 0)
            {
                stringToSign = string.Format(
                                        CultureInfo.InvariantCulture,
                                        "{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n{6}\n{7}\n{8}\n{9}\n{10}\n{11}\n{12}",
                                        permissions,
                                        GetDateTimeOrEmpty(startTime),
                                        GetDateTimeOrEmpty(expiryTime),
                                        resourceName,
                                        accessPolicyIdentifier,
                                        ipAddressOrRange == null ? string.Empty : ipAddressOrRange.ToString(),
                                        GetProtocolString(protocols),
                                        sasVersion,
                                        cacheControl,
                                        contentDisposition,
                                        contentEncoding,
                                        contentLanguage,
                                        contentType);
            }
            else
            {
                stringToSign = string.Format(
                        CultureInfo.InvariantCulture,
                        "{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n{6}\n{7}\n{8}\n{9}\n{10}",
                        permissions,
                        GetDateTimeOrEmpty(startTime),
                        GetDateTimeOrEmpty(expiryTime),
                        resourceName,
                        accessPolicyIdentifier,
                        sasVersion,
                        cacheControl,
                        contentDisposition,
                        contentEncoding,
                        contentLanguage,
                        contentType);
            }

            return ComputeHmac256(keyValue, stringToSign);
        }

        private static UriQueryBuilder GetSignature(
            SharedAccessFilePolicy policy,
            SharedAccessFileHeaders headers,
            string accessPolicyIdentifier,
            string resourceType,
            string signature,
            string accountKeyName,
            string sasVersion,
            SharedAccessProtocol? protocols,
            IPAddressOrRange ipAddressOrRange)
        {
            UriQueryBuilder builder = new UriQueryBuilder();

            AddEscapedIfNotNull(builder, SignedVersion, sasVersion);
            AddEscapedIfNotNull(builder, SignedResource, resourceType);
            AddEscapedIfNotNull(builder, SignedIdentifier, accessPolicyIdentifier);
            AddEscapedIfNotNull(builder, SignedKey, accountKeyName);
            AddEscapedIfNotNull(builder, Signature, signature);
            AddEscapedIfNotNull(builder, SignedProtocols, GetProtocolString(protocols));
            AddEscapedIfNotNull(builder, SignedIP, ipAddressOrRange == null ? null : ipAddressOrRange.ToString());

            if (policy != null)
            {
                AddEscapedIfNotNull(builder, SignedStart, GetDateTimeOrNull(policy.SharedAccessStartTime));
                AddEscapedIfNotNull(builder, SignedExpiry, GetDateTimeOrNull(policy.SharedAccessExpiryTime));

                string permissions = SharedAccessFilePolicy.PermissionsToString(policy.Permissions);
                if (!string.IsNullOrEmpty(permissions))
                {
                    AddEscapedIfNotNull(builder, SignedPermissions, permissions);
                }
            }

            if (headers != null)
            {
                AddEscapedIfNotNull(builder, CacheControl, headers.CacheControl);
                AddEscapedIfNotNull(builder, ContentType, headers.ContentType);
                AddEscapedIfNotNull(builder, ContentEncoding, headers.ContentEncoding);
                AddEscapedIfNotNull(builder, ContentLanguage, headers.ContentLanguage);
                AddEscapedIfNotNull(builder, ContentDisposition, headers.ContentDisposition);
            }

            return builder;
        }
        #endregion

        #region general signing       

        private static string ComputeHmac256(byte[] key, string message)
        {
            using (HashAlgorithm hashAlgorithm = new HMACSHA256(key))
            {
                byte[] messageBuffer = Encoding.UTF8.GetBytes(message);
                return Convert.ToBase64String(hashAlgorithm.ComputeHash(messageBuffer));
            }
        }

        private static string GetDateTimeOrEmpty(DateTimeOffset? value)
        {
            string result = GetDateTimeOrNull(value) ?? string.Empty;
            return result;
        }

        private static string GetDateTimeOrNull(DateTimeOffset? value)
        {
            string result = value != null ? value.Value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) : null;
            return result;
        }

        private static string GetProtocolString(SharedAccessProtocol? protocols)
        {
            if (!protocols.HasValue)
            {
                return null;
            }

            if ((protocols.Value != SharedAccessProtocol.HttpsOnly) && (protocols.Value != SharedAccessProtocol.HttpsOrHttp))
            {
                throw new ArgumentException(String.Format(CultureInfo.InvariantCulture, "InvalidProtocolsInSAS", protocols.Value));
            }

            return protocols.Value == SharedAccessProtocol.HttpsOnly ? "https" : "https,http";
        }
        private static void AddEscapedIfNotNull(UriQueryBuilder builder, string name, string value)
        {
            if (value != null)
            {
                builder.Add(name, value);
            }
        }
        #endregion
    }
}
