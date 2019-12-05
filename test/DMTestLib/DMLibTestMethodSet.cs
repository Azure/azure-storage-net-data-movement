//------------------------------------------------------------------------------
// <copyright file="DMLibTestMethodSet.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;

    public enum DMLibTestMethodSet
    {
        AllValidDirection,
        Cloud2Cloud,
        AllAsync,
        AllSync,
        AllServiceSideSync,
        CloudSource,
        CloudBlobSource,
        CloudFileSource,
        LocalSource,
        CloudDest,
        CloudBlobDest,
        CloudFileDest,
        LocalDest,

        DirAllValidDirection,
        DirCloud2Cloud,
        DirAllAsync,
        DirAllSync,
        DirAllServiceSideSync,
        DirCloudSource,
        DirCloudBlobSource,
        DirCloudFileSource,
        DirLocalSource,
        DirCloudDest,
        DirCloudBlobDest,
        DirCloudFileDest,
        DirLocalDest,
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
    public class DMLibTestMethodSetAttribute : MultiDirectionTestMethodSetAttribute
    {
        public static DMLibTestMethodSetAttribute AllValidDirectionSet;
        public static DMLibTestMethodSetAttribute DirAllValidDirectionSet;

        static DMLibTestMethodSetAttribute()
        {
            // All valid directions
            AllValidDirectionSet = new DMLibTestMethodSetAttribute();
            // Sync copy
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Local, DMLibDataType.Cloud));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Stream, DMLibDataType.Cloud));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.Local));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.Stream));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudFile, DMLibDataType.Cloud));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.CloudFile));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudBlob));
            
            // Async copy
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.URI, DMLibDataType.Cloud, DMLibCopyMethod.ServiceSideAsyncCopy));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideAsyncCopy));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudFile, DMLibDataType.BlockBlob, DMLibCopyMethod.ServiceSideAsyncCopy));

            // Service side sync copying
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudFile, DMLibDataType.Cloud, DMLibCopyMethod.ServiceSideSyncCopy));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy));
            AllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy));

            // All valid directory transfer directions
            DirAllValidDirectionSet = new DMLibTestMethodSetAttribute();
            // Sync copy
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Local, DMLibDataType.Cloud));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.Local));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudFile, DMLibDataType.Cloud));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.CloudFile));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudBlob));

            // Async copy
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideAsyncCopy));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideAsyncCopy));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudFile, DMLibDataType.BlockBlob, DMLibCopyMethod.ServiceSideAsyncCopy));

            // Service side sync copying
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudFile, DMLibDataType.Cloud, DMLibCopyMethod.ServiceSideSyncCopy));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.Cloud, DMLibDataType.CloudFile, DMLibCopyMethod.ServiceSideSyncCopy));
            DirAllValidDirectionSet.AddTestMethodAttribute(new DMLibTestMethodAttribute(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy));
        }

        public DMLibTestMethodSetAttribute()
        {
        }

        /// <summary>
        /// Create a new instance of <see cref="DMLibTestMethodSetAttribute"/> containing specific
        /// valid transfer directions from a query string. Query string format:
        ///     propertyName1=value1,propertyName2=value2...
        /// e.g.
        /// To specify all valid async copy directions to blob:
        ///     DestType=CloudBlob,IsAsync=true
        /// </summary>
        /// <param name="queryString">Query string</param>
        public DMLibTestMethodSetAttribute(string queryString)
        {
            this.AddTestMethodAttribute(AllValidDirectionSet);

            DMLibDirectionFilter directionFilter = new DMLibDirectionFilter(queryString);
            this.AddDirectionFilter(directionFilter);
        }

        public DMLibTestMethodSetAttribute(DMLibTestMethodSet directionSet)
        {
            switch (directionSet)
            {
                case DMLibTestMethodSet.AllValidDirection:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    break;
                case DMLibTestMethodSet.Cloud2Cloud:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.Cloud,
                        DestType = DMLibDataType.Cloud,
                    });
                    break;
                case DMLibTestMethodSet.AllAsync:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        CopyMethod = DMLibCopyMethod.ServiceSideAsyncCopy,
                    });
                    break;
                case DMLibTestMethodSet.AllSync:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        CopyMethod = DMLibCopyMethod.SyncCopy,
                    });
                    break;
                case DMLibTestMethodSet.AllServiceSideSync:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        CopyMethod = DMLibCopyMethod.ServiceSideSyncCopy,
                    });
                    break;
                case DMLibTestMethodSet.CloudSource:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.Cloud,
                    });
                    break;
                case DMLibTestMethodSet.CloudBlobSource:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.CloudBlob,
                    });
                    break;
                case DMLibTestMethodSet.CloudFileSource:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.CloudFile,
                    });
                    break;
                case DMLibTestMethodSet.LocalSource:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.Local,
                    });
                    break;
                case DMLibTestMethodSet.CloudDest:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.Cloud,
                    });
                    break;
                case DMLibTestMethodSet.CloudBlobDest:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.CloudBlob,
                    });
                    break;
                case DMLibTestMethodSet.CloudFileDest:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.CloudFile,
                    });
                    break;
                case DMLibTestMethodSet.LocalDest:
                    this.AddTestMethodAttribute(AllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.Local,
                    });
                    break;
                case DMLibTestMethodSet.DirAllValidDirection:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    break;
                case DMLibTestMethodSet.DirCloud2Cloud:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.Cloud,
                        DestType = DMLibDataType.Cloud,
                    });
                    break;
                case DMLibTestMethodSet.DirAllAsync:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        CopyMethod = DMLibCopyMethod.ServiceSideAsyncCopy
                    });
                    break;
                case DMLibTestMethodSet.DirAllSync:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        CopyMethod = DMLibCopyMethod.SyncCopy
                    });
                    break;
                case DMLibTestMethodSet.DirAllServiceSideSync:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        CopyMethod = DMLibCopyMethod.ServiceSideSyncCopy
                    });
                    break;
                case DMLibTestMethodSet.DirCloudSource:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.Cloud,
                    });
                    break;
                case DMLibTestMethodSet.DirCloudBlobSource:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.CloudBlob,
                    });
                    break;
                case DMLibTestMethodSet.DirCloudFileSource:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.CloudFile,
                    });
                    break;
                case DMLibTestMethodSet.DirLocalSource:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        SourceType = DMLibDataType.Local,
                    });
                    break;
                case DMLibTestMethodSet.DirCloudDest:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.Cloud,
                    });
                    break;
                case DMLibTestMethodSet.DirCloudBlobDest:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.CloudBlob,
                    });
                    break;
                case DMLibTestMethodSet.DirCloudFileDest:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.CloudFile,
                    });
                    break;
                case DMLibTestMethodSet.DirLocalDest:
                    this.AddTestMethodAttribute(DirAllValidDirectionSet);
                    this.AddDirectionFilter(new DMLibDirectionFilter()
                    {
                        DestType = DMLibDataType.Local,
                    });
                    break;
                default:
                    throw new ArgumentException(string.Format("Invalid MultiDirectionSet: {0}", directionSet.ToString()), "directionSet");
            }
        }
    }
}
