//------------------------------------------------------------------------------
// <copyright file="AllTransferDirectionTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using MS.Test.Common.MsTestLib;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.File;
    using System.Threading.Tasks;
#if DNXCORE50
    using Xunit;
    using System.Threading.Tasks;

    [Collection(Collections.Global)]
    public class AllTransferDirectionTest : DMLibTestBase, IClassFixture<AllTransferDirectionFixture>, IDisposable
    {
        public AllTransferDirectionTest()
        {
            MyTestInitialize();
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            MyTestCleanup();
        }
#else
    [TestClass]
    public class AllTransferDirectionTest : DMLibTestBase
    {
#endif

#region Additional test attributes

        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            Test.Info("Class Initialize: AllTransferDirectionTest");
            DMLibTestBase.BaseClassInitialize(testContext);

            DMLibTestBase.CleanupSource = false;

            AllTransferDirectionTest.PrepareSourceData();
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            DMLibTestBase.BaseClassCleanup();
        }

        [TestInitialize()]
        public void MyTestInitialize()
        {
            base.BaseTestInitialize();

            AllTransferDirectionTest.CleanupAllDestination();
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            base.BaseTestCleanup();
        }
#endregion

        private static Dictionary<string, DMLibDataInfo> sourceDataInfos = new Dictionary<string, DMLibDataInfo>();
        private static Dictionary<string, FileNode> expectedFileNodes = new Dictionary<string, FileNode>();
        private static Dictionary<string, FileNode> singleObjectNodes = new Dictionary<string, FileNode>();
        private static Dictionary<string, DirNode> directoryNodes = new Dictionary<string, DirNode>();

        private static DMLibDataInfo GetSourceDataInfo(string key)
        {
            DMLibDataInfo result;
            if (!sourceDataInfos.ContainsKey(key))
            {
                result = new DMLibDataInfo(string.Empty);

                sourceDataInfos.Add(key, result);
            }

            return sourceDataInfos[key];
        }

        private static string GetSourceDataInfoKey(DMLibTransferDirection direction)
        {
            if (direction.SourceType != DMLibDataType.URI)
            {
                return direction.SourceType.ToString();
            }
            else
            {
                return GetTransferFileName(direction);
            }
        }

        private static void PrepareSourceData()
        {
            PrepareDirSourceData(5 * 1024 * 1024);
            PrepareSingleObjectSourceData(5 * 1024 * 1024);

            // Generate source data
            foreach (var pair in sourceDataInfos)
            {
                DMLibDataType sourceDataType;
                if (Enum.TryParse<DMLibDataType>(pair.Key, out sourceDataType))
                {
                    DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(sourceDataType);
                    sourceAdaptor.Cleanup();
                    sourceAdaptor.CreateIfNotExists();

                    sourceAdaptor.GenerateData(pair.Value);
                }
            }

            // Generate source data for URI source separately since it's destination related
            DataAdaptor<DMLibDataInfo> uriSourceAdaptor = GetSourceAdaptor(DMLibDataType.URI);
            uriSourceAdaptor.Cleanup();
            uriSourceAdaptor.CreateIfNotExists();

            DMLibTestContext.SourceType = DMLibDataType.URI;
            DMLibTestContext.IsAsync = true;

            DMLibDataType[] uriDestDataTypes = { DMLibDataType.CloudFile, DMLibDataType.BlockBlob, DMLibDataType.PageBlob, DMLibDataType.AppendBlob };
            foreach (DMLibDataType uriDestDataType in uriDestDataTypes)
            {
                DMLibTestContext.DestType = uriDestDataType;
                string sourceDataInfoKey = GetTransferString(DMLibDataType.URI, uriDestDataType, true);

                uriSourceAdaptor.GenerateData(sourceDataInfos[sourceDataInfoKey]);
            }
        }

        private static void CleanupAllDestination()
        {
            // Clean up destination
            foreach (DMLibDataType destDataType in DataTypes)
            {
                if (destDataType != DMLibDataType.URI)
                {
                    DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(destDataType);
                    destAdaptor.Cleanup();
                    destAdaptor.CreateIfNotExists();
                }
            }
        }

        private static void PrepareDirSourceData(long fileSizeInB)
        {
            foreach (DMLibTransferDirection direction in GetAllDirectoryValidDirections())
            {
                string dirName = GetTransferDirName(direction);
                string fileName = dirName;
                string sourceDataInfoKey = GetSourceDataInfoKey(direction);

                DMLibDataInfo sourceDataInfo = GetSourceDataInfo(sourceDataInfoKey);

                DirNode subDirNode = new DirNode(dirName);
                DMLibDataHelper.AddOneFileInBytes(subDirNode, fileName, fileSizeInB);

                sourceDataInfo.RootNode.AddDirNode(subDirNode);

                directoryNodes.Add(dirName, subDirNode);

                expectedFileNodes.Add(fileName, subDirNode.GetFileNode(fileName));
            }
        }

        private static void PrepareSingleObjectSourceData(long fileSizeInB)
        {
            foreach (DMLibTransferDirection direction in GetAllValidDirections())
            {
                string fileName = GetTransferFileName(direction);
                string sourceDataInfoKey = GetSourceDataInfoKey(direction);

                DMLibDataInfo sourceDataInfo = GetSourceDataInfo(sourceDataInfoKey);

                DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, fileName, fileSizeInB);

                FileNode sourceFileNode = sourceDataInfo.RootNode.GetFileNode(fileName);
                singleObjectNodes.Add(fileName, sourceFileNode);

                expectedFileNodes.Add(fileName, sourceFileNode);
            }
        }

        private static List<TransferItem> GetTransformItemsForAllDirections(bool resume)
        {
            List<TransferItem> allItems = new List<TransferItem>();
            allItems.AddRange(GetTransformItemsForAllSingleTransferDirections(resume));
            allItems.AddRange(GetTransformItemsForAllDirTransferDirections(resume));

            return allItems;
        }

        private static List<TransferItem> GetTransformItemsForAllSingleTransferDirections(bool resume)
        {
            List<TransferItem> allItems = new List<TransferItem>();
            foreach (DMLibTransferDirection direction in GetAllValidDirections())
            {
                if (resume && (direction.SourceType == DMLibDataType.Stream || direction.DestType == DMLibDataType.Stream))
                {
                    continue;
                }

                string fileName = GetTransferFileName(direction);
                DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(direction.SourceType);
                DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(direction.DestType);

                FileNode fileNode = singleObjectNodes[fileName];
                TransferItem item = new TransferItem()
                {
                    SourceObject = sourceAdaptor.GetTransferObject(string.Empty, fileNode),
                    DestObject = destAdaptor.GetTransferObject(string.Empty, fileNode),
                    SourceType = direction.SourceType,
                    DestType = direction.DestType,
                    IsServiceCopy = direction.IsAsync,
                    TransferContext = new SingleTransferContext()
                    {
                        SetAttributesCallbackAsync = AllTransferDirectionTest.SetAttributesCallbackMethodAsync
                    }
                };
                allItems.Add(item);
            }

            return allItems;
        }

        private static List<TransferItem> GetTransformItemsForAllDirTransferDirections(bool resume)
        {
            List<TransferItem> allItems = new List<TransferItem>();
            foreach (DMLibTransferDirection direction in GetAllDirectoryValidDirections())
            {
                string dirName = GetTransferDirName(direction);
                DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(direction.SourceType);
                DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(direction.DestType);

                DirNode dirNode = directoryNodes[dirName];

                dynamic options = DMLibTestBase.GetDefaultTransferDirectoryOptions(direction.SourceType, direction.DestType);
                options.Recursive = true;

                TransferItem item = new TransferItem()
                {
                    SourceObject = sourceAdaptor.GetTransferObject(string.Empty, dirNode),
                    DestObject = destAdaptor.GetTransferObject(string.Empty, dirNode),
                    SourceType = direction.SourceType,
                    DestType = direction.DestType,
                    IsServiceCopy = direction.IsAsync,
                    IsDirectoryTransfer = true,
                    Options = options,
                    TransferContext = new DirectoryTransferContext()
                    {
                        SetAttributesCallbackAsync = AllTransferDirectionTest.SetAttributesCallbackMethodAsync
                    }
                };

                allItems.Add(item);
            }

            return allItems;
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ResumeInAllDirections()
        {
            ResumeInAllDirectionsHelper(false);
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void ResumeInAllDirectoryDirections()
        {
            ResumeInAllDirectionsHelper(true);
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
#if DNXCORE50
        [TestStartEnd()]
#endif
        public void TransferInAllDirections()
        {
            List<TransferItem> allItems = AllTransferDirectionTest.GetTransformItemsForAllDirections(resume: false);

            // Execution
            var result = this.RunTransferItems(allItems, new TestExecutionOptions<DMLibDataInfo>());

            // Verify all files are transfered successfully
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception occurs.");
            foreach (DMLibDataType destDataType in DataTypes)
            {
                if (DMLibDataType.Stream == destDataType || DMLibDataType.URI == destDataType)
                {
                    continue;
                }

                DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(destDataType);
                DMLibDataInfo destDataInfo = destAdaptor.GetTransferDataInfo(string.Empty);

                foreach (FileNode destFileNode in destDataInfo.EnumerateFileNodes())
                {
                    FileNode sourceFileNode = expectedFileNodes[destFileNode.Name];

                    if (IsCloudService(destDataType))
                    {
                        IDictionary<string, string> metadata = new Dictionary<string, string>();
                        metadata.Add("aa", "bb");
                        sourceFileNode.ContentLanguage = "EN";
                        sourceFileNode.Metadata = metadata;
                    }
                    Test.Assert(DMLibDataHelper.Equals(sourceFileNode, destFileNode), "Verify transfer result.");
                }
            }
        }

        private void ResumeInAllDirectionsHelper(bool directoryTransfer)
        {
            List<TransferItem> allItems = directoryTransfer ? AllTransferDirectionTest.GetTransformItemsForAllDirTransferDirections(resume: true) 
                : AllTransferDirectionTest.GetTransformItemsForAllSingleTransferDirections(true);

            int fileCount = expectedFileNodes.Keys.Count;

            // Execution and store checkpoints
            CancellationTokenSource tokenSource = new CancellationTokenSource();

            TransferContext transferContext = null;

            if (directoryTransfer)
            {
                transferContext = new DirectoryTransferContext();
            }
            else
            {
                transferContext = new SingleTransferContext();
            }

            var progressChecker = new ProgressChecker(fileCount, 1024 * fileCount);
            transferContext.ProgressHandler = progressChecker.GetProgressHandler();
            allItems.ForEach(item =>
            {
                item.CancellationToken = tokenSource.Token;
                item.TransferContext = transferContext;
            });

            var options = new TestExecutionOptions<DMLibDataInfo>();
            options.DisableDestinationFetch = true;

            // Checkpoint names
            const string PartialStarted = "PartialStarted",
                         AllStarted = "AllStarted",
                         AllStartedAndWait = "AllStartedAndWait",
                         BeforeCancel = "BeforeCancel",
                         AfterCancel = "AfterCancel";
            Dictionary<string, TransferCheckpoint> checkpoints = new Dictionary<string, TransferCheckpoint>();

            TransferItem randomItem = allItems[random.Next(0, allItems.Count)];

            randomItem.AfterStarted = () =>
            {
                Test.Info("Store check point after transfer item: {0}.", randomItem.ToString());
                checkpoints.Add(PartialStarted, transferContext.LastCheckpoint);
            };

            options.AfterAllItemAdded = () =>
            {
                progressChecker.DataTransferred.WaitOne();
                checkpoints.Add(AllStarted, transferContext.LastCheckpoint);
                Thread.Sleep(1000);
                checkpoints.Add(AllStartedAndWait, transferContext.LastCheckpoint);
                Thread.Sleep(1000);
                checkpoints.Add(BeforeCancel, transferContext.LastCheckpoint);
                tokenSource.Cancel();
                checkpoints.Add(AfterCancel, transferContext.LastCheckpoint);
            };

            var result = this.RunTransferItems(allItems, options);

            // Resume with stored checkpoints in random order
            var checkpointList = new List<KeyValuePair<string, TransferCheckpoint>>();
            checkpointList.AddRange(checkpoints);
            checkpointList.Shuffle();

            foreach (var pair in checkpointList)
            {
                Test.Info("===Resume with checkpoint '{0}'===", pair.Key);
                options = new TestExecutionOptions<DMLibDataInfo>();
                options.DisableDestinationFetch = true;

                progressChecker.Reset();

                if (directoryTransfer)
                {
                    transferContext = new DirectoryTransferContext(DMLibTestHelper.RandomReloadCheckpoint(pair.Value))
                    {
                        ProgressHandler = progressChecker.GetProgressHandler(),

                        // The checkpoint can be stored when DMLib doesn't check overwrite callback yet.
                        // So it will case an skip file error if the desination file already exists and 
                        // We don't have overwrite callback here.
                        ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY()
                    };
                }
                else
                {
                    transferContext = new SingleTransferContext(DMLibTestHelper.RandomReloadCheckpoint(pair.Value))
                    {
                        ProgressHandler = progressChecker.GetProgressHandler(),

                        // The checkpoint can be stored when DMLib doesn't check overwrite callback yet.
                        // So it will case an skip file error if the desination file already exists and 
                        // We don't have overwrite callback here.
                        ShouldOverwriteCallbackAsync = DMLibInputHelper.GetDefaultOverwiteCallbackY()
                    };
                }

                int expectedFailureCount = 0;

                transferContext.FileFailed += (resource, eventArgs) =>
                {
                    TransferException exception = eventArgs.Exception as TransferException;
                    if (null != exception && exception.ErrorCode == TransferErrorCode.MismatchCopyId)
                    {
                        Interlocked.Increment(ref expectedFailureCount);
                    }
                };

                TransferEventChecker eventChecker = new TransferEventChecker();
                eventChecker.Apply(transferContext);

                List<TransferItem> itemsToResume = allItems.Select(item =>
                {
                    TransferItem itemToResume = item.Clone();
                    itemToResume.TransferContext = transferContext;
                    return itemToResume;
                }).ToList();

                result = this.RunTransferItems(itemsToResume, options);

                foreach (DMLibDataType destDataType in DataTypes)
                {
                    if (DMLibDataType.URI == destDataType)
                    {
                        continue;
                    }

                    DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(destDataType);
                    DMLibDataInfo destDataInfo = destAdaptor.GetTransferDataInfo(string.Empty);

                    foreach (FileNode destFileNode in destDataInfo.EnumerateFileNodes())
                    {
                        string fileName = destFileNode.Name;
                        FileNode sourceFileNode = expectedFileNodes[fileName];

                        Test.Assert(DMLibDataHelper.Equals(sourceFileNode, destFileNode), "Verify transfer result.");
                    }
                }

                if (!directoryTransfer)
                {
                    Test.Assert(result.Exceptions.Count == expectedFailureCount, "Verify no error happens. Expect {0}, Actual: {1}", expectedFailureCount, result.Exceptions.Count);
                }
                else
                {
                    Test.Assert(result.Exceptions.Count == 0, "Verify no exception happens. Actual: {0}", result.Exceptions.Count);
                    Test.Assert(eventChecker.FailedFilesNumber == expectedFailureCount, "Verify no unexpected error happens. Expect {0}, Actual: {1}", expectedFailureCount, eventChecker.FailedFilesNumber);
                }
            }
        }

        private static async Task SetAttributesCallbackMethodAsync(object destObj)
        {
            await Task.Run(() =>
            {
                if (destObj is CloudBlob || destObj is CloudFile)
                {
                    dynamic destCloudObj = destObj;
                    destCloudObj.Properties.ContentLanguage = "EN";

                    if (!destCloudObj.Metadata.ContainsKey("aa"))
                    {
                        destCloudObj.Metadata.Add("aa", "bb");
                    }
                }
            });
        }

        private static string GetTransferFileName(DMLibTransferDirection direction)
        {
            return GetTransferString(direction.SourceType, direction.DestType, direction.IsAsync);
        }

        private static string GetTransferDirName(DMLibTransferDirection direction)
        {
            return "dir" + GetTransferString(direction.SourceType, direction.DestType, direction.IsAsync);
        }

        private static string GetTransferString(DMLibDataType sourceType, DMLibDataType destType, bool isAsync)
        {
            return sourceType.ToString() + destType.ToString() + (isAsync ? "async" : ""); 
        }

        private static IEnumerable<DMLibTransferDirection> GetAllValidDirections()
        {
            return EnumerateAllDirections(validSyncDirections, validAsyncDirections);
        }

        private static IEnumerable<DMLibTransferDirection> GetAllDirectoryValidDirections()
        {
            return EnumerateAllDirections(dirValidSyncDirections, dirValidAsyncDirections);
        }

        private static IEnumerable<DMLibTransferDirection> EnumerateAllDirections(bool[][] syncDirections, bool[][] asyncDirections)
        {
            for (int sourceIndex = 0; sourceIndex < DataTypes.Length; ++sourceIndex)
            {
                for (int destIndex = 0; destIndex < DataTypes.Length; ++destIndex)
                {
                    DMLibDataType sourceDataType = DataTypes[sourceIndex];
                    DMLibDataType destDataType = DataTypes[destIndex];

                    if (syncDirections[sourceIndex][destIndex])
                    {
                        yield return new DMLibTransferDirection()
                        {
                            SourceType = sourceDataType,
                            DestType = destDataType,
                            IsAsync = false,
                        };
                    }

                    if (asyncDirections[sourceIndex][destIndex])
                    {
                        yield return new DMLibTransferDirection()
                        {
                            SourceType = sourceDataType,
                            DestType = destDataType,
                            IsAsync = true,
                        };
                    }
                }
            }
        }

        // [SourceType][DestType]
        private static bool[][] validSyncDirections = 
        {
            //          stream, uri, local, xsmb, block, page, append
            new bool[] {false, false, false, true, true, true, true}, // stream
            new bool[] {false, false, false, false, false, false, false}, // uri
            new bool[] {false, false, false, true, true, true, true}, // local
            new bool[] {true, false, true, true, true, true, true}, // xsmb
            new bool[] {true, false, true, true, true, false, false}, // block
            new bool[] {true, false, true, true, false, true, false}, // page
            new bool[] {true, false, true, true, false, false, true}, // append
        };

        // [SourceType][DestType]
        private static bool[][] validAsyncDirections = 
        {
            //          stream, uri, local, xsmb, block, page, append
            new bool[] {false, false, false, false, false, false, false}, // stream
            new bool[] {false, false, false, true, true, true, true}, // uri
            new bool[] {false, false, false, false, false, false, false}, // local
            new bool[] {false, false, false, true, true, false, false}, // xsmb
            new bool[] {false, false, false, true, true, false, false}, // block
            new bool[] {false, false, false, true, false, true, false}, // page
            new bool[] {false, false, false, true, false, false, true}, // append
        };

        // [SourceType][DestType]
        private static bool[][] dirValidSyncDirections = 
        {
            //          stream, uri, local, xsmb, block, page, append
            new bool[] {false, false, false, false, false, false, false}, // stream
            new bool[] {false, false, false, false, false, false, false}, // uri
            new bool[] {false, false, false, true, true, true, true}, // local
            new bool[] {false, false, true, true, true, true, true}, // xsmb
            new bool[] {false, false, true, true, true, false, false}, // block
            new bool[] {false, false, true, true, false, true, false}, // page
            new bool[] {false, false, true, true, false, false, true}, // append
        };

        // [SourceType][DestType]
        private static bool[][] dirValidAsyncDirections = 
        {
            //          stream, uri, local, xsmb, block, page, append
            new bool[] {false, false, false, false, false, false, false}, // stream
            new bool[] {false, false, false, false, false, false, false}, // uri
            new bool[] {false, false, false, false, false, false, false}, // local
            new bool[] {false, false, false, true, true, false, false}, // xsmb
            new bool[] {false, false, false, true, true, false, false}, // block
            new bool[] {false, false, false, true, false, true, false}, // page
            new bool[] {false, false, false, true, false, false, true}, // append
        };

        private static DMLibDataType[] DataTypes = 
        {
            DMLibDataType.Stream,
            DMLibDataType.URI,
            DMLibDataType.Local,
            DMLibDataType.CloudFile,
            DMLibDataType.BlockBlob,
            DMLibDataType.PageBlob,
            DMLibDataType.AppendBlob
        };

        private static int GetValidDirectionsIndex(DMLibDataType dataType)
        {
            switch (dataType)
            {
                case DMLibDataType.Stream:
                    return 0;
                case DMLibDataType.URI:
                    return 1;
                case DMLibDataType.Local:
                    return 2;
                case DMLibDataType.CloudFile:
                    return 3;
                case DMLibDataType.BlockBlob:
                    return 4;
                case DMLibDataType.PageBlob:
                    return 5;
                case DMLibDataType.AppendBlob:
                    return 6;
                default:
                    throw new ArgumentException(string.Format("Invalid data type {0}", dataType), "dataType");;
            }
        }
    }

    internal class DMLibTransferDirection
    {
        public DMLibDataType SourceType
        {
            get;
            set;
        }

        public DMLibDataType DestType
        {
            get;
            set;
        }

        public bool IsAsync
        {
            get;
            set;
        }
    }
}
