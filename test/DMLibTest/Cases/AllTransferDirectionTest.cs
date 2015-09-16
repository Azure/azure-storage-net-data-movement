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

    [TestClass]
    public class AllTransferDirectionTest : DMLibTestBase
    {
        #region Additional test attributes
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            DMLibTestBase.BaseClassInitialize(testContext);
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
        }

        [TestCleanup()]
        public void MyTestCleanup()
        {
            base.BaseTestCleanup();
        }
        #endregion

        private Dictionary<string, FileNode> PrepareSourceData(long fileSizeInB)
        {
            var sourceFileNodes = new Dictionary<string, FileNode>();
            var sourceDataInfos = new Dictionary<string, DMLibDataInfo>();

            // Prepare source data info
            foreach (DMLibTransferDirection direction in GetAllValidDirections())
            {
                string fileName = GetTransferFileName(direction);

                DMLibDataInfo sourceDataInfo;
                string sourceDataInfoKey;
                if (direction.SourceType != DMLibDataType.URI)
                {
                    sourceDataInfoKey = direction.SourceType.ToString();
                }
                else
                {
                    sourceDataInfoKey = GetTransferFileName(direction);
                }

                if (sourceDataInfos.ContainsKey(sourceDataInfoKey))
                {
                    sourceDataInfo = sourceDataInfos[sourceDataInfoKey];
                }
                else
                {
                    sourceDataInfo = new DMLibDataInfo(string.Empty);
                    sourceDataInfos[sourceDataInfoKey] = sourceDataInfo;
                }

                DMLibDataHelper.AddOneFileInBytes(sourceDataInfo.RootNode, fileName, fileSizeInB);

                FileNode sourceFileNode = sourceDataInfo.RootNode.GetFileNode(fileName);
                sourceFileNodes.Add(fileName, sourceFileNode);
            }

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
                string sourceDataInfoKey = GetTransferFileName(DMLibDataType.URI, uriDestDataType, true);

                uriSourceAdaptor.GenerateData(sourceDataInfos[sourceDataInfoKey]);
            }

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

            return sourceFileNodes;
        }

        private List<TransferItem> GetTransformItemsForAllDirections(Dictionary<string, FileNode> fileNodes)
        {
            List<TransferItem> allItems = new List<TransferItem>();
            foreach (DMLibTransferDirection direction in GetAllValidDirections())
            {
                string fileName = GetTransferFileName(direction);
                DataAdaptor<DMLibDataInfo> sourceAdaptor = GetSourceAdaptor(direction.SourceType);
                DataAdaptor<DMLibDataInfo> destAdaptor = GetDestAdaptor(direction.DestType);

                FileNode fileNode = fileNodes[fileName];
                TransferItem item = new TransferItem()
                {
                    SourceObject = sourceAdaptor.GetTransferObject(fileNode),
                    DestObject = destAdaptor.GetTransferObject(fileNode),
                    SourceType = direction.SourceType,
                    DestType = direction.DestType,
                    IsServiceCopy = direction.IsAsync,
                };
                allItems.Add(item);
            }

            return allItems;
        }

        [TestMethod]
        [TestCategory(Tag.Function)]
        public void ResumeInAllDirections()
        {
            long fileSizeInByte = 10 * 1024 * 1024;
            Dictionary<string, FileNode> sourceFileNodes = this.PrepareSourceData(fileSizeInByte);
            List<TransferItem> allItems = this.GetTransformItemsForAllDirections(sourceFileNodes);

            int fileCount = sourceFileNodes.Keys.Count;

            // Execution and store checkpoints
            CancellationTokenSource tokenSource = new CancellationTokenSource();

            var transferContext = new TransferContext();
            var progressChecker = new ProgressChecker(fileCount, fileSizeInByte * fileCount);
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
            var checkpointList = new List<KeyValuePair<string,TransferCheckpoint>>();
            checkpointList.AddRange(checkpoints);
            checkpointList.Shuffle();

            foreach(var pair in checkpointList)
            {
                Test.Info("===Resume with checkpoint '{0}'===", pair.Key);
                options = new TestExecutionOptions<DMLibDataInfo>();
                options.DisableDestinationFetch = true;

                progressChecker.Reset();
                transferContext = new TransferContext(pair.Value)
                {
                    ProgressHandler = progressChecker.GetProgressHandler(),

                    // The checkpoint can be stored when DMLib doesn't check overwrite callback yet.
                    // So it will case an skip file error if the desination file already exists and 
                    // We don't have overwrite callback here.
                    OverwriteCallback = DMLibInputHelper.GetDefaultOverwiteCallbackY()
                };

                List<TransferItem> itemsToResume = allItems.Select(item =>
                {
                    TransferItem itemToResume = item.Clone();
                    itemToResume.TransferContext = transferContext;
                    return itemToResume;
                }).ToList();

                result = this.RunTransferItems(itemsToResume, options);

                int resumeFailCount = 0;
                foreach (DMLibDataType destDataType in DataTypes)
                {
                    DataAdaptor<DMLibDataInfo> destAdaptor = GetSourceAdaptor(destDataType);
                    DMLibDataInfo destDataInfo = destAdaptor.GetTransferDataInfo(string.Empty);

                    foreach (FileNode destFileNode in destDataInfo.EnumerateFileNodes())
                    {
                        string fileName = destFileNode.Name;
                        if (!fileName.Contains(DMLibDataType.Stream.ToString()))
                        {
                            FileNode sourceFileNode = sourceFileNodes[fileName];
                            Test.Assert(DMLibDataHelper.Equals(sourceFileNode, destFileNode), "Verify transfer result.");
                        }
                        else
                        {
                            resumeFailCount++;
                        }
                    }
                }

                Test.Assert(result.Exceptions.Count == resumeFailCount, "Verify resume failure count: expected {0}, actual {1}.", resumeFailCount, result.Exceptions.Count);

                foreach (var resumeException in result.Exceptions)
                {
                    Test.Assert(resumeException is NotSupportedException, "Verify resume exception is NotSupportedException.");
                }
            }
        }

        [TestMethod]
        [TestCategory(Tag.BVT)]
        public void TransferInAllDirections()
        {
            // Prepare source data
            Dictionary<string, FileNode> sourceFileNodes = this.PrepareSourceData(10 * 1024 * 1024);
            List<TransferItem> allItems = this.GetTransformItemsForAllDirections(sourceFileNodes);

            // Execution
            var result = this.RunTransferItems(allItems, new TestExecutionOptions<DMLibDataInfo>());

            // Verify all files are transfered successfully
            Test.Assert(result.Exceptions.Count == 0, "Verify no exception occurs.");
            foreach (DMLibDataType destDataType in DataTypes)
            {
                DataAdaptor<DMLibDataInfo> destAdaptor = GetSourceAdaptor(destDataType);
                DMLibDataInfo destDataInfo = destAdaptor.GetTransferDataInfo(string.Empty);

                foreach (FileNode destFileNode in destDataInfo.EnumerateFileNodes())
                {
                    FileNode sourceFileNode = sourceFileNodes[destFileNode.Name];
                    Test.Assert(DMLibDataHelper.Equals(sourceFileNode, destFileNode), "Verify transfer result.");
                }
            }
        }

        private static string GetTransferFileName(DMLibTransferDirection direction)
        {
            return GetTransferFileName(direction.SourceType, direction.DestType, direction.IsAsync);
        }

        private static string GetTransferFileName(DMLibDataType sourceType, DMLibDataType destType, bool isAsync)
        {
            return sourceType.ToString() + destType.ToString() + (isAsync ? "async" : ""); 
        }

        private static IEnumerable<DMLibTransferDirection> GetAllValidDirections()
        {
            for (int sourceIndex = 0; sourceIndex < DataTypes.Length; ++sourceIndex)
            {
                for (int destIndex = 0; destIndex < DataTypes.Length; ++destIndex)
                {
                    DMLibDataType sourceDataType = DataTypes[sourceIndex];
                    DMLibDataType destDataType = DataTypes[destIndex];

                    if (validSyncDirections[sourceIndex][destIndex])
                    {
                        yield return new DMLibTransferDirection()
                        {
                            SourceType = sourceDataType,
                            DestType = destDataType,
                            IsAsync = false,
                        };
                    }

                    if (validAsyncDirections[sourceIndex][destIndex])
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
