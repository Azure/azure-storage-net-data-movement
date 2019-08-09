//------------------------------------------------------------------------------
// <copyright file="SearchPatternTest.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace DMLibTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DMLibTestCodeGen;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MS.Test.Common.MsTestLib;

    [MultiDirectionTestClass]
    public class SearchPatternTest : DMLibDataPreparedTestBase
#if DNXCORE50
        , IDisposable
#endif
    {
        // test class for filepattern option
        // test files for all test cases are organized in a directory as following:
        //
        //    folder1  -+->  subfolder1  -+-> subfolder3
        //              |                 |
        //              |                 +-> testfile2
        //              |                 |
        //              |                 +-> 4testfile
        //              |
        //              +->  subfolder2  -+-> subfolder4 -> test5
        //              |                 |
        //              |                 +-> TESTFILE345
        //              |                 |        
        //              |                 +-> testfile234
        //              |                 |
        //              |                 +-> testYfile
        //              |                 |
        //              |                 +-> f_arbitrary.exe
        //              |                 |
        //              |                 +-> 测试x文件
        //              |
        //              +->  testfile1
        //              |
        //              +->  TestFile2
        //              |
        //              +->  测试文件2
        //
        //
        //    folder2  -+->  folder_file (this is a file)
        //              |
        //              +->  testfile1 (this is a empty folder though named like a file)
        //              |
        //              +->  测试文件三
        //              |
        //              +->  测试四文件
        //
        //    folder3
        //
        //    testfile
        //
        //    TeSTfIle (Only exists in cloud bloub source cause blob name is case-sensitive)
        //
        //    testfile1
        //
        //    testfile2
        //
        //    testXfile
        //
        //    testXXfile
        //
        //    "测试文件"
        //
        //    "..a123"

        private static DMLibDataType[] sourceDataTypes;

        // local search pattern
#region source is local
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_WildChar_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "testfile2"));
            if (CrossPlatformHelpers.IsWindows)
            {
                nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "TESTFILE345"));
            }

            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testfile234"));

            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "testfile1"));

            if (CrossPlatformHelpers.IsWindows)
            {
                nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            }
            
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult, "testfile*");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_WildChar_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "testfile1"));
            if (CrossPlatformHelpers.IsWindows)
            {
                nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            }

            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult, "testfile?");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_WildChar_3()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXXfile"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "test*file");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_WildChar_4()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "test?file");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_FilePath_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "4testfile"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult, FormalizeSearchPattern ("folder1\\subfolder1\\4testfile"));
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_FilePath_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "TESTFILE345"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testfile234"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testYfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "f_arbitrary.exe"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "测试x文件"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, FormalizeSearchPattern("folder1\\subfolder2\\*"));
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_FilePath_3()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "f_arbitrary.exe"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, FormalizeSearchPattern("folder1\\subfolder2\\f_arbitrary.exe"));
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_NoPattern_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "4testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "subfolder4", "test5"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testfile234"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "TESTFILE345"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testYfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "f_arbitrary.exe"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "测试x文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "测试文件2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "folder_file"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试文件三"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试四文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "测试文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_NoPattern_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "测试文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_UnMatchedPattern()
        {
            DMLibDataInfo expectedResult = new DMLibDataInfo(string.Empty);
            this.TestSearchPattern(true, expectedResult, "unmatched*");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_DoubleDotValid_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "..a*");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_DoubleDotValid_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "..a123");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirLocalSource)]
        public void FilePattern_Local_DoubleDotValid_3()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "..*");
        }
#endregion // source is local

        // cloud file search pattern
#region source is cloud file
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_FileName_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "testX");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_FileName_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "testXfile");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_FilePath_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testYfile"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "folder1/subfolder2/testYfile");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_FilePath_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, FormalizeSearchPattern("folder1\\TestFile2"));
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_NoPattern_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "4testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "subfolder4", "test5"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testfile234"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "TESTFILE345"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testYfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "f_arbitrary.exe"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "测试x文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "测试文件2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "folder_file"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试文件三"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试四文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "测试文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_NoPattern_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_Unicode_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "测试文件"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "测试文件");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_Unicode_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试四文件"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "folder2/测试四文件");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudFileSource)]
        public void FilePattern_CloudFile_RecursivePattern()
        {
            this.TestSearchPatternError(true, "testfile", "Search pattern is not supported in recursive mode when the source is an Azure file directory.");
        }
#endregion // source is cloud file

        // cloud blob search pattern
#region source is cloud blob
        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void FilePattern_CloudBlob_Prefix_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult, "testfile");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void FilePattern_CloudBlob_Prefix_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "TeSTfIle"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult, "TeSTfIle");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void FilePattern_CloudBlob_Prefix_3()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "subfolder4", "test5"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testfile234"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "TESTFILE345"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testYfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "f_arbitrary.exe"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "测试x文件"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult, "folder1/subfolder2");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void FilePattern_CloudBlob_Prefix_4()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult, "folder1/TestFile");
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethod(DMLibDataType.CloudBlob)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideAsyncCopy)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void FilePattern_CloudBlob_NoPattern_1()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder1", "4testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "subfolder4", "test5"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testfile234"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "TESTFILE345"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "testYfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "f_arbitrary.exe"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "subfolder2", "测试x文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "TestFile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder1", "测试文件2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "folder_file"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试文件三"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "folder2", "测试四文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "TeSTfIle"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile1"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testfile2"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "testXXfile"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "测试文件"));
            nodesToKeep.Add(DMLibDataHelper.GetFileNode(expectedResult.RootNode, "..a123"));
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(true, expectedResult);
        }

        [TestCategory(Tag.Function)]
        [DMLibTestMethodSet(DMLibTestMethodSet.DirCloudBlobSource)]
        [DMLibTestMethod(DMLibDataType.CloudBlob, DMLibCopyMethod.ServiceSideSyncCopy)]
        public void FilePattern_CloudBlob_NoPattern_2()
        {
            HashSet<FileNode> nodesToKeep = new HashSet<FileNode>();
            DMLibDataInfo expectedResult = SourceDataInfo.Clone();
            DMLibDataHelper.RemoveAllFileNodesExcept(expectedResult.RootNode, nodesToKeep);

            this.TestSearchPattern(false, expectedResult);
        }
#endregion

        private static void PrepareSourceData()
        {
            DMLibDataInfo sourceFileTree = new DMLibDataInfo(string.Empty);
            DirNode dirNode1 = new DirNode("folder1");
            DirNode subDir1 = new DirNode("subfolder1");
            subDir1.AddDirNode(new DirNode("subfolder3"));
            subDir1.AddFileNode(GenerateFileNode("testfile2"));
            subDir1.AddFileNode(GenerateFileNode("4testfile"));
            dirNode1.AddDirNode(subDir1);

            DirNode subDir2 = new DirNode("subfolder2");
            DirNode subDir4 = new DirNode("subfolder4");
            subDir4.AddFileNode(GenerateFileNode("test5"));
            subDir2.AddDirNode(subDir4);
            
            subDir2.AddFileNode(GenerateFileNode("TESTFILE345"));
            subDir2.AddFileNode(GenerateFileNode("testfile234"));
            subDir2.AddFileNode(GenerateFileNode("testYfile"));
            subDir2.AddFileNode(GenerateFileNode("f_arbitrary.exe"));
            subDir2.AddFileNode(GenerateFileNode("测试x文件"));
            dirNode1.AddDirNode(subDir2);

            dirNode1.AddFileNode(GenerateFileNode("testfile1"));
            dirNode1.AddFileNode(GenerateFileNode("TestFile2"));
            dirNode1.AddFileNode(GenerateFileNode("测试文件2"));
            sourceFileTree.RootNode.AddDirNode(dirNode1);

            DirNode dirNode2 = new DirNode("folder2");
            dirNode2.AddFileNode(GenerateFileNode("folder_file"));
            dirNode2.AddDirNode(new DirNode("testfile1"));
            dirNode2.AddFileNode(GenerateFileNode("测试文件三"));
            dirNode2.AddFileNode(GenerateFileNode("测试四文件"));
            sourceFileTree.RootNode.AddDirNode(dirNode2);

            DirNode dirNode3 = new DirNode("folder3");
            sourceFileTree.RootNode.AddDirNode(dirNode3);

            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("testfile"));
            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("testfile1"));
            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("testfile2"));
            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("testXfile"));
            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("testXXfile"));
            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("测试文件"));
            sourceFileTree.RootNode.AddFileNode(GenerateFileNode("..a123"));

            DMLibDataInfo blobSourceFileTree = sourceFileTree.Clone();
            blobSourceFileTree.RootNode.AddFileNode(GenerateFileNode("TeSTfIle"));

            Test.Info("Start to generate test data, will take a while...");
            foreach (DMLibDataType dataType in sourceDataTypes)
            {
                if (IsCloudBlob(dataType))
                {
                    PrepareSourceData(dataType, blobSourceFileTree.Clone());
                }
                else
                {
                    PrepareSourceData(dataType, sourceFileTree.Clone());
                }
            }
            Test.Info("Done");
        }

        private static FileNode GenerateFileNode(string name)
        {
            return new FileNode(name)
            {
                SizeInByte = 1024,
            };
        }

        private void TestSearchPattern(bool recursive, DMLibDataInfo expectedResult, string searchPattern = null)
        {
            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = recursive;
                    transferOptions.SearchPattern = searchPattern;
                    item.Options = transferOptions;
                },
            };

            var testResult = this.ExecuteTestCase(null, options);

            VerificationHelper.VerifyTransferSucceed(testResult, expectedResult);
        }

        private void TestSearchPatternError(bool recursive, string searchPattern, string expectedErrorMessage)
        {
            var options = new TestExecutionOptions<DMLibDataInfo>()
            {
                IsDirectoryTransfer = true,
                TransferItemModifier = (notUsed, item) =>
                {
                    dynamic transferOptions = DefaultTransferDirectoryOptions;
                    transferOptions.Recursive = recursive;
                    transferOptions.SearchPattern = searchPattern;
                    item.Options = transferOptions;
                },
            };

            var testResult = this.ExecuteTestCase(null, options);

            if (testResult.Exceptions.Count != 1)
            {
                Test.Error("Should be exactly one exception, actual: {0}", testResult.Exceptions.Count);
            }

            VerificationHelper.VerifyExceptionErrorMessage(testResult.Exceptions[0], expectedErrorMessage);
        }

        private string FormalizeSearchPattern(string searchPattern)
        {
            return CrossPlatformHelpers.IsWindows ? searchPattern : searchPattern.Replace('\\', '/');
        }

#region Initialization and cleanup methods

#if DNXCORE50
        public SearchPatternTest()
        {
            Test.Info("Class Initialize: SearchPatternTest");
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
#endif
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            DMLibTestBase.BaseClassInitialize(testContext);
            DMLibTestBase.CleanupSource = false;

            sourceDataTypes = new DMLibDataType[] {
                DMLibDataType.Local,
                DMLibDataType.BlockBlob,
                DMLibDataType.PageBlob,
                DMLibDataType.AppendBlob,
                DMLibDataType.CloudFile,
            };

            SearchPatternTest.PrepareSourceData();
        }

        

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            foreach (DMLibDataType dataType in sourceDataTypes)
            {
                CleanupSourceData(dataType);
            }

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
    }
}
