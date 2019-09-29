using DMLibTest.Cases;
using System;

// when test class T implements Xunit.IClassFixture<C>, C's ctor is called once, before
// the first test in T runs, and C.Dispose is called once, after the last test in T runs
namespace DMLibTest
{
    public class AccessConditionTestFixture : IDisposable
    {
        public AccessConditionTestFixture()
        {
            AccessConditionTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            AccessConditionTest.MyClassCleanup();
        }
    }

    public class AllTransferDirectionFixture : IDisposable
    {
        public AllTransferDirectionFixture()
        {
            AllTransferDirectionTest.MyClassInitialize(null);
        }
        public void Dispose()
        {
            AllTransferDirectionTest.MyClassCleanup();
        }
    }

    public class BigFileTestFixture : IDisposable
    {
        public BigFileTestFixture()
        {
            BigFileTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            BigFileTest.MyClassCleanup();
        }
    }

    public class BlockSizeTestFixture : IDisposable
    {
        public BlockSizeTestFixture()
        {
            BlockSizeTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            BlockSizeTest.MyClassCleanup();
        }
    }

    public class BVTFixture : IDisposable
    {
        public BVTFixture()
        {
            BVT.MyClassInitialize(null);
        }

        public void Dispose()
        {
            BVT.MyClassCleanup();
        }
    }

    public class CheckContentMD5TestFixture : IDisposable
    {
        public CheckContentMD5TestFixture()
        {
            CheckContentMD5Test.MyClassInitialize(null);
        }

        public void Dispose()
        {
            CheckContentMD5Test.MyClassCleanup();
        }
    }

    public class DelimiterTestFixture : IDisposable
    {
        public DelimiterTestFixture()
        {
            DelimiterTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            DelimiterTest.MyClassCleanup();
        }
    }

    public class DummyTransferTestFixture : IDisposable
    {
        public DummyTransferTestFixture()
        {
            DummyTransferTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            DummyTransferTest.MyClassCleanup();
        }
    }

    public class FollowSymlinkTestFixture : IDisposable
    {
        public FollowSymlinkTestFixture()
        {
            FollowSymlinkTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            FollowSymlinkTest.MyClassCleanup();
        }
    }

    public class LongFilePathTestFixture : IDisposable
    {
        public LongFilePathTestFixture()
        {
            LongFilePathTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            LongFilePathTest.MyClassCleanup();
        }
    }

    public class MetadataTestFixture : IDisposable
    {
        public MetadataTestFixture()
        {
            MetadataTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            MetadataTest.MyClassCleanup();
        }
    }

    public class StreamTestFixture : IDisposable
    {
        public StreamTestFixture()
        {
            StreamTest.MyClassInitialize(null);
        }
        public void Dispose()
        {
            StreamTest.MyClassCleanup();
        }
    }

    public class OverwriteTestFixture : IDisposable
    {
        public OverwriteTestFixture()
        {
            OverwriteTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            OverwriteTest.MyClassCleanup();
        }
    }

    public class ProgressHandlerTestFixture : IDisposable
    {
        public ProgressHandlerTestFixture()
        {
            ProgressHandlerTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            ProgressHandlerTest.MyClassCleanup();
        }
    }

    public class ResumeTestFixture : IDisposable
    {
        public ResumeTestFixture()
        {
            ResumeTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            ResumeTest.MyClassCleanup();
        }
    }
	
    public class SASTokenVersionTestFixture : IDisposable
    {
        public SASTokenVersionTestFixture()
        {
            SASTokenVersionTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            SASTokenVersionTest.MyClassCleanup();
        }
    }

    public class SearchPatternTestFixture : IDisposable
    {
        public SearchPatternTestFixture()
        {
            SearchPatternTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            SearchPatternTest.MyClassCleanup();
        }
    }

    public class SetAttributesTestFixture : IDisposable
    {
        public SetAttributesTestFixture()
        {
            SetAttributesTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            SetAttributesTest.MyClassCleanup();
        }
    }

    public class ShouldTransferTestFixture : IDisposable
    {
        public ShouldTransferTestFixture()
        {
            ShouldTransferTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            ShouldTransferTest.MyClassCleanup();
        }
    }

    public class SnapshotTestFixture : IDisposable
    {
        public SnapshotTestFixture()
        {
            SnapshotTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            SnapshotTest.MyClassCleanup();
        }
    }

    public class SMBPropertiesTestFixture : IDisposable
    {
        public SMBPropertiesTestFixture()
        {
            SMBPropertiesTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            SMBPropertiesTest.MyClassCleanup();
        }
    }

    public class UnsupportedDirectionTestFixture : IDisposable
    {
        public UnsupportedDirectionTestFixture()
        {
            UnsupportedDirectionTest.MyClassInitialize(null);
        }

        public void Dispose()
        {
            UnsupportedDirectionTest.MyClassCleanup();
        }
    }
}
