namespace DMLibTestCodeGen
{
    internal static class DMLibTransferDirectionExtensions
    {
        public static bool ShouldBeIgnored(this DMLibTransferDirection transferDirection)
        {
            return IsCopyTest(transferDirection) || IsSourceCloudFileTest(transferDirection);
        }

        public static bool IsCopyTest(this DMLibTransferDirection transferDirection)
        {
            return transferDirection.SourceType.IsLocal() || transferDirection.DestType.IsLocal();
        }

        public static bool IsSourceCloudFileTest(this DMLibTransferDirection transferDirection)
        {
            return transferDirection.SourceType == DMLibDataType.CloudFile;
        }
    }
}