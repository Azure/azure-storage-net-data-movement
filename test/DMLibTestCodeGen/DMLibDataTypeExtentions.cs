namespace DMLibTestCodeGen
{
    internal static class DMLibDataTypeExtentions
    {
        public static bool IsLocal(this DMLibDataType datatype)
        {
            return datatype == DMLibDataType.Local || datatype == DMLibDataType.Stream;
        }
    }
}