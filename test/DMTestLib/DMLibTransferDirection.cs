//------------------------------------------------------------------------------
// <copyright file="DMLibTransferDirection.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System.Collections.Generic;

    public class DMLibTransferDirection : TestMethodDirection, ITestDirection<DMLibDataType>
    {
        public DMLibTransferDirection(
            DMLibDataType sourceType,
            DMLibDataType destType,
            DMLibCopyMethod copyMethod,
            List<string> tags)
            : base(tags)
        {
            this.SourceType = sourceType;
            this.DestType = destType;
            this.CopyMethod = copyMethod;
        }

        public DMLibCopyMethod CopyMethod
        {
            get;
            private set;
        }

        public DMLibDataType SourceType
        {
            get;
            private set;
        }

        public DMLibDataType DestType
        {
            get;
            private set;
        }

        public override bool Equals(object obj)
        {
            DMLibTransferDirection other = obj as DMLibTransferDirection;
            if (other == null)
            {
                return false;
            }

            return this.SourceType == other.SourceType &&
                this.DestType == other.DestType &&
                this.CopyMethod == other.CopyMethod;
        }

        public override int GetHashCode()
        {
            int factor = 31;
            int hash = (int)this.CopyMethod;
            hash = hash * factor + (int)this.SourceType;
            hash = hash * factor + (int)this.DestType;

            return hash;
        }

        public override string GetTestMethodNameSuffix()
        {
            // [SourceType]2[DestType][ServiceSideAsyncCopy]
            return string.Format("{0}2{1}{2}",
                this.SourceType.ToString(),
                this.DestType.ToString(),
                this.CopyMethod);
        }

        protected override IEnumerable<string> GetExtraTags()
        {
            yield break;
            //yield return string.Format("{0}2{1}{2}", this.SourceType, this.DestType, this.IsAsync ? "Async" : string.Empty);

            //if (this.IsAsync)
            //{
            //    yield return MultiDirectionTag.Async;
            //}
        }
    }
}
