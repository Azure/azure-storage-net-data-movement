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
            bool isAsync,
            List<string> tags)
            : base(tags)
        {
            this.SourceType = sourceType;
            this.DestType = destType;
            this.IsAsync = isAsync;
        }

        public bool IsAsync
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
                this.IsAsync == other.IsAsync;
        }

        public override int GetHashCode()
        {
            int factor = 31;
            int hash = this.IsAsync ? 1 : 0;
            hash = hash * factor + (int)this.SourceType;
            hash = hash * factor + (int)this.DestType;

            return hash;
        }

        public override string GetTestMethodNameSuffix()
        {
            // [SourceType]2[DestType][Async]
            return string.Format("{0}2{1}{2}",
                this.SourceType.ToString(),
                this.DestType.ToString(),
                this.IsAsync ? "Async" : string.Empty);
        }

        protected override IEnumerable<string> GetExtraTags()
        {
            yield return string.Format("{0}2{1}{2}", this.SourceType, this.DestType, this.IsAsync ? "Async" : string.Empty);

            if (this.IsAsync)
            {
                yield return MultiDirectionTag.Async;
            }
        }
    }
}
