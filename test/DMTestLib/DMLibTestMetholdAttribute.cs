//------------------------------------------------------------------------------
// <copyright file="DMLibTestMetholdAttribute.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System;
    using System.Collections.Generic;

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
    public class DMLibTestMethodAttribute : MultiDirectionTestMethodAttribute, ITestDirection<DMLibDataType>
    {
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

        public DMLibTestMethodAttribute(
           DMLibDataType dataType,
           DMLibCopyMethod copymethod = DMLibCopyMethod.SyncCopy,
           string[] tags = null)
           : this(
           dataType,
           DMLibDataType.Unspecified,
           copymethod,
           tags)
        {
        }

        public DMLibTestMethodAttribute(
            DMLibDataType sourceType,
            DMLibDataType destType,
            DMLibCopyMethod copymethod = DMLibCopyMethod.SyncCopy,
            string[] tags = null)
            : base(tags)
        {
            this.SourceType = sourceType;
            this.DestType = destType;
            this.CopyMethod = copymethod;
        }

        internal override IEnumerable<TestMethodDirection> ExtractDirections()
        {
            if (this.DestType == DMLibDataType.Unspecified)
            {
                foreach (DMLibDataType sourceType in this.SourceType.Extract())
                {
                    DMLibTransferDirection transferDirection =
                        new DMLibTransferDirection(
                            sourceType,
                            sourceType,
                            this.CopyMethod,
                            new List<string>(this.Tags));
                    yield return transferDirection;
                }
            }
            else
            {
                foreach (DMLibDataType sourceType in this.SourceType.Extract())
                {
                    foreach (DMLibDataType destType in this.DestType.Extract())
                    {
                        DMLibTransferDirection transferDirection =
                            new DMLibTransferDirection(
                                sourceType,
                                destType,
                                this.CopyMethod,
                                new List<string>(this.Tags));
                        yield return transferDirection;
                    }
                }
            }
        }
    }
}
