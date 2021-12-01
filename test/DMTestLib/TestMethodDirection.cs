//------------------------------------------------------------------------------
// <copyright file="TestMethodDirection.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTestCodeGen
{
    using System.Collections.Generic;

    public abstract class TestMethodDirection
    {
        public bool Ignore { get; private set; }

        public List<string> Tags
        {
            get;
            private set;
        }

        public TestMethodDirection(List<string> tags, bool ignore = false)
        {
            Ignore = ignore;

            if (null != tags)
            {
                this.Tags = new List<string>(tags);
            }
            else
            {
                this.Tags = new List<string>();
            }
        }

        public abstract string GetTestMethodNameSuffix();

        protected abstract IEnumerable<string> GetExtraTags();

        public IEnumerable<string> GetTags()
        {
            foreach(var tag in Tags)
            {
                yield return tag;
            }

            foreach (var extraTag in this.GetExtraTags())
            {
                yield return extraTag;
            }
        }
    }
}
