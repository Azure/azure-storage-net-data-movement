using System;
using System.Reflection;
using Xunit;
using Xunit.Sdk;

namespace Microsoft.VisualStudio.TestTools.UnitTesting
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    [TraitDiscoverer("TestCategoryDiscoverer", "MsTestWrapper")]
    public class TestCategoryAttribute : Attribute, ITraitAttribute
    {
        public TestCategoryAttribute(string category)
        { }
    }

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class TestMethodAttribute : FactAttribute
    { }

    // Initialization and cleanup attributes do nothing because xUnit
    // handles these tasks through constructors and IDisposable:
    // http://xunit.github.io/docs/shared-context.html
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class ClassCleanupAttribute : Attribute
    { }

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class ClassInitializeAttribute : Attribute
    { }

    [AttributeUsage(AttributeTargets.Class)]
    public sealed class TestClassAttribute : Attribute
    { }

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class TestCleanupAttribute : Attribute
    { }

    [AttributeUsage(AttributeTargets.Method)]
    public sealed class TestInitializeAttribute : Attribute
    { }
}
