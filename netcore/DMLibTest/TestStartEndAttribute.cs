using MS.Test.Common.MsTestLib;
using System.Reflection;
using Xunit.Sdk;

namespace DMLibTest
{
    public class TestStartEndAttribute : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
            Test.Start(methodUnderTest.DeclaringType.Name, methodUnderTest.Name);
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Test.End(methodUnderTest.DeclaringType.Name, methodUnderTest.Name);
        }
    }
}
