// By default, xUnit runs tests in parallel. DMLibTest relies on a static class
// for logging (MS.Test.Common.MsTestLib.Test), and the architecture of the test
// code doesn't enable a straightforward way to make to make it thread-safe.
// http://xunit.github.io/docs/running-tests-in-parallel.html
// http://xunit.github.io/docs/capturing-output.html
[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]