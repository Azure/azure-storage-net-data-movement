using System.Collections.Generic;
using Xunit.Abstractions;
using Xunit.Sdk;

public class TestCategoryDiscoverer : ITraitDiscoverer
{
    public IEnumerable<KeyValuePair<string, string>> GetTraits(IAttributeInfo traitAttribute)
    {
        IEnumerator<object> enumerator = traitAttribute.GetConstructorArguments().GetEnumerator();
        while (enumerator.MoveNext())
        {
            yield return new KeyValuePair<string, string>("Category", enumerator.Current.ToString());
        }
        yield break;
    }
}
