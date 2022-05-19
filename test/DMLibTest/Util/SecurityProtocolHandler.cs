using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace DMLibTest
{
    internal class SecurityProtocolHandler : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            return base.SendAsync(request, cancellationToken);
        }
    }
}