using System.Text.Json.Nodes;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;

namespace Kiss.Elastic.Sync
{
    public sealed class GetAllFromElastic
    {
        private readonly ElasticsearchClient _elasticsearchClient;

        public GetAllFromElastic()
        {
            var elasticBaseUrl = Helpers.GetRequiredEnvironmentVariable("ELASTIC_BASE_URL");
            var username = Helpers.GetRequiredEnvironmentVariable("ELASTIC_USERNAME");
            var password = Helpers.GetRequiredEnvironmentVariable("ELASTIC_PASSWORD");

            var clientSettings = new ElasticsearchClientSettings(new Uri(elasticBaseUrl))
                .Authentication(new BasicAuthentication(username, password))
            // skip checking the certificate because we run Elastic internally, with a local certificate
                .ServerCertificateValidationCallback((a, b, c, d) => true);

            _elasticsearchClient = new ElasticsearchClient(clientSettings);
        }


        public async IAsyncEnumerable<JsonObject> GetAllDocuments(string bron)
        {
            const string Prefix = "search-";
            var indexName = string.Create(bron.Length + Prefix.Length, bron, (a, b) =>
            {
                Prefix.CopyTo(a);
                b.AsSpan().ToLowerInvariant(a[Prefix.Length..]);
            });

            // 1 minute is used in the elasticsearch examples
            var scrollDuration = TimeSpan.FromMinutes(1);

            var searchResponse = await _elasticsearchClient.SearchAsync<JsonObject>(x => x
                    .Index(indexName)
                    .Size(1000)
                    // scrolling is the most efficient way to loop through big result sets
                    .Scroll(scrollDuration));

            if (!searchResponse.IsSuccess())
            {
                throw new Exception("search failed: " + searchResponse.ToString());
            }

            var scrollId = searchResponse.ScrollId;
            var hits = searchResponse.Hits;

            while (scrollId is not null && hits.Count > 0)
            {
                foreach (var obj in hits.Select(x => x.Source).Select(x => x?[bron]).OfType<JsonObject>())
                {
                    yield return obj;
                }

                // get the next result set by specifying the scrollId we got previously
                var scrollResponse = await _elasticsearchClient.ScrollAsync<JsonObject>(new ScrollRequest
                {
                    ScrollId = scrollId,
                    Scroll = scrollDuration,
                });

                if (!scrollResponse.IsSuccess())
                {
                    throw new Exception("scroll failed: " + scrollResponse.ToString());
                }

                scrollId = scrollResponse.ScrollId;
                hits = scrollResponse.Hits;
            }

            if (scrollId is not null)
            {
                // it's best practice to clear the active scroll when you are done
                await _elasticsearchClient.ClearScrollAsync(x => x.ScrollId(searchResponse.ScrollId!));
            }
        }


    }
}
