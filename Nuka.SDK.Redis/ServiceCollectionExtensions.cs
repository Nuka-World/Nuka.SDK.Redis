using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Nuka.SDK.Redis
{
    [ExcludeFromCodeCoverage]
    public static class ServiceCollectionExtensions
    {
        private const int DefaultConnectTimeoutMs = 30000;
        private const int RetryBackoffMs = 500;

        public static IServiceCollection AddNukeRedisCache(
            this IServiceCollection serviceCollection,
            IConfiguration configuration)
        {
            var redisConfig = configuration.GetSection("Redis");
            var connectionString = redisConfig["ConnectionString"];
            var connectTimeout = GetConnectionTimeout(redisConfig["ConnectTimeout"]);
            var requestTimeout = GetConnectionTimeout(redisConfig["RequestTimeout"]);

            if (string.IsNullOrEmpty(connectionString))
            {
                serviceCollection.AddDistributedMemoryCache();
            }
            else
            {
                serviceCollection.Configure<RedisCacheOptions>(options =>
                {
                    options.ConfigurationOptions = ConfigurationOptions.Parse(connectionString);
                    options.ConfigurationOptions.ReconnectRetryPolicy =
                        new ExponentialRetry(RetryBackoffMs, connectTimeout);
                    options.ConfigurationOptions.ConnectTimeout = connectTimeout;
                    options.ConfigurationOptions.AsyncTimeout = requestTimeout;
                });

                serviceCollection.AddSingleton<IDistributedCache, NukaRedisCache>();
            }

            return serviceCollection;
        }

        private static int GetConnectionTimeout(string connectTimeout)
        {
            if (string.IsNullOrWhiteSpace(connectTimeout))
            {
                return DefaultConnectTimeoutMs;
            }

            if (!int.TryParse(connectTimeout, out var connectTimeoutMs))
            {
                throw new ArgumentOutOfRangeException(nameof(connectTimeout), connectTimeout,
                    "The value provided for Redis:ConnectTimeout is not a valid integer.");
            }

            return connectTimeoutMs;
        }
    }
}