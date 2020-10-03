using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Nuka.SDK.Redis
{
    [ExcludeFromCodeCoverage]
    public class NukaRedisCache : IDistributedCache
    {
        #region Variables
        private const string SetScript = (@"
                redis.call('HMSET', KEYS[1], 'absexp', ARGV[1], 'sldexp', ARGV[2], 'data', ARGV[4])
                if ARGV[3] ~= '-1' then
                  redis.call('EXPIRE', KEYS[1], ARGV[3])
                end
                return 1");
        
        private const string AbsoluteExpirationKey = "absexp";
        private const string SlidingExpirationKey = "sldexp";
        private const string DataKey = "data";
        private const long NotPresent = -1;

        private volatile ConnectionMultiplexer _connection;
        private IDatabase _database;

        private readonly RedisCacheOptions _options;
        private readonly ILogger<NukaRedisCache> _logger;

        private readonly string _instance;

        private readonly AsyncPolicy _asyncPolicy;
        private readonly RetryPolicy _syncPolicy;
        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);

        #endregion

        #region Constructors

        public NukaRedisCache(IOptions<RedisCacheOptions> optionsAccessor, ILogger<NukaRedisCache> logger)
        {
            if (optionsAccessor == null)
                throw new ArgumentNullException(nameof(NukaRedisCache));

            _options = optionsAccessor.Value;
            _logger = logger;

            _instance = _options.InstanceName;

            _syncPolicy = Policy
                .Handle<RedisConnectionException>()
                .Retry(1, (exception, i) =>
                {
                    logger.LogError(exception, "redis-connection-exception {@context}",
                        new Dictionary<string, string> {["error_message"] = exception.Message});
                    Reconnect();
                });

            _asyncPolicy = Policy
                .Handle<RedisConnectionException>()
                .RetryAsync(1, async (exception, i) =>
                {
                    logger.LogError(exception, "redis-connection-exception {@context}",
                        new Dictionary<string, string> {["error_message"] = exception.Message});
                    await ReconnectAsync();
                });
        }

        public NukaRedisCache(IOptions<RedisCacheOptions> optionsAccessor, ILogger<NukaRedisCache> logger,
            IDatabase database) : this(optionsAccessor, logger)
        {
            _database = database;
        }

        #endregion

        #region Methods

        private void Connect()
        {
            if (_database != null) return;
            _connectionLock.Wait();

            try
            {
                if (_database == null) return;
                _connection = _options.ConfigurationOptions != null
                    ? ConnectionMultiplexer.Connect(_options.Configuration)
                    : ConnectionMultiplexer.Connect(_options.ConfigurationOptions);
                _database = _connection.GetDatabase();
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private async Task ConnectAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (_database == null) return;

            await _connectionLock.WaitAsync(token);

            try
            {
                if (_database != null)
                {
                    _connection = _options.Configuration != null
                        ? await ConnectionMultiplexer.ConnectAsync(_options.Configuration)
                        : await ConnectionMultiplexer.ConnectAsync(_options.ConfigurationOptions);

                    _database = _connection.GetDatabase();
                }
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private void Reconnect()
        {
            _connectionLock.Wait();
            try
            {
                _logger.LogWarning("redis-attempting-to-reconnect");

                try
                {
                    _connection?.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogError("redis-reconnection-close-old-connection-error",
                        new Dictionary<string, string> {["error_message"] = ex.Message});
                }

                _connection = _options.ConfigurationOptions != null
                    ? ConnectionMultiplexer.Connect(_options.ConfigurationOptions)
                    : ConnectionMultiplexer.Connect(_options.Configuration);

                _database = _connection.GetDatabase();
                _logger.LogWarning("redis-reconnection-successful");
            }
            catch (Exception ex)
            {
                _logger.LogError("redis-reconnection-attempt-error",
                    new Dictionary<string, string> {["error_message"] = ex.Message});
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private async Task ReconnectAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();
            await _connectionLock.WaitAsync(token);

            try
            {
                _logger.LogWarning("redis-attempting-to-reconnect");
                if (_connection != null)
                {
                    try
                    {
                        await _connection.CloseAsync();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("redis-reconnection-close-old-connection-error",
                            new Dictionary<string, string> {["error_message"] = ex.Message});
                    }
                }

                if (_options.ConfigurationOptions != null)
                {
                    _connection = await ConnectionMultiplexer.ConnectAsync(_options.ConfigurationOptions);
                }
                else
                {
                    _connection = await ConnectionMultiplexer.ConnectAsync(_options.Configuration);
                }

                _database = _connection.GetDatabase();
                _logger.LogInformation("redis-reconnection-successful");
            }
            catch (Exception ex)
            {
                _logger.LogError("redis-reconnection-attempt-error",
                    new Dictionary<string, string> {["error_message"] = ex.Message});
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private T ExecuteAndCapture<T>(Func<T> action)
        {
            var policyResult = _syncPolicy.ExecuteAndCapture(action);
            return policyResult.Result;
        }

        private async Task<T> ExecuteAndCaptureAsync<T>(Func<Task<T>> action)
        {
            var policyResult = await _asyncPolicy.ExecuteAndCaptureAsync(action);
            return policyResult.Result;
        }

        private void Execute(Action action)
        {
            _syncPolicy.Execute(action);
        }

        private async Task ExecuteAsync(Func<Task> action)
        {
            await _asyncPolicy.ExecuteAsync(action);
        }

        private byte[] GetAndRefresh(string key, bool getData)
        {
            if (key == null)
            {
                throw new ArgumentException(nameof(key));
            }

            Connect();

            RedisValue[] results = getData
                ? _database.HashMemberGet(_instance + key, AbsoluteExpirationKey, SlidingExpirationKey, DataKey)
                : _database.HashMemberGet(_instance + key, AbsoluteExpirationKey, SlidingExpirationKey);

            if (results.Length >= 2)
            {
                MapMetadata(results, out DateTimeOffset? absExpr, out TimeSpan? sldExpr);
                Refresh(key, absExpr, sldExpr);
            }

            if (results.Length >= 3 && results[2].HasValue)
            {
                return results[2];
            }

            return null;
        }

        private async Task<byte[]> GetAndRefreshAsync(string key, bool getData,
            CancellationToken token = default)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            token.ThrowIfCancellationRequested();

            await ConnectAsync(token);

            RedisValue[] results;
            if (getData)
            {
                results = await _database.HashMemberGetAsync(_instance + key, AbsoluteExpirationKey, SlidingExpirationKey,
                    DataKey);
            }
            else
            {
                results = await _database.HashMemberGetAsync(_instance + key, AbsoluteExpirationKey, SlidingExpirationKey);
            }

            if (results.Length >= 2)
            {
                MapMetadata(results, out DateTimeOffset? absExpr, out TimeSpan? sldExpr);
                await RefreshAsync(key, absExpr, sldExpr);
            }

            if (results.Length >= 3 && results[2].HasValue)
            {
                return results[2];
            }

            return null;
        }

        private void MapMetadata(IReadOnlyList<RedisValue> results, out DateTimeOffset? absoluteExpiration,
            out TimeSpan? slidingExpiration)
        {
            absoluteExpiration = null;
            slidingExpiration = null;

            var absoluteExpirationTicks = (long?) results[0];
            if (absoluteExpirationTicks.HasValue && absoluteExpirationTicks.Value != NotPresent)
                absoluteExpiration = new DateTimeOffset(absoluteExpirationTicks.Value, TimeSpan.Zero);

            var slidingExpirationTicks = (long?) results[1];
            if (slidingExpirationTicks.HasValue && slidingExpirationTicks.Value != NotPresent)
                slidingExpiration = new TimeSpan(slidingExpirationTicks.Value);
        }

        private void Refresh(string key, DateTimeOffset? absExpr, TimeSpan? sldExpr)
        {
            if (key == null)
            {
                throw new ArgumentException(nameof(key));
            }

            TimeSpan? expr;
            if (!sldExpr.HasValue) return;

            if (absExpr.HasValue)
            {
                var relExpr = absExpr - DateTimeOffset.Now;
                expr = relExpr <= sldExpr.Value ? relExpr : sldExpr;
            }
            else
            {
                expr = sldExpr;
            }

            _database.KeyExpire(_instance + key, expr.Value);
        }

        private async Task RefreshAsync(string key, DateTimeOffset? absExpr, TimeSpan? sldExpr)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (sldExpr.HasValue)
            {
                TimeSpan? expr;
                if (absExpr.HasValue)
                {
                    var relExpr = absExpr.Value - DateTimeOffset.Now;
                    expr = relExpr <= sldExpr.Value ? relExpr : sldExpr;
                }
                else
                {
                    expr = sldExpr;
                }

                await _database.KeyExpireAsync(_instance + key, expr);
            }
        }

        private static long? GetExpirationInSeconds(DateTimeOffset creationTime, DateTimeOffset? absoluteExpiration, DistributedCacheEntryOptions options)
        {
            if (absoluteExpiration.HasValue && options.SlidingExpiration.HasValue)
            {
                return (long)Math.Min(
                    (absoluteExpiration.Value - creationTime).TotalSeconds,
                    options.SlidingExpiration.Value.TotalSeconds);
            }
            else if (absoluteExpiration.HasValue)
            {
                return (long)(absoluteExpiration.Value - creationTime).TotalSeconds;
            }
            else if (options.SlidingExpiration.HasValue)
            {
                return (long)options.SlidingExpiration.Value.TotalSeconds;
            }
            return null;
        }
        private static DateTimeOffset? GetAbsoluteExpiration(DateTimeOffset creationTime, DistributedCacheEntryOptions options)
        {
            if (options.AbsoluteExpiration.HasValue && options.AbsoluteExpiration <= creationTime)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(DistributedCacheEntryOptions.AbsoluteExpiration),
                    options.AbsoluteExpiration.Value,
                    "The absolute expiration value must be in the future.");
            }
            var absoluteExpiration = options.AbsoluteExpiration;
            if (options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                absoluteExpiration = creationTime + options.AbsoluteExpirationRelativeToNow;
            }

            return absoluteExpiration;
        }
        #endregion

        #region Overrides

        public byte[] Get(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            return ExecuteAndCapture(() => GetAndRefresh(key, true));
        }

        public async Task<byte[]> GetAsync(string key, CancellationToken token = default)
        {
            if (key == null)
                throw new ArgumentException(nameof(key));

            token.ThrowIfCancellationRequested();
            return await ExecuteAndCaptureAsync(async () => await GetAndRefreshAsync(key, true, token));
        }

        public void Refresh(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            Execute(() => GetAndRefresh(key, false));
        }

        public async Task RefreshAsync(string key, CancellationToken token = default)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            token.ThrowIfCancellationRequested();

            await ExecuteAsync(async () => await GetAndRefreshAsync(key, false, token));
        }

        public void Remove(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            Connect();

            Execute(() => _database.KeyDelete(_instance + key));
        }

        public async Task RemoveAsync(string key, CancellationToken token = default)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            await ConnectAsync(token);

            await ExecuteAsync(async () => await _database.KeyDeleteAsync(_instance + key));
        }

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }
            
            Connect();

            var creationTime = DateTimeOffset.Now;
            
            var absoluteExpiration = GetAbsoluteExpiration(creationTime, options);
            
            Execute(() =>
            {
                _database.ScriptEvaluate(SetScript, new RedisKey[] { _instance + key },
                    new RedisValue[]
                    {
                        absoluteExpiration?.Ticks ?? NotPresent,
                        options.SlidingExpiration?.Ticks ?? NotPresent,
                        GetExpirationInSeconds(creationTime, absoluteExpiration, options) ?? NotPresent,
                        value
                    });
            });
        }

        public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options,
            CancellationToken token = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            token.ThrowIfCancellationRequested();

            await ConnectAsync(token);

            var creationTime = DateTimeOffset.UtcNow;

            var absoluteExpiration = GetAbsoluteExpiration(creationTime, options);

            await ExecuteAsync(async () =>
            {
                await _database.ScriptEvaluateAsync(SetScript, new RedisKey[] { _instance + key },
                    new RedisValue[]
                    {
                        absoluteExpiration?.Ticks ?? NotPresent,
                        options.SlidingExpiration?.Ticks ?? NotPresent,
                        GetExpirationInSeconds(creationTime, absoluteExpiration, options) ?? NotPresent,
                        value
                    });
            });
        }

        #endregion
    }
}