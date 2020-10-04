using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Nuka.SDK.Redis.App.Controllers
{
    [ApiController]
    [Route("api/users")]
    [ExcludeFromCodeCoverage]
    public class ExampleController : Controller
    {
        private readonly ILogger<ExampleController> _logger;
        private readonly IDistributedCache _cache;

        public ExampleController(ILogger<ExampleController> logger, IDistributedCache cache)
        {
            _logger = logger;
            _cache = cache;
        }

        // ** GET api/v1/users/{userId} **
        [HttpGet("{userid}", Name = "User")]
        public async Task<IActionResult> GetAsync(string userid)
        {
            try
            {
                var result = await _cache.GetStringAsync($"test_app_{userid}");
                return Ok(result);
            }
            catch (RedisConnectionException)
            {
                return StatusCode(500, "Error while retrieving cache value");
            }
            catch (Exception)
            {
                return StatusCode(500, "Error while retrieving cache value");
            }
        }

        // ** PUT api/v1/users/{userId} **
        [HttpPut("{userId}", Name = "User")]
        public async Task<IActionResult> PutUserAsync(string userId)
        {
            try
            {
                await _cache.SetStringAsync($"test_app_{userId}", userId,
                    new DistributedCacheEntryOptions {AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5)});
                return Ok();
            }
            catch (Exception e)
            {
                _logger.LogError("set-user-error", $"Error while setting cache value: {e.Message}");
                return StatusCode(500, "Error while retrieving cache value");
            }
        }
    }
}