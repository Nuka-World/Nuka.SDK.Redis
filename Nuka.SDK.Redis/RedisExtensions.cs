using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Nuka.SDK.Redis
{
    [ExcludeFromCodeCoverage]
    public static class RedisExtensions
    {
        private const string HmGetScript = "return redis.call('HMGET', KEYS[1], unpack(ARGV))";

        internal static RedisValue[] HashMemberGet(
            this IDatabase database,
            string key,
            params string[] members)
        {
            return (RedisValue[]) database.ScriptEvaluate(
                HmGetScript,
                new RedisKey[] {key},
                GetRedisMembers(members));
        }

        internal static async Task<RedisValue[]> HashMemberGetAsync(
            this IDatabase database,
            string key,
            params string[] members)
        {
            var result = (RedisValue[]) await database.ScriptEvaluateAsync(
                HmGetScript,
                new RedisKey[] {key},
                GetRedisMembers(members));
            return result;
        }

        private static RedisValue[] GetRedisMembers(params string[] members)
        {
            return members.Select(member => (RedisValue) member).ToArray();
        }
    }
}