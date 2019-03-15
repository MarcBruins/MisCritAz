using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using MisCritAz.Messaging;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace MisCritAz.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MessageController : ControllerBase
    {
        private readonly IServiceBusMessageSender _messageSender;
        private readonly IMemoryCache _cache;

        public MessageController(IServiceBusMessageSender messageSender, IMemoryCache cache)
        {
            _messageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        }

        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            if (_cache.TryGetValue("CacheKey", out var val))
            {
                var value = (ImmutableList<SampleMessage>)val;
                return value.Select(v => $"Sender: {v.Sender} Receiver: {v.Receiver} Body: {v.Body}").ToArray();
            }
            return new[] { "no results" };
        }

        // POST api/values
        [HttpPost]
        public Task Post([FromBody] string value)
        {
            return _messageSender.ProcessMessageImpl(new SampleMessage { Body = value });
        }
    }
}
