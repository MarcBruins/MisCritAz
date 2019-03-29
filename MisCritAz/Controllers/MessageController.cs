using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using MisCritAz.Messaging;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace MisCritAz.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MessageController : ControllerBase
    {
        private readonly IServiceBusMessageSender _messageSender;
        private readonly IMemoryCache _cache;
        private readonly ILogger<MessageController> _logger;

        public MessageController(IServiceBusMessageSender messageSender, IMemoryCache cache, ILogger<MessageController> logger = null)
        {
            _messageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _logger = logger;
        }

        // GET api/values
        [HttpGet]
        public IActionResult Get()
        {
            if (_cache.TryGetValue("CacheKey", out var val))
            {
                var value = (ImmutableList<SampleMessage>)val;
                var values = value.Select(v => $"Sender: {v.Sender} Receiver: {v.Receiver} Body: {v.Body}").ToArray();
                return Ok(values);
            }
            return NoContent();
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] SampleMessage value)
        {
            if (value == null) return BadRequest(new { error = "Request body missing" });
            try
            {
                await _messageSender.ProcessMessageImpl(new SampleMessage
                {
                    Body = value.Body ?? "empty"
                });

                return Ok();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save blob");
            }

            return StatusCode((int)HttpStatusCode.InternalServerError, new { error = "Failed to send message." });
        }
    }
}
