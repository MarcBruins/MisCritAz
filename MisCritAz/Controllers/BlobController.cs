using Microsoft.AspNetCore.Mvc;
using MisCritAz.Storage;
using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Localization;
using Microsoft.Extensions.Logging;

namespace MisCritAz.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BlobController : ControllerBase
    {
        private readonly IBlobWriter _blobWriter;
        private readonly IBlobReader _blobReader;
        private readonly ILogger<BlobController> _logger;

        public BlobController(IBlobWriter blobWriter, IBlobReader blobReader, ILogger<BlobController> logger = null)
        {
            _blobWriter = blobWriter ?? throw new ArgumentNullException(nameof(blobWriter));
            _blobReader = blobReader ?? throw new ArgumentNullException(nameof(blobReader));
            _logger = logger;
        }

        // GET api/values
        [HttpGet("/{container}/{blob}")]
        public async Task<ActionResult<SampleBlobData>> Get([FromQuery]string container, [FromQuery]string blob)
        {
            var data = await _blobReader.GetBlob(container ?? "default-container", blob ?? "defaultblob").ConfigureAwait(false);
            if (data == null)
                return NotFound();

            return new OkObjectResult(data);
        }

        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] SampleBlobData value)
        {
            if (value == null) return BadRequest(new {error = "Request body missing"});
            try
            {
                await _blobWriter.Upload(new SampleBlobData
                {
                    Body = value.Body ?? "empty",
                    Name = value.Name ?? "defaultblob",
                    Container = value.Container ?? "default-container"
                });

                return Ok();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save blob");
            }

            return StatusCode((int)HttpStatusCode.InternalServerError, new {error = "Failed to save blob data."});
        }
    }
}
