/*
 * Copyright (c) 2026, Sjofn LLC
 * All rights reserved.
 *
 * - Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * - Neither the name of the openmetaverse.co nor the names
 *   of its contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LibreMetaverse.StructuredData;
using LibreMetaverse.Tests.TestHelpers;
using NUnit.Framework;

namespace LibreMetaverse.Tests
{
    /// <summary>
    /// Regression coverage for the Caps seed-request retry loop. A transient seed failure used to
    /// dispose the shared CancellationTokenSource and then re-enter the request, which read the
    /// disposed token synchronously, threw ObjectDisposedException before any await, and recursed
    /// into an unbounded synchronous loop -> StackOverflowException (SIGSEGV). The loop below must
    /// instead retry with backoff, abort on 404, and cancel cleanly.
    /// </summary>
    [TestFixture]
    [Category("Capabilities")]
    public class CapsSeedRequestTests
    {
        private static readonly Uri SeedUri = new Uri("http://fake/seed");
        private static string SeedPath => SeedUri.GetLeftPart(UriPartial.Path);

        private static (FakeGridClient client, Simulator sim, Caps caps) CreateCaps()
        {
            var client = new FakeGridClient();
            var sim = new Simulator(client, new IPEndPoint(IPAddress.Loopback, 13), 0);
            // Constructed while the fake client's network is shut down, so the constructor's own
            // seed request short-circuits and we drive RunSeedRequestLoopAsync explicitly below.
            var caps = new Caps(sim, SeedUri) { _seedInitialDelayMs = 5 };
            return (client, sim, caps);
        }

        [Test]
        public async Task SeedRequest_PersistentTransientFailures_RetriesWithoutOverflowAndCancelsCleanly()
        {
            var (client, _, caps) = CreateCaps();
            try
            {
                // Every attempt throws a network-style error: the exact path that previously recursed.
                client.AddHttpThrowForPath(SeedPath);

                using var cts = new CancellationTokenSource();
                var loop = caps.RunSeedRequestLoopAsync(cts.Token);

                // Give the loop time to iterate several times. The old recursive code would have
                // blown the stack here instead of looping.
                await Task.Delay(250);

                Assert.That(client.CapturedRequests.Count, Is.GreaterThanOrEqualTo(3),
                    "the loop should keep retrying transient failures");
                Assert.That(loop.IsCompleted, Is.False,
                    "the loop should keep retrying until cancelled");

                cts.Cancel();

                var finished = await Task.WhenAny(loop, Task.Delay(TimeSpan.FromSeconds(5)));
                Assert.That(finished, Is.SameAs(loop), "the loop should stop promptly after cancellation");
                await loop; // must not throw
                Assert.That(caps._Caps, Is.Empty);
            }
            finally
            {
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task SeedRequest_RetriesThenSucceeds_PopulatesCapsAndRaisesEventOnce()
        {
            var (client, _, caps) = CreateCaps();
            try
            {
                var expectedCap = new Uri("http://fake/gettexture");
                var capsMap = new OSDMap { ["GetTexture"] = OSD.FromUri(expectedCap) };
                var llsdXml = Encoding.UTF8.GetString(OSDParser.SerializeLLSDXmlBytes(capsMap));

                // Two transient HTTP failures, then a valid caps document.
                client.AddHttpResponseSequenceForPath(SeedPath,
                    (HttpStatusCode.InternalServerError, string.Empty, "text/plain"),
                    (HttpStatusCode.InternalServerError, string.Empty, "text/plain"),
                    (HttpStatusCode.OK, llsdXml, "application/llsd+xml"));

                var receivedCount = 0;
                caps.CapabilitiesReceived += (_, _) => Interlocked.Increment(ref receivedCount);

                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await caps.RunSeedRequestLoopAsync(cts.Token);

                Assert.Multiple(() =>
                {
                    Assert.That(caps.CapabilityURI("GetTexture"), Is.EqualTo(expectedCap));
                    Assert.That(receivedCount, Is.EqualTo(1), "CapabilitiesReceived should fire exactly once");
                    Assert.That(client.CapturedRequests.Count, Is.EqualTo(3), "two failures then one success");
                });
            }
            finally
            {
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task SeedRequest_NotFound_AbortsWithoutRetry()
        {
            var (client, _, caps) = CreateCaps();
            try
            {
                client.AddHttpResponseForPath(SeedPath, HttpStatusCode.NotFound, string.Empty, "text/plain");

                var receivedCount = 0;
                caps.CapabilitiesReceived += (_, _) => Interlocked.Increment(ref receivedCount);

                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await caps.RunSeedRequestLoopAsync(cts.Token);

                Assert.Multiple(() =>
                {
                    Assert.That(client.CapturedRequests.Count, Is.EqualTo(1), "404 must not be retried");
                    Assert.That(caps._Caps, Is.Empty);
                    Assert.That(receivedCount, Is.EqualTo(0));
                });
            }
            finally
            {
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }
    }
}
