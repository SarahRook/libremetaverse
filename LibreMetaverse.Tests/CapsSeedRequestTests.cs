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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using LibreMetaverse.StructuredData;
using LibreMetaverse.Tests.TestHelpers;
using NUnit.Framework;

namespace LibreMetaverse.Tests
{
    /// <summary>
    /// Regression coverage for the seed capability retry. A failed seed request used to dispose the
    /// shared CancellationTokenSource and immediately re-issue the request, which read the disposed
    /// token synchronously, failed again and recursed until the process died with a stack overflow.
    /// Each scenario below used to crash the process that way.
    /// </summary>
    [TestFixture]
    [Category("Capabilities")]
    public class CapsSeedRequestTests
    {
        private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(5);

        private int _savedRetryDelayMs;
        private FakeGridClient? _client;
        private Caps? _caps;

        [SetUp]
        public void SetUp()
        {
            _savedRetryDelayMs = Caps.SeedRetryDelayMs;
            Caps.SeedRetryDelayMs = 10;
        }

        [TearDown]
        public void TearDown()
        {
            _caps?.Disconnect(true);
            if (_client != null)
            {
                SetConnected(_client, false);
                _client.Dispose();
            }
            _caps = null;
            _client = null;
            Caps.SeedRetryDelayMs = _savedRetryDelayMs;
        }

        [Test]
        public async Task SeedRequest_Failures_AreRetriedUntilDisconnect()
        {
            var attempts = 0;
            StartCaps((_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new HttpRequestException("Simulated network failure");
            });

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref attempts) >= 3, WaitLimit), Is.True,
                "failed seed requests should keep being retried");

            _caps!.Disconnect(true);
            var atDisconnect = Volatile.Read(ref attempts);
            await Task.Delay(300); // many retry intervals

            // Only an attempt that was already past its retry delay when Disconnect() ran may still reach the server
            Assert.That(Volatile.Read(ref attempts), Is.LessThanOrEqualTo(atDisconnect + 1), "no retries after Disconnect");
        }

        [Test]
        public async Task SeedRequest_DisconnectWhileInFlight_DoesNotRetry()
        {
            var attempts = 0;
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            StartCaps((_, ct) =>
            {
                Interlocked.Increment(ref attempts);
                started.TrySetResult(true);
                return HangUntilCancelled(ct);
            });
            Assert.That(await Task.WhenAny(started.Task, Task.Delay(WaitLimit)), Is.SameAs(started.Task));

            _caps!.Disconnect(true);
            await Task.Delay(300);

            Assert.That(Volatile.Read(ref attempts), Is.EqualTo(1));
        }

        [Test]
        public void SeedRequest_HttpTimeout_IsRetried()
        {
            // HttpClient reports a timeout as an OperationCanceledException; it must not be mistaken for Disconnect().
            var attempts = 0;
            StartCaps((_, ct) =>
            {
                Interlocked.Increment(ref attempts);
                return HangUntilCancelled(ct);
            }, httpTimeout: TimeSpan.FromMilliseconds(50));

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref attempts) >= 2, WaitLimit), Is.True);
        }

        [Test]
        public void SeedRequest_ZeroRetryDelay_DoesNotRecurse()
        {
            // Task.Delay(0) completes synchronously; without a minimum delay a synchronously failing request
            // would re-enter on the same stack and recurse into the original stack overflow.
            Caps.SeedRetryDelayMs = 0;
            var attempts = 0;
            StartCaps((_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new HttpRequestException("Simulated synchronous failure");
            });

            Assert.That(SpinWait.SpinUntil(() => Volatile.Read(ref attempts) >= 3, WaitLimit), Is.True);
        }

        [Test]
        public async Task SeedRequest_FailedResponsesThenSuccess_PopulatesCapsOnce()
        {
            var expectedCap = new Uri("http://fake/gettexture");
            var attempts = 0;
            var subscribed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            StartCaps(async (_, _) =>
            {
                if (Interlocked.Increment(ref attempts) <= 2)
                {
                    return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = new ByteArrayContent(Array.Empty<byte>()) };
                }
                await subscribed.Task; // don't deliver the caps before the test is listening
                var capsMap = new OSDMap { ["GetTexture"] = OSD.FromUri(expectedCap) };
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(OSDParser.SerializeLLSDXmlBytes(capsMap)) };
            });

            var raised = 0;
            var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _caps!.CapabilitiesReceived += (_, _) => { Interlocked.Increment(ref raised); received.TrySetResult(true); };
            subscribed.SetResult(true);

            Assert.That(await Task.WhenAny(received.Task, Task.Delay(WaitLimit)), Is.SameAs(received.Task));
            await Task.Delay(100); // no further seed request should follow the success

            Assert.Multiple(() =>
            {
                Assert.That(_caps.CapabilityURI("GetTexture"), Is.EqualTo(expectedCap));
                Assert.That(Volatile.Read(ref raised), Is.EqualTo(1));
                Assert.That(Volatile.Read(ref attempts), Is.EqualTo(3), "two failed responses then one success");
            });
        }

        [Test]
        public async Task SeedRequest_ThrowingCapabilitiesReceivedHandler_DoesNotReseed()
        {
            // A subscriber's exception must not trigger a re-seed: in production each re-seed also starts
            // another EventQueueClient without stopping the previous one.
            var attempts = 0;
            var subscribed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            StartCaps(async (_, _) =>
            {
                Interlocked.Increment(ref attempts);
                await subscribed.Task;
                var capsMap = new OSDMap { ["GetTexture"] = OSD.FromUri(new Uri("http://fake/gettexture")) };
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(OSDParser.SerializeLLSDXmlBytes(capsMap)) };
            });

            var raised = 0;
            var received = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _caps!.CapabilitiesReceived += (_, _) =>
            {
                Interlocked.Increment(ref raised);
                received.TrySetResult(true);
                throw new InvalidOperationException("Simulated subscriber failure");
            };
            subscribed.SetResult(true);

            Assert.That(await Task.WhenAny(received.Task, Task.Delay(WaitLimit)), Is.SameAs(received.Task));
            await Task.Delay(300); // many retry intervals

            Assert.Multiple(() =>
            {
                Assert.That(Volatile.Read(ref attempts), Is.EqualTo(1));
                Assert.That(Volatile.Read(ref raised), Is.EqualTo(1));
            });
        }

        /// <summary>
        /// Creates a Caps whose seed requests are answered by <paramref name="seedHandler"/>. The grid
        /// connection is marked as up so the constructor issues the real seed request.
        /// </summary>
        private void StartCaps(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> seedHandler,
            TimeSpan? httpTimeout = null)
        {
            _client = new FakeGridClient();
            _client.HttpCapsClient.Dispose();
            _client.HttpCapsClient = new HttpCapsClient(new StubHandler(seedHandler));
            if (httpTimeout.HasValue) { _client.HttpCapsClient.Timeout = httpTimeout.Value; }
            SetConnected(_client, true);

            var sim = new Simulator(_client, new IPEndPoint(IPAddress.Loopback, 13), 0);
            _caps = new Caps(sim, new Uri("http://fake/seed"));
        }

        private static async Task<HttpResponseMessage> HangUntilCancelled(CancellationToken ct)
        {
            await Task.Delay(Timeout.Infinite, ct);
            throw new InvalidOperationException("unreachable");
        }

        private static void SetConnected(GridClient client, bool connected) =>
            typeof(NetworkManager).GetProperty(nameof(NetworkManager.Connected))!.SetValue(client.Network, connected);

        private sealed class StubHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _send;

            public StubHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) { _send = send; }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) => _send(request, ct);
        }
    }
}
