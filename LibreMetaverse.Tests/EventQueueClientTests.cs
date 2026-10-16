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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LibreMetaverse.Http;
using LibreMetaverse.StructuredData;
using LibreMetaverse.Tests.TestHelpers;
using NUnit.Framework;

namespace LibreMetaverse.Tests
{
    /// <summary>
    /// Regression coverage for the EventQueueGet poll loop spinning at 100% CPU once its HttpClient
    /// was disposed (e.g. by GridClient.Dispose()) while the queue was never stopped: every POST threw
    /// ObjectDisposedException at once, and RequestCompletedHandler returned early for a disconnected
    /// simulator without cancelling or scheduling a backoff, so the loop re-POSTed with zero delay
    /// forever. The loop must instead stop on terminal failures and never re-poll immediately after
    /// a failed poll.
    /// </summary>
    [TestFixture]
    [Category("Capabilities")]
    public class EventQueueClientTests
    {
        private static readonly Uri EventQueueUri = new Uri("http://fake/eventqueue");
        private static readonly Uri SeedUri = new Uri("http://fake/seed");

        private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(2);

        [Test]
        public async Task DisposedHttpClient_DisconnectedSimulator_StopsPollingInsteadOfSpinning()
        {
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: false);
            var http = new ProbeHttpCapsClient(new ScriptedHttpHandler((_, token) => HangUntilCancelledAsync(token)));
            client.HttpCapsClient = http;
            http.Dispose();

            using var disposedPosts = new DisposedClientPostCounter();
            var eq = new EventQueueClient(EventQueueUri, sim);
            try
            {
                eq.Start();

                // Previously this never completed: thousands of zero-delay POSTs per second.
                var status = await StatusWithinAsync(GetPollingTask(eq), StopTimeout);

                Assert.Multiple(() =>
                {
                    Assert.That(status, Is.EqualTo(TaskStatus.RanToCompletion),
                        "the polling task should finish cleanly instead of spinning");
                    Assert.That(eq.Running, Is.False, "the queue should report it is no longer running");
                    Assert.That(disposedPosts.Count, Is.InRange(1, 3),
                        "only a handful of POSTs should hit the disposed client");
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task DisposedHttpClient_ConnectedSimulator_IsTerminal()
        {
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: true);
            var http = new ProbeHttpCapsClient(new ScriptedHttpHandler((_, token) => HangUntilCancelledAsync(token)));
            client.HttpCapsClient = http;
            http.Dispose();

            using var disposedPosts = new DisposedClientPostCounter();
            var eq = new EventQueueClient(EventQueueUri, sim);
            try
            {
                eq.Start();

                // A disposed client can never succeed again, so retrying (even with backoff) is pointless.
                var status = await StatusWithinAsync(GetPollingTask(eq), StopTimeout);

                Assert.Multiple(() =>
                {
                    Assert.That(status, Is.EqualTo(TaskStatus.RanToCompletion),
                        "ObjectDisposedException should stop the polling loop");
                    Assert.That(eq.Running, Is.False);
                    Assert.That(disposedPosts.Count, Is.EqualTo(1), "a disposed client must not be retried");
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task DisconnectedSimulator_NoLongerUsingQueue_StopsPollingAfterFirstResponse()
        {
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: false);
            var handler = new ScriptedHttpHandler((_, _) => Task.FromResult(EmptyEventQueueReply()));
            client.HttpCapsClient = new HttpCapsClient(handler);

            var eq = new EventQueueClient(EventQueueUri, sim);
            try
            {
                eq.Start();

                // The simulator has no caps, so this queue is no longer its own and nothing consumes its
                // events; previously the loop silently re-POSTed immediately, forever.
                var status = await StatusWithinAsync(GetPollingTask(eq), StopTimeout);

                Assert.Multiple(() =>
                {
                    Assert.That(status, Is.EqualTo(TaskStatus.RanToCompletion),
                        "the polling task should finish once the sim is disconnected");
                    Assert.That(eq.Running, Is.False);
                    Assert.That(handler.CountFor(EventQueueUri), Is.EqualTo(1));
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task ConnectingSimulator_StillUsingQueue_BacksOffAndHoldsEventsUntilConnected()
        {
            // A neighbour's seed capability can arrive before its circuit is up, so a simulator's own event
            // queue may poll before the simulator is connected. It must keep polling, but back off instead of
            // re-polling at once, and leave events unacknowledged (so the server delivers them again) until then.
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: false);
            var capsDocument = new OSDMap { ["EventQueueGet"] = OSD.FromUri(EventQueueUri) };
            var seedPath = SeedUri.GetLeftPart(UriPartial.Path);
            var batch = new OSDArray
            {
                new OSDMap { ["message"] = OSD.FromString("TestEvent"), ["body"] = new OSDMap() }
            };
            var firstPollGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var acks = new List<int?>();
            var handler = new ScriptedHttpHandler(async (request, token) =>
            {
                if (request.RequestUri!.GetLeftPart(UriPartial.Path) == seedPath)
                {
                    return Respond(HttpStatusCode.OK,
                        Encoding.UTF8.GetString(OSDParser.SerializeLLSDXmlBytes(capsDocument)), "application/llsd+xml");
                }

                var ack = AckOf(await request.Content!.ReadAsByteArrayAsync().ConfigureAwait(false));
                bool firstPoll;
                lock (acks)
                {
                    acks.Add(ack);
                    firstPoll = acks.Count == 1;
                }
                // Hold the first reply until the test has configured the queue.
                if (firstPoll) { await firstPollGate.Task.ConfigureAwait(false); }
                // The server delivers the same batch until it is acknowledged.
                return ack == 1 ? await HangUntilCancelledAsync(token).ConfigureAwait(false) : EventQueueReply(batch);
            });
            client.HttpCapsClient = new HttpCapsClient(handler);
            bool Acknowledged() { lock (acks) { return acks.Contains(1); } }

            var caps = new Caps(sim, SeedUri);
            sim.Caps = caps;
            try
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    await caps.RunSeedRequestLoopAsync(cts.Token);
                }
                var eq = caps.EventQueue;
                Assert.That(eq, Is.Not.Null, "the seed capabilities should have started an event queue");
                eq._initialRetryDelayMs = 100;
                var delivered = 0;
                // Count deliveries rather than decode them.
                eq.OnEvent = (_, _) => Interlocked.Increment(ref delivered);
                firstPollGate.SetResult(true);

                await Task.Delay(TimeSpan.FromMilliseconds(500));
                int?[] acksWhileConnecting;
                lock (acks) { acksWhileConnecting = acks.ToArray(); }

                Assert.Multiple(() =>
                {
                    Assert.That(eq.Running, Is.True, "the simulator's own queue should keep polling while it connects");
                    // Backing off from 100 ms allows about three polls in this time; it used to re-poll at once.
                    Assert.That(acksWhileConnecting.Length, Is.InRange(2, 5));
                    Assert.That(acksWhileConnecting, Has.None.EqualTo(1),
                        "events must not be acknowledged before they can be delivered");
                    Assert.That(Volatile.Read(ref delivered), Is.Zero, "events must wait for the connection");
                });

                sim.connected = true;

                Assert.That(await WaitUntilAsync(() => Volatile.Read(ref delivered) > 0 && Acknowledged(),
                        TimeSpan.FromSeconds(5)), Is.True,
                    "once connected, the held events should be delivered and acknowledged");
                Assert.That(Volatile.Read(ref delivered), Is.EqualTo(1), "the held event should be delivered once");
            }
            finally
            {
                firstPollGate.TrySetResult(true);
                caps.Disconnect(true);
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task GridClientDispose_WhileQueueIsStillPolling_StopsInsteadOfSpinning()
        {
            // The production scenario: the simulator is already marked disconnected but its event queue
            // was never stopped, and GridClient.Dispose() then tears the HTTP client down underneath the
            // in-flight long-poll.
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: false);
            var handler = new ScriptedHttpHandler((_, token) => HangUntilCancelledAsync(token));
            client.HttpCapsClient = new ProbeHttpCapsClient(handler);

            using var disposedPosts = new DisposedClientPostCounter();
            var eq = new EventQueueClient(EventQueueUri, sim);
            try
            {
                eq.Start();
                Assert.That(await WaitUntilAsync(() => handler.CountFor(EventQueueUri) == 1, StopTimeout), Is.True,
                    "the first long-poll should be in flight");
                Assert.That(eq.Running, Is.True);

                client.Dispose();

                var status = await StatusWithinAsync(GetPollingTask(eq), StopTimeout);

                Assert.Multiple(() =>
                {
                    Assert.That(status, Is.EqualTo(TaskStatus.RanToCompletion),
                        "the polling task should finish cleanly instead of spinning");
                    Assert.That(eq.Running, Is.False);
                    Assert.That(disposedPosts.Count, Is.LessThanOrEqualTo(3),
                        "only a handful of POSTs should hit the disposed client");
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task DisposedHttpClient_AlreadyReplaced_KeepsPollingWithTheReplacement()
        {
            // GridClient.HttpCapsClient is settable. An ObjectDisposedException from a client that has since
            // been replaced says nothing about its replacement, so the queue should retry rather than stop.
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: true);
            var replacementHandler = new ScriptedHttpHandler((_, token) => HangUntilCancelledAsync(token));
            var replacement = new HttpCapsClient(replacementHandler);
            var originalHandler = new ScriptedHttpHandler((_, _) =>
            {
                client.HttpCapsClient = replacement;
                return Task.FromException<HttpResponseMessage>(
                    new ObjectDisposedException(typeof(HttpCapsClient).FullName));
            });
            var original = new HttpCapsClient(originalHandler);
            client.HttpCapsClient = original;

            var eq = new EventQueueClient(EventQueueUri, sim);
            try
            {
                eq.Start();

                Assert.That(await WaitUntilAsync(() => replacementHandler.CountFor(EventQueueUri) == 1, StopTimeout),
                    Is.True, "the queue should go on polling with the replacement client");
                Assert.Multiple(() =>
                {
                    Assert.That(eq.Running, Is.True);
                    Assert.That(originalHandler.CountFor(EventQueueUri), Is.EqualTo(1));
                });
            }
            finally
            {
                eq.Dispose();
                original.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        /// <summary>Poll failures for which RequestCompletedHandler schedules no backoff.</summary>
        public enum FailureWithoutBackoff
        {
            /// <summary>The server closed the held-open long-poll without a response (normal idle expiry).</summary>
            LongPollExpired,
            /// <summary>The request timed out (HttpClient.Timeout).</summary>
            RequestTimedOut,
            /// <summary>The server answered at once with an error status.</summary>
            ErrorStatus
        }

        [TestCase(FailureWithoutBackoff.LongPollExpired)]
        [TestCase(FailureWithoutBackoff.RequestTimedOut)]
        [TestCase(FailureWithoutBackoff.ErrorStatus)]
        public async Task FailedPoll_IsNeverRetriedImmediately(FailureWithoutBackoff failure)
        {
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: true);
            var handler = new ScriptedHttpHandler((_, _) => Fail(failure));
            client.HttpCapsClient = new HttpCapsClient(handler);

            var eq = new EventQueueClient(EventQueueUri, sim);
            try
            {
                eq.Start();
                await Task.Delay(TimeSpan.FromSeconds(1));
                var times = handler.TimesFor(EventQueueUri);

                Assert.Multiple(() =>
                {
                    Assert.That(eq.Running, Is.True, "this failure is transient; the queue should keep polling");
                    // The ~100 ms floor allows about ten polls a second; these used to re-poll with zero delay.
                    Assert.That(times.Length, Is.InRange(2, 15));
                    Assert.That(Gaps(times).Min(), Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(75)));
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        private static Task<HttpResponseMessage> Fail(FailureWithoutBackoff failure)
        {
            switch (failure)
            {
                case FailureWithoutBackoff.LongPollExpired:
#if NET8_0_OR_GREATER
                    return Task.FromException<HttpResponseMessage>(
                        new HttpRequestException(HttpRequestError.ResponseEnded, "The response ended prematurely."));
#else
                    return Task.FromException<HttpResponseMessage>(new HttpRequestException(
                        "An error occurred while sending the request.",
                        new WebException("The underlying connection was closed.", WebExceptionStatus.ConnectionClosed)));
#endif
                case FailureWithoutBackoff.RequestTimedOut:
                    return Task.FromException<HttpResponseMessage>(new TaskCanceledException("The request timed out."));
                default:
                    return Task.FromResult(Respond(HttpStatusCode.NotFound, string.Empty, "text/plain"));
            }
        }

        [Test]
        public async Task RetryBackoff_GrowsAcrossConsecutiveFailures_AndRestartsAfterSuccess()
        {
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: true);
            var requests = 0;
            var handler = new ScriptedHttpHandler((_, _) => Interlocked.Increment(ref requests) == 3
                ? Task.FromResult(EmptyEventQueueReply())
                : Task.FromException<HttpResponseMessage>(new HttpRequestException("Simulated network failure")));
            client.HttpCapsClient = new HttpCapsClient(handler);

            var eq = new EventQueueClient(EventQueueUri, sim) { _initialRetryDelayMs = 200 };
            try
            {
                eq.Start();
                Assert.That(await WaitUntilAsync(() => handler.CountFor(EventQueueUri) >= 5, TimeSpan.FromSeconds(5)),
                    Is.True, "expected five polls: fail, fail, succeed, fail, fail");
                var gaps = Gaps(handler.TimesFor(EventQueueUri));

                Assert.Multiple(() =>
                {
                    Assert.That(gaps[0], Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(170)), "first backoff step");
                    // Every failure used to restart from the first step, so the delay never grew.
                    Assert.That(gaps[1], Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(360)),
                        "a consecutive failure should double the backoff");
                    // Had the backoff kept doubling through the success, this gap would be at least ~800 ms.
                    Assert.That(gaps[3], Is.LessThan(TimeSpan.FromMilliseconds(600)),
                        "the backoff should restart from the first step after a success");
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task HandlerException_AfterSuccessfulPoll_BacksOff()
        {
            // A successful reply schedules no backoff, and an exception while handling it skipped any, so a
            // reply that broke the handler every time made the loop re-poll at once, forever.
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: true);
            // Events must be maps; dispatching this one throws.
            var handler = new ScriptedHttpHandler((_, _) =>
                Task.FromResult(EventQueueReply(new OSDArray { OSD.FromString("not a map") })));
            client.HttpCapsClient = new HttpCapsClient(handler);

            var eq = new EventQueueClient(EventQueueUri, sim) { _initialRetryDelayMs = 200, OnEvent = (_, _) => { } };
            try
            {
                eq.Start();
                await Task.Delay(TimeSpan.FromSeconds(1));
                var times = handler.TimesFor(EventQueueUri);

                Assert.Multiple(() =>
                {
                    Assert.That(eq.Running, Is.True, "a reply the handler can't process is no reason to stop");
                    // Backing off from 200 ms allows about three polls a second.
                    Assert.That(times.Length, Is.InRange(2, 4));
                    Assert.That(Gaps(times).Min(), Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(170)));
                });
            }
            finally
            {
                eq.Dispose();
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task SimulatorDisconnect_WhenAlreadyMarkedDisconnected_StillStopsEventQueue()
        {
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: false);
            var capsDocument = new OSDMap { ["EventQueueGet"] = OSD.FromUri(EventQueueUri) };
            var seedPath = SeedUri.GetLeftPart(UriPartial.Path);
            var handler = new ScriptedHttpHandler((request, token) =>
                request.RequestUri!.GetLeftPart(UriPartial.Path) == seedPath
                    ? Task.FromResult(Respond(HttpStatusCode.OK,
                        Encoding.UTF8.GetString(OSDParser.SerializeLLSDXmlBytes(capsDocument)), "application/llsd+xml"))
                    : HangUntilCancelledAsync(token));
            client.HttpCapsClient = new HttpCapsClient(handler);

            // The constructor's own seed request short-circuits (the fake client's network is shut down),
            // so drive the seed loop explicitly to bring up a polling event queue.
            var caps = new Caps(sim, SeedUri);
            sim.Caps = caps;
            try
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    await caps.RunSeedRequestLoopAsync(cts.Token);
                }
                Assert.That(await WaitUntilAsync(() => handler.CountFor(EventQueueUri) == 1, StopTimeout), Is.True,
                    "the event queue's first long-poll should be in flight");
                Assert.That(caps.IsEventQueueRunning, Is.True);

                // Previously this returned early because the sim wasn't connected, leaving the queue running.
                sim.Disconnect(false);

                Assert.Multiple(() =>
                {
                    Assert.That(caps.IsEventQueueRunning, Is.False, "Disconnect should stop the event queue");
                    Assert.That(sim.Caps, Is.Null, "Disconnect should detach the torn-down caps");
                });
            }
            finally
            {
                caps.Disconnect(true);
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        [Test]
        public async Task StaleRun_FinishingAfterRestart_DoesNotStopTheNewRun()
        {
            // Stop() waits only so long for the polling task, so Stop() then Start() can begin a new run while
            // the old one is still handling its last reply. When that reply then made the old run stop (here
            // because the queue is no longer its simulator's), it used to cancel the new run instead.
            var client = new FakeGridClient();
            var sim = CreateSimulator(client, connected: false);
            var polls = 0;
            var handler = new ScriptedHttpHandler((_, token) => Interlocked.Increment(ref polls) == 1
                ? Task.FromResult(EmptyEventQueueReply())
                : HangUntilCancelledAsync(token));
            client.HttpCapsClient = new HttpCapsClient(handler);

            var replyReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseReply = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var eq = new EventQueueClient(EventQueueUri, sim)
            {
                // Runs on the polling task between receiving a successful reply and handling it.
                OnConnected = () =>
                {
                    replyReceived.TrySetResult(true);
                    releaseReply.Task.Wait(TimeSpan.FromSeconds(10));
                }
            };
            var stopping = Task.CompletedTask;
            try
            {
                eq.Start();
                var oldRun = GetPollingTask(eq);
                Assert.That(await StatusWithinAsync(replyReceived.Task, StopTimeout),
                    Is.EqualTo(TaskStatus.RanToCompletion), "the first reply should arrive");

                stopping = Task.Run(() => eq.Stop(true));
                Assert.That(await WaitUntilAsync(() => !eq.Running, StopTimeout), Is.True);
                eq.Start();
                Assert.That(await WaitUntilAsync(() => handler.CountFor(EventQueueUri) == 2, StopTimeout), Is.True,
                    "the new run should be polling");

                releaseReply.SetResult(true);
                var oldRunStatus = await StatusWithinAsync(oldRun, StopTimeout);

                Assert.Multiple(() =>
                {
                    Assert.That(oldRunStatus, Is.EqualTo(TaskStatus.RanToCompletion), "the old run should finish");
                    Assert.That(eq.Running, Is.True, "the old run must not stop the new one");
                });
            }
            finally
            {
                releaseReply.TrySetResult(true);
                eq.Dispose();
                await stopping;
                try { client.Dispose(); } catch { /* ignore */ }
            }
        }

        #region Helpers

        private static Simulator CreateSimulator(GridClient client, bool connected)
        {
            return new Simulator(client, new IPEndPoint(IPAddress.Loopback, 13), 0) { connected = connected };
        }

        private static Task GetPollingTask(EventQueueClient eq)
        {
            var field = typeof(EventQueueClient).GetField("_eqTask", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.That(field, Is.Not.Null, "EventQueueClient._eqTask not found; update this helper");
            var task = field!.GetValue(eq) as Task;
            Assert.That(task, Is.Not.Null, "the polling task was never started");
            return task!;
        }

        /// <summary>Waits up to <paramref name="timeout"/> for <paramref name="task"/>, then returns its status.</summary>
        private static async Task<TaskStatus> StatusWithinAsync(Task task, TimeSpan timeout)
        {
            await Task.WhenAny(task, Task.Delay(timeout));
            return task.Status;
        }

        private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (sw.Elapsed > timeout) { return false; }
                await Task.Delay(10);
            }
            return true;
        }

        private static TimeSpan[] Gaps(IReadOnlyList<TimeSpan> times)
        {
            return times.Zip(times.Skip(1), (earlier, later) => later - earlier).ToArray();
        }

        private static HttpResponseMessage Respond(HttpStatusCode status, string body, string mediaType)
        {
            var response = new HttpResponseMessage(status) { Content = new StringContent(body) };
            response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(mediaType);
            return response;
        }

        /// <summary>A well-formed EventQueueGet reply carrying <paramref name="events"/>.</summary>
        private static HttpResponseMessage EventQueueReply(OSDArray events, int id = 1)
        {
            var body = new OSDMap { ["events"] = events, ["id"] = OSD.FromInteger(id) };
            return Respond(HttpStatusCode.OK, Encoding.UTF8.GetString(OSDParser.SerializeLLSDXmlBytes(body)),
                "application/llsd+xml");
        }

        /// <summary>A well-formed EventQueueGet reply carrying no events.</summary>
        private static HttpResponseMessage EmptyEventQueueReply() => EventQueueReply(new OSDArray());

        /// <summary>The "ack" an EventQueueGet request carries, or null if it carries none.</summary>
        private static int? AckOf(byte[] requestBody)
        {
            var osd = OSDParser.DeserializeLLSDXml(requestBody);
            // The queue posts its serialized payload wrapped as an LLSD binary value.
            if (osd is OSDBinary wrapped) { osd = OSDParser.DeserializeLLSDXml(wrapped.AsBinary()); }
            return osd is OSDMap map && map.TryGetValue("ack", out var ack) && ack.Type == OSDType.Integer
                ? ack.AsInteger()
                : (int?)null;
        }

        /// <summary>Simulates a long-poll the server holds open until the request is cancelled.</summary>
        private static async Task<HttpResponseMessage> HangUntilCancelledAsync(CancellationToken token)
        {
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("unreachable");
        }

        /// <summary>
        /// HttpMessageHandler that answers every request through a script and records when each
        /// request arrived, so tests can count POSTs and measure the spacing between them.
        /// </summary>
        private sealed class ScriptedHttpHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _respond;
            private readonly Stopwatch _clock = Stopwatch.StartNew();
            private readonly List<(string Path, TimeSpan At)> _requests = new List<(string, TimeSpan)>();

            public ScriptedHttpHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond)
            {
                _respond = respond;
            }

            /// <summary>Arrival times (since construction) of the requests made to <paramref name="uri"/>.</summary>
            public TimeSpan[] TimesFor(Uri uri)
            {
                var path = uri.GetLeftPart(UriPartial.Path);
                lock (_requests)
                {
                    return _requests.Where(r => r.Path == path).Select(r => r.At).ToArray();
                }
            }

            public int CountFor(Uri uri) => TimesFor(uri).Length;

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                lock (_requests)
                {
                    _requests.Add((request.RequestUri!.GetLeftPart(UriPartial.Path), _clock.Elapsed));
                }
                return _respond(request, cancellationToken);
            }
        }

        /// <summary>
        /// Distinct HttpCapsClient type, so the ObjectDisposedExceptions thrown by this test's disposed
        /// client can be told apart from any other disposed client in the process.
        /// </summary>
        private sealed class ProbeHttpCapsClient : HttpCapsClient
        {
            public ProbeHttpCapsClient(HttpMessageHandler handler) : base(handler) { }
        }

        /// <summary>
        /// Counts POST attempts rejected by a disposed <see cref="ProbeHttpCapsClient"/>. HttpClient throws
        /// ObjectDisposedException before any handler runs, so a handler can't see these attempts; each
        /// one throws a fresh exception (merely rethrown up the await chain), so counting the distinct
        /// first-chance exception instances naming the probe type counts the attempts.
        /// </summary>
        private sealed class DisposedClientPostCounter : IDisposable
        {
            private static readonly string ProbeTypeName = typeof(ProbeHttpCapsClient).FullName!;
            private readonly HashSet<Exception> _seen = new HashSet<Exception>();

            public DisposedClientPostCounter()
            {
                AppDomain.CurrentDomain.FirstChanceException += OnFirstChanceException;
            }

            public int Count
            {
                get { lock (_seen) { return _seen.Count; } }
            }

            private void OnFirstChanceException(object sender, FirstChanceExceptionEventArgs e)
            {
                if (e.Exception is ObjectDisposedException ode && ode.ObjectName == ProbeTypeName)
                {
                    lock (_seen) { _seen.Add(ode); }
                }
            }

            public void Dispose()
            {
                AppDomain.CurrentDomain.FirstChanceException -= OnFirstChanceException;
            }
        }

        #endregion Helpers
    }
}
