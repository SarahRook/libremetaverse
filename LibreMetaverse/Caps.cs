/*
 * Copyright (c) 2006-2016, openmetaverse.co
 * Copyright (c) 2019-2026, Sjofn LLC.
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
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LibreMetaverse.Packets;
using LibreMetaverse.StructuredData;
using LibreMetaverse.Interfaces;
using LibreMetaverse.Http;
using System.Net.Http;

namespace LibreMetaverse
{
    /// <summary>
    /// Capabilities is the name of the bidirectional HTTP REST protocol
    /// used to communicate non-real-time transactions such as teleporting or
    /// group messaging
    /// </summary>
    public partial class Caps
    {
        /// <summary>
        /// Triggered when an event is received via the EventQueueGet
        /// capability
        /// </summary>
        /// <param name="capsKey">Event name</param>
        /// <param name="message">Decoded event data</param>
        /// <param name="simulator">The simulator that generated the event</param>
        //public delegate void EventQueueCallback(string message, StructuredData.OSD body, Simulator simulator);

        public delegate void EventQueueCallback(string capsKey, IMessage message, Simulator simulator);

        /// <summary>Reference to the simulator this system is connected to</summary>
        public Simulator Simulator;

        internal Uri _SeedCapsURI;
        internal Dictionary<string, Uri> _Caps = new Dictionary<string, Uri>();

        private readonly CancellationTokenSource _HttpCts = new CancellationTokenSource();
        private Task? _seedTask;
        private EventQueueClient? _EventQueueClient = null;

        // Exponential backoff bounds for transient seed-capability request failures.
        private const int InitialSeedRetryDelayMs = 1_000;
        private const int MaxSeedRetryDelayMs = 30_000;

        // Initial retry delay in milliseconds. Overridable by tests to avoid slow runs;
        // not a constant for that reason.
        internal int _seedInitialDelayMs = InitialSeedRetryDelayMs;

        /// <summary>Capabilities URI this system was initialized with</summary>
        public Uri SeedCapsURI => _SeedCapsURI;

        /// <summary>Whether the capabilities event queue is connected and
        /// listening for incoming events</summary>
        public bool IsEventQueueRunning => _EventQueueClient != null && _EventQueueClient.Running;

        public EventQueueClient? EventQueue => _EventQueueClient;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="simulator"></param>
        /// <param name="seedcaps"></param>
        internal Caps(Simulator simulator, Uri seedcaps)
        {
            Simulator = simulator;
            _SeedCapsURI = seedcaps;

            _seedTask = MakeSeedRequestAsync();
        }

        public void Disconnect(bool immediate)
        {
            Logger.Info($"Caps system for {Simulator} is {(immediate ? "aborting" : "disconnecting")}", Simulator.Client);

            DisposalHelper.SafeCancelAndDispose(_HttpCts, (m, e) => { if (e != null) Logger.Debug(m, e); else Logger.Debug(m); });

            // Observe the seed-request loop so its exceptions are surfaced and teardown is clean.
            // Cancelling the CTS above makes the loop exit promptly.
            var seedTask = _seedTask;
            if (seedTask != null)
            {
                DisposalHelper.SafeWaitTask(seedTask, TimeSpan.FromSeconds(2),
                    (m, e) => { if (e != null) Logger.Debug(m, e); else Logger.Debug(m); });
            }

            _EventQueueClient?.Dispose();
        }

        /// <summary>
        /// Request the URI of a named capability
        /// </summary>
        /// <param name="capability">Name of the capability to request</param>
        /// <returns>The URI of the requested capability, or null if not found</returns>
        public Uri? CapabilityURI(string capability)
        {
            return _Caps.TryGetValue(capability, out var cap) ? cap : null;
        }

        /// <summary>
        /// Return the cap names.
        /// </summary>
        public Dictionary<string, Uri>.KeyCollection Capabilities()
        {
            return _Caps.Keys;
        }

        /// <summary>
        /// Useful for debugging, but not particularly a good idea
        /// </summary>
        /// <param name="cap">Capability address</param>
        /// <returns>Name of capability if it exists</returns>
        public string CapabilityNameFromURI(Uri cap)
        {
            return _Caps.First(x => x.Value == cap).Key;
        }

        /// <summary>
        /// Request preferred URI for texture fetch capability
        /// </summary>
        /// <returns>URI of preferred capability or null, or null if not found</returns>
        public Uri? GetTextureCapURI()
        {
            if (_Caps.TryGetValue("ViewerAsset", out var cap)) { return cap; }
            return _Caps.TryGetValue("GetTexture", out cap) ? cap : null;
        }

        /// <summary>
        /// Request preferred URI for object mesh fetch capability
        /// </summary>
        /// <returns>URI of preferred capability or null, or null if not found</returns>
        public Uri? GetMeshCapURI()
        {
            if (_Caps.TryGetValue("ViewerAsset", out var cap)) { return cap; }
            if (_Caps.TryGetValue("GetMesh2", out cap)) { return cap; }
            return _Caps.TryGetValue("GetMesh", out cap) ? cap : null;
        }
        public static OSDArray AllCapabilities = new OSDArray
        {
            "AbuseCategories",
            "AcceptFriendship",
            "AcceptGroupInvite",
            "AgentPreferences",
            "AgentProfile",
            "AgentState",
            "AttachmentResources",
            "AvatarPickerSearch",
            "AvatarRenderInfo",
            "CharacterProperties",
            "ChatSessionRequest",
            "CopyInventoryFromNotecard",
            "CreateInventoryCategory",
            "DeclineFriendship",
            "DeclineGroupInvite",
            "DispatchRegionInfo",
            "DirectDelivery",
            "EnvironmentSettings",
            "EstateAccess",
            "EstateChangeInfo",
            "EventQueueGet",
            "ExtEnvironment",
            "FetchLib2",
            "FetchLibDescendents2",
            "FetchInventory2",
            "FetchInventoryDescendents2",
            "IncrementCOFVersion",
            "RequestTaskInventory",
            "InterestList",
            "InventoryThumbnailUpload",
            "GetDisplayNames",
            "GetExperiences",
            "AgentExperiences",
            "FindExperienceByName",
            "GetExperienceInfo",
            "GetAdminExperiences",
            "GetCreatorExperiences",
            "ExperiencePreferences",
            "GroupExperiences",
            "UpdateExperience",
            "IsExperienceAdmin",
            "IsExperienceContributor",
            "RegionExperiences",
            "ExperienceQuery",
            "GetMesh",
            "GetMesh2",
            "GetMetadata",
            "GetObjectCost",
            "GetObjectPhysicsData",
            "GetTexture",
            "GroupAPIv1",
            "GroupMemberData",
            "GroupProposalBallot",
            "HomeLocation",
            "LandResources",
            "LSLSyntax",
            "MapLayer",
            "MapLayerGod",
            "MeshUploadFlag",
            "ModifyMaterialParams",
            "ModifyRegion",
            "NavMeshGenerationStatus",
            "NewFileAgentInventory",
            "NewFileAgentInventoryVariablePrice",
            "ObjectAnimation",
            "ObjectMedia",
            "ObjectMediaNavigate",
            "ObjectNavMeshProperties",
            "ParcelPropertiesUpdate",
            "ParcelVoiceInfoRequest",
            "ProductInfoRequest",
            "ProvisionVoiceAccountRequest",
            "ReadOfflineMsgs",
            "RegionObjects",
            "RegionSchedule",
            "RemoteParcelRequest",
            "RenderMaterials",
            "RequestTextureDownload",
            "ResourceCostSelected",
            "RetrieveNavMeshSrc",
            "SearchStatRequest",
            "SearchStatTracking",
            "SendPostcard",
            "SendUserReport",
            "SendUserReportWithScreenshot",
            "ServerReleaseNotes",
            "SetDisplayName",
            "SimConsoleAsync",
            "SimulatorFeatures",
            "StartGroupProposal",
            "TerrainNavMeshProperties",
            "TextureStats",
            "UntrustedSimulatorMessage",
            "UpdateAgentInformation",
            "UpdateAgentLanguage",
            "UpdateAvatarAppearance",
            "UpdateGestureAgentInventory",
            "UpdateGestureTaskInventory",
            "UpdateNotecardAgentInventory",
            "UpdateNotecardTaskInventory",
            "UpdateScriptAgent",
            "UpdateScriptTask",
            "UpdateSettingsAgentInventory",
            "UpdateSettingsTaskInventory",
            "UploadAgentProfileImage",
            "UpdateMaterialAgentInventory",
            "UpdateMaterialTaskInventory",
            "UploadBakedTexture",
            "UserInfo",
            "ViewerAsset",
            "ViewerBenefits",
            "ViewerMetrics",
            "ViewerStartAuction",
            "ViewerStats",
            "VoiceSignalingRequest",
            // AIS3
            "InventoryAPIv3",
            "LibraryAPIv3"
        };
        private async Task MakeSeedRequestAsync()
        {
            if (Simulator == null || !Simulator.Client.Network.Connected) { return; }

            CancellationToken token;
            try
            {
                // Capture the token exactly once. Re-reading _HttpCts.Token after the source is
                // disposed (on Disconnect) throws ObjectDisposedException; an already-captured
                // token stays safe to observe. See EventQueueClient.Create for the same pattern.
                token = _HttpCts.Token;
            }
            catch (ObjectDisposedException)
            {
                // Caps was already disconnected; nothing to do.
                return;
            }

            await RunSeedRequestLoopAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Repeatedly attempts the seed capability request until it succeeds, the simulator
        /// reports the capability is gone (HTTP 404), or the supplied token is cancelled
        /// (disconnect). Transient failures back off exponentially. This is a single loop —
        /// never recursion — so a persistent failure can never overflow the stack.
        /// </summary>
        internal async Task RunSeedRequestLoopAsync(CancellationToken token)
        {
            var delayMs = 0;

            while (!token.IsCancellationRequested)
            {
                if (delayMs > 0)
                {
                    try
                    {
                        await Task.Delay(delayMs, token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }

                HttpResponseMessage? response;
                byte[]? data;
                try
                {
                    (response, data) = await Simulator.Client.HttpCapsClient.PostAsync(
                        _SeedCapsURI, OSDFormat.Xml, AllCapabilities, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Disconnected/cancelled — stop quietly.
                    return;
                }
                catch (Exception ex)
                {
                    LogSeedRetry($"Seed capability request for {Simulator} failed: {ex.Message}. Retrying.", delayMs);
                    delayMs = NextRetryDelay(delayMs);
                    continue;
                }

                // A 404 means the seed capability is gone; retrying will not help.
                if (response is { StatusCode: HttpStatusCode.NotFound })
                {
                    Logger.Error($"Seed capability for {Simulator} returned a 404, capability system is aborting");
                    return;
                }

                if (response is { IsSuccessStatusCode: false })
                {
                    LogSeedRetry($"Seed capability for {Simulator} returned {response.StatusCode}. Retrying.", delayMs);
                    delayMs = NextRetryDelay(delayMs);
                    continue;
                }

                if (ProcessSeedResponse(data, token))
                {
                    return;
                }

                // Response was empty or unparseable; treat as transient and retry.
                LogSeedRetry($"Seed capability for {Simulator} returned an unusable response. Retrying.", delayMs);
                delayMs = NextRetryDelay(delayMs);
            }
        }

        /// <summary>
        /// Log a transient seed-request failure: the first failure (before any backoff) at Warn,
        /// subsequent failures at Debug to avoid log spam while retrying.
        /// </summary>
        private void LogSeedRetry(string message, int currentDelayMs)
        {
            if (currentDelayMs == 0) { Logger.Warn(message, Simulator.Client); }
            else { Logger.Debug(message); }
        }

        /// <summary>
        /// Binary exponential back-off (bounded) with a small jitter derived from
        /// Environment.TickCount so multiple clients don't retry in lockstep.
        /// </summary>
        private int NextRetryDelay(int currentMs)
        {
            var next = currentMs == 0 ? _seedInitialDelayMs
                                      : Math.Min(MaxSeedRetryDelayMs, currentMs * 2);
            next += Math.Abs(Environment.TickCount) % (next / 8 + 1);
            return next;
        }

        /// <summary>
        /// Parse a successful seed response, wire up the capabilities, and start dependent
        /// subsystems. Returns true on success, false if the response was empty or unparseable
        /// (so the caller can retry).
        /// </summary>
        private bool ProcessSeedResponse(byte[]? responseData, CancellationToken token)
        {
            if (responseData == null) { return false; }

            OSDMap? respMap;
            try
            {
                respMap = OSDParser.Deserialize(responseData) as OSDMap;
            }
            catch (Exception ex)
            {
                var respText = System.Text.Encoding.UTF8.GetString(responseData);
                Logger.Warn($"Invalid caps response; '{respText}' for seed request: {ex.Message}", Simulator.Client);
                return false;
            }

            if (respMap == null) { return false; }

            foreach (var cap in respMap.Keys)
            {
                var maybeUri = respMap[cap]?.AsUri();
                if (maybeUri != null)
                {
                    _Caps[cap] = maybeUri;
                    Simulator.Client.CapsRateLimiter.RegisterCapUri(cap, maybeUri);
                }
            }

            if (_Caps.TryGetValue("EventQueueGet", out var eventQueueGetCap))
            {
                Logger.Trace($"Starting event queue for {Simulator}", Simulator.Client);

                _EventQueueClient = new EventQueueClient(eventQueueGetCap, Simulator);
                _EventQueueClient.OnConnected += EventQueueConnectedHandler;
                _EventQueueClient.OnEvent += EventQueueEventHandler;
                _EventQueueClient.Start();
            }

            if (_Caps.TryGetValue("SimulatorFeatures", out var simFeaturesCap))
            {
                Logger.Trace($"Retrieving Simulator Features for {Simulator}", Simulator.Client);
                Simulator.Features = new SimulatorFeatures(Simulator);
                _ = FetchSimulatorFeaturesAsync(simFeaturesCap, token);
            }

            OnCapabilitiesReceived(Simulator);
            return true;
        }

        private async Task FetchSimulatorFeaturesAsync(Uri cap, CancellationToken token)
        {
            try
            {
                var (response, data) = await Simulator.Client.HttpCapsClient.GetAsync(cap, token).ConfigureAwait(false);
                Simulator.Features.SetFeatures(response, data, null);
            }
            catch (Exception ex)
            {
                Simulator.Features.SetFeatures(null, null, ex);
            }
        }

        private void EventQueueConnectedHandler()
        {
            Simulator.Client.Network.RaiseConnectedEvent(Simulator);
        }

        /// <summary>
        /// Process any incoming events, check to see if we have a message created for the event,
        /// </summary>
        /// <param name="eventName"></param>
        /// <param name="body"></param>
        private void EventQueueEventHandler(string eventName, OSDMap body)
        {
            IMessage? message = Messages.MessageUtils.DecodeEvent(eventName, body);
            if (message != null)
            {
                Simulator.Client.Network.CapsEvents.BeginRaiseEvent(eventName, message, Simulator);

                #region Stats Tracking
                if (Simulator.Client.Settings.Packets.TrackUtilization)
                {
                    Simulator.Client.Stats.Update(eventName, LibreMetaverse.Stats.Type.Message, 0, body.ToString().Length);
                }
                #endregion
            }
            else
            {
                Logger.Warn($"No Message handler exists for event {eventName}. Unable to decode. Will try Generic Handler next");
                Logger.Debug("Please report this information at https://github.com/cinderblocks/libremetaverse/issues: " + Environment.NewLine + body);

                // try generic decoder next which takes a caps event and tries to match it to an existing packet
                if (body.Type == OSDType.Map)
                {
                    OSDMap map = body;
                    Packet? packet = Packet.BuildPacket(eventName, map);
                    if (packet != null)
                    {
                        var incomingPacket = new NetworkManager.IncomingPacket
                        {
                            Simulator = Simulator,
                            Packet = packet
                        };

                        Logger.DebugLog($"Serializing {packet.Type} capability with generic handler",
                            Simulator.Client);

                        Simulator.Client.Network.EnqueueIncoming(incomingPacket);
                    }
                    else
                    {
                        Logger.Warn($"No Packet or Message handler exists for {eventName}");
                    }
                }
            }
        }

        /// <summary>Raised whenever the capabilities have been received from a simulator</summary>
        public event EventHandler<CapabilitiesReceivedEventArgs>? CapabilitiesReceived;

        /// <summary>
        /// Raises the CapabilitiesReceived event
        /// </summary>
        /// <param name="simulator">Simulator we received the capabilities from</param>
        private void OnCapabilitiesReceived(Simulator simulator)
        {
            CapabilitiesReceived?.Invoke(this, new CapabilitiesReceivedEventArgs(simulator));
        }
    }

    #region EventArgs

    public class CapabilitiesReceivedEventArgs : EventArgs
    {
        /// <summary>The simulator that received a capabilities</summary>
        public Simulator Simulator { get; }

        public CapabilitiesReceivedEventArgs(Simulator simulator)
        {
            Simulator = simulator;
        }
    }

    #endregion
}

