package org.thoughtcrime.securesms.calls;

import static org.thoughtcrime.securesms.calls.CallUtil.getIconFromChat;
import static org.thoughtcrime.securesms.calls.CallUtil.getNameFromChat;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Person;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.graphics.drawable.Icon;
import android.media.AudioAttributes;
import android.media.AudioManager;
import android.media.RingtoneManager;
import android.net.Uri;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.telecom.DisconnectCause;
import android.util.Log;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.RequiresApi;
import androidx.core.app.NotificationManagerCompat;
import androidx.core.content.ContextCompat;
import androidx.core.telecom.CallAttributesCompat;
import androidx.core.telecom.CallControlResult;
import androidx.core.telecom.CallControlScope;
import androidx.core.telecom.CallEndpointCompat;
import androidx.core.telecom.CallException;
import androidx.core.telecom.CallsManager;
import androidx.core.util.Pair;
import androidx.lifecycle.FlowLiveDataConversions;
import androidx.lifecycle.LiveData;
import androidx.lifecycle.MediatorLiveData;
import androidx.lifecycle.MutableLiveData;
import androidx.lifecycle.Observer;
import chat.delta.rpc.Rpc;
import chat.delta.rpc.RpcException;
import com.b44t.messenger.DcChat;
import com.b44t.messenger.DcContext;
import com.b44t.messenger.DcEvent;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import kotlin.Unit;
import kotlin.coroutines.Continuation;
import kotlin.coroutines.CoroutineContext;
import kotlin.coroutines.EmptyCoroutineContext;
import kotlinx.coroutines.BuildersKt;
import kotlinx.coroutines.CoroutineScope;
import kotlinx.coroutines.CoroutineScopeKt;
import kotlinx.coroutines.Dispatchers;
import kotlinx.coroutines.flow.Flow;
import kotlinx.coroutines.flow.FlowKt;
import org.thoughtcrime.securesms.ApplicationContext;
import org.thoughtcrime.securesms.R;
import org.thoughtcrime.securesms.connect.DcEventCenter;
import org.thoughtcrime.securesms.connect.DcHelper;
import org.thoughtcrime.securesms.util.Util;
import org.webrtc.PeerConnection;
import org.webrtc.VideoTrack;

@RequiresApi(api = Build.VERSION_CODES.O)
public class CallCoordinator implements DcEventCenter.DcEventDelegate {
  private static final String TAG = "CallCoordinator";

  // Notification channels
  private static final String CHANNEL_ID_INCOMING = "voip_incoming_calls_v2";
  private static final String CHANNEL_ID_ONGOING = "voip_ongoing_calls";
  private static final String CHANNEL_ID_MISSED = "voip_missed_calls";
  private static final int NOTIFICATION_ID_CALL = 1001;

  private static final int PI_ANSWER = 0;
  private static final int PI_DECLINE = 1;
  private static final int PI_FULLSCREEN = 2;
  private static final int PI_HANGUP = 3;
  private static final int PI_ONGOING_CONTENT = 4;

  private static final String CALL_IDENTIFIER_SCHEME = "deltachat:";

  // Recovery state machine tuning. The grace period absorbs transient DISCONNECTED states
  // caused by short radio hiccups during a mobile-data handoff. The answer timeout is
  // generous because Core renegotiation rides the same async pipeline as the initial
  // setup: near-instant on chatmail-fast, but bound by IDLE/poll on classic IMAP.
  private static final long RECONNECT_DISCONNECTED_GRACE_MS = 7000L;
  private static final long RECONNECT_ANSWER_TIMEOUT_MS = 30000L;
  private static final int RECONNECT_MAX_ATTEMPTS = 3;

  private static CallCoordinator instance;
  private final Context appContext;
  private final CallsManager callsManager;
  private final NotificationManagerCompat notificationManager;
  private final Rpc rpc;
  private CoroutineScope audioFlowScope;
  private final Handler mainHandler = new Handler(Looper.getMainLooper());
  private final ExecutorService eventExecutor = Executors.newSingleThreadExecutor();

  // LiveData for Observable State
  private final MutableLiveData<PeerConnection.PeerConnectionState> connectionState =
      new MutableLiveData<>(PeerConnection.PeerConnectionState.NEW);
  private final MutableLiveData<VideoTrack> localVideoTrack = new MutableLiveData<>();
  private final MutableLiveData<VideoTrack> remoteVideoTrack = new MutableLiveData<>();
  private final MutableLiveData<Boolean> localAudioEnabled = new MutableLiveData<>(true);
  private final MutableLiveData<Boolean> localVideoEnabled = new MutableLiveData<>(true);
  private final MutableLiveData<Boolean> remoteAudioEnabled = new MutableLiveData<>(true);
  private final MutableLiveData<Boolean> remoteVideoEnabled = new MutableLiveData<>(true);
  private final MutableLiveData<Boolean> isRelayUsed = new MutableLiveData<>(false);
  private final MutableLiveData<String> errorMessage = new MutableLiveData<>();
  private final MutableLiveData<String> displayName = new MutableLiveData<>();
  private final MutableLiveData<Icon> displayIcon = new MutableLiveData<>();
  private final MutableLiveData<Boolean> outgoingCallPlaced = new MutableLiveData<>(false);
  private final MutableLiveData<Boolean> answeredElsewhere = new MutableLiveData<>(false);
  private final MutableLiveData<Boolean> isFrontCamera = new MutableLiveData<>(true);
  private final MutableLiveData<Boolean> mediaCaptureReady = new MutableLiveData<>(false);
  private final MutableLiveData<RecoveryState> recoveryState =
      new MutableLiveData<>(RecoveryState.IDLE);
  // Synchronous mirror of recoveryState used by the peer-state funnel and timed runnables,
  // which need an immediate view of the most recent recovery decision (postValue is async).
  private volatile RecoveryState recoveryStateSync = RecoveryState.IDLE;

  // Audio Routing Support
  private final MediatorLiveData<CallEndpointCompat> currentAudioEndpoint =
      new MediatorLiveData<>();
  private final MediatorLiveData<List<CallEndpointCompat>> availableAudioEndpoints =
      new MediatorLiveData<>();
  private LiveData<CallEndpointCompat> currentAudioEndpointSource;
  private LiveData<List<CallEndpointCompat>> availableAudioEndpointsSource;

  private CallService callService;
  private ServiceConnection serviceConnection;
  private boolean isServiceBound = false;

  // Call metadata, single source of truth
  private Integer activeAccId;
  private Integer activeCallId;
  private Integer activeChatId;
  private boolean isIncomingCall;
  private boolean startsWithVideo;
  private String pendingOfferSdp;
  private boolean hasNotifiedBackend = false;
  private boolean hasAutoSelectedEndpoint = false;
  private boolean pendingMediaCapture = false;
  private boolean answerInProgress = false;
  private final AtomicBoolean teardownInProgress = new AtomicBoolean(false);
  private Observer<Boolean> answerMediaObserver;
  private int callGeneration = 0;

  // ICE-restart recovery state
  private int restartAttempts = 0;
  private String currentRestartId; // UUID identifying the in-flight restart transaction
  private long currentCallGenerationAtRestartStart; // callGeneration snapshot when started
  private Runnable restartGraceRunnable;
  private Runnable restartAnswerTimeoutRunnable;
  private boolean awaitingAnswer = false;
  // Stashed remote answer SDP if it arrives before the local offer is fully prepared
  // (defensive: in practice offer SDP is gathered synchronously after restartIce and an
  // answer cannot arrive that fast; this is purely for race-free state).

  private CallControlScope activeCallControlScope;
  private CallViewModel activeCallViewModel;
  private CallEndpointCompat preferredStartingEndpoint;

  private final Runnable outgoingRingtoneRunnable =
      () -> {
        synchronized (CallCoordinator.this) {
          if (callService != null && hasActiveCall()) {
            callService.startOutgoingRingtone();
          }
        }
      };

  private CallCoordinator(Context context) {
    this.appContext = context.getApplicationContext();
    this.rpc = DcHelper.getRpc(this.appContext);
    this.callsManager = new CallsManager(this.appContext);
    this.notificationManager = NotificationManagerCompat.from(this.appContext);

    addEventListeners();
    createNotificationChannels();
    registerTelecom();
    createServiceConnection();
  }

  public static synchronized CallCoordinator getInstance(Context context) {
    if (instance == null) {
      instance = new CallCoordinator(context);
    }
    return instance;
  }

  private void createNotificationChannels() {
    notificationManager.deleteNotificationChannel("voip_incoming_calls");

    Uri ringtoneUri = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE);
    AudioAttributes ringtoneAttributes =
        new AudioAttributes.Builder()
            .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
            .setUsage(AudioAttributes.USAGE_NOTIFICATION_RINGTONE)
            .setLegacyStreamType(AudioManager.STREAM_RING)
            .build();

    NotificationChannel incomingChannel =
        new NotificationChannel(
            CHANNEL_ID_INCOMING, "Incoming Calls", NotificationManager.IMPORTANCE_HIGH);
    incomingChannel.setDescription("Notifications for incoming DeltaChat calls");
    incomingChannel.setSound(ringtoneUri, ringtoneAttributes);
    incomingChannel.setVibrationPattern(new long[] {0, 1000, 1000});

    NotificationChannel ongoingChannel =
        new NotificationChannel(
            CHANNEL_ID_ONGOING, "Active Calls", NotificationManager.IMPORTANCE_DEFAULT);
    ongoingChannel.setDescription("Notifications for active DeltaChat calls");
    ongoingChannel.setSound(null, null);

    NotificationChannel missedChannel =
        new NotificationChannel(
            CHANNEL_ID_MISSED, "Missed Calls", NotificationManager.IMPORTANCE_HIGH);
    missedChannel.setDescription("Notifications for missed DeltaChat calls");

    notificationManager.createNotificationChannel(incomingChannel);
    notificationManager.createNotificationChannel(ongoingChannel);
    notificationManager.createNotificationChannel(missedChannel);
  }

  private void registerTelecom() {
    try {
      int capabilities =
          CallsManager.CAPABILITY_BASELINE | CallsManager.CAPABILITY_SUPPORTS_VIDEO_CALLING;
      callsManager.registerAppWithTelecom(capabilities);
      Log.d(TAG, "Successfully registered through Telecom");
    } catch (Exception e) {
      Log.e(TAG, "Failed to register with Telecom", e);
    }
  }

  private void disconnectTelecom(DisconnectCause cause) {
    // Snapshot the scope locally and null out the field atomically under the coordinator
    // monitor. This makes the disconnect attempt idempotent: a racing teardown cannot see a
    // half-cleared scope, and a second concurrent invocation observes null and bails out.
    CallControlScope scope;
    synchronized (this) {
      scope = activeCallControlScope;
      activeCallControlScope = null;
    }
    if (scope == null) {
      Log.w(TAG, "No active CallControlScope, skipping disconnect (cause=" + cause.getCode() + ")");
      return;
    }

    scope.disconnect(
        cause,
        new Continuation<CallControlResult>() {
          @NonNull
          @Override
          public CoroutineContext getContext() {
            return EmptyCoroutineContext.INSTANCE;
          }

          @Override
          public void resumeWith(@NonNull Object result) {
            Log.d(TAG, "Telecom disconnect completed: " + result);
          }
        });
  }

  private void addEventListeners() {
    DcEventCenter eventCenter = DcHelper.getEventCenter(this.appContext);
    eventCenter.removeObservers(this);

    eventCenter.addMultiAccountObserver(DcContext.DC_EVENT_INCOMING_CALL, this);
    eventCenter.addMultiAccountObserver(DcContext.DC_EVENT_INCOMING_CALL_ACCEPTED, this);
    eventCenter.addMultiAccountObserver(DcContext.DC_EVENT_OUTGOING_CALL_ACCEPTED, this);
    eventCenter.addMultiAccountObserver(DcContext.DC_EVENT_CALL_ENDED, this);
    // DC_EVENT_CALL_UPDATE_RECEIVED is emitted by Core's `send_call_update` plumbing and
    // carries an opaque JSON payload with id/type/sdp for ICE-restart renegotiation.
    eventCenter.addMultiAccountObserver(DcContext.DC_EVENT_CALL_UPDATE_RECEIVED, this);
  }

  private void createServiceConnection() {
    serviceConnection =
        new ServiceConnection() {
          @Override
          public void onServiceConnected(ComponentName name, IBinder service) {
            synchronized (CallCoordinator.this) {
              CallService.LocalBinder binder = (CallService.LocalBinder) service;
              callService = binder.getService();
              Log.d(TAG, "Bound to CallService");

              if (activeAccId == null || activeCallId == null) {
                Log.d(TAG, "Call already ended, not initializing service");
                stopAndUnbindService();
                notificationManager.cancel(NOTIFICATION_ID_CALL);
                return;
              }

              if (!isIncomingCall) {

                // For outgoing call, show notification immediately
                String calleeName = displayName.getValue();
                if (calleeName == null) {
                  calleeName = "Unknown";
                }

                showOrUpdateOngoingNotification(
                    appContext.getString(R.string.calling_person, calleeName));
              }

              // Initialize call
              callService.initializeCall();

              if (!isIncomingCall) {
                mainHandler.removeCallbacks(outgoingRingtoneRunnable);
                mainHandler.postDelayed(outgoingRingtoneRunnable, 1500);
              }

              if (pendingMediaCapture) {
                pendingMediaCapture = false;
                callService.startMediaCapture();
              }
            }
          }

          @Override
          public void onServiceDisconnected(ComponentName name) {
            synchronized (CallCoordinator.this) {
              callService = null;
              isServiceBound = false;
            }
            Log.d(TAG, "Unbound from CallService");
          }
        };
  }

  private synchronized void startAndBindService() {
    Intent serviceIntent = new Intent(this.appContext, CallService.class);
    this.appContext.startService(serviceIntent);

    // Set isServiceBound based on bindService return value
    isServiceBound =
        this.appContext.bindService(serviceIntent, serviceConnection, Context.BIND_AUTO_CREATE);

    Log.d(TAG, "Bind service result: " + isServiceBound);
  }

  private synchronized void stopAndUnbindService() {
    if (isServiceBound) {
      try {
        this.appContext.unbindService(serviceConnection);
        Log.d(TAG, "Service unbound successfully");
      } catch (IllegalArgumentException e) {
        Log.e(TAG, "Service was not registered or already unbound", e);
      } finally {
        isServiceBound = false;
        callService = null;
      }
    }

    Intent serviceIntent = new Intent(this.appContext, CallService.class);
    this.appContext.stopService(serviceIntent);
  }

  // LiveData Getters

  /**
   * ICE-restart recovery lifecycle visible to the UI. While this is anything other than {@link
   * RecoveryState#IDLE}, the UI should keep showing the call screen with a "reconnecting" indicator
   * even if the underlying peer state is DISCONNECTED or transiently FAILED.
   */
  public enum RecoveryState {
    /** No recovery in flight. Default. */
    IDLE,
    /**
     * DISCONNECTED observed but we are waiting out a short grace period (a few seconds) to let
     * transient radio hiccups self-resolve before triggering an ICE restart.
     */
    AWAITING_GRACE,
    /**
     * Recovery in flight: an offer is being prepared, sent over Core, or an answer is expected back
     * from the peer.
     */
    RECONNECTING
  }

  public LiveData<PeerConnection.PeerConnectionState> getConnectionState() {
    return connectionState;
  }

  public LiveData<RecoveryState> getRecoveryState() {
    return recoveryState;
  }

  public LiveData<VideoTrack> getLocalVideoTrack() {
    return localVideoTrack;
  }

  public LiveData<VideoTrack> getRemoteVideoTrack() {
    return remoteVideoTrack;
  }

  public LiveData<Boolean> getLocalAudioEnabled() {
    return localAudioEnabled;
  }

  public LiveData<Boolean> getLocalVideoEnabled() {
    return localVideoEnabled;
  }

  public LiveData<Boolean> getRemoteAudioEnabled() {
    return remoteAudioEnabled;
  }

  public LiveData<Boolean> getRemoteVideoEnabled() {
    return remoteVideoEnabled;
  }

  public LiveData<Boolean> getIsRelayUsed() {
    return isRelayUsed;
  }

  public LiveData<String> getErrorMessage() {
    return errorMessage;
  }

  public LiveData<String> getDisplayName() {
    return displayName;
  }

  public LiveData<Icon> getDisplayIcon() {
    return displayIcon;
  }

  public LiveData<Boolean> getOutgoingCallPlaced() {
    return outgoingCallPlaced;
  }

  public LiveData<Boolean> getAnsweredElsewhere() {
    return answeredElsewhere;
  }

  public LiveData<CallEndpointCompat> getCurrentAudioEndpoint() {
    return currentAudioEndpoint;
  }

  public LiveData<List<CallEndpointCompat>> getAvailableAudioEndpoints() {
    return availableAudioEndpoints;
  }

  static CallEndpointCompat findPreferredEndpoint(
      List<CallEndpointCompat> endpoints, boolean startsWithVideo) {
    if (endpoints == null || endpoints.isEmpty()) return null;

    for (CallEndpointCompat endpoint : endpoints) {
      int type = endpoint.getType();
      if (type == CallEndpointCompat.TYPE_BLUETOOTH
          || type == CallEndpointCompat.TYPE_WIRED_HEADSET) {
        return endpoint;
      }
    }

    int fallbackType =
        startsWithVideo ? CallEndpointCompat.TYPE_SPEAKER : CallEndpointCompat.TYPE_EARPIECE;

    for (CallEndpointCompat endpoint : endpoints) {
      if (endpoint.getType() == fallbackType) {
        return endpoint;
      }
    }

    return null;
  }

  public LiveData<Boolean> getIsFrontCamera() {
    return isFrontCamera;
  }

  public LiveData<Boolean> getMediaCaptureReady() {
    return mediaCaptureReady;
  }

  // State Update Methods (CallService)

  public void updateConnectionState(PeerConnection.PeerConnectionState state) {
    Log.d(TAG, "updateConnectionState: " + state);
    connectionState.postValue(state);

    synchronized (this) {
      if (state == PeerConnection.PeerConnectionState.CONNECTED) {
        // Any successful connection while recovery is active cancels the timer budget and
        // marks the recovery as successful. Recovery keeps state coherent even if the
        // CONNECTION state was preceded by a brief FAILED that we absorbed via the FAILED
        // arm below.
        if (recoveryStateSync != RecoveryState.IDLE) {
          Log.d(TAG, "updateConnectionState: CONNECTED during recovery - success");
          cancelRecoveryTimers();
          setRecoveryState(RecoveryState.IDLE);
          awaitingAnswer = false;
          currentRestartId = null;
          restartAttempts = 0;
        }
      }
    }

    if (state == PeerConnection.PeerConnectionState.DISCONNECTED) {
      handlePeerDisconnected();
    } else if (state == PeerConnection.PeerConnectionState.FAILED) {
      handlePeerFailed();
    } else if (state == PeerConnection.PeerConnectionState.CLOSED) {
      // CLOSED is only emitted after explicit `peerConnection.close()`, which only happens
      // during teardown. Always tear down.
      handleConnectionEnded(state);
    }
  }

  /**
   * Raw DISCONNECTED observed on the peer connection. Triggers the grace timer; if recovery is not
   * yet CONNECTED when it expires, we attempt an ICE restart.
   */
  private void handlePeerDisconnected() {
    final boolean shouldStartGrace;
    synchronized (this) {
      if (!hasActiveCall() || activeCallId == null) {
        return;
      }
      if (recoveryStateSync != RecoveryState.IDLE) {
        Log.d(TAG, "DISCONNECTED observed during recovery, ignoring");
        return;
      }
      shouldStartGrace = true;
    }
    if (!shouldStartGrace) return;

    Log.d(TAG, "DISCONNECTED observed, starting recovery grace period");
    setRecoveryState(RecoveryState.AWAITING_GRACE);
    if (restartGraceRunnable != null) {
      mainHandler.removeCallbacks(restartGraceRunnable);
    }
    restartGraceRunnable =
        () -> {
          synchronized (CallCoordinator.this) {
            if (recoveryStateSync != RecoveryState.AWAITING_GRACE) {
              return;
            }
            PeerConnection.PeerConnectionState current = connectionState.getValue();
            if (current == PeerConnection.PeerConnectionState.CONNECTED) {
              Log.d(TAG, "Grace expired but peer is CONNECTED, aborting recovery");
              setRecoveryState(RecoveryState.IDLE);
              return;
            }
          }
          Log.d(TAG, "Grace period expired, triggering ICE restart");
          beginRestartAttempt("disconnected-grace");
        };
    mainHandler.postDelayed(restartGraceRunnable, RECONNECT_DISCONNECTED_GRACE_MS);
  }

  /**
   * Raw FAILED observed on the peer connection. Triggers an immediate recovery attempt. If recovery
   * has already exhausted its budget, falls through to {@link
   * #handleConnectionEnded(PeerConnection.PeerConnectionState)}.
   */
  private void handlePeerFailed() {
    final boolean shouldAttemptOrTeardown;
    synchronized (this) {
      if (!hasActiveCall() || activeCallId == null) {
        return;
      }
      if (recoveryStateSync != RecoveryState.IDLE) {
        // Already attempting recovery; ignore the raw FAILED so we don't
        // prematurely tear down.
        Log.d(TAG, "FAILED observed during recovery, ignoring");
        return;
      }
      shouldAttemptOrTeardown = true;
    }
    if (!shouldAttemptOrTeardown) return;

    if (restartAttempts >= RECONNECT_MAX_ATTEMPTS) {
      Log.w(TAG, "FAILED observed with restart budget exhausted, tearing down");
      handleConnectionEnded(PeerConnection.PeerConnectionState.FAILED);
      return;
    }
    Log.d(TAG, "FAILED observed, triggering ICE restart immediately");
    beginRestartAttempt("peer-failed");
  }

  public void updateLocalVideoTrack(VideoTrack track) {
    Log.d(TAG, "updateLocalVideoTrack: " + (track != null));
    localVideoTrack.postValue(track);
  }

  public void updateRemoteVideoTrack(VideoTrack track) {
    Log.d(TAG, "updateRemoteVideoTrack: " + (track != null));
    remoteVideoTrack.postValue(track);
  }

  public void updateRemoteMutedState(boolean audioEnabled, boolean videoEnabled) {
    Log.d(TAG, "updateRemoteMutedState: audio=" + audioEnabled + ", video=" + videoEnabled);
    remoteAudioEnabled.postValue(audioEnabled);
    remoteVideoEnabled.postValue(videoEnabled);
  }

  public void updateRelayUsage(Boolean isRelay) {
    Log.d(TAG, "updateRelayUsage: " + isRelay);
    isRelayUsed.postValue(isRelay);
  }

  public void updateFrontCamera(boolean front) {
    Log.d(TAG, "updateFrontCamera: " + front);
    isFrontCamera.postValue(front);
  }

  public void updateMediaCaptureReady(boolean ready) {
    Log.d(TAG, "updateMediaCaptureReady: " + ready);
    mediaCaptureReady.postValue(ready);
  }

  public void reportError(String error) {
    Log.e(TAG, "reportError: " + error);
    errorMessage.postValue(error);
  }

  // Delayed Media Initialization Support

  public synchronized void startMediaCapture() {
    Log.d(TAG, "startMediaCapture");
    if (callService != null) {
      callService.startMediaCapture();
    } else {
      Log.d(TAG, "Service not ready, deferring media capture");
      pendingMediaCapture = true;
    }
  }

  public synchronized void answerCall(boolean fromTelecom) {
    Log.d(TAG, "answerCall: fromTelecom=" + fromTelecom);

    if (!isIncomingCall) {
      Log.w(TAG, "Not an incoming call");
      return;
    }

    if (answerInProgress) {
      Log.d(TAG, "Answer already in progress");
      return;
    }

    answerInProgress = true;

    if (callService != null) {
      callService.stopRingtone();
    }

    // Only notify Telecom if the answer originated from UI
    if (!fromTelecom) {
      CallControlScope scope = activeCallControlScope;
      if (scope != null) {
        scope.answer(
            CallAttributesCompat.CALL_TYPE_VIDEO_CALL,
            new Continuation<CallControlResult>() {
              @NonNull
              @Override
              public CoroutineContext getContext() {
                return EmptyCoroutineContext.INSTANCE;
              }

              @Override
              public void resumeWith(@NonNull Object result) {
                if (result instanceof CallControlResult) {
                  Log.d(TAG, "Answer succeeded with CallControlScope");
                } else if (result instanceof kotlin.Result.Failure) {
                  Log.e(TAG, "Answer failed", ((kotlin.Result.Failure) result).exception);
                  reportError("Failed to answer call");
                }
              }
            });
      }
    }

    if (!hasMicrophonePermission()) {
      Log.w(TAG, "Mic permission missing, prompting user with notification");

      String callerName = displayName.getValue();
      if (callerName == null) callerName = "Unknown";

      launchCallActivity();
      showPermissionNeededNotification(callerName);

      return;
    }

    answerAfterPermissions();
  }

  public void answerWebRTC() {
    Log.d(TAG, "answerWebRTC");

    if (!isIncomingCall) {
      Log.w(TAG, "Not an incoming call");
      return;
    }

    if (pendingOfferSdp == null) {
      Log.e(TAG, "No pending offer SDP");
      reportError("Call data missing");
      return;
    }

    // Tell service to process offer and create answer
    if (callService != null) {
      callService.answerIncomingCall();
    } else {
      Log.e(TAG, "Cannot answer, service not ready");
      reportError("Service not ready");
    }
  }

  public synchronized String getPendingOfferSdp() {
    return pendingOfferSdp;
  }

  public synchronized void clearPendingOfferSdp() {
    pendingOfferSdp = null;
  }

  // RPC Signaling (CallService)

  public synchronized void handleOfferReady(String offerSdp) {
    Log.d(TAG, "Offer SDP ready, sending via RPC");

    if (activeAccId == null || activeCallId == null) {
      Log.d(TAG, "Call ended, not handling offer");
      return;
    }

    new Thread(
            () -> {
              try {
                // RPC returns the final callId
                int callId =
                    rpc.placeOutgoingCall(activeAccId, activeChatId, offerSdp, startsWithVideo);

                Log.d(TAG, "Outgoing call initiated, final callId: " + callId);

                // Update our stored callId
                this.activeCallId = callId;

                completeOutgoingCall(activeAccId, callId, activeChatId);

              } catch (RpcException e) {
                Log.e(TAG, "Failed to send offer with RPC", e);
                reportError("Failed to initiate call: " + e.getMessage());
              }
            })
        .start();
  }

  public void handleAnswerReady(String answerSdp) {
    Log.d(TAG, "handleAnswerReady, sending via RPC");

    new Thread(
            () -> {
              try {
                rpc.acceptIncomingCall(activeAccId, activeCallId, answerSdp);
                Log.d(TAG, "Answer sent successfully");

              } catch (RpcException e) {
                Log.e(TAG, "Failed to send answer with RPC", e);
                reportError("Failed to answer call: " + e.getMessage());
              }
            })
        .start();
  }

  // ICE Restart signalling (CallService -> coordinator -> Core)

  /**
   * Caller-side: a fresh ICE-restart offer SDP has been produced locally. Send it to the peer via
   * Core's {@code send_call_update} RPC.
   */
  public synchronized void handleIceRestartOfferReady(String offerSdp) {
    Log.d(TAG, "handleIceRestartOfferReady");
    if (!hasActiveCall() || activeAccId == null || activeCallId == null) {
      Log.w(TAG, "handleIceRestartOfferReady: no active call");
      return;
    }
    if (recoveryStateSync != RecoveryState.RECONNECTING) {
      Log.w(TAG, "handleIceRestartOfferReady: recovery not active, dropping restart offer");
      return;
    }

    String id = currentRestartId;
    if (id == null) {
      Log.w(TAG, "handleIceRestartOfferReady: missing currentRestartId");
      completeRestartAttemptWithFailure("missing-restart-id");
      return;
    }

    String payload = RestartNegotiationPayload.buildOfferWithId(id, offerSdp);
    boolean sent = sendCallUpdateViaCore(activeAccId, activeCallId, payload);
    if (!sent) {
      Log.w(TAG, "handleIceRestartOfferReady: Core RPC not available; recovery will not complete");
      completeRestartAttemptWithFailure("rpc-not-available");
      return;
    }
    awaitingAnswer = true;
    scheduleAnswerTimeout(id);
  }

  /**
   * Callee-side: a fresh ICE-restart answer SDP has been produced locally. Send it back to the peer
   * via Core's {@code send_call_update} RPC.
   */
  public synchronized void handleIceRestartAnswerReady(String answerSdp) {
    Log.d(TAG, "handleIceRestartAnswerReady");
    if (!hasActiveCall() || activeAccId == null || activeCallId == null) {
      Log.w(TAG, "handleIceRestartAnswerReady: no active call");
      return;
    }
    if (recoveryStateSync != RecoveryState.RECONNECTING) {
      Log.w(TAG, "handleIceRestartAnswerReady: recovery not active, dropping restart answer");
      return;
    }

    String id = currentRestartId;
    if (id == null) {
      Log.w(TAG, "handleIceRestartAnswerReady: missing currentRestartId");
      completeRestartAttemptWithFailure("missing-restart-id");
      return;
    }

    String payload = RestartNegotiationPayload.buildAnswer(id, answerSdp);
    if (!sendCallUpdateViaCore(activeAccId, activeCallId, payload)) {
      Log.w(TAG, "handleIceRestartAnswerReady: Core RPC not available");
      completeRestartAttemptWithFailure("rpc-not-available");
    }
  }

  /**
   * Wraps Core's {@code send_call_update(accId, callId, jsonPayload)} RPC. Until Core's PR lands
   * and {@code scripts/update-rpc-bindings.sh} regenerates {@code Rpc.java}, this method is a no-op
   * and recovery will fall back to failure → teardown. Once the generated method is available,
   * replace the body with:
   *
   * <pre>{@code
   * rpc.sendCallUpdate(accId, callId, jsonPayload);
   * return true;
   * }</pre>
   */
  private boolean sendCallUpdateViaCore(int accId, int callId, String jsonPayload) {
    Log.d(
        TAG,
        "sendCallUpdateViaCore: not yet wired (accId="
            + accId
            + ", callId="
            + callId
            + "); awaiting Core PR + regenerated bindings");
    return false;
  }

  // Network-triggered recovery (CallService)

  /**
   * Called by the call-scoped network callback when the active network has changed to a different
   * {@link Network}. Triggers an immediate recovery attempt by the original caller (the peer is
   * responsible for answering). The callee waits for the offer.
   */
  public void onNetworkHandoffDetected() {
    Log.d(TAG, "onNetworkHandoffDetected");
    beginRestartAttempt("network-handoff");
  }

  /**
   * Called by the call-scoped network callback when the active network has been continuously
   * observed as lost past the grace window. Triggers an immediate recovery attempt by the original
   * caller; the callee waits.
   */
  public void onNetworkLostDetected() {
    Log.d(TAG, "onNetworkLostDetected");
    beginRestartAttempt("network-lost");
  }

  private void beginRestartAttempt(String reason) {
    final boolean shouldRun;
    final boolean isCaller;
    synchronized (this) {
      if (!hasActiveCall() || activeAccId == null || activeCallId == null) {
        return;
      }
      if (recoveryStateSync != RecoveryState.IDLE) {
        Log.d(TAG, "beginRestartAttempt: recovery already in flight, ignoring (" + reason + ")");
        return;
      }
      if (restartAttempts >= RECONNECT_MAX_ATTEMPTS) {
        Log.w(TAG, "beginRestartAttempt: budget exhausted, tearing down (" + reason + ")");
        // Schedule the teardown off the monitor so we don't deadlock on ourselves.
        shouldRun = false;
        isCaller = false;
      } else {
        shouldRun = true;
        isCaller = !isIncomingCall;
      }
    }
    if (!shouldRun) {
      mainHandler.post(() -> handleConnectionEnded(PeerConnection.PeerConnectionState.FAILED));
      return;
    }

    restartAttempts++;
    currentRestartId = UUID.randomUUID().toString();
    currentCallGenerationAtRestartStart = callGeneration;
    awaitingAnswer = false;
    setRecoveryState(RecoveryState.RECONNECTING);
    Log.d(
        TAG,
        "beginRestartAttempt: starting attempt "
            + restartAttempts
            + "/"
            + RECONNECT_MAX_ATTEMPTS
            + " (reason="
            + reason
            + ", caller="
            + isCaller
            + ", id="
            + currentRestartId
            + ")");

    if (!isCaller) {
      // Callee role: surface RECONNECTING in the UI but wait for the peer's restart offer
      // to arrive via onCallUpdateReceived. The offer's `id` becomes our currentRestartId so
      // we never accidentally apply a stale answer from a different transaction. The
      // restartAttempts counter is bumped above so this callee-initiated wait counts against
      // our budget too: if the caller never sends an offer, we'll fall through to teardown
      // via handleConnectionEnded() on the next FAILED observation or DISCONNECTED timeout.
      Log.d(TAG, "Callee role: awaiting caller's restart offer");
      return;
    }

    if (callService != null) {
      callService.requestIceRestart();
    } else {
      Log.w(TAG, "beginRestartAttempt: callService not available, failing attempt");
      completeRestartAttemptWithFailure("service-not-ready");
    }
  }

  private void scheduleAnswerTimeout(String id) {
    if (restartAnswerTimeoutRunnable != null) {
      mainHandler.removeCallbacks(restartAnswerTimeoutRunnable);
    }
    restartAnswerTimeoutRunnable =
        () -> {
          synchronized (CallCoordinator.this) {
            if (!id.equals(currentRestartId)) {
              return;
            }
            if (recoveryStateSync != RecoveryState.RECONNECTING) {
              return;
            }
            if (!awaitingAnswer) {
              return;
            }
          }
          Log.w(TAG, "ICE restart answer timed out for id=" + id);
          completeRestartAttemptWithFailure("answer-timeout");
        };
    mainHandler.postDelayed(restartAnswerTimeoutRunnable, RECONNECT_ANSWER_TIMEOUT_MS);
  }

  private void cancelRecoveryTimers() {
    if (restartGraceRunnable != null) {
      mainHandler.removeCallbacks(restartGraceRunnable);
      restartGraceRunnable = null;
    }
    if (restartAnswerTimeoutRunnable != null) {
      mainHandler.removeCallbacks(restartAnswerTimeoutRunnable);
      restartAnswerTimeoutRunnable = null;
    }
  }

  private void setRecoveryState(RecoveryState newState) {
    recoveryStateSync = newState;
    if (Looper.myLooper() == Looper.getMainLooper()) {
      recoveryState.setValue(newState);
    } else {
      recoveryState.postValue(newState);
    }
  }

  /**
   * Called when a recovery attempt definitively fails (timeout, RPC unavailable, malformed remote
   * payload, or budget exhausted). Runs the standard teardown path exactly once via {@link
   * #handleConnectionEnded(PeerConnection.PeerConnectionState)}.
   */
  private void completeRestartAttemptWithFailure(String reason) {
    Log.w(TAG, "completeRestartAttemptWithFailure: " + reason);
    synchronized (this) {
      cancelRecoveryTimers();
      awaitingAnswer = false;
      if (recoveryStateSync == RecoveryState.IDLE) {
        return;
      }
      setRecoveryState(RecoveryState.IDLE);
    }
    mainHandler.post(
        () -> {
          if (hasActiveCall()) {
            handleConnectionEnded(PeerConnection.PeerConnectionState.FAILED);
          }
        });
  }

  // Remote negotiation dispatch (Core → coordinator → CallService)

  /**
   * Called from the Core event dispatch when {@code DC_EVENT_CALL_UPDATE_RECEIVED} fires. Parses
   * the opaque JSON payload, validates the transaction {@code id}, and routes to the appropriate
   * {@code CallService} entry point.
   */
  private void onCallUpdateReceived(int accId, int callId, String payload) {
    Log.d(TAG, "onCallUpdateReceived: accId=" + accId + ", callId=" + callId);
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse(payload);
    if (!parsed.valid) {
      Log.w(TAG, "onCallUpdateReceived: malformed/empty payload");
      return;
    }

    synchronized (this) {
      if (!hasActiveCall()
          || activeAccId == null
          || !activeAccId.equals(accId)
          || activeCallId == null
          || !activeCallId.equals(callId)) {
        Log.d(TAG, "onCallUpdateReceived: ignoring - not the active call");
        return;
      }
      if (callGeneration != currentCallGenerationAtRestartStart
          && currentCallGenerationAtRestartStart != 0
          && recoveryStateSync == RecoveryState.IDLE) {
        Log.d(TAG, "onCallUpdateReceived: stale generation, ignoring");
        return;
      }
    }

    if (parsed.isOffer()) {
      handleRemoteRestartOffer(parsed.id, parsed.sdp);
    } else if (parsed.isAnswer()) {
      handleRemoteRestartAnswer(parsed.id, parsed.sdp);
    } else {
      Log.w(TAG, "onCallUpdateReceived: unknown type " + parsed.type);
    }
  }

  private void handleRemoteRestartOffer(String id, String sdp) {
    synchronized (this) {
      if (recoveryStateSync == RecoveryState.IDLE) {
        // Callee path: an unexpected offer arrived with no in-flight recovery. Begin one
        // and surface RECONNECTING in the UI so the user sees the recovery banner.
        currentRestartId = id;
        currentCallGenerationAtRestartStart = callGeneration;
        restartAttempts++;
        setRecoveryState(RecoveryState.RECONNECTING);
      } else if (!id.equals(currentRestartId)) {
        Log.w(TAG, "handleRemoteRestartOffer: id mismatch, ignoring");
        return;
      }
    }
    Log.d(TAG, "handleRemoteRestartOffer: routing to callService");
    if (callService != null) {
      callService.acceptRemoteRestartOffer(sdp);
    } else {
      Log.w(TAG, "handleRemoteRestartOffer: callService not available");
      completeRestartAttemptWithFailure("service-not-ready");
    }
  }

  private void handleRemoteRestartAnswer(String id, String sdp) {
    synchronized (this) {
      if (recoveryStateSync != RecoveryState.RECONNECTING) {
        Log.d(TAG, "handleRemoteRestartAnswer: not in recovery, dropping");
        return;
      }
      if (!id.equals(currentRestartId)) {
        Log.w(TAG, "handleRemoteRestartAnswer: id mismatch, ignoring");
        return;
      }
      awaitingAnswer = false;
    }
    cancelRecoveryTimers();
    if (callService != null) {
      callService.applyRemoteRestartAnswer(sdp);
    } else {
      Log.w(TAG, "handleRemoteRestartAnswer: callService not available");
      completeRestartAttemptWithFailure("service-not-ready");
    }
  }

  // Call Control Methods (CallViewModel)

  public synchronized void showIncomingCallScreen(int callId) {
    if (!isIncomingCall) {
      Log.d(TAG, "Not an incoming call");
      return;
    }

    if (activeCallId == null || !activeCallId.equals(callId)) {
      Log.d(TAG, "Call ID mismatch (active: " + activeCallId + ", requested: " + callId + ")");
      return;
    }

    if (answerInProgress || hasOngoingCall()) {
      Log.d(TAG, "Call already being answered or ongoing");
      return;
    }

    // Launch CallActivity
    Intent intent = new Intent(appContext, CallActivity.class);
    intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_SINGLE_TOP);
    appContext.startActivity(intent);

    Log.d(TAG, "Showing incoming call screen: " + callId);
  }

  public synchronized void endCall(boolean fromTelecom) {
    Log.d(TAG, "endCall: fromTelecom=" + fromTelecom);

    if (!hasActiveCall()) {
      Log.w(TAG, "No active call to end");
      return;
    }

    // Explicit end: cancel any in-flight recovery attempt.
    if (recoveryStateSync != RecoveryState.IDLE) {
      Log.d(TAG, "endCall: cancelling in-flight ICE restart recovery");
      cancelRecoveryTimers();
      setRecoveryState(RecoveryState.IDLE);
      awaitingAnswer = false;
      currentRestartId = null;
      restartAttempts = 0;
    }

    if (callService != null) {
      callService.stopRingtone();
    }

    notifyBackendCallEnded();

    if (!fromTelecom) {
      DisconnectCause cause =
          isIncomingCall && !answerInProgress
              ? new DisconnectCause(DisconnectCause.REJECTED)
              : new DisconnectCause(DisconnectCause.LOCAL);
      disconnectTelecom(cause);
    }

    // End call on service
    if (callService != null) {
      callService.endCall();
    }

    // Cleanup
    cleanupCall(activeAccId, activeCallId);
  }

  public synchronized void setAudioEnabled(boolean enabled) {
    Log.d(TAG, "setAudioEnabled: " + enabled);

    localAudioEnabled.postValue(enabled);

    if (callService != null) {
      callService.setAudioEnabled(enabled);

      callService.sendMutedState(enabled, Boolean.TRUE.equals(localVideoEnabled.getValue()));
    }
  }

  public synchronized void setVideoEnabled(boolean enabled) {
    Log.d(TAG, "setVideoEnabled: " + enabled);

    if (callService != null) {
      boolean success = callService.setVideoEnabled(enabled);
      if (!success && enabled) {
        enabled = false;
        reportError("Camera unavailable");
      }
    }

    localVideoEnabled.postValue(enabled);

    if (callService != null) {
      callService.sendMutedState(Boolean.TRUE.equals(localAudioEnabled.getValue()), enabled);
    }
  }

  public synchronized void switchCamera() {
    Log.d(TAG, "switchCamera");
    if (callService != null) {
      callService.switchCamera();
    }
  }

  public synchronized void startOutgoingCall() {
    Log.d(TAG, "startOutgoingCall");
    if (callService != null) {
      callService.startOutgoingCall();
    }
  }

  // Setup Audio Endpoint Flow Collection

  private CallEndpointCompat getPreferredStartingEndpoint(boolean startsWithVideo) {
    try {
      Flow<List<CallEndpointCompat>> endpointsFlow =
          callsManager.getAvailableStartingCallEndpoints();

      // Block and get first emission from Flow
      List<CallEndpointCompat> endpoints =
          BuildersKt.runBlocking(
              EmptyCoroutineContext.INSTANCE,
              (scope, continuation) -> FlowKt.first(endpointsFlow, continuation));

      CallEndpointCompat preferred = findPreferredEndpoint(endpoints, startsWithVideo);
      if (preferred != null) {
        Log.d(TAG, "Preferred endpoint: " + preferred.getName() + ", type=" + preferred.getType());
      }

      return preferred;

    } catch (Exception e) {
      Log.e(TAG, "Failed to get preferred starting endpoint", e);
      return null;
    }
  }

  private synchronized void setupAudioEndpointCollection(CallControlScope scope) {
    Log.d(TAG, "Setting up audio endpoint flow collection");

    // Create CoroutineScope for Flow collection
    audioFlowScope = CoroutineScopeKt.CoroutineScope(Dispatchers.getMain());

    availableAudioEndpointsSource =
        FlowLiveDataConversions.asLiveData(
            scope.getAvailableEndpoints(), audioFlowScope.getCoroutineContext(), 5000L);

    availableAudioEndpoints.addSource(
        availableAudioEndpointsSource,
        value -> {
          Log.d(
              TAG,
              "Available audio endpoints changed, count: " + (value != null ? value.size() : 0));
          availableAudioEndpoints.setValue(value);

          // Turns out the system Bluetooth/AudioService will trigger a second audio change
          // And Telecom tries to prevent it but doesn't always success, so a delayed
          // switch is still needed as a backup
          if (!hasAutoSelectedEndpoint) {
            hasAutoSelectedEndpoint = true;

            if (value != null && !value.isEmpty()) {
              final CallEndpointCompat finalPreferred =
                  findPreferredEndpoint(value, startsWithVideo);

              mainHandler.postDelayed(
                  () -> {
                    if (finalPreferred != null) {
                      Log.d(TAG, "Auto-selecting endpoint: " + finalPreferred.getName());
                      requestAudioEndpointChange(finalPreferred);
                    } else {
                      Log.d(TAG, "No preferred endpoint found");
                    }

                    // Only start collecting current audio endpoint after we made the selection
                    // The delay is to avoid a Telecom problem where at the beginning it will switch
                    // endpoints rapidly
                    startCurrentEndpointCollection(scope);
                  },
                  1000);
            }
          } else {
            mainHandler.postDelayed(() -> startCurrentEndpointCollection(scope), 500);
          }
        });
  }

  private synchronized void startCurrentEndpointCollection(CallControlScope scope) {
    if (currentAudioEndpointSource != null) {
      return;
    }

    if (audioFlowScope == null) {
      // Likely in cleaning
      return;
    }

    Log.d(TAG, "Starting current endpoint flow collection");

    // Only add source if not already started
    currentAudioEndpointSource =
        FlowLiveDataConversions.asLiveData(
            scope.getCurrentCallEndpoint(), audioFlowScope.getCoroutineContext(), 5000L);

    currentAudioEndpoint.addSource(
        currentAudioEndpointSource,
        value1 -> {
          Log.d(
              TAG,
              "Current audio endpoint changed: " + (value1 != null ? value1.getName() : "null"));
          currentAudioEndpoint.setValue(value1);
        });
  }

  // Request Audio Endpoint Change
  public synchronized void requestAudioEndpointChange(CallEndpointCompat endpoint) {
    Log.d(TAG, "Requesting audio endpoint change to: " + endpoint.getName());

    if (activeCallControlScope == null) {
      Log.w(TAG, "No active call scope, cannot change endpoint");
      return;
    }

    activeCallControlScope.requestEndpointChange(
        endpoint,
        new Continuation<CallControlResult>() {
          @NonNull
          @Override
          public CoroutineContext getContext() {
            return EmptyCoroutineContext.INSTANCE;
          }

          @Override
          public void resumeWith(@NonNull Object result) {
            if (result instanceof CallControlResult.Success) {
              Log.d(TAG, "Audio endpoint change succeeded");
            } else if (result instanceof CallControlResult.Error) {
              int errorCode = ((CallControlResult.Error) result).getErrorCode();
              Log.e(TAG, "Audio endpoint change failed with error code: " + errorCode);
              reportError("Failed to change audio device");
            }
          }
        });
  }

  // Helper (CallService)

  public synchronized String fetchIceServers() throws RpcException {
    return rpc.iceServers(activeAccId);
  }

  @Override
  public void handleEvent(@NonNull DcEvent event) {
    int eventId = event.getId();
    int accId = event.getAccountId();
    int callId = event.getData1Int(); // This is not semantically correct but shall work fine

    // It seems this observer can fire on either main or background thread
    // Always move to background
    eventExecutor.execute(
        () -> {
          boolean hasVideo;

          switch (eventId) {
            case DcContext.DC_EVENT_INCOMING_CALL:
              try {
                hasVideo = this.rpc.callInfo(accId, callId).hasVideo;
              } catch (RpcException e) {
                Log.e(TAG, "Rpc.callInfo() failed", e);
                hasVideo = false;
              }
              onIncomingCall(accId, callId, event.getData2Str(), hasVideo);
              break;
            case DcContext.DC_EVENT_INCOMING_CALL_ACCEPTED:
              boolean fromThisDevice = event.getData2Int() != 0; // Data2 is from_this_device
              onIncomingCallAccepted(callId, fromThisDevice);
              break;
            case DcContext.DC_EVENT_OUTGOING_CALL_ACCEPTED:
              String answerSDP = event.getData2Str();
              onOutgoingCallAccepted(callId, answerSDP);
              break;
            case DcContext.DC_EVENT_CALL_ENDED:
              // This event is problematic because it can trigger in both directions,
              // in addition to multiple other scenarios which cannot easily be distinguished
              // May cause problems in edge cases
              onCallEnded(accId, callId);
              break;
            case DcContext.DC_EVENT_CALL_UPDATE_RECEIVED:
              String payload = event.getData2Str();
              onCallUpdateReceived(accId, callId, payload);
              break;
          }
        });
  }

  private synchronized void onIncomingCall(
      int accId, int callId, String offerSdp, boolean startsWithVideo) {
    Log.d(TAG, "onIncomingCall: accId=" + accId + ", callId=" + callId);

    Pair<DcChat, String> result = setupIncomingCallState(accId, callId, offerSdp, startsWithVideo);
    if (result == null) return;

    DcChat dcChat = result.first;
    String callerName = result.second;
    Icon callerIcon = getIconFromChat(this.appContext, dcChat);

    displayIcon.postValue(callerIcon);

    // Add to CallsManager
    CallAttributesCompat callAttributes = createCallAttributes(callerName, callId, true);
    addCallToTelecom(callAttributes);

    // Show CallStyle notification
    showIncomingCallNotification(callerName, callerIcon);

    startAndBindService();
  }

  public synchronized void handleIncomingCallFromConversation(
      int accId, int callId, String offerSdp, boolean hasVideo) {
    Log.d(TAG, "handleIncomingCallFromConversation: accId=" + accId + ", callId=" + callId);

    if (offerSdp == null || offerSdp.isEmpty()) {
      Log.e(TAG, "Cannot start incoming call: no SDP offer");
      return;
    }

    Pair<DcChat, String> result = setupIncomingCallState(accId, callId, offerSdp, hasVideo);
    if (result == null) return;

    DcChat dcChat = result.first;
    String callerName = result.second;

    new Thread(
            () -> {
              Icon callerIcon = getIconFromChat(this.appContext, dcChat);
              displayIcon.postValue(callerIcon);
            })
        .start();

    // Add to CallsManager
    CallAttributesCompat callAttributes = createCallAttributes(callerName, callId, true);
    addCallToTelecom(callAttributes);

    startAndBindService();

    launchCallActivity();
  }

  private synchronized void onIncomingCallAccepted(int callId, boolean fromThisDevice) {
    Log.d(TAG, "onIncomingCallAccepted: callId=" + callId + ", fromThisDevice=" + fromThisDevice);

    if (!fromThisDevice) {
      onCallAnsweredOnOtherDevice();
      return;
    }

    if (activeCallId == null || !activeCallId.equals(callId)) {
      Log.w(TAG, "Accepted call ID doesn't match active call");
      return;
    }

    String callerName = displayName.getValue();
    if (callerName == null) {
      callerName = "Unknown";
    }

    showOrUpdateOngoingNotification(appContext.getString(R.string.call_with, callerName));
  }

  private synchronized void onCallAnsweredOnOtherDevice() {
    Log.d(TAG, "Call was answered on another device");

    if (!hasActiveCall()) {
      Log.d(TAG, "No active call, ignoring");
      return;
    }

    // Prevent notifyBackendCallEnded() from firing during WebRTC teardown.
    // The call is still active on the other device.
    hasNotifiedBackend = true;

    if (callService != null) {
      callService.stopRingtone();
    }

    notificationManager.cancel(NOTIFICATION_ID_CALL);

    answeredElsewhere.postValue(true);

    disconnectTelecom(new DisconnectCause(DisconnectCause.REMOTE));

    if (callService != null) {
      callService.endCall();
    }

    cleanupCall(activeAccId, activeCallId);
  }

  private void onOutgoingCallAccepted(int callId, String answerSdp) {
    Log.d(TAG, "onOutgoingCallAccepted: callId=" + callId + ", got answer SDP");

    if (activeCallId == null || !activeCallId.equals(callId)) {
      Log.w(TAG, "Answered call ID doesn't match active call");
      return;
    }

    if (callService != null) {
      callService.stopRingtone();
      callService.handleAnswerSdp(answerSdp);
    }

    // Call control scope should transition to ACTIVE
    if (activeCallControlScope != null) {
      activeCallControlScope.setActive(
          new Continuation<CallControlResult>() {
            @NonNull
            @Override
            public CoroutineContext getContext() {
              return EmptyCoroutineContext.INSTANCE;
            }

            @Override
            public void resumeWith(@NonNull Object result) {
              if (result instanceof CallControlResult) {
                Log.d(TAG, "Outgoing call set to active: " + result);
              } else if (result instanceof kotlin.Result.Failure) {
                Log.e(TAG, "Failed to set active", ((kotlin.Result.Failure) result).exception);
              }
            }
          });
    }

    String calleeName = displayName.getValue();
    if (calleeName == null) {
      calleeName = "Unknown";
    }
    showOrUpdateOngoingNotification(appContext.getString(R.string.call_with, calleeName));
  }

  private void onCallEnded(int accId, int callId) {
    final boolean shouldNotifyMissed;
    final int notifyAcc, notifyChat, notifyCall;

    synchronized (this) {
      if (!hasActiveCall()) {
        Log.w(TAG, "No active call, ignoring");
        return;
      }

      if (!activeAccId.equals(accId) || !activeCallId.equals(callId)) {
        Log.w(
            TAG,
            "Event IDs don't match active call "
                + "(active: accId="
                + activeAccId
                + " callId="
                + activeCallId
                + ", event: accId="
                + accId
                + " callId="
                + callId
                + "), ignoring");
        return;
      }

      if (callService != null) {
        callService.stopRingtone();
      }

      // Cancel recovery before disconnecting so we don't leave an in-flight restart timer
      // firing against an already-ended call.
      if (recoveryStateSync != RecoveryState.IDLE) {
        cancelRecoveryTimers();
        setRecoveryState(RecoveryState.IDLE);
        awaitingAnswer = false;
        currentRestartId = null;
      }

      disconnectTelecom(new DisconnectCause(DisconnectCause.REMOTE));

      if (callService != null) {
        callService.endCall();
      }

      shouldNotifyMissed = isIncomingCall && !answerInProgress;
      notifyAcc = activeAccId;
      notifyChat = activeChatId;
      notifyCall = activeCallId;

      // Clear active states
      cleanupCall(accId, callId);
    }

    if (shouldNotifyMissed) {
      Util.runOnBackground(
          () ->
              DcHelper.getNotificationCenter(appContext)
                  .notifyMessage(notifyAcc, notifyChat, notifyCall));
    }
  }

  private synchronized void handleConnectionEnded(PeerConnection.PeerConnectionState state) {
    Log.d(TAG, "handleConnectionEnded: " + state);

    if (activeCallId == null) {
      Log.w(TAG, "Call already ended or no active call");
      return;
    }

    // peerConnection.close() inside CallService.disposeWebRTC() synchronously triggers a second
    // onConnectionChange(CLOSED) on the WebRTC signaling thread, which posts back here via
    // the main Handler. Block that reentrant entry so the disconnect+cleanup sequence runs
    // exactly once even when CLOSED is reported more than once.
    if (!teardownInProgress.compareAndSet(false, true)) {
      Log.d(TAG, "Teardown already in progress, ignoring reentrant connection end");
      return;
    }

    if (callService != null) {
      callService.stopRingtone();
    }

    notifyBackendCallEnded();

    DisconnectCause cause;
    if (state == PeerConnection.PeerConnectionState.FAILED) {
      cause = new DisconnectCause(DisconnectCause.REMOTE, "PeerConnection failed");
    } else {
      cause = new DisconnectCause(DisconnectCause.LOCAL, "PeerConnection closed");
    }
    disconnectTelecom(cause);

    if (callService != null) {
      callService.endCall();
    }

    // Cleanup
    if (hasActiveCall()) {
      cleanupCall(activeAccId, activeCallId);
    }

    teardownInProgress.set(false);
  }

  /** Cleanup call state, used for clean up not initialized from backend events */
  public synchronized void cleanupCall(int accId, int callId) {
    Log.d(TAG, "cleanupCall: accId=" + accId + ", callId=" + callId);

    if (!hasActiveCall()) {
      Log.d(TAG, "No active call to clean up");
      return;
    }

    if (!activeAccId.equals(accId) || !activeCallId.equals(callId)) {
      Log.w(TAG, "Cleanup IDs don't match active call, aborting");
      return;
    }

    // Clear state. Note: activeCallControlScope is intentionally left alone here - callers
    // that need to actually terminate the Telecom registration must call disconnectTelecom()
    // BEFORE cleanupCall(), so that the scope is still valid for the disconnect attempt.
    this.activeAccId = null;
    this.activeCallId = null;
    this.activeChatId = null;
    this.activeCallViewModel = null;
    this.preferredStartingEndpoint = null;
    this.isIncomingCall = false;
    this.startsWithVideo = false;
    this.pendingOfferSdp = null;
    this.hasNotifiedBackend = false;
    this.hasAutoSelectedEndpoint = false;
    this.pendingMediaCapture = false;
    this.answerInProgress = false;
    this.restartAttempts = 0;
    this.currentRestartId = null;
    this.awaitingAnswer = false;
    cancelRecoveryTimers();
    if (recoveryStateSync != RecoveryState.IDLE) {
      setRecoveryState(RecoveryState.IDLE);
    }

    if (callService != null) {
      try {
        callService.stopForegroundAndDismiss();
      } catch (Exception e) {
        Log.w(TAG, "stopForegroundAndDismiss failed", e);
      }
    }

    // Drop the scope last so that any reentrant disconnectTelecom() racing the cleanup can
    // still observe a non-null scope during this invocation.
    this.activeCallControlScope = null;

    mainHandler.removeCallbacks(outgoingRingtoneRunnable);

    mainHandler.post(
        () -> {
          if (answerMediaObserver != null) {
            getMediaCaptureReady().removeObserver(answerMediaObserver);
            answerMediaObserver = null;
            Log.d(TAG, "Removed answer media observer");
          }

          if (currentAudioEndpointSource != null) {
            currentAudioEndpoint.removeSource(currentAudioEndpointSource);
            currentAudioEndpointSource = null;
            Log.d(TAG, "Removed current audio endpoint source");
          }

          if (availableAudioEndpointsSource != null) {
            availableAudioEndpoints.removeSource(availableAudioEndpointsSource);
            availableAudioEndpointsSource = null;
            Log.d(TAG, "Removed available audio endpoints source");
          }
        });

    // Clear LiveData
    connectionState.postValue(PeerConnection.PeerConnectionState.CLOSED);
    clearLiveData();

    if (audioFlowScope != null) {
      CoroutineScopeKt.cancel(audioFlowScope, null);
      audioFlowScope = null;
      Log.d(TAG, "Cancelled audio flow scope");
    }

    // Clear notifications
    notificationManager.cancel(NOTIFICATION_ID_CALL);

    // Stop service
    stopAndUnbindService();

    Log.d(TAG, "Call cleanup complete");
  }

  private synchronized void notifyBackendCallEnded() {
    if (hasNotifiedBackend) {
      Log.d(TAG, "Backend already notified of call end");
      return;
    }

    if (activeAccId == null || activeCallId == null || activeCallId < 0) {
      Log.w(TAG, "Cannot notify backend, invalid callId");
      return;
    }

    hasNotifiedBackend = true;

    new Thread(
            () -> {
              try {
                rpc.endCall(activeAccId, activeCallId);
                Log.d(TAG, "Backend notified: call ended");
              } catch (RpcException e) {
                Log.e(TAG, "Failed to notify backend of call end", e);
              }
            })
        .start();
  }

  private void resetLiveDataForNewCall() {
    connectionState.postValue(PeerConnection.PeerConnectionState.NEW);
    answeredElsewhere.postValue(false); // clearLiveData() must not reset answeredElsewhere
    mediaCaptureReady.postValue(false);
    recoveryState.postValue(RecoveryState.IDLE);
    clearLiveData();
  }

  private void clearLiveData() {
    localVideoTrack.postValue(null);
    remoteVideoTrack.postValue(null);
    localAudioEnabled.postValue(true);
    localVideoEnabled.postValue(true);
    remoteAudioEnabled.postValue(true);
    remoteVideoEnabled.postValue(true);
    isRelayUsed.postValue(false);
    errorMessage.postValue(null);
    outgoingCallPlaced.postValue(false);
    currentAudioEndpoint.postValue(null);
    availableAudioEndpoints.postValue(null);
    mediaCaptureReady.postValue(false);
  }

  public synchronized void initiateOutgoingCall(int accId, int chatId, boolean startsWithVideo) {
    Log.d(TAG, "Initiating outgoing call:accId=" + accId + ", chatId=" + chatId);

    if (hasActiveCall()) {
      Log.w(TAG, "Already have an active call, cannot start new one");
      return;
    }

    resetLiveDataForNewCall();

    callGeneration++;
    this.activeCallId = -1; // Placeholder call ID for Intent
    this.activeAccId = accId;
    this.activeChatId = chatId;
    this.isIncomingCall = false;
    this.startsWithVideo = startsWithVideo;
    this.pendingOfferSdp = null;

    // Get callee info
    DcContext dcContext = ApplicationContext.getDcAccounts().getAccount(accId);
    DcChat dcChat = dcContext.getChat(chatId);
    String calleeName = getNameFromChat(dcChat);
    displayName.postValue(calleeName);

    // This is fine because icon is UI only and not used elsewhere
    new Thread(
            () -> {
              Icon calleeIcon = getIconFromChat(this.appContext, dcChat);
              displayIcon.postValue(calleeIcon);
            })
        .start();

    this.preferredStartingEndpoint = getPreferredStartingEndpoint(startsWithVideo);

    if (hasMicrophonePermission()) {
      startAndBindService();
    }

    launchCallActivity();
  }

  public synchronized void completeOutgoingCall(int accId, int callId, int chatId) {
    Log.d(TAG, "Completing outgoing call with accId=" + accId + ", callId=" + callId);

    if (activeAccId == null || activeCallId == null || activeChatId == null) {
      Log.w(TAG, "No active call, cannot complete setting up outgoing call");
      return;
    }

    if (!activeChatId.equals(chatId) || !activeAccId.equals(accId)) {
      Log.w(TAG, "Cannot complete outgoing call,mismatch in call parameters");
      return;
    }

    this.activeCallId = callId;
    outgoingCallPlaced.postValue(true);

    // Get callee info
    String calleeName = displayName.getValue();

    // Create call attributes
    CallAttributesCompat callAttributes = createCallAttributes(calleeName, activeCallId, false);

    // Add call to CallsManager
    addCallToTelecom(callAttributes);
  }

  @Nullable
  private Pair<DcChat, String> setupIncomingCallState(
      int accId, int callId, String offerSdp, boolean startsWithVideo) {
    if (hasActiveCall()) {
      Log.w(TAG, "Already have an active call, ignoring incoming call");
      return null;
    }

    resetLiveDataForNewCall();

    callGeneration++;
    this.activeAccId = accId;
    this.activeCallId = callId;
    this.isIncomingCall = true;
    this.startsWithVideo = startsWithVideo;
    this.pendingOfferSdp = offerSdp;

    DcContext dcContext = ApplicationContext.getDcAccounts().getAccount(accId);
    int chatId = dcContext.getMsg(callId).getChatId();
    this.activeChatId = chatId;
    DcChat dcChat = dcContext.getChat(chatId);
    String callerName = getNameFromChat(dcChat);

    displayName.postValue(callerName);

    this.preferredStartingEndpoint = getPreferredStartingEndpoint(startsWithVideo);

    return new Pair<>(dcChat, callerName);
  }

  public synchronized void ensureServiceStarted() {
    if (isServiceBound || !hasActiveCall()) {
      return;
    }
    Log.d(TAG, "Starting service after permission grant");
    startAndBindService();
  }

  private void addCallToTelecom(CallAttributesCompat callAttributes) {
    final int thisGeneration = callGeneration;
    try {
      callsManager.addCall(
          callAttributes,
          // These callbacks are only invoked by remote surfaces (BT headset, watch, Android
          // Auto)
          // onAnswer
          (callType, continuation) -> {
            Log.d(TAG, "CallControlScope: onAnswer with type: " + callType);
            if (isIncomingCall) {
              answerCall(true);
            }
            return Unit.INSTANCE;
          },
          // onDisconnect
          (disconnectCause, continuation) -> {
            Log.d(TAG, "CallControlScope: onDisconnect, cause: " + disconnectCause);
            endCall(true);
            return Unit.INSTANCE;
          },
          // onSetActive
          continuation -> {
            Log.d(TAG, "CallControlScope: onSetActive");
            return Unit.INSTANCE;
          },
          // onSetInactive
          continuation -> {
            Log.d(TAG, "CallControlScope: onSetInactive");
            return Unit.INSTANCE;
          },
          // CallControlScope lambda
          scope -> {
            synchronized (CallCoordinator.this) {
              if (thisGeneration != callGeneration || !hasActiveCall()) {
                scope.disconnect(
                    new DisconnectCause(DisconnectCause.LOCAL),
                    new Continuation<CallControlResult>() {
                      @NonNull
                      @Override
                      public CoroutineContext getContext() {
                        return EmptyCoroutineContext.INSTANCE;
                      }

                      @Override
                      public void resumeWith(@NonNull Object result) {
                        Log.d(TAG, "Orphaned scope disconnected: " + result);
                      }
                    });
                return Unit.INSTANCE;
              }

              Log.d(TAG, "CallControlScope initialized");
              activeCallControlScope = scope;
            }

            mainHandler.post(() -> setupAudioEndpointCollection(scope));

            return Unit.INSTANCE;
          },
          new Continuation<Unit>() {
            @NonNull
            @Override
            public CoroutineContext getContext() {
              return EmptyCoroutineContext.INSTANCE;
            }

            @Override
            public void resumeWith(@NonNull Object result) {
              Log.d(TAG, "addCall completed");
            }
          });
    } catch (CallException e) {
      Log.e(TAG, "Failed to add call to Telecom", e);
    }
  }

  private CallAttributesCompat createCallAttributes(
      String callerName, int callId, boolean isIncomingCall) {
    Uri addressUri = Uri.parse(CALL_IDENTIFIER_SCHEME + callId);

    return new CallAttributesCompat(
        callerName,
        addressUri,
        isIncomingCall
            ? CallAttributesCompat.DIRECTION_INCOMING
            : CallAttributesCompat.DIRECTION_OUTGOING,
        CallAttributesCompat.CALL_TYPE_VIDEO_CALL,
        CallAttributesCompat.SUPPORTS_SET_INACTIVE,
        this.preferredStartingEndpoint);
  }

  private void showIncomingCallNotification(String callerName, Icon callerIcon) {
    // Check notification permission
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
      if (!hasNotificationPermission()) {
        Log.w(TAG, "POST_NOTIFICATIONS permission not granted, cannot show notification");
        return;
      }
    }

    // Check full screen intent permission on Android 14+
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
      if (!canUseFullScreenIntent()) {
        Log.w(TAG, "Full screen intent permission not granted, notification will appear normally");
      }
    }

    // Answer intent
    Intent answerIntent = new Intent(this.appContext, CallActivity.class);
    answerIntent.setAction(CallActivity.ACTION_ANSWER_CALL);
    answerIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_SINGLE_TOP);

    PendingIntent answerPendingIntent =
        PendingIntent.getActivity(
            this.appContext,
            PI_ANSWER,
            answerIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

    // Decline intent
    Intent declineIntent = new Intent(this.appContext, CallActionReceiver.class);
    declineIntent.setAction(CallActivity.ACTION_DECLINE_CALL);

    PendingIntent declinePendingIntent =
        PendingIntent.getBroadcast(
            this.appContext,
            PI_DECLINE,
            declineIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

    // Full screen intent
    Intent fullScreenIntent = new Intent(this.appContext, CallActivity.class);
    fullScreenIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);

    PendingIntent fullScreenPendingIntent =
        PendingIntent.getActivity(
            this.appContext,
            PI_FULLSCREEN,
            fullScreenIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

    Notification.Builder builder;
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
      // Android 12+, CallStyle
      Person caller =
          new Person.Builder().setName(callerName).setIcon(callerIcon).setImportant(true).build();

      builder =
          new Notification.Builder(this.appContext, CHANNEL_ID_INCOMING)
              .setSmallIcon(R.drawable.icon_notification)
              .setContentTitle("Incoming call")
              .setContentText("Incoming call from " + callerName)
              .setStyle(
                  Notification.CallStyle.forIncomingCall(
                          caller, declinePendingIntent, answerPendingIntent)
                      .setIsVideo(startsWithVideo))
              .addPerson(caller)
              .setFullScreenIntent(fullScreenPendingIntent, true)
              .setOngoing(true)
              .setCategory(Notification.CATEGORY_CALL);
    } else {
      // Android 8-12: Notification with actions
      builder =
          new Notification.Builder(this.appContext, CHANNEL_ID_INCOMING)
              .setSmallIcon(R.drawable.icon_notification)
              .setContentTitle("Incoming call")
              .setContentText("Incoming call from " + callerName)
              .setFullScreenIntent(fullScreenPendingIntent, true)
              .setOngoing(true)
              .setColorized(true)
              .setCategory(Notification.CATEGORY_CALL)
              .addAction(
                  new Notification.Action.Builder(
                          Icon.createWithResource(this.appContext, R.drawable.baseline_call_end_24),
                          "Decline",
                          declinePendingIntent)
                      .build())
              .addAction(
                  new Notification.Action.Builder(
                          Icon.createWithResource(this.appContext, R.drawable.baseline_call_24),
                          "Answer",
                          answerPendingIntent)
                      .build());
    }

    Notification notification = builder.build();
    notification.flags |= Notification.FLAG_INSISTENT;
    notificationManager.notify(NOTIFICATION_ID_CALL, notification);
  }

  private Notification buildOngoingCallNotification(
      String statusText, String displayName, Icon icon) {
    Intent activityIntent = new Intent(this.appContext, CallActivity.class);
    activityIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_SINGLE_TOP);

    Intent hangupIntent = new Intent(this.appContext, CallActionReceiver.class);
    hangupIntent.setAction(CallActivity.ACTION_HANGUP_CALL);

    PendingIntent hangupPendingIntent =
        PendingIntent.getBroadcast(
            this.appContext,
            PI_HANGUP,
            hangupIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

    PendingIntent contentIntent =
        PendingIntent.getActivity(
            this.appContext,
            PI_ONGOING_CONTENT,
            activityIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

    Notification.Builder builder;
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
      Person person =
          new Person.Builder().setName(displayName).setIcon(icon).setImportant(true).build();

      builder =
          new Notification.Builder(this.appContext, CHANNEL_ID_ONGOING)
              .setSmallIcon(R.drawable.icon_notification)
              .setContentTitle("Ongoing call")
              .setContentText(statusText)
              .setContentIntent(contentIntent)
              .setStyle(Notification.CallStyle.forOngoingCall(person, hangupPendingIntent))
              .addPerson(person)
              .setOngoing(true)
              .setCategory(Notification.CATEGORY_CALL);
    } else {
      builder =
          new Notification.Builder(this.appContext, CHANNEL_ID_ONGOING)
              .setSmallIcon(R.drawable.icon_notification)
              .setContentTitle("Ongoing call")
              .setContentText(statusText)
              .setContentIntent(contentIntent)
              .setOngoing(true)
              .setColorized(true)
              .setCategory(Notification.CATEGORY_CALL)
              .addAction(
                  new Notification.Action.Builder(
                          Icon.createWithResource(this.appContext, R.drawable.baseline_call_end_24),
                          "Hang up",
                          hangupPendingIntent)
                      .build());
    }

    return builder.build();
  }

  private void showOrUpdateOngoingNotification(String statusText) {
    if (callService == null) {
      Log.w(TAG, "Cannot show notification, service not ready");
      return;
    }

    // Check notification permission
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
      if (!hasNotificationPermission()) {
        Log.w(TAG, "POST_NOTIFICATIONS permission not granted, cannot update notification");
        return;
      }
    }

    String name = displayName.getValue();
    if (name == null) {
      name = "Unknown";
    }

    Icon icon = displayIcon.getValue();

    Notification notification = buildOngoingCallNotification(statusText, name, icon);
    callService.startForegroundWithNotification(NOTIFICATION_ID_CALL, notification);

    Log.d(TAG, "Ongoing notification shown/updated: " + statusText);
  }

  private void launchCallActivity() {
    Intent intent = new Intent(this.appContext, CallActivity.class);
    intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_SINGLE_TOP);
    this.appContext.startActivity(intent);

    Log.d(TAG, "Launching CallActivity, hasActiveViewModel: " + (activeCallViewModel != null));
  }

  public synchronized void setActiveCallViewModel(CallViewModel viewModel) {
    this.activeCallViewModel = viewModel;
  }

  public synchronized void clearActiveCallViewModel() {
    this.activeCallViewModel = null;
  }

  public synchronized boolean hasActiveCall() {
    return activeAccId != null && activeCallId != null;
  }

  public synchronized boolean hasOngoingCall() {
    if (activeCallId == null) return false;

    PeerConnection.PeerConnectionState state = connectionState.getValue();
    return state != null && state != PeerConnection.PeerConnectionState.NEW;
  }

  public synchronized boolean isIncomingCall() {
    return isIncomingCall;
  }

  public synchronized boolean isStartsWithVideo() {
    return startsWithVideo;
  }

  public synchronized boolean isAnswerInProgress() {
    return answerInProgress;
  }

  public synchronized void setStartsWithVideo(boolean startsWithVideo) {
    this.startsWithVideo = startsWithVideo;
  }

  // Permission helpers

  public boolean hasMicrophonePermission() {
    return ContextCompat.checkSelfPermission(appContext, Manifest.permission.RECORD_AUDIO)
        == PackageManager.PERMISSION_GRANTED;
  }

  public boolean hasCameraPermission() {
    return ContextCompat.checkSelfPermission(appContext, Manifest.permission.CAMERA)
        == PackageManager.PERMISSION_GRANTED;
  }

  public boolean hasMediaPermissions() {
    return hasMicrophonePermission() && hasCameraPermission();
  }

  @RequiresApi(api = Build.VERSION_CODES.TIRAMISU)
  public boolean hasNotificationPermission() {
    return ContextCompat.checkSelfPermission(appContext, Manifest.permission.POST_NOTIFICATIONS)
        == PackageManager.PERMISSION_GRANTED;
  }

  @RequiresApi(api = Build.VERSION_CODES.UPSIDE_DOWN_CAKE)
  public boolean canUseFullScreenIntent() {
    NotificationManager notificationManager =
        (NotificationManager) appContext.getSystemService(Context.NOTIFICATION_SERVICE);
    return notificationManager != null && notificationManager.canUseFullScreenIntent();
  }

  private void showPermissionNeededNotification(String callerName) {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
      if (!hasNotificationPermission()) {
        Log.w(TAG, "Cannot show permission-needed notification: no notification permission");
        return;
      }
    }

    Intent activityIntent = new Intent(appContext, CallActivity.class);
    activityIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);

    PendingIntent contentIntent =
        PendingIntent.getActivity(
            appContext,
            PI_FULLSCREEN,
            activityIntent,
            PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

    String title = appContext.getString(R.string.call_with, callerName);
    String text = appContext.getString(R.string.call_grant_mic_permission);

    Notification.Builder builder =
        new Notification.Builder(appContext, CHANNEL_ID_INCOMING)
            .setSmallIcon(R.drawable.icon_notification)
            .setContentTitle(title)
            .setContentText(text)
            .setContentIntent(contentIntent)
            .setFullScreenIntent(contentIntent, true)
            .setOngoing(true)
            .setCategory(Notification.CATEGORY_CALL);

    notificationManager.notify(NOTIFICATION_ID_CALL, builder.build());

    Log.d(TAG, "Showing permission-needed notification");
  }

  public void answerAfterPermissions() {
    Log.d(TAG, "answerAfterPermissions");

    String callerName = displayName.getValue();
    if (callerName == null) callerName = "Unknown";
    showOrUpdateOngoingNotification(appContext.getString(R.string.call_with, callerName));

    ensureServiceStarted();
    startMediaCapture();

    mainHandler.post(
        () -> {
          LiveData<Boolean> mediaReady = getMediaCaptureReady();

          if (Boolean.TRUE.equals(mediaReady.getValue())) {
            answerWebRTC();
            return;
          }

          answerMediaObserver =
              new Observer<Boolean>() {
                @Override
                public void onChanged(Boolean ready) {
                  if (Boolean.TRUE.equals(ready)) {
                    mediaReady.removeObserver(this);
                    answerMediaObserver = null;
                    answerWebRTC();
                  }
                }
              };
          mediaReady.observeForever(answerMediaObserver);
        });
  }
}
