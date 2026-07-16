package org.thoughtcrime.securesms.calls;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Unit tests for {@link RestartNegotiationPayload}. Covers the wire-format JSON encoding and
 * decoding used for ICE-restart renegotiation between Android and Core's {@code send_call_update}
 * RPC.
 */
public class RestartNegotiationPayloadTest {

  private static final String SAMPLE_SDP =
      "v=0\r\n"
          + "o=- 1 2 IN IP4 0.0.0.0\r\n"
          + "s=-\r\n"
          + "t=0 0\r\n"
          + "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n";

  @Test
  public void buildOffer_generatesFreshIdEachTime() {
    String a = RestartNegotiationPayload.buildOffer(SAMPLE_SDP);
    String b = RestartNegotiationPayload.buildOffer(SAMPLE_SDP);

    assertThat(a).isNotNull();
    assertThat(b).isNotNull();
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  public void buildOffer_carriesOfferTypeAndSuppliedSdp() {
    String payload = RestartNegotiationPayload.buildOffer(SAMPLE_SDP);
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse(payload);

    assertThat(parsed.valid).isTrue();
    assertThat(parsed.isOffer()).isTrue();
    assertThat(parsed.isAnswer()).isFalse();
    assertThat(parsed.sdp).isEqualTo(SAMPLE_SDP);
    assertThat(parsed.id).isNotEmpty();
  }

  @Test
  public void buildAnswer_echoesSuppliedId() {
    String id = RestartNegotiationPayload.generateId();
    String payload = RestartNegotiationPayload.buildAnswer(id, SAMPLE_SDP);
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse(payload);

    assertThat(parsed.valid).isTrue();
    assertThat(parsed.isAnswer()).isTrue();
    assertThat(parsed.id).isEqualTo(id);
    assertThat(parsed.sdp).isEqualTo(SAMPLE_SDP);
  }

  @Test
  public void buildOfferWithId_usesSuppliedIdVerbatim() {
    String id = RestartNegotiationPayload.generateId();
    String payload = RestartNegotiationPayload.buildOfferWithId(id, SAMPLE_SDP);
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse(payload);

    assertThat(parsed.valid).isTrue();
    assertThat(parsed.isOffer()).isTrue();
    assertThat(parsed.id).isEqualTo(id);
    assertThat(parsed.sdp).isEqualTo(SAMPLE_SDP);
  }

  @Test
  public void parse_rejectsNullPayload() {
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse(null);

    assertThat(parsed.valid).isFalse();
    assertThat(parsed.id).isNull();
    assertThat(parsed.type).isNull();
    assertThat(parsed.sdp).isNull();
  }

  @Test
  public void parse_rejectsEmptyPayload() {
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse("");

    assertThat(parsed.valid).isFalse();
  }

  @Test
  public void parse_rejectsMalformedJson() {
    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse("{not-json-at-all}");

    assertThat(parsed.valid).isFalse();
  }

  @Test
  public void parse_rejectsMissingFields() {
    assertThat(RestartNegotiationPayload.parse("{}").valid).isFalse();
    assertThat(RestartNegotiationPayload.parse("{\"id\":\"x\"}").valid).isFalse();
    assertThat(RestartNegotiationPayload.parse("{\"id\":\"x\",\"type\":\"offer\"}").valid)
        .isFalse();
    assertThat(
            RestartNegotiationPayload.parse("{\"id\":\"x\",\"type\":\"offer\",\"sdp\":\"\"}").valid)
        .isFalse();
  }

  @Test
  public void parse_rejectsUnknownType() {
    // Even if structurally valid, an unknown type means "neither offer nor answer" and the
    // caller will discard it.
    String payload = "{\"id\":\"abc\",\"type\":\"unknown\",\"sdp\":\"x\"}";

    RestartNegotiationPayload.Parsed parsed = RestartNegotiationPayload.parse(payload);

    // Schema-level validity is true (all fields present and non-empty); the caller decides
    // what to do with an unknown type.
    assertThat(parsed.valid).isTrue();
    assertThat(parsed.isOffer()).isFalse();
    assertThat(parsed.isAnswer()).isFalse();
  }

  @Test
  public void answerTo_doesNotMatchOfferFrom_differentIds() {
    String offerId = RestartNegotiationPayload.generateId();
    String staleAnswerId = RestartNegotiationPayload.generateId();

    // A round-trip: caller builds an offer; the callee echoes back its answer with the
    // caller's id. A stale answer with a different id must be detected by the caller's
    // id-match guard, not at the codec level - the codec always validates structurally.
    String offer = RestartNegotiationPayload.buildOfferWithId(offerId, SAMPLE_SDP);
    String staleAnswer = RestartNegotiationPayload.buildAnswer(staleAnswerId, "v=0\r\n");

    RestartNegotiationPayload.Parsed offerParsed = RestartNegotiationPayload.parse(offer);
    RestartNegotiationPayload.Parsed staleAnswerParsed =
        RestartNegotiationPayload.parse(staleAnswer);

    assertThat(offerParsed.id).isEqualTo(offerId);
    assertThat(staleAnswerParsed.id).isNotEqualTo(offerId);
  }
}
