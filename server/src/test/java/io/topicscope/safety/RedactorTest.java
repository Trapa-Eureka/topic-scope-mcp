package io.topicscope.safety;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RedactorTest {

  // Fake secrets are assembled at runtime so this file itself never holds something key-shaped.
  static final String FAKE_AWS = "AKIA" + "ABCDEFGHIJKLMNOP";
  static final String FAKE_GITHUB = "ghp_" + "a".repeat(36);
  static final String FAKE_ANTHROPIC = "sk-" + "ant-" + "b".repeat(40);
  static final String FAKE_JWT = "eyJ" + "hbGciOiJIUzI1NiJ9" + "." + "eyJ" + "zdWIiOiIxMjM0NTY3ODkwIn0" + "." + "c2lnbmF0dXJlLXBhcnQ";
  static final String FAKE_PEM = "-----BEGIN " + "RSA PRIVATE KEY-----\nMIIEow\nabc\n-----END " + "RSA PRIVATE KEY-----";

  @Test
  void namesThatSayTheyHoldASecret() {
    for (String name : new String[] {"password", "Password", "db_password", "user.passwd", "client-secret", "apiKey", "api_key", "X-Api-Key",
        "access_token", "refreshToken", "Authorization", "set-cookie", "privateKey", "aws_secret_access_key", "sessionId", "pwd", "auth", "PIN"}) {
      assertThat(Redactor.secretName(name)).as(name).isTrue();
    }
  }

  @Test
  void namesThatDoNot() {
    for (String name : new String[] {"user", "author", "key", "keys", "partitionKey", "id", "callsign", "path", "passenger", "pinned", "compass",
        "content-type", "", null}) {
      assertThat(Redactor.secretName(name)).as(String.valueOf(name)).isFalse();
    }
  }

  @Test
  void secretShapesAreReplacedWhereverTheyStand() {
    assertThat(Redactor.text("aws " + FAKE_AWS + " done").text()).isEqualTo("aws [REDACTED] done");
    assertThat(Redactor.text("gh=" + FAKE_GITHUB).text()).isEqualTo("gh=[REDACTED]");
    assertThat(Redactor.text(FAKE_ANTHROPIC).text()).isEqualTo("[REDACTED]");
    assertThat(Redactor.text("jwt " + FAKE_JWT + ".").text()).isEqualTo("jwt [REDACTED].");
    assertThat(Redactor.text("before\n" + FAKE_PEM + "\nafter").text()).isEqualTo("before\n[REDACTED]\nafter");
    assertThat(Redactor.text(FAKE_PEM.substring(0, 50)).text()).as("a private key that was cut off is hidden to its end").isEqualTo("[REDACTED]");
  }

  @Test
  void namedValuesInPlainText() {
    assertThat(Redactor.text("user=bob password=hunter2 retries=3").text()).isEqualTo("user=bob password=[REDACTED] retries=3");
    assertThat(Redactor.text("api_key: abc123, region: eu").text()).isEqualTo("api_key: [REDACTED], region: eu");
    assertThat(Redactor.text("\"token\": \"abc\"").text()).isEqualTo("\"token\": \"[REDACTED]\"");
    assertThat(Redactor.text("https://example.org/x?access_token=abc&page=2").text()).isEqualTo("https://example.org/x?access_token=[REDACTED]&page=2");
  }

  @Test
  void authorizationHeadersAndUrlPasswords() {
    assertThat(Redactor.text("Authorization: Bearer abcdefgh12345678").text()).isEqualTo("Authorization: Bearer [REDACTED]");
    assertThat(Redactor.text("authorization: basic dXNlcjpwYXNz").text()).isEqualTo("authorization: basic [REDACTED]");
    assertThat(Redactor.text("tcp://operator:s3cret@broker:1883").text()).isEqualTo("tcp://operator:[REDACTED]@broker:1883");
  }

  @Test
  void ordinaryTextIsLeftAlone() {
    for (String text : new String[] {
        "Wind shear advisory — 활주로 09",
        "http://host:8080/path?x=1",
        "12:30:00 key: abc value: 5",
        "author: Kim, pinned: true",
        "{not json} a=b"}) {
      Redactor.Result r = Redactor.text(text);
      assertThat(r.text()).isEqualTo(text);
      assertThat(r.redacted()).isFalse();
    }
    assertThat(Redactor.text(null)).isEqualTo(new Redactor.Result("", false));
  }

  @Test
  void redactingTwiceChangesNothingMore() {
    String once = Redactor.text("password=hunter2 Authorization: Bearer abcdefgh12345678 " + FAKE_AWS).text();
    assertThat(Redactor.text(once).text()).isEqualTo(once);
  }
}
