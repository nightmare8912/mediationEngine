package org.prats.security;
import org.junit.jupiter.api.Test;
import org.prats.testing.TestDatabase;
import org.prats.testing.TestMessagePipeline;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
class UserAccountServiceTest {
    private TestMessagePipeline pipeline;
    @BeforeEach void start() { pipeline = new TestMessagePipeline(); }
    @AfterEach void stop() { pipeline.close(); }
    @Test void registrationHashesPasswordsAndSessionsRequireRealCredentials() throws Exception {
        TestDatabase database = pipeline.database; AccountManager accounts = pipeline.accounts;
        accounts.register("operator", "Strong-pass-123!".toCharArray()).join();
        try(var c=database.getDataSource().getConnection(); var rs=c.createStatement().executeQuery("SELECT password FROM users WHERE user_name='operator'")) {
            assertTrue(rs.next()); assertTrue(rs.getString(1).startsWith("pbkdf2-sha256$600000$")); assertFalse(rs.getString(1).contains("Strong-pass"));
        }
        assertThrows(java.util.concurrent.CompletionException.class,()->accounts.register("operator","Another-pass-123!".toCharArray()).join());
        assertThrows(java.util.concurrent.CompletionException.class,()->accounts.login("operator","wrong".toCharArray()).join());
        var session=accounts.login("operator","Strong-pass-123!".toCharArray()).join();
        assertEquals("operator",accounts.session(session.token()).orElseThrow().username());
        accounts.logout(session.token()); assertTrue(accounts.session(session.token()).isEmpty());
        assertThrows(java.util.concurrent.CompletionException.class,()->accounts.login("missing","Strong-pass-123!".toCharArray()).join());
    }
    @Test void inactiveAccountsAndUnsafeInputsAreRejected() throws Exception {
        TestDatabase database=pipeline.database; AccountManager accounts=pipeline.accounts;
        assertThrows(IllegalArgumentException.class,()->accounts.register("bad';DROP TABLE users;--","Strong-pass-123!".toCharArray()));
        assertThrows(IllegalArgumentException.class,()->accounts.register("valid","tiny".toCharArray()));
        accounts.register("disabled","Strong-pass-123!".toCharArray()).join();
        try(var c=database.getDataSource().getConnection(); var s=c.createStatement()){s.executeUpdate("UPDATE users SET active='N'");}
        assertThrows(java.util.concurrent.CompletionException.class,()->accounts.login("disabled","Strong-pass-123!".toCharArray()).join());
    }
}
