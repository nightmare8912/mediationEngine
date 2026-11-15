
package org.prats.security.hasher;

import org.prats.loggers.EngineLogger;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;

import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

public final class PasswordHasher {

    private static final EngineLogger log =
            EngineLogger.getLogger(PasswordHasher.class);

    // =========================================================
    // 1. CRYPTOGRAPHIC CONFIGURATION
    // =========================================================

    /*
     * PBKDF2 (Password-Based Key Derivation Function 2).
     *
     * HMAC-SHA256 is used internally as the
     * pseudorandom function.
     *
     * Unlike ordinary SHA-256 hashing, PBKDF2 deliberately
     * performs repeated cryptographic calculations.
     *
     * This makes brute-force password guessing
     * significantly more expensive.
     */
    private static final String ALGORITHM =
            "PBKDF2WithHmacSHA256";

    /*
     * Default PBKDF2 iteration count used when
     * generating NEW password hashes.
     *
     * Existing hashes retain their original iteration
     * counts in their stored representation.
     */
    private static final int ITERATIONS = 600_000;

    /*
     * Maximum iteration count accepted during verification.
     *
     * This prevents a malformed or manipulated database
     * value from requesting an extremely expensive
     * PBKDF2 calculation.
     */
    private static final int MAX_VERIFY_ITERATIONS =
            2_000_000;

    /*
     * Derived hash length in BITS.
     *
     * 256 bits = 32 bytes.
     */
    private static final int HASH_LENGTH = 256;

    /*
     * Salt length in BYTES.
     *
     * 16 bytes = 128 bits.
     */
    private static final int SALT_LENGTH = 16;

    /*
     * Maximum length of our serialized password hash.
     *
     * Our current representation is much smaller,
     * but this provides a defensive parsing limit.
     */
    private static final int MAX_STORED_HASH_LENGTH = 512;

    /*
     * Cryptographically secure random number generator.
     *
     * A single shared SecureRandom instance can be used
     * safely by multiple threads.
     */
    private static final SecureRandom SECURE_RANDOM =
            new SecureRandom();

    /*
     * Stored password hash representation:
     *
     * algorithm$iterations$base64Salt$base64Hash
     *
     * Example:
     *
     * PBKDF2WithHmacSHA256$600000$SALT$HASH
     *
     * Note:
     * SALT and HASH above are illustrative placeholders.
     */
    private static final String SEPARATOR = "$";


    // =========================================================
    // 2. GENERATE RANDOM SALT
    // =========================================================

    /**
     * Generates a cryptographically random salt.
     *
     * A fresh salt must be generated whenever a new
     * password hash is created.
     *
     * Two users with identical passwords should still
     * receive different stored password hashes.
     *
     * @return Random salt containing 16 bytes.
     */
    public byte[] generateSalt() {

        log.debug(() ->
                "[PasswordHasher][generateSalt] "
                        + "Generating cryptographically secure salt"
                        + " | saltLengthBytes=" + SALT_LENGTH
        );

        /*
         * Allocate a byte array for the salt.
         */
        byte[] salt = new byte[SALT_LENGTH];

        /*
         * Fill the array with cryptographically
         * secure random values.
         */
        SECURE_RANDOM.nextBytes(salt);

        log.debug(() ->
                "[PasswordHasher][generateSalt] "
                        + "Salt generated successfully"
                        + " | saltLengthBytes=" + salt.length
        );

        return salt;
    }


    // =========================================================
    // 3. DERIVE PASSWORD HASH
    // =========================================================

    /**
     * Derives a password hash using PBKDF2-HMAC-SHA256.
     *
     * This method performs the cryptographic calculation.
     *
     * It does NOT generate a salt.
     *
     * Instead, it receives:
     *
     * 1. The password.
     * 2. The salt.
     * 3. The iteration count.
     *
     * The method is deterministic.
     *
     * Given identical inputs, it produces identical
     * derived bytes.
     *
     * That property allows password verification
     * without storing or recovering plaintext passwords.
     *
     * @param password   Password to derive.
     * @param salt       Salt used during derivation.
     * @param iterations Number of PBKDF2 iterations.
     *
     * @return Derived password hash as a byte array.
     */
    private byte[] deriveHash(
            char[] password,
            byte[] salt,
            int iterations
    ) {

        // -----------------------------------------------------
        // VALIDATE ARGUMENTS
        // -----------------------------------------------------

        Objects.requireNonNull(
                password,
                "Password cannot be null"
        );

        Objects.requireNonNull(
                salt,
                "Salt cannot be null"
        );

        if (password.length == 0) {

            throw new IllegalArgumentException(
                    "Password cannot be empty"
            );
        }

        if (salt.length != SALT_LENGTH) {

            throw new IllegalArgumentException(
                    "Invalid salt length"
            );
        }

        if (iterations < 1 ||
                iterations > MAX_VERIFY_ITERATIONS) {

            throw new IllegalArgumentException(
                    "Invalid PBKDF2 iteration count"
            );
        }

        log.debug(() ->
                "[PasswordHasher][deriveHash] "
                        + "Starting PBKDF2 derivation"
                        + " | algorithm=" + ALGORITHM
                        + " | iterations=" + iterations
                        + " | hashLengthBits=" + HASH_LENGTH
        );

        // -----------------------------------------------------
        // CREATE PASSWORD-BASED KEY SPECIFICATION
        // -----------------------------------------------------

        /*
         * PBEKeySpec describes the parameters for
         * password-based key derivation.
         *
         * Arguments:
         *
         * password:
         *     Original password characters.
         *
         * salt:
         *     Random bytes generated during registration
         *     or recovered from the stored representation
         *     during verification.
         *
         * iterations:
         *     Number of PBKDF2 iterations.
         *
         * HASH_LENGTH:
         *     Desired output length in bits.
         *
         * PBEKeySpec itself does not execute PBKDF2.
         */
        PBEKeySpec specification =
                new PBEKeySpec(
                        password,
                        salt,
                        iterations,
                        HASH_LENGTH
                );

        try {

            // -------------------------------------------------
            // OBTAIN CRYPTOGRAPHIC IMPLEMENTATION
            // -------------------------------------------------

            /*
             * SecretKeyFactory retrieves the Java
             * implementation of our selected algorithm.
             */
            SecretKeyFactory factory =
                    SecretKeyFactory.getInstance(
                            ALGORITHM
                    );

            // -------------------------------------------------
            // PERFORM PBKDF2 DERIVATION
            // -------------------------------------------------

            /*
             * generateSecret() performs the PBKDF2
             * computation.
             *
             * getEncoded() retrieves the derived
             * bytes from the resulting secret key.
             */
            byte[] hash = factory
                    .generateSecret(specification)
                    .getEncoded();

            log.debug(() ->
                    "[PasswordHasher][deriveHash] "
                            + "PBKDF2 derivation completed"
                            + " | derivedLengthBytes=" + hash.length
            );

            return hash;

        } catch (GeneralSecurityException exception) {

            log.error(
                    "[PasswordHasher][deriveHash] "
                            + "PBKDF2 derivation failed",
                    exception
            );

            throw new IllegalStateException(
                    "Unable to derive password hash",
                    exception
            );

        } finally {

            /*
             * Clear the password copy stored inside
             * PBEKeySpec.
             *
             * The original password char[] is owned
             * by the caller and is not modified here.
             */
            specification.clearPassword();

            log.debug(() ->
                    "[PasswordHasher][deriveHash] "
                            + "PBEKeySpec password cleared"
            );
        }
    }


    // =========================================================
    // 4. HASH PASSWORD FOR DATABASE STORAGE
    // =========================================================

    /**
     * Creates a salted password hash suitable
     * for database storage.
     *
     * Processing steps:
     *
     * 1. Validate the password.
     * 2. Generate a new random salt.
     * 3. Derive the hash using PBKDF2.
     * 4. Encode salt and hash using Base64.
     * 5. Include the algorithm and iteration count.
     * 6. Return the complete stored representation.
     *
     * Stored format:
     *
     * algorithm$iterations$salt$hash
     *
     * IMPORTANT:
     *
     * The plaintext password is never included
     * in the returned representation.
     *
     * @param password Password to hash.
     *
     * @return Serialized password hash for storage.
     */
    public String hashPassword(
            char[] password
    ) {

        // -----------------------------------------------------
        // VALIDATE PASSWORD
        // -----------------------------------------------------

        Objects.requireNonNull(
                password,
                "Password cannot be null"
        );

        if (password.length == 0) {

            log.warn(
                    "[PasswordHasher][hashPassword] "
                            + "Hashing rejected: empty password"
            );

            throw new IllegalArgumentException(
                    "Password cannot be empty"
            );
        }

        log.debug(() ->
                "[PasswordHasher][hashPassword] "
                        + "Password hashing requested"
                        + " | algorithm=" + ALGORITHM
                        + " | iterations=" + ITERATIONS
        );

        // -----------------------------------------------------
        // GENERATE SALT
        // -----------------------------------------------------

        /*
         * Always generate a new salt.
         *
         * Even identical passwords will normally produce
         * different stored hashes because their salts differ.
         */
        byte[] salt = generateSalt();

        byte[] hash = null;

        try {

            // -------------------------------------------------
            // DERIVE HASH
            // -------------------------------------------------

            /*
             * Use the CURRENT default iteration count
             * when creating a new password hash.
             *
             * This lets us increase ITERATIONS later
             * without breaking older stored hashes.
             */
            hash = deriveHash(
                    password,
                    salt,
                    ITERATIONS
            );

            // -------------------------------------------------
            // BASE64 ENCODING
            // -------------------------------------------------

            /*
             * Salt and hash are binary data.
             *
             * Base64 converts binary bytes into
             * printable strings.
             *
             * Base64 is an encoding mechanism,
             * not encryption.
             */
            String encodedSalt =
                    Base64.getEncoder()
                            .encodeToString(salt);

            String encodedHash =
                    Base64.getEncoder()
                            .encodeToString(hash);

            log.debug(() ->
                    "[PasswordHasher][hashPassword] "
                            + "Cryptographic data encoded"
                            + " | encoding=Base64"
            );

            // -------------------------------------------------
            // BUILD STORED REPRESENTATION
            // -------------------------------------------------

            /*
             * We store:
             *
             * 1. Algorithm identifier
             * 2. Iteration count
             * 3. Random salt
             * 4. Derived password hash
             *
             * During login, verifyPassword() extracts
             * these values and recomputes the hash.
             */
            String storedHash =
                    ALGORITHM
                            + SEPARATOR
                            + ITERATIONS
                            + SEPARATOR
                            + encodedSalt
                            + SEPARATOR
                            + encodedHash;

            log.info(
                    "[PasswordHasher][hashPassword] "
                            + "Password hashing completed successfully"
            );

            return storedHash;

        } finally {

            // -------------------------------------------------
            // CLEAR TEMPORARY CRYPTOGRAPHIC BUFFERS
            // -------------------------------------------------

            /*
             * Clear temporary arrays once the stored
             * representation has been created.
             *
             * The original password char[] belongs
             * to the caller.
             */
            Arrays.fill(salt, (byte) 0);

            if (hash != null) {

                Arrays.fill(hash, (byte) 0);
            }

            log.debug(() ->
                    "[PasswordHasher][hashPassword] "
                            + "Temporary cryptographic buffers cleared"
            );
        }
    }


    // =========================================================
    // 5. VERIFY PASSWORD DURING LOGIN
    // =========================================================

    /**
     * Verifies an entered password against a previously
     * stored PBKDF2 password hash.
     *
     * This method is intended for authentication.
     *
     * Processing steps:
     *
     * 1. Validate the supplied arguments.
     * 2. Parse the stored password representation.
     * 3. Extract the algorithm and iteration count.
     * 4. Decode the original salt and expected hash.
     * 5. Derive a new hash using the supplied password.
     * 6. Compare the derived and expected hashes.
     * 7. Clear temporary cryptographic buffers.
     *
     * IMPORTANT:
     *
     * We do NOT generate a new salt during verification.
     *
     * We use the ORIGINAL salt stored during registration.
     *
     * We also use the ORIGINAL iteration count stored
     * alongside the password hash.
     *
     * @param password   Password entered during login.
     * @param storedHash Previously stored password hash.
     *
     * @return true if the password matches;
     *         false otherwise.
     */
    public boolean verifyPassword(
            char[] password,
            String storedHash
    ) {

        // -----------------------------------------------------
        // VALIDATE ARGUMENTS
        // -----------------------------------------------------

        if (password == null || password.length == 0) {

            log.debug(
                    "[PasswordHasher][verifyPassword] "
                            + "Verification rejected: password is missing"
            );

            return false;
        }

        if (storedHash == null || storedHash.isBlank()) {

            log.warn(
                    "[PasswordHasher][verifyPassword] "
                            + "Verification rejected: stored hash is missing"
            );

            return false;
        }

        /*
         * Prevent excessive parsing and allocation
         * from malformed stored representations.
         */
        if (storedHash.length() > MAX_STORED_HASH_LENGTH) {

            log.warn(
                    "[PasswordHasher][verifyPassword] "
                            + "Verification rejected: stored hash exceeds size limit"
            );

            return false;
        }

        log.debug(() ->
                "[PasswordHasher][verifyPassword] "
                        + "Password verification requested"
        );

        /*
         * These arrays contain temporary cryptographic
         * data and must be cleared after verification.
         */
        byte[] salt = null;
        byte[] expectedHash = null;
        byte[] computedHash = null;

        try {

            // -------------------------------------------------
            // PARSE STORED REPRESENTATION
            // -------------------------------------------------

            /*
             * Expected format:
             *
             * algorithm$iterations$base64Salt$base64Hash
             *
             * split("\\$", -1) preserves empty fields.
             *
             * The '$' is escaped because split() uses
             * regular expressions, where '$' otherwise
             * has a special meaning.
             */
            String[] parts =
                    storedHash.split("\\$", -1);

            if (parts.length != 4) {

                log.warn(
                        "[PasswordHasher][verifyPassword] "
                                + "Invalid stored hash format: expected four components"
                );

                return false;
            }

            // -------------------------------------------------
            // VALIDATE ALGORITHM
            // -------------------------------------------------

            String storedAlgorithm = parts[0];

            /*
             * Only our supported PBKDF2 algorithm
             * is accepted.
             *
             * Never execute an algorithm selected
             * arbitrarily by untrusted input.
             */
            if (!ALGORITHM.equals(storedAlgorithm)) {

                log.warn(
                        "[PasswordHasher][verifyPassword] "
                                + "Unsupported stored password algorithm"
                );

                return false;
            }

            // -------------------------------------------------
            // EXTRACT ITERATION COUNT
            // -------------------------------------------------

            /*
             * Convert the stored iteration count
             * from String to int.
             *
             * For example:
             *
             * "600000" -> 600000
             */
            int storedIterations =
                    Integer.parseInt(parts[1]);

            /*
             * Reject zero, negative, or excessively
             * large iteration counts.
             *
             * The upper bound helps protect the JVM
             * against unexpectedly expensive derivations.
             */
            if (storedIterations < 1 ||
                    storedIterations > MAX_VERIFY_ITERATIONS) {

                log.warn(
                        "[PasswordHasher][verifyPassword] "
                                + "Invalid stored PBKDF2 iteration count"
                );

                return false;
            }

            // -------------------------------------------------
            // DECODE SALT AND EXPECTED HASH
            // -------------------------------------------------

            /*
             * The salt and expected hash were encoded
             * using Base64 during registration.
             *
             * Decode them back into byte arrays.
             */
            salt = Base64.getDecoder()
                    .decode(parts[2]);

            expectedHash = Base64.getDecoder()
                    .decode(parts[3]);

            // -------------------------------------------------
            // VALIDATE DECODED DATA
            // -------------------------------------------------

            if (salt.length != SALT_LENGTH) {

                log.warn(
                        "[PasswordHasher][verifyPassword] "
                                + "Invalid stored salt length"
                );

                return false;
            }

            if (expectedHash.length != HASH_LENGTH / 8) {

                log.warn(
                        "[PasswordHasher][verifyPassword] "
                                + "Invalid stored derived hash length"
                );

                return false;
            }

            log.debug(() ->
                    "[PasswordHasher][verifyPassword] "
                            + "Stored password hash parsed successfully"
                            + " | algorithm=" + ALGORITHM
                            + " | iterations=" + storedIterations
            );

            // -------------------------------------------------
            // RECOMPUTE PASSWORD HASH
            // -------------------------------------------------

            /*
             * This is the most important step.
             *
             * We use:
             *
             * 1. The password entered during login.
             * 2. The ORIGINAL stored salt.
             * 3. The ORIGINAL stored iteration count.
             *
             * If the password is correct, PBKDF2
             * produces the same derived bytes.
             */
            computedHash = deriveHash(
                    password,
                    salt,
                    storedIterations
            );

            // -------------------------------------------------
            // COMPARE HASHES
            // -------------------------------------------------

            /*
             * MessageDigest.isEqual() provides a comparison
             * suitable for cryptographic byte arrays.
             *
             * Prefer this over an ordinary early-exit
             * byte-by-byte comparison.
             *
             * This comparison does NOT decrypt a password.
             *
             * It checks whether the newly derived bytes
             * match the bytes stored during registration.
             */
            boolean matches = MessageDigest.isEqual(
                    computedHash,
                    expectedHash
            );

            log.debug(() ->
                    "[PasswordHasher][verifyPassword] "
                            + "Password verification completed"
                            + " | matches=" + matches
            );

            /*
             * This method only checks the password.
             *
             * AuthenticationService should decide
             * whether to create a user session.
             */
            return matches;

        } catch (IllegalArgumentException exception) {

            /*
             * Integer.parseInt() throws
             * NumberFormatException for invalid numbers.
             *
             * Base64 decoding can also throw
             * IllegalArgumentException for malformed input.
             *
             * These represent invalid stored data.
             *
             * Do not log the stored password hash
             * or any sensitive input.
             */
            log.warn(
                    "[PasswordHasher][verifyPassword] "
                            + "Password verification failed: malformed stored representation"
            );

            return false;

        } finally {

            // -------------------------------------------------
            // CLEAR TEMPORARY CRYPTOGRAPHIC DATA
            // -------------------------------------------------

            /*
             * Clearing these buffers reduces the lifetime
             * of sensitive derived material in JVM memory.
             *
             * We never clear the caller's original password
             * array inside PasswordHasher.
             *
             * That remains the caller's responsibility.
             */
            if (salt != null) {

                Arrays.fill(salt, (byte) 0);
            }

            if (expectedHash != null) {

                Arrays.fill(expectedHash, (byte) 0);
            }

            if (computedHash != null) {

                Arrays.fill(computedHash, (byte) 0);
            }

            log.debug(() ->
                    "[PasswordHasher][verifyPassword] "
                            + "Temporary verification buffers cleared"
            );
        }
    }
}
