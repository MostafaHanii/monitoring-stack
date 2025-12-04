# Snowflake Key Pair Authentication Setup

This guide explains how to generate a Key Pair (Private/Public Key) and configure it for the Snowflake Kafka Connector.

## 1. Generate Key Pair (OpenSSL)

You need `openssl` installed on your machine. Run these commands in your terminal (e.g., Git Bash, PowerShell, or macOS Terminal).

### Step 1: Generate Private Key
Generate a 2048-bit RSA private key.
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```
*   This creates a file named `rsa_key.p8`.
*   **Keep this file secure!** This is your private key.

### Step 2: Generate Public Key
Extract the public key from the private key.
```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```
*   This creates a file named `rsa_key.pub`.

## 2. Configure Snowflake User

You need to assign the **Public Key** to your Snowflake user.

1.  Open the `rsa_key.pub` file and copy the content **excluding** the `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----` lines. It should be one long string.
2.  Log in to your Snowflake web interface (Snowsight).
3.  Run the following SQL command (replace `<YOUR_USER>` and `<PUBLIC_KEY_CONTENT>`):

```sql
ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='<PUBLIC_KEY_CONTENT>';
```

**Example:**
```sql
ALTER USER MY_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';
```

## 3. Format Private Key for Connector

The Kafka Connect JSON configuration requires the **Private Key** (`rsa_key.p8`) to be a single line without headers or newlines.

1.  Open `rsa_key.p8`.
2.  Remove `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----`.
3.  Remove all newlines so it is just one long string.

**Example for `snowflake-sink.json`:**
```json
"snowflake.private.key": "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQ..."
```

## 4. Verify

To verify the key works, you can try to connect using SnowSQL or a Python script, but if the `ALTER USER` command succeeded, you are generally good to go.
