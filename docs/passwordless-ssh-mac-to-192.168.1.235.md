# Passwordless SSH: Mac → 192.168.1.235

Run these steps **on your Mac** in Terminal to log in to `psardar@192.168.1.235` without entering a password.

---

## 1. Check for an existing SSH key

```bash
ls ~/.ssh/id_ed25519.pub 2>/dev/null || ls ~/.ssh/id_rsa.pub 2>/dev/null
```

- If a path is printed, you have a key. Note whether it’s **id_ed25519** or **id_rsa** and use that in the steps below.
- If nothing is printed, create a key (step 2a).

---

## 2a. (Only if you have no key) Create an SSH key

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
```

Then use **id_ed25519** in the following steps.

---

## 2b. Copy your key to 192.168.1.235

You will be asked for your **password once**. After this, you won’t need it for SSH.

**If you have ed25519:**

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub psardar@192.168.1.235
```

**If you have RSA only:**

```bash
ssh-copy-id -i ~/.ssh/id_rsa.pub psardar@192.168.1.235
```

---

## 3. Make SSH use the key automatically

So the Mac always uses this key for 192.168.1.235 and doesn’t prompt for a password:

```bash
mkdir -p ~/.ssh
```

**If you used id_ed25519**, run:

```bash
cat >> ~/.ssh/config << 'EOF'

Host 192.168.1.235
    User psardar
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
EOF
```

**If you used id_rsa**, run the same but with `id_rsa`:

```bash
cat >> ~/.ssh/config << 'EOF'

Host 192.168.1.235
    User psardar
    IdentityFile ~/.ssh/id_rsa
    IdentitiesOnly yes
EOF
```

Then set safe permissions:

```bash
chmod 600 ~/.ssh/config
```

---

## 4. Test

```bash
ssh psardar@192.168.1.235
```

You should get a shell **without being asked for a password**.

---

## Optional: use a short host name

Add a `Host` alias in `~/.ssh/config` so you can type `ssh HNX` instead of the IP:

```
Host HNX
    HostName 192.168.1.235
    User psardar
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```

Then run:

```bash
ssh HNX
```

(Use `id_rsa` in `IdentityFile` if that’s the key you use.)
